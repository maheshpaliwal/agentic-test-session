package integration

import (
	"bufio"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tsession/collector/store"
	"tsession/core"
)

// TestE2E_ExampleAppBinary runs the real cmd/exampleapp binary as a separate
// process with only env vars set (autoinit enabled path, no runtime.Start in
// the test process).
func TestE2E_ExampleAppBinary(t *testing.T) {
	pgDSN, redisAddr := itEnv(t)
	schemaSetup(t, pgDSN, "app_it_bin")
	ns := "itbin:"
	delNS(t, redisAddr, ns)
	in := newInfra(t, store.NewMem())
	col := colAPI{in.colSrv.URL}

	bin := filepath.Join(t.TempDir(), "exampleapp")
	if out, err := exec.Command("go", "build", "-o", bin, "./cmd/exampleapp").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"TSESSION_ENABLED=true", "TSESSION_SERVICE=orders-bin",
		"TSESSION_CONTROLPLANE_URL="+in.cpSrv.URL, "TSESSION_COLLECTOR_URL="+in.colSrv.URL,
		"EXAMPLEAPP_LISTEN="+addr, "EXAMPLEAPP_PG_DSN="+withParam(pgDSN, "search_path", "app_it_bin"),
		"EXAMPLEAPP_REDIS_ADDR="+redisAddr, "EXAMPLEAPP_NAMESPACE="+ns)
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Signal(os.Interrupt); _, _ = cmd.Process.Wait() })
	ready := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if strings.Contains(sc.Text(), "listening") {
				close(ready)
			}
			t.Log("app:", sc.Text())
		}
	}()
	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		t.Fatal("exampleapp did not start")
	}
	base := "http://" + addr

	s := in.start(t, "header")
	time.Sleep(1500 * time.Millisecond) // subscriber lives in the other process; let it see the session
	code, raw := doReq(t, "POST", base+"/orders", orderBody{SKU: "widget", Qty: 1, Customer: "bin-cust", Jobs: []string{"flaky_job"}}, s.Header())
	if code != 201 {
		t.Fatalf("POST /orders %d %s", code, raw)
	}
	// the binary's exporter is in another process: stop() is what drains; no local Flush.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := s.StopAndWait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	evs := col.events(t, s.ID)
	t.Logf("incomplete=%v\n%s", info.Incomplete, fmtSeq(evs))
	for _, k := range []core.Kind{core.KindAPIIn, core.KindDBWrite, core.KindCacheWrite, core.KindQueuePub, core.KindWorkerStart, core.KindWorkerEnd, core.KindAPIOut} {
		if len(ofKind(evs, k)) == 0 {
			t.Errorf("binary run: no %s", k)
		}
	}
	if info.Incomplete {
		t.Errorf("session incomplete")
	}
	for _, e := range evs {
		if e.Service != "orders-bin" {
			t.Errorf("service %q", e.Service)
			break
		}
	}
	if n := len(named(evs, core.KindAPIOut, "/receipts")); n != 1 {
		t.Errorf("api.out /receipts = %d, want 1", n)
	}
	// no session => no events
	before := len(col.sessions(t))
	doReq(t, "POST", base+"/orders", orderBody{SKU: "widget", Qty: 1, Customer: "bin-none"}, nil)
	time.Sleep(1200 * time.Millisecond)
	if after := len(col.sessions(t)); after != before {
		t.Errorf("sessions %d -> %d for header-less request", before, after)
	}
}
