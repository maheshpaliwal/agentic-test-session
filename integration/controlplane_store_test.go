package integration

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// cpRun is one life of a control plane on a file store, wired as
// cmd/controlplane does it.
type cpRun struct {
	fs     *controlplane.FileStore
	mgr    *controlplane.Manager
	srv    *httptest.Server
	cancel context.CancelFunc
}

func startCP(t *testing.T, path, collectorURL string) *cpRun {
	t.Helper()
	fs, err := controlplane.NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	mgr := controlplane.NewManager(controlplane.Config{
		Store:     fs,
		Collector: &controlplane.HTTPCollector{BaseURL: collectorURL},
		TimeUnit:  50 * time.Millisecond, PollInterval: 50 * time.Millisecond, SweepInterval: 50 * time.Millisecond,
		Retention: 300 * time.Millisecond, // finished sessions move to the archive quickly
	})
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Run(ctx)
	return &cpRun{fs: fs, mgr: mgr, srv: httptest.NewServer(mgr.Handler()), cancel: cancel}
}

// stop ends the run. clean is the binary's shutdown (fold the journal, close
// the store); otherwise the process is simply gone and the files stay as
// they are.
func (r *cpRun) stop(t *testing.T, clean bool) {
	t.Helper()
	r.srv.Close()
	r.cancel()
	r.mgr.Close()
	if clean {
		if err := r.fs.Compact(); err != nil {
			t.Fatalf("compact: %v", err)
		}
	}
	if err := r.fs.Close(); err != nil { // releases the file handle; it does not fold the journal
		t.Fatalf("close store: %v", err)
	}
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// The control plane's file store, end to end with a real collector: sessions
// survive a clean and an unclean restart in the state they had, an archived
// session can be read and deleted after a restart, and a delete stays in
// effect when the process dies before the journal is folded.
func TestControlPlaneFileStoreAcrossRestarts(t *testing.T) {
	col := collector.New(store.NewMem(), collector.Config{Window: 50 * time.Millisecond})
	colSrv := httptest.NewServer(col.Handler())
	defer colSrv.Close()
	defer col.Close()
	path := filepath.Join(t.TempDir(), "sessions.json")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := func(r *cpRun) client.Options {
		return client.Options{BaseURL: r.srv.URL, CollectorURL: colSrv.URL, QuiesceSeconds: 1, PollInterval: 20 * time.Millisecond}
	}

	// First life: one session finishes and ages into the archive, one stays active.
	run := startCP(t, path, colSrv.URL)
	o := opts(run)
	o.Name = "finished run"
	done, err := client.Start(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Ingest(ctx, "i1", []core.Event{{SessionID: done.ID, TraceID: "tr1", SpanID: "a", Service: "svc", Instance: "i1",
		Kind: core.KindAPIIn, Name: "GET /x", EndHLC: 1, LocalSeq: 1, StartNs: 1, EndNs: 2}}); err != nil {
		t.Fatal(err)
	}
	if info, err := done.StopAndWait(ctx); err != nil || info.State != "SEALED" {
		t.Fatalf("stop: %+v %v", info, err)
	}
	o.Name = "still running"
	live, err := client.Start(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); fileSize(path+".archive") == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the finished session never moved to the archive")
		}
		time.Sleep(20 * time.Millisecond)
	}
	run.stop(t, true)
	if n := fileSize(path + ".journal"); n != 0 {
		t.Fatalf("a clean stop must leave no journal, found %d bytes", n)
	}

	// Second life: both are back as they were; the archived one can be read and the active one stopped.
	run = startCP(t, path, colSrv.URL)
	list, err := client.List(ctx, opts(run), "")
	if err != nil || len(list) != 2 {
		t.Fatalf("sessions after a clean restart: %+v %v", list, err)
	}
	doneAgain, err := client.Attach(ctx, opts(run), done.ID)
	if err != nil || doneAgain.Info().State != "SEALED" || doneAgain.Info().Name != "finished run" {
		t.Fatalf("archived session after restart: %+v %v", doneAgain, err)
	}
	if sum, err := doneAgain.Summary(ctx); err != nil || sum.EventCount != 1 || !sum.Sealed {
		t.Fatalf("its events: %+v %v", sum, err)
	}
	liveAgain, err := client.Attach(ctx, opts(run), live.ID)
	if err != nil || liveAgain.Info().State != "ACTIVE" {
		t.Fatalf("active session after restart: %+v %v", liveAgain, err)
	}
	if info, err := liveAgain.StopAndWait(ctx); err != nil || info.State != "SEALED" {
		t.Fatalf("stop after restart: %+v %v", info, err)
	}
	// Delete the archived session, then die without a clean shutdown.
	if err := doneAgain.Delete(ctx); err != nil {
		t.Fatalf("delete of an archived session: %v", err)
	}
	if sum, err := client.Detached(opts(run), done.ID).Summary(ctx); err != nil || sum.EventCount != 0 {
		t.Fatalf("the collector must have dropped its events: %+v %v", sum, err)
	}
	run.stop(t, false)
	if fileSize(path+".journal") == 0 {
		t.Fatal("test setup: an unclean stop should leave the journal behind")
	}

	// Third life, from the journal: the delete held, the stop held.
	run = startCP(t, path, colSrv.URL)
	defer run.stop(t, true)
	if _, err := client.Attach(ctx, opts(run), done.ID); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("the deleted session is back after an unclean restart: %v", err)
	}
	last, err := client.Attach(ctx, opts(run), live.ID)
	if err != nil || last.Info().State != "SEALED" || last.Info().Name != "still running" {
		t.Fatalf("the stopped session after an unclean restart: %+v %v", last, err)
	}
	if list, err := client.List(ctx, opts(run), ""); err != nil || len(list) != 1 {
		t.Fatalf("sessions after an unclean restart: %+v %v", list, err)
	}
}
