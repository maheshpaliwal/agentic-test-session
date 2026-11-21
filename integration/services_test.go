package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// buildBinary builds one of the service binaries into dir. moduleDir is
// relative to the repository root, pkg relative to moduleDir.
func buildBinary(t *testing.T, dir, name, moduleDir, pkg string) string {
	t.Helper()
	bin := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	cmd.Dir = filepath.Join("..", moduleDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build %s: %v\n%s", name, err, out))
	}
	return bin
}

// freeAddr returns a loopback address whose port was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type proc struct {
	name string
	cmd  *exec.Cmd
	out  *bytes.Buffer
}

func startProc(t *testing.T, name, bin, dir string, args ...string) *proc {
	t.Helper()
	p := &proc{name: name, cmd: exec.Command(bin, args...), out: &bytes.Buffer{}}
	p.cmd.Dir = dir
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	p.cmd.Env = append(os.Environ(), "TSESSION_ENABLED=") // the services must not pick up SDK settings
	if err := p.cmd.Start(); err != nil {
		panic(fmt.Sprintf("start %s: %v", name, err))
	}
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	})
	return p
}

// terminate sends SIGTERM and returns the exit code of the process.
func (p *proc) terminate(t *testing.T) int {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		panic(fmt.Sprintf("signal %s: %v", p.name, err))
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
		return p.cmd.ProcessState.ExitCode()
	case <-time.After(30 * time.Second):
		panic(fmt.Sprintf("%s did not stop within 30s of SIGTERM; output:\n%s", p.name, p.out))

	}
}

func waitHealthy(t *testing.T, p *proc, addr string) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("%s never answered /healthz on %s; output:\n%s", p.name, addr, p.out))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The three service binaries, built and run as a user runs them: they start,
// talk to each other, record and seal a session, and stop cleanly on SIGTERM.
// The packages behind them have unit tests; their main functions (flags, wiring,
// shutdown order) are only exercised here.
func TestServiceBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs three binaries")
	}
	dir := t.TempDir()
	collectorBin := buildBinary(t, dir, "collector", "collector", "./cmd/collector")
	cpBin := buildBinary(t, dir, "controlplane", ".", "./cmd/controlplane")
	dashBin := buildBinary(t, dir, "dashboard", ".", "./cmd/dashboard")

	for _, bin := range []string{collectorBin, cpBin, dashBin} {
		out, err := exec.Command(bin, "-version").CombinedOutput()
		if err != nil || !strings.HasPrefix(string(out), "tsession-") || !strings.Contains(string(out), "(go") {
			panic(fmt.Sprintf("%s -version: %q %v", filepath.Base(bin), out, err))
		}
	}

	colAddr, cpAddr, dashAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	storeFile := filepath.Join(dir, "sessions.json")
	col := startProc(t, "collector", collectorBin, dir, "--listen", colAddr, "--bundle-dir", filepath.Join(dir, "bundles"), "--window", "50ms")
	waitHealthy(t, col, colAddr)
	cp := startProc(t, "controlplane", cpBin, dir, "--listen", cpAddr, "--collector-url", "http://"+colAddr, "--store-file", storeFile)
	waitHealthy(t, cp, cpAddr)
	dash := startProc(t, "dashboard", dashBin, dir, "-listen", dashAddr, "-control-plane", "http://"+cpAddr, "-collector", "http://"+colAddr)
	waitHealthy(t, dash, dashAddr)

	// A second control plane on the same port fails at once and says which setting to change.
	dup := exec.Command(cpBin, "--listen", cpAddr, "--collector-url", "http://"+colAddr)
	if out, err := dup.CombinedOutput(); err == nil || !strings.Contains(string(out), "address already in use") ||
		!strings.Contains(string(out), "--addr") || strings.Contains(string(out), "control plane listening") {
		panic(fmt.Sprintf("second control plane on a taken port: %v\n%s", err, out))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := client.Options{BaseURL: "http://" + cpAddr, CollectorURL: "http://" + colAddr, Name: "binaries", QuiesceSeconds: 1}
	sess, err := client.Start(ctx, opts)
	if err != nil {
		panic(fmt.Sprintf("start session: %v\ncontrol plane output:\n%s", err, cp.out))
	}
	info, err := sess.StopAndWait(ctx)
	if err != nil || info.State != "SEALED" || info.SealError != "" {
		panic(fmt.Sprintf("stop: %+v %v\ncontrol plane output:\n%s\ncollector output:\n%s", info, err, cp.out, col.out))
	}

	// The dashboard proxies both services.
	resp, err := http.Get("http://" + dashAddr + "/api/cp/v1/sessions")
	if err != nil {
		panic(err)
	}
	var listed []client.Info
	err = json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	if err != nil || len(listed) != 1 || listed[0].ID != sess.ID {
		panic(fmt.Sprintf("sessions through the dashboard: %+v %v", listed, err))
	}
	for _, addr := range []string{colAddr, cpAddr} {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			panic(err)
		}
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(body.String(), "tsession_build_info") {
			panic(fmt.Sprintf("metrics on %s: %d\n%.300s", addr, resp.StatusCode, body.String()))
		}
	}

	// SIGTERM: each exits 0, and the control plane leaves its state in the one file.
	for _, p := range []*proc{dash, cp, col} {
		if code := p.terminate(t); code != 0 {
			panic(fmt.Sprintf("%s exited %d after SIGTERM; output:\n%s", p.name, code, p.out))
		}
	}
	if fi, err := os.Stat(storeFile + ".journal"); err == nil && fi.Size() > 0 {
		panic(fmt.Sprintf("the control plane left a journal of %d bytes after a clean stop", fi.Size()))
	}
	snapshot, err := os.ReadFile(storeFile)
	if err != nil || !strings.Contains(string(snapshot), fmt.Sprintf("%q", sess.ID)) {
		panic(fmt.Sprintf("the session is not in the store file after a clean stop: %v\n%.300s", err, snapshot))
	}
}
