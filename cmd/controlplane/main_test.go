package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// brokenStore panics whenever a session is to be stored as DRAINING: every
// sweep that finds a session past its TTL panics.
type brokenStore struct{ *controlplane.MemStore }

func (s brokenStore) Put(x controlplane.Session) error {
	if x.State == controlplane.StateDraining {
		panic("the store broke at postgres://app:hunter2@db/tsession")
	}
	return s.MemStore.Put(x)
}

// The command runs the sweep so that a panic in it does not end the process
// at once (a bare `go m.Run` would, with every session). The sweep is started
// again, /healthz says that the control plane is degraded meanwhile, and a
// sweep that panics every time uses up the panic budget: the control plane
// shuts down in order and the command exits 1, so that a supervisor starts it
// again and an operator sees it.
func TestASweepThatKeepsPanickingEndsTheCommandWithStatus1(t *testing.T) {
	var logs lockedBuffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	code := make(chan int, 1)
	go func() {
		code <- serve(ctx, stop, log, ln, controlplane.Config{Store: brokenStore{controlplane.NewMemStore()}, Logger: log,
			SweepInterval: 2 * time.Millisecond, TimeUnit: time.Millisecond})
	}()
	health := func() (int, string) {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if c, body := health(); c != 200 || body != "ok\n" {
		panic(fmt.Sprintf("/healthz before anything happened: %d %q", c, body))
	}
	resp, err := http.Post(base+"/v1/sessions", "application/json", strings.NewReader(`{"name":"ttl","ttl_seconds":1}`))
	if err != nil || resp.StatusCode != http.StatusCreated {
		panic(fmt.Sprintf("start: %v %v", err, resp))
	}
	resp.Body.Close()

	degraded := ""
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		if c, body := health(); c == http.StatusServiceUnavailable && degraded == "" {
			degraded = body
		}
		select {
		case got := <-code:
			if got != 1 {
				panic(fmt.Sprintf("the command ended with status %d, want 1\n%s", got, logs.String()))
			}
		default:
			if time.Now().After(deadline) {
				panic("the command did not end:\n" + logs.String())
			}
			continue
		}
		break
	}
	if !strings.HasPrefix(degraded, "degraded: ") || !strings.Contains(degraded, "sweep") {
		t.Errorf("/healthz while the sweep waited for its restart: %q", degraded)
	}
	out := logs.String()
	for _, want := range []string{"goroutine=sweep", "a background goroutine panicked and is restarted", "the panic budget of the service is spent",
		"the service gives up and exits with status 1", "the panic budget is spent: 5 panics of background goroutines within 10m0s"} {
		if !strings.Contains(out, want) {
			t.Errorf("the log lacks %q:\n%s", want, out)
		}
	}
	if n := strings.Count(out, "goroutine=sweep"); n != 5 {
		t.Errorf("%d panics of the sweep logged, want 5:\n%s", n, out)
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("the log shows a credential:\n%s", out)
	}
	if _, err := http.Get(base + "/healthz"); err == nil {
		t.Errorf("the command still listens after it gave up")
	}
}

// A shutdown that was asked for ends with status 0.
func TestAShutdownThatWasAskedForEndsWithStatus0(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&lockedBuffer{}, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	code := make(chan int, 1)
	go func() { code <- serve(ctx, stop, log, ln, controlplane.Config{Logger: log}) }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/healthz"); err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			panic("the command never served")
		}
	}
	stop()
	select {
	case got := <-code:
		if got != 0 {
			panic(fmt.Sprintf("status %d, want 0", got))
		}
	case <-time.After(20 * time.Second):
		panic("the command did not end")
	}
}
