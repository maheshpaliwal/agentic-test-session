package collector

import (
	"bytes"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A store that panics in a retention sweep used to end the collector, with
// every event in its reorder windows. Now the panic is logged and counted,
// the loop is started again with backoff, and sweeps go on; Close does not
// wait for anything that is gone, and nothing leaks.
func TestRetentionLoopSurvivesAStorePanic(t *testing.T) {
	before := runtime.NumGoroutine()
	var logs syncBuf
	var left atomic.Int32
	left.Store(2)
	spy := &sweepSpy{Store: store.NewMem()}
	spy.override = func(time.Time, string, int) ([]store.SessionInfo, error) {
		if left.Add(-1) >= 0 {
			panic("the store broke at postgres://app:hunter2@db/tsession")
		}
		return nil, nil
	}
	c := New(spy, Config{Retention: time.Hour, RetentionInterval: 2 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	metric := func() string {
		var b bytes.Buffer
		if _, err := c.tel.reg.WriteTo(&b); err != nil {
			panic(err)
		}
		return b.String()
	}
	// Two panics, then the loop sweeps again: 100ms and 200ms of backoff.
	for deadline := time.Now().Add(10 * time.Second); spy.pages.Load() < 5; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("the retention loop did not come back: %d sweeps\n%s", spy.pages.Load(), logs.String()))
		}
	}
	if m := metric(); !strings.Contains(m, `tsession_collector_goroutine_panics_total{goroutine="retention"} 2`+"\n") {
		t.Errorf("metrics:\n%s", m)
	}
	out := logs.String()
	if n := strings.Count(out, "a background goroutine panicked and is restarted"); n != 2 || !strings.Contains(out, "level=ERROR") ||
		!strings.Contains(out, "goroutine=retention") || !strings.Contains(out, "restart_in=200ms") {
		t.Errorf("%d restarts logged:\n%s", n, out)
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("the log shows a credential:\n%s", out)
	}
	c.Close()
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("%d goroutines after Close, %d before", runtime.NumGoroutine(), before))
		}
	}
}

// Close while the loop waits for its restart does not wait for the backoff.
func TestCloseDoesNotWaitForARetentionRestart(t *testing.T) {
	spy := &sweepSpy{Store: store.NewMem()}
	spy.override = func(time.Time, string, int) ([]store.SessionInfo, error) { panic("always") }
	c := New(spy, Config{Retention: time.Hour, RetentionInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&syncBuf{}, nil))})
	for deadline := time.Now().Add(10 * time.Second); spy.pages.Load() < 4; time.Sleep(time.Millisecond) { // 100+200+400ms
		if time.Now().After(deadline) {
			panic("the loop was not restarted")
		}
	}
	began := time.Now() // the next restart is 800ms away
	c.Close()
	if took := time.Since(began); took > 400*time.Millisecond {
		panic(fmt.Sprintf("Close took %v", took))
	}
}
