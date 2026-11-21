package controlplane

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// panicCollector is a flakyCollector whose calls panic while their counter
// is above zero, as a collector client with a bug would.
type panicCollector struct {
	*flakyCollector
	drainPanics, sealPanics atomic.Int32
}

func (c *panicCollector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	if c.drainPanics.Add(-1) >= 0 {
		panic("drain-status of " + id + " broke at http://ci:hunter2@10.0.0.9/")
	}
	return c.flakyCollector.DrainStatus(ctx, id)
}

func (c *panicCollector) Seal(ctx context.Context, id string) (SealResult, error) {
	if c.sealPanics.Add(-1) >= 0 {
		var none []int
		_ = none[len(id)]
	}
	return c.flakyCollector.Seal(ctx, id)
}

// panicCount reads a sample of tsession_controlplane_goroutine_panics_total.
func panicMetric(t *testing.T, m *Manager, goroutine string, n int) bool {
	t.Helper()
	return strings.Contains(scrapeMetrics(t, m), fmt.Sprintf("tsession_controlplane_goroutine_panics_total{goroutine=%q} %d\n", goroutine, n))
}

// A drain that panics used to end the control plane, and every session with
// it. Now the panic is logged and counted, the drain is started again, and
// the session is sealed all the same; Close waits for nothing that is gone.
func TestDrainSurvivesAPanic(t *testing.T) {
	before := runtime.NumGoroutine()
	var logs logBuffer
	col := &panicCollector{flakyCollector: newFlaky()}
	col.failing.Store(false)
	col.drainPanics.Store(2)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: logs.logger()})
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	got := waitState(t, m, s.ID, StateSealed)
	if got.Incomplete || got.SealError != "" {
		panic(fmt.Sprintf("the session did not seal cleanly after the restarts: %+v", got))
	}
	if !panicMetric(t, m, "drain", 2) {
		t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
	}
	out := logs.String()
	if n := strings.Count(out, "a background goroutine panicked and is restarted"); n != 2 || strings.Count(out, "level=ERROR") < 2 ||
		!strings.Contains(out, "goroutine=drain") || !strings.Contains(out, "restart_in=100ms") || !strings.Contains(out, "restart_in=200ms") ||
		!strings.Contains(out, "(*panicCollector).DrainStatus") {
		t.Errorf("%d restarts logged:\n%s", n, out)
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("the log shows a credential:\n%s", out)
	}
	m.Close()
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("%d goroutines after Close, %d before", runtime.NumGoroutine(), before))
		}
	}
}

// Close during the backoff of a drain that panicked does not wait for it.
func TestCloseDoesNotWaitForADrainThatWaitsForItsRestart(t *testing.T) {
	col := &panicCollector{flakyCollector: newFlaky()}
	col.drainPanics.Store(1 << 30)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: (&logBuffer{}).logger()})
	s, err := m.Start(StartRequest{Name: "p"})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	for deadline := time.Now().Add(5 * time.Second); !panicMetric(t, m, "drain", 2); time.Sleep(time.Millisecond) { // 100ms
		if time.Now().After(deadline) {
			panic("the drain was not restarted")
		}
	}
	began := time.Now() // the second restart is 200ms away
	m.Close()
	if took := time.Since(began); took > 120*time.Millisecond {
		panic(fmt.Sprintf("Close took %v", took))
	}
}

// A seal attempt that panics is over, counted, and the next one is made
// after the backoff: the seal still happens, and a Delete in between does
// not wait for an attempt that will never report back.
func TestSealRetrySurvivesAPanic(t *testing.T) {
	var logs logBuffer
	col := &panicCollector{flakyCollector: newFlaky()}
	m := newRetryMgr(t, col, func(c *Config) { c.Logger = logs.logger() })
	s := waitPending(t, m, StartRequest{Name: "p"})
	col.sealPanics.Store(2)
	col.failing.Store(false)
	got := waitSession(t, m, s.ID, "sealed on retry", func(s Session) bool { return !s.SealPending })
	if got.SealError != "" || got.BundleURI == "" || pendingCount(m) != 0 {
		panic(fmt.Sprintf("after the panics: %+v", got))
	}
	if !panicMetric(t, m, "seal_retry", 2) {
		t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
	}
	if out := logs.String(); strings.Count(out, "a background goroutine panicked and has ended") != 2 || !strings.Contains(out, "index out of range") {
		t.Errorf("log:\n%s", out)
	}

	// An attempt that always panics: Delete goes through.
	col.failing.Store(true)
	s2 := waitPending(t, m, StartRequest{Name: "p2"})
	col.sealPanics.Store(1 << 30)
	for deadline := time.Now().Add(5 * time.Second); !panicMetric(t, m, "seal_retry", 3); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the seal was not retried")
		}
	}
	done := make(chan error, 1)
	go func() { done <- m.Delete(context.Background(), s2.ID) }()
	select {
	case err := <-done:
		if err != nil {
			panic(err)
		}
	case <-time.After(5 * time.Second):
		panic("Delete waits for a seal attempt that panicked")
	}
}

// panicStore is a Store whose Put panics while its counter is above zero.
type panicStore struct {
	*MemStore
	panics atomic.Int32
}

func (s *panicStore) Put(x Session) error {
	if x.State == StateDraining && s.panics.Add(-1) >= 0 {
		panic("store broke")
	}
	return s.MemStore.Put(x)
}

// The sweep is what expires sessions and retries seals. Started with
// RunInBackground it is started again after a panic, and the session whose
// expiry panicked expires on a later sweep.
func TestSweepIsRestartedAfterAPanic(t *testing.T) {
	st := &panicStore{MemStore: NewMemStore()}
	st.panics.Store(1)
	col := newFlaky()
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, Store: st, PollInterval: 2 * time.Millisecond, SweepInterval: 2 * time.Millisecond,
		TimeUnit: time.Millisecond, Logger: (&logBuffer{}).logger()})
	ctx, cancel := context.WithCancel(context.Background())
	m.RunInBackground(ctx)
	t.Cleanup(func() { cancel(); m.Close() })
	s, err := m.Start(StartRequest{Name: "ttl", TTLSeconds: 1, QuiesceSeconds: 1})
	if err != nil {
		panic(err)
	}
	waitSession(t, m, s.ID, "ended by its TTL", func(s Session) bool { return s.State.Terminal() })
	if !panicMetric(t, m, "sweep", 1) {
		t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
	}
}

// A compaction that panicked before its last step is over: writers and Close
// do not wait for it, and the next one may start.
func TestCompactionThatPanickedDoesNotHoldTheStore(t *testing.T) {
	f, err := NewFileStore(filepath.Join(t.TempDir(), "sessions.json"))
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	f.wmu.Lock()
	f.compacting = done
	f.wmu.Unlock()
	f.compactionPanicked(done, true)
	select {
	case <-done:
	default:
		panic("the compaction is not over")
	}
	f.compactionPanicked(done, true) // told twice, or after its last step: nothing happens
	if err := f.Put(Session{ID: "ts_1", State: StateActive}); err != nil {
		panic(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- f.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			panic(err)
		}
	case <-time.After(5 * time.Second):
		panic("Close waits for a compaction that panicked")
	}
}
