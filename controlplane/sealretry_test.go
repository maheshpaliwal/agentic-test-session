package controlplane

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyCollector is an in-process Collector whose Seal fails while failing is
// set. It records the order of seal and delete calls.
type flakyCollector struct {
	failing     atomic.Bool
	deleteFails atomic.Bool
	busy        atomic.Bool // drain-status reports pending work, so the drain times out
	// slow makes Seal block until its context is done and then take a while
	// longer to return, like a call that is slow to notice a cancellation.
	slow  atomic.Bool
	seals atomic.Int32
	res   SealResult

	mu    sync.Mutex
	calls []string // "seal" (when the call returns) and "delete", in order
}

func (c *flakyCollector) DrainStatus(context.Context, string) (DrainStatus, error) {
	if c.busy.Load() {
		return DrainStatus{Pending: 1}, nil
	}
	return DrainStatus{}, nil
}

func (c *flakyCollector) Seal(ctx context.Context, _ string) (SealResult, error) {
	c.seals.Add(1)
	slow := c.slow.Load()
	if slow {
		<-ctx.Done()
		time.Sleep(30 * time.Millisecond)
	}
	c.mu.Lock()
	c.calls = append(c.calls, "seal")
	c.mu.Unlock()
	if slow {
		return SealResult{}, ctx.Err()
	}
	if c.failing.Load() {
		return SealResult{}, errors.New("collector store: connection refused")
	}
	return c.res, nil
}

func (c *flakyCollector) Delete(context.Context, string) error {
	if c.deleteFails.Load() {
		return errors.New("store down")
	}
	c.mu.Lock()
	c.calls = append(c.calls, "delete")
	c.mu.Unlock()
	return nil
}

func (c *flakyCollector) callLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func newFlaky() *flakyCollector {
	c := &flakyCollector{res: SealResult{BundleURI: "file:///bundle/late"}}
	c.failing.Store(true)
	return c
}

// newRetryMgr builds a Manager with a tiny seal budget and a running sweep.
func newRetryMgr(t *testing.T, c Collector, mut func(*Config)) *Manager {
	t.Helper()
	cfg := Config{
		Collector:       c,
		PollInterval:    2 * time.Millisecond,
		SweepInterval:   2 * time.Millisecond,
		TimeUnit:        10 * time.Millisecond,
		SealRetryBudget: 20 * time.Millisecond,
	}
	if mut != nil {
		mut(&cfg)
	}
	m := NewManager(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go m.Run(ctx)
	t.Cleanup(func() { cancel(); m.Close() })
	return m
}

// waitSession polls until ok accepts the session.
func waitSession(t *testing.T, m *Manager, id, what string, ok func(Session) bool) Session {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, found := m.Get(id); found && ok(s) {
			return s
		}
		time.Sleep(2 * time.Millisecond)
	}
	s, _ := m.Get(id)
	t.Fatalf("session %s never reached %q: %+v", id, what, s)
	return s
}

// waitPending starts and stops a session and waits until its seal has failed
// past the budget: SEALED with seal_error and the pending marker.
func waitPending(t *testing.T, m *Manager, req StartRequest) Session {
	t.Helper()
	s, err := m.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		t.Fatal(err)
	}
	got := waitState(t, m, s.ID, StateSealed)
	if !got.SealPending || got.SealError == "" || !got.Incomplete || got.BundleURI != "" || got.EndedAt.IsZero() {
		t.Fatalf("seal budget exhausted: %+v", got)
	}
	return got
}

// waitSeals waits for the collector to see more than n seal calls.
func waitSeals(t *testing.T, c *flakyCollector, n int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for c.seals.Load() <= n {
		if time.Now().After(deadline) {
			t.Fatalf("no seal attempt after the first %d", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func pendingCount(m *Manager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending)
}

func scrapeMetrics(t *testing.T, m *Manager) string {
	t.Helper()
	return do(t, m.Handler(), "GET", "/metrics", "127.0.0.1:7070", "", nil).Body.String()
}

func sessionJSON(t *testing.T, m *Manager, id string) string {
	t.Helper()
	return do(t, m.Handler(), "GET", "/v1/sessions/"+id, "127.0.0.1:7070", "", nil).Body.String()
}

// A seal that fails for longer than SealRetryBudget must still happen once
// the collector is back: the session stays SEALED, loses seal_error and the
// pending marker, and gains the seal result.
func TestSealRecoversAfterBudgetExhausted(t *testing.T) {
	for _, tc := range []struct {
		name           string
		req            StartRequest
		busy           bool // the drain runs into max_drain
		ttl            bool // ended by the sweep, not by Stop
		res            SealResult
		wantIncomplete bool
	}{
		{name: "clean", req: StartRequest{QuiesceSeconds: 1}, res: SealResult{BundleURI: "file:///b/1"}},
		{name: "collector reports incomplete", req: StartRequest{QuiesceSeconds: 1},
			res: SealResult{BundleURI: "file:///b/2", Incomplete: true}, wantIncomplete: true},
		{name: "drain timed out", req: StartRequest{QuiesceSeconds: 1, MaxDrainSeconds: 2}, busy: true,
			res: SealResult{BundleURI: "file:///b/3"}, wantIncomplete: true},
		{name: "ended by ttl", req: StartRequest{QuiesceSeconds: 1, TTLSeconds: 1}, ttl: true,
			res: SealResult{BundleURI: "file:///b/4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newFlaky()
			c.res = tc.res
			c.busy.Store(tc.busy)
			m := newRetryMgr(t, c, nil)
			var s Session
			if tc.ttl {
				s, _ = m.Start(tc.req)
				s = waitState(t, m, s.ID, StateSealed)
				if !s.SealPending || s.SealError == "" || s.Reason != "ttl" {
					t.Fatalf("ttl seal failure: %+v", s)
				}
			} else {
				s = waitPending(t, m, tc.req)
			}
			if s.DrainTimedOut != tc.busy {
				t.Fatalf("drain_timed_out=%v, want %v", s.DrainTimedOut, tc.busy)
			}
			if len(m.Active().Active) != 0 {
				t.Fatal("a session with a pending seal must not be in the active set")
			}
			if body := sessionJSON(t, m, s.ID); !strings.Contains(body, `"seal_pending":true`) {
				t.Fatalf("marker not in the session JSON: %s", body)
			}
			if got := scrapeMetrics(t, m); !strings.Contains(got, "tsession_controlplane_seals_pending 1\n") {
				t.Fatalf("pending gauge:\n%s", got)
			}

			c.failing.Store(false)
			got := waitSession(t, m, s.ID, "sealed by a retry", func(s Session) bool { return !s.SealPending })
			if got.State != StateSealed || got.SealError != "" || got.BundleURI != tc.res.BundleURI ||
				got.Incomplete != tc.wantIncomplete || !got.EndedAt.Equal(s.EndedAt) {
				t.Fatalf("recovered: %+v", got)
			}
			if body := sessionJSON(t, m, s.ID); strings.Contains(body, "seal_pending") || strings.Contains(body, "seal_error") {
				t.Fatalf("marker or error still in the session JSON: %s", body)
			}
			// Nothing is left to retry.
			n := c.seals.Load()
			time.Sleep(30 * time.Millisecond)
			if c.seals.Load() != n || pendingCount(m) != 0 {
				t.Fatalf("still retrying after success: %d -> %d seals, %d pending", n, c.seals.Load(), pendingCount(m))
			}
			metrics := scrapeMetrics(t, m)
			for _, want := range []string{
				"tsession_controlplane_seals_pending 0\n",
				`tsession_controlplane_seal_retries_total{result="success"} 1`,
				"tsession_controlplane_seal_failures_total 1\n",
				"tsession_controlplane_seals_abandoned_total 0\n",
			} {
				if !strings.Contains(metrics, want) {
					t.Errorf("missing %q in\n%s", want, metrics)
				}
			}
		})
	}
}

// The pending marker is persisted: after a restart on a FileStore the retry
// resumes at once, also for a session that retention moved to the archive.
func TestSealPendingSurvivesRestart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		archived bool
	}{{"hot", false}, {"archived", true}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.json")
			fs, err := NewFileStore(path)
			closeLater(t, fs)
			if err != nil {
				t.Fatal(err)
			}
			c := newFlaky()
			cfg := Config{Store: fs, Collector: c, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond,
				SealRetryBudget: 20 * time.Millisecond, Retention: time.Hour}
			m1 := NewManager(cfg) // no Run: nothing retries before the restart
			s := waitPending(t, m1, StartRequest{Name: "persist", QuiesceSeconds: 1})
			m1.Close()
			if tc.archived {
				m1.Sweep(time.Now().Add(2 * time.Hour))
				if _, ok := fs.MemStore.Get(s.ID); ok {
					t.Fatal("session not archived")
				}
			}

			fs2, err := NewFileStore(path)
			closeLater(t, fs2)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := fs2.Get(s.ID); !ok || !got.SealPending || got.SealError == "" || got.State != StateSealed {
				t.Fatalf("reload: %+v %v", got, ok)
			}
			c.failing.Store(false)
			before := c.seals.Load()
			cfg.Store = fs2
			m2 := NewManager(cfg) // again no Run: the retry must start on its own
			defer m2.Close()
			got := waitSession(t, m2, s.ID, "sealed after restart", func(s Session) bool { return !s.SealPending })
			if got.SealError != "" || got.BundleURI != "file:///bundle/late" || got.Incomplete || got.Name != "persist" {
				t.Fatalf("after restart: %+v", got)
			}
			if n := c.seals.Load() - before; n != 1 {
				t.Fatalf("%d seal calls after restart, want 1", n)
			}
			m2.Close() // the store shows the result before the file is rewritten: wait for the retry to return

			fs3, err := NewFileStore(path)
			closeLater(t, fs3)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := fs3.Get(s.ID); !ok || got.SealPending || got.SealError != "" || got.BundleURI != "file:///bundle/late" {
				t.Fatalf("persisted: %+v %v", got, ok)
			}
			if l := fs3.List(); len(l) != 1 {
				t.Fatalf("list after restart: %+v", l)
			}
		})
	}
}

// Deleting a session with a pending seal is allowed and ends the retries.
func TestDeleteStopsPendingSeal(t *testing.T) {
	c := newFlaky()
	m := newRetryMgr(t, c, nil)
	s := waitPending(t, m, StartRequest{QuiesceSeconds: 1})

	// A failed collector delete keeps the session, and its retries.
	c.deleteFails.Store(true)
	if err := m.Delete(context.Background(), s.ID); !errors.Is(err, ErrCollector) {
		t.Fatalf("collector failure: %v", err)
	}
	if got, ok := m.Get(s.ID); !ok || !got.SealPending || pendingCount(m) != 1 {
		t.Fatalf("after failed delete: %+v %v, %d pending", got, ok, pendingCount(m))
	}
	waitSeals(t, c, c.seals.Load())
	c.deleteFails.Store(false)

	if err := m.Delete(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get(s.ID); ok {
		t.Fatal("still readable after delete")
	}
	n := c.seals.Load()
	time.Sleep(50 * time.Millisecond)
	if got := c.seals.Load(); got != n || pendingCount(m) != 0 {
		t.Fatalf("retries continued after delete: %d -> %d seals, %d pending", n, got, pendingCount(m))
	}
	if log := c.callLog(); log[len(log)-1] != "delete" {
		t.Fatalf("a seal reached the collector after the delete: %v", log[max(0, len(log)-4):])
	}
	if _, ok := m.Get(s.ID); ok {
		t.Fatal("a retry brought the deleted session back")
	}
}

// Delete cancels a seal attempt that is in flight and waits for it, so the
// collector sees the delete last.
func TestDeleteWaitsForSealInFlight(t *testing.T) {
	c := newFlaky()
	m := newRetryMgr(t, c, func(cfg *Config) { cfg.SealRetryMaxInterval = 2 * time.Millisecond })
	s := waitPending(t, m, StartRequest{QuiesceSeconds: 1})
	c.slow.Store(true)
	waitSeals(t, c, c.seals.Load()) // a slow attempt is in flight (or just back)
	if err := m.Delete(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond) // longer than a slow attempt takes to return
	log := c.callLog()
	if i := slices.Index(log, "delete"); i != len(log)-1 {
		t.Fatalf("a seal returned after the delete: %v", log[max(0, len(log)-4):])
	}
	if _, ok := m.Get(s.ID); ok || pendingCount(m) != 0 {
		t.Fatalf("session or retry left behind (%d pending)", pendingCount(m))
	}
}

// Retries back off up to SealRetryMaxInterval and end with SealPendingBudget:
// the marker is cleared and seal_error stays.
func TestSealPendingBudgetHonoured(t *testing.T) {
	const budget, maxInterval = 80 * time.Millisecond, 8 * time.Millisecond
	c := newFlaky()
	m := newRetryMgr(t, c, func(cfg *Config) {
		cfg.SealPendingBudget = budget
		cfg.SealRetryMaxInterval = maxInterval
	})
	s := waitPending(t, m, StartRequest{QuiesceSeconds: 1})

	var maxDelay time.Duration
	got := waitSession(t, m, s.ID, "given up", func(cur Session) bool {
		m.mu.Lock()
		if r := m.pending[cur.ID]; r != nil {
			maxDelay = max(maxDelay, r.delay)
		}
		m.mu.Unlock()
		return !cur.SealPending
	})
	if el := time.Since(s.EndedAt); el < budget {
		t.Fatalf("gave up after %v, before the %v budget", el, budget)
	}
	if got.State != StateSealed || got.SealError == "" || !got.Incomplete || got.BundleURI != "" {
		t.Fatalf("given up: %+v", got)
	}
	if maxDelay <= 2*time.Millisecond || maxDelay > maxInterval {
		t.Fatalf("backoff reached %v, want above the 2ms start and at most %v", maxDelay, maxInterval)
	}
	// It is over for good, even though the collector is back.
	c.failing.Store(false)
	n := c.seals.Load()
	time.Sleep(40 * time.Millisecond)
	if c.seals.Load() != n || pendingCount(m) != 0 {
		t.Fatalf("retried after the budget: %d -> %d seals, %d pending", n, c.seals.Load(), pendingCount(m))
	}
	if cur, _ := m.Get(s.ID); cur.SealError == "" || cur.SealPending {
		t.Fatalf("after the budget: %+v", cur)
	}
	metrics := scrapeMetrics(t, m)
	for _, want := range []string{
		"tsession_controlplane_seals_abandoned_total 1\n",
		"tsession_controlplane_seals_pending 0\n",
		`tsession_controlplane_seal_retries_total{result="failure"}`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("missing %q in\n%s", want, metrics)
		}
	}
	// The session can still be deleted.
	if err := m.Delete(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
}
