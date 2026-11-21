package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
)

// healthz asks GET /healthz, from a Host the guard would refuse.
func healthz(t *testing.T, m *Manager) (int, string) {
	t.Helper()
	w := do(t, m.Handler(), "GET", "/healthz", "10.1.2.3:7070", "", nil)
	return w.Code, w.Body.String()
}

// A drain that panics every time is given up after maxDrainPanics panics:
// the session does not stay DRAINING, and in every SDK's active set, for
// good. It ends incomplete, with a reason that says that the control plane
// failed, and never complete: also not once the seal, which the background
// retry makes up for, has succeeded. While the drain waits for a restart
// /healthz says so.
func TestADrainThatKeepsPanickingEndsTheSessionIncomplete(t *testing.T) {
	var logs logBuffer
	col := &panicCollector{flakyCollector: newFlaky()}
	col.failing.Store(false)
	col.drainPanics.Store(1 << 30)
	m := newRetryMgr(t, col, func(c *Config) { c.Logger = logs.logger() })
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	degraded := ""
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		if code, body := healthz(t, m); code == http.StatusServiceUnavailable {
			degraded = body
		}
		if got, _ := m.Get(s.ID); got.State.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			panic("the session stays DRAINING:\n" + logs.String())
		}
	}
	if degraded != "degraded: a background goroutine panicked and waits to be restarted: drain; the log has the panic\n" {
		t.Errorf("/healthz while the drain waited for its restart: %q", degraded)
	}
	got, _ := m.Get(s.ID)
	if got.State != StateSealed || !got.Incomplete || got.EndedAt.IsZero() || len(got.IncompleteReasons) != 1 ||
		!strings.Contains(got.IncompleteReasons[0], "the control plane failed while it drained the session: its drain panicked 3 times (string)") {
		panic(fmt.Sprintf("the session a failed drain left: %+v", got))
	}
	if strings.Contains(got.IncompleteReasons[0], "hunter2") {
		panic("the reason shows the panic value: " + got.IncompleteReasons[0])
	}
	if a := m.Active(); slices.Contains(a.Active, s.ID) {
		panic(fmt.Sprintf("the SDKs are still told to record for it: %+v", a))
	}
	if !panicMetric(t, m, "drain", maxDrainPanics) {
		t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
	}
	// The seal the drain never made is made in the background; the session
	// stays incomplete.
	sealed := waitSession(t, m, s.ID, "sealed in the background", func(s Session) bool { return !s.SealPending })
	if sealed.SealError != "" || sealed.BundleURI == "" || !sealed.Incomplete || len(sealed.IncompleteReasons) != 1 {
		panic(fmt.Sprintf("after the background seal: %+v", sealed))
	}
	if code, body := healthz(t, m); code != 200 || body != "ok\n" {
		t.Errorf("/healthz after the drain was given up: %d %q", code, body)
	}
	if n := strings.Count(logs.String(), "goroutine=drain"); n != maxDrainPanics {
		t.Errorf("%d drain panics logged, want %d:\n%s", n, maxDrainPanics, logs.String())
	}
}

// lateCollector never goes quiet, and panics once: the first time it is
// asked after at.
type lateCollector struct {
	*flakyCollector
	at       time.Time
	panicked atomic.Bool
}

func (c *lateCollector) DrainStatus(context.Context, string) (DrainStatus, error) {
	if time.Now().After(c.at) && c.panicked.CompareAndSwap(false, true) {
		panic("status broke")
	}
	return DrainStatus{Pending: 1}, nil
}

// max_drain counts from the stop, not from the restart of a drain that
// panicked: a drain that panics late in its time must not get all of it
// again.
func TestMaxDrainCountsFromTheStopAcrossARestart(t *testing.T) {
	col := &lateCollector{flakyCollector: newFlaky()}
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, PollInterval: 5 * time.Millisecond, TimeUnit: 100 * time.Millisecond, Logger: (&logBuffer{}).logger()})
	defer m.Close()
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 6}) // 600ms
	if err != nil {
		panic(err)
	}
	began := time.Now()
	col.at = began.Add(450 * time.Millisecond) // the restart, 100ms later, is at 550ms
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	var got Session
	for deadline := began.Add(10 * time.Second); !got.State.Terminal(); time.Sleep(time.Millisecond) {
		if got, _ = m.Get(s.ID); time.Now().After(deadline) {
			panic(fmt.Sprintf("the session never ended: %+v", got))
		}
	}
	took := time.Since(began)
	if !col.panicked.Load() || !got.DrainTimedOut || !got.Incomplete || !panicMetric(t, m, "drain", 1) {
		panic(fmt.Sprintf("the drain: panicked=%v %+v", col.panicked.Load(), got))
	}
	// Counted from the restart it would have ended 600ms after 550ms.
	if took < 550*time.Millisecond || took > 950*time.Millisecond {
		panic(fmt.Sprintf("the drain ended %v after the stop; max_drain is 600ms", took))
	}
}

// A drain that panicked while it recorded the end of the session keeps its
// cancel function: an Abort while it waits for its restart ends it, and it
// does not seal or finish the aborted session afterwards.
func TestAbortStopsADrainThatWaitsForItsRestart(t *testing.T) {
	for _, site := range []string{"drain.finish", "drain.persist"} {
		restore, hits := faultpoint.PanicAt(site, 1)
		col := newFlaky()
		col.failing.Store(false)
		m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: (&logBuffer{}).logger()})
		s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
		if err != nil {
			panic(err)
		}
		if _, err := m.Stop(s.ID); err != nil {
			panic(err)
		}
		for deadline := time.Now().Add(10 * time.Second); hits() == 0 || !panicMetric(t, m, "drain", 1); time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				panic(site + ": the drain never got there")
			}
		}
		m.mu.Lock()
		cancel := m.cancels[s.ID]
		m.mu.Unlock()
		if cancel == nil {
			panic(site + ": the drain lost its cancel function when it panicked")
		}
		seals := col.seals.Load()
		if got, err := m.Abort(s.ID); err != nil || got.State != StateAborted {
			panic(fmt.Sprintf("%s: abort: %v %+v", site, err, got))
		}
		if !returnsWithin(5*time.Second, m.Close) { // the drain's goroutine has ended, without waiting out its backoff
			panic(site + ": Close waits for the aborted drain")
		}
		if got, _ := m.Get(s.ID); got.State != StateAborted || col.seals.Load() != seals {
			panic(fmt.Sprintf("%s: after the abort: %+v, %d seal calls, %d before", site, got, col.seals.Load(), seals))
		}
		restore()
	}
}

// A drain that panicked after it had stored the end of the session finishes
// what was left when it is started again: the SDKs are told.
func TestADrainThatPanickedAfterItStoredTheEndStillTellsTheSDKs(t *testing.T) {
	st := &panicStore{MemStore: NewMemStore()}
	col := newFlaky()
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, Store: afterPut{st}, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: (&logBuffer{}).logger()})
	defer m.Close()
	ch, cancel := m.Subscribe()
	defer cancel()
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	st.panics.Store(1)
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	waitState(t, m, s.ID, StateSealed)
	for deadline := time.After(10 * time.Second); ; {
		select {
		case a := <-ch:
			if !slices.Contains(a.Active, s.ID) {
				if !panicMetric(t, m, "drain", 1) {
					t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
				}
				return
			}
		case <-deadline:
			panic("the SDKs were never told that the session ended")
		}
	}
}

// afterPut is a Store that stores a finished session and then panics, while
// its counter is above zero.
type afterPut struct{ *panicStore }

func (s afterPut) Put(x Session) error {
	err := s.MemStore.Put(x)
	if x.State == StateSealed && s.panics.Add(-1) >= 0 {
		panic("store broke after the write")
	}
	return err
}

// A sweep that panicked after it had moved a session to DRAINING, before it
// started the drain, used to leave it DRAINING for good: nothing drained it,
// and a stop of a DRAINING session does nothing. The next sweep starts the
// drain.
func TestASessionLeftDrainingByASweepThatPanickedIsDrained(t *testing.T) {
	restore, hits := faultpoint.PanicAt("sweep.expired", 1)
	defer restore()
	col := newFlaky()
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, SweepInterval: 2 * time.Millisecond, TimeUnit: time.Millisecond,
		Logger: (&logBuffer{}).logger()})
	ctx, cancel := context.WithCancel(context.Background())
	m.RunInBackground(ctx)
	t.Cleanup(func() { cancel(); m.Close() })
	s, err := m.Start(StartRequest{Name: "ttl", TTLSeconds: 1, QuiesceSeconds: 1})
	if err != nil {
		panic(err)
	}
	got := waitSession(t, m, s.ID, "ended by its TTL", func(s Session) bool { return s.State.Terminal() })
	if hits() != 1 || got.State != StateExpired || got.Incomplete || !panicMetric(t, m, "sweep", 1) {
		panic(fmt.Sprintf("after %d panics: %+v", hits(), got))
	}
}

// A sweep that panics every time uses up the panic budget: it is not started
// again, the owner is told once, and /healthz stays degraded with the reason.
// The API still answers.
func TestASweepThatKeepsPanickingSpendsThePanicBudget(t *testing.T) {
	restore, hits := faultpoint.PanicAt("sweep.top", -1)
	defer restore()
	var logs logBuffer
	spent := make(chan string, 4)
	m := NewManager(Config{SweepInterval: time.Millisecond, Logger: logs.logger(), OnPanicBudgetSpent: func(reason string) { spent <- reason }})
	ctx, cancel := context.WithCancel(context.Background())
	m.RunInBackground(ctx)
	t.Cleanup(func() { cancel(); m.Close() })
	var reason string
	select {
	case reason = <-spent:
	case <-time.After(20 * time.Second):
		panic("the budget was never spent:\n" + logs.String())
	}
	if !strings.Contains(reason, "the panic budget is spent: 5 panics of background goroutines within 10m0s") || m.Health() != reason {
		panic(fmt.Sprintf("reason %q, health %q", reason, m.Health()))
	}
	if code, body := healthz(t, m); code != http.StatusServiceUnavailable || body != "degraded: "+reason+"\n" {
		panic(fmt.Sprintf("/healthz: %d %q", code, body))
	}
	time.Sleep(50 * time.Millisecond)
	if hits() != 5 || len(spent) != 0 || !panicMetric(t, m, "sweep", 5) {
		panic(fmt.Sprintf("%d panics, told %d more times\n%s", hits(), len(spent), scrapeMetrics(t, m)))
	}
	if !strings.Contains(logs.String(), "is not restarted: the panic budget of the service is spent") {
		t.Errorf("log:\n%s", logs.String())
	}
	if _, err := m.Start(StartRequest{Name: "still served"}); err != nil {
		panic(err)
	}
}

// Without an owner to tell, the Manager says in its log that it stays
// degraded.
func TestAManagerWithoutAnOwnerStaysDegraded(t *testing.T) {
	restore, _ := faultpoint.PanicAt("sweep.top", -1)
	defer restore()
	var logs logBuffer
	m := NewManager(Config{SweepInterval: time.Millisecond, Logger: logs.logger()})
	ctx, cancel := context.WithCancel(context.Background())
	m.RunInBackground(ctx)
	t.Cleanup(func() { cancel(); m.Close() })
	for deadline := time.Now().Add(20 * time.Second); !strings.Contains(logs.String(), "stays degraded"); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(logs.String())
		}
	}
	if code, _ := healthz(t, m); code != http.StatusServiceUnavailable {
		panic(fmt.Sprintf("/healthz: %d", code))
	}
}

// A seal attempt that panicked while it recorded its outcome had already
// taken the retry off the list. The session is still stored with its seal
// pending, so the retry is armed again; nothing else would look at the
// session before the next start.
func TestASealRetryThatPanickedWhileRecordingIsArmedAgain(t *testing.T) {
	col := newFlaky()
	m := newRetryMgr(t, col, func(c *Config) { c.Logger = (&logBuffer{}).logger() })
	s := waitPending(t, m, StartRequest{Name: "p"})
	restore, hits := faultpoint.PanicAt("sealretry.persist", 1)
	defer restore()
	col.failing.Store(false)
	got := waitSession(t, m, s.ID, "sealed on a later attempt", func(s Session) bool { return !s.SealPending })
	if hits() != 1 || got.SealError != "" || got.BundleURI == "" || pendingCount(m) != 0 || !panicMetric(t, m, "seal_retry", 1) {
		panic(fmt.Sprintf("after %d panics: %+v, %d pending", hits(), got, pendingCount(m)))
	}
}

// After a panic the next attempt waits for the backoff, which doubles like
// after a failure: an attempt that panics every time is not made at every
// sweep.
func TestSealRetriesBackOffAfterAPanic(t *testing.T) {
	col := newFlaky()
	m := newRetryMgr(t, col, func(c *Config) {
		c.Logger, c.SealRetryMaxInterval = (&logBuffer{}).logger(), time.Hour
	})
	s := waitPending(t, m, StartRequest{Name: "p"})
	restore, hits := faultpoint.PanicAt("sealretry.top", -1)
	defer restore()
	for deadline := time.Now().Add(10 * time.Second); hits() < 3; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the seal is not retried after a panic")
		}
	}
	began, first := time.Now(), hits()
	time.Sleep(300 * time.Millisecond)
	// The backoff began at 2ms and doubles: 300ms hold a handful of attempts,
	// where an attempt per sweep (2ms) would be more than a hundred.
	if n := hits() - first; n > 12 {
		panic(fmt.Sprintf("%d attempts in %v: the retries do not back off after a panic", n, time.Since(began)))
	}
	m.mu.Lock()
	r := m.pending[s.ID]
	m.mu.Unlock()
	if r == nil || r.delay < 64*time.Millisecond {
		panic(fmt.Sprintf("the backoff after the panics: %+v", r))
	}
}

// SealPendingBudget ends the retries of an attempt that panics, as it ends
// those of one that fails: seal_error stays and says what happened.
func TestSealPendingBudgetEndsRetriesThatPanic(t *testing.T) {
	var logs logBuffer
	col := newFlaky()
	m := newRetryMgr(t, col, func(c *Config) {
		c.Logger, c.SealPendingBudget, c.SealRetryMaxInterval = logs.logger(), 150*time.Millisecond, 10*time.Millisecond
	})
	s := waitPending(t, m, StartRequest{Name: "p"})
	restore, hits := faultpoint.PanicAt("sealretry.top", -1)
	defer restore()
	got := waitSession(t, m, s.ID, "given up", func(s Session) bool { return !s.SealPending })
	if got.SealError == "" || !got.Incomplete || got.BundleURI != "" || pendingCount(m) != 0 || hits() == 0 {
		panic(fmt.Sprintf("after %d panics: %+v, %d pending", hits(), got, pendingCount(m)))
	}
	if out := scrapeMetrics(t, m); !strings.Contains(out, "tsession_controlplane_seals_abandoned_total 1\n") {
		t.Errorf("metrics:\n%s", out)
	}
	n := hits()
	time.Sleep(50 * time.Millisecond)
	if hits() != n || !strings.Contains(logs.String(), "giving up") {
		panic(fmt.Sprintf("%d more attempts after the budget\n%s", hits()-n, logs.String()))
	}
}

// panicOn is a log handler that panics on a record whose message contains a
// text: a logger that breaks while it reports a panic.
type panicOn struct {
	slog.Handler
	text string
}

func (h panicOn) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.text) {
		panic("the logger broke")
	}
	return h.Handler.Handle(ctx, r)
}

// What a goroutine that panicked left undone is put right before the panic
// is logged: a logger that panics there must not leave a seal attempt in
// flight for good (it would never be retried, and a Delete would wait for
// it).
func TestBookkeepingAfterAPanicDoesNotDependOnTheLogger(t *testing.T) {
	col := newFlaky()
	m := newRetryMgr(t, col, func(c *Config) {
		c.Logger = slog.New(panicOn{(&logBuffer{}).logger().Handler(), "a background goroutine panicked"})
	})
	s := waitPending(t, m, StartRequest{Name: "p"})
	restore, hits := faultpoint.PanicAt("sealretry.top", 1)
	defer restore()
	col.failing.Store(false)
	got := waitSession(t, m, s.ID, "sealed on the attempt after the panic", func(s Session) bool { return !s.SealPending })
	if hits() != 1 || got.SealError != "" || !panicMetric(t, m, "seal_retry", 1) {
		panic(fmt.Sprintf("after %d panics: %+v", hits(), got))
	}
}

// A compaction that panics is counted like the panics of the other
// goroutines, and its bookkeeping is put right: writers and Close do not
// wait for it, the next one runs, and nothing is lost.
func TestACompactionThatPanicsIsCountedAndDoesNotHoldTheStore(t *testing.T) {
	for _, site := range []string{"compaction.write", "compaction.finish"} {
		restore, hits := faultpoint.PanicAt(site, 1)
		var logs logBuffer
		path := filepath.Join(t.TempDir(), "sessions.json")
		f, err := NewFileStore(path, WithStoreLogger(logs.logger()))
		if err != nil {
			panic(err)
		}
		m := NewManager(Config{Store: f, Logger: logs.logger()})
		f.wmu.Lock()
		f.compactMin, f.compactAt = 1, f.journalSize+1
		f.wmu.Unlock()
		if !returnsWithin(20*time.Second, func() {
			for i := range 50 {
				if err := f.Put(Session{ID: fmt.Sprintf("ts_%02d", i), State: StateSealed}); err != nil {
					panic(err)
				}
				time.Sleep(time.Millisecond)
			}
		}) {
			panic(site + ": writers wait for a compaction that panicked")
		}
		if hits() != 1 {
			panic(fmt.Sprintf("%s: %d panics", site, hits()))
		}
		for deadline := time.Now().Add(10 * time.Second); !panicMetric(t, m, "compaction", 1); time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				panic(site + ": the panic of the compaction is not counted:\n" + scrapeMetrics(t, m))
			}
		}
		// A later compaction ran: the snapshot was written.
		f.lockIdle()
		snap := f.snapSize
		f.wmu.Unlock()
		if snap == 0 {
			panic(site + ": no compaction ran after the one that panicked")
		}
		m.Close()
		if !returnsWithin(10*time.Second, func() { _ = f.Close() }) {
			panic(site + ": Close waits for a compaction that panicked")
		}
		f2, err := NewFileStore(path)
		if err != nil {
			panic(err)
		}
		if n := len(f2.List()); n != 50 {
			panic(fmt.Sprintf("%s: %d sessions after the reload, want 50", site, n))
		}
		_ = f2.Close()
		if out := logs.String(); !strings.Contains(out, "goroutine=compaction") || !strings.Contains(out, "level=ERROR") {
			t.Errorf("%s: log:\n%s", site, out)
		}
		restore()
	}
}
