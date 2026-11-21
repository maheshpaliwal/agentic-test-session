package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
)

// signature is the change-detection tuple of a drain-status response.
func (d DrainStatus) signature() [5]int64 {
	return [5]int64{int64(d.Pending), d.LastEventUnixNs, d.ReceivedUnixNs, d.EventCount, d.DroppedEvents}
}

// maxDrainPanics is how many panics the drain of one session may have. It
// is started again after the ones before the last (it reads everything it
// needs anew, as after a restart of the process); the last ends the session
// instead, incomplete, with a reason that says that the control plane failed
// (see failDrainLocked). A drain that panics every time must not keep its
// session DRAINING, and in every SDK's active set, for good.
const maxDrainPanics = 3

// drainRun is what the drain of a session keeps across its restarts after a
// panic.
type drainRun struct {
	id string
	// begin is when the drain first started: max_drain and the delivery
	// grace count from it, not from a restart.
	begin time.Time
	// restarted: the drain has panicked before, so the session may already
	// be finished in part.
	restarted bool
}

func (m *Manager) startDrainLocked(id string) {
	ctx, cancel := context.WithCancel(m.rootCtx)
	m.cancels[id] = cancel
	m.wg.Add(1)
	d := &drainRun{id: id, begin: time.Now()}
	gosafe.Go(gosafe.Options{Name: "drain", Restarts: maxDrainPanics - 1, Stop: ctx.Done(), Done: m.wg.Done, Watch: m.watch,
		OnPanic: m.onPanic(func(p gosafe.Panic) { m.drainPanicked(ctx, d, p) })},
		func() { m.drain(ctx, d) })
}

// drainPanicked is told of every panic of a drain, on its goroutine.
func (m *Manager) drainPanicked(ctx context.Context, d *drainRun, p gosafe.Panic) {
	d.restarted = true
	if p.RestartIn > 0 || ctx.Err() != nil {
		return // started again, or aborted or closing meanwhile
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failDrainLocked(d.id, p)
}

// failDrainLocked ends a session whose drain was given up after
// maxDrainPanics panics: SEALED and incomplete, never complete, with the
// reason. The delivery reports are not judged (that may be what panics). The
// collector may not have sealed it, so the seal is left to the background
// retry, which keeps the session incomplete when it succeeds.
func (m *Manager) failDrainLocked(id string, p gosafe.Panic) {
	delete(m.cancels, id)
	delete(m.delivery, id)
	cur, ok := m.live[id]
	if !ok || cur.State != StateDraining {
		return // finished or aborted meanwhile
	}
	cur.State, cur.EndedAt, cur.Incomplete = StateSealed, time.Now().UTC(), true
	cur.IncompleteReasons = []string{fmt.Sprintf("the control plane failed while it drained the session: its drain panicked %d times (%s) and was given up, "+
		"so the session was ended without waiting for its events and without checking their delivery; "+
		"this is a bug in tsession, and the log of the control plane has the stack", p.N, p.Type)}
	if m.cfg.Collector != nil {
		cur.SealError, cur.SealPending = "the drain failed in the control plane before the session was sealed", true
		m.armSealRetryLocked(id, time.Now().Add(m.cfg.PollInterval))
	}
	m.tel.finished.With(string(cur.State)).Inc()
	// Whatever becomes of the write, the session leaves the active set: the
	// SDKs must stop recording for it.
	defer m.broadcastLocked()
	defer func() {
		if r := recover(); r != nil {
			m.indexLocked(cur)
			m.log.Error("a session whose drain failed could not be stored; it is ended in memory only", "id", id, "type", fmt.Sprintf("%T", r))
		}
	}()
	if err := m.putLocked(cur); err != nil {
		m.log.Error("persist session whose drain failed", "id", id, "err", err)
	}
}

// onPanic is what the control plane's goroutines do with a recovered panic:
// run after, if any, to put right what the goroutine left undone, then count
// it (tsession_controlplane_goroutine_panics_total) and log it at error
// level. In that order: a logger that panics must not keep the bookkeeping
// from being done, and bookkeeping that panics does not keep the panic from
// being logged.
func (m *Manager) onPanic(after func(gosafe.Panic)) func(gosafe.Panic) {
	report := gosafe.Slog(m.log, m.countPanic)
	return func(p gosafe.Panic) {
		defer report(p)
		if after == nil {
			return
		}
		defer func() {
			if r := recover(); r != nil {
				m.log.Error("putting right what a goroutine that panicked left undone panicked too", "goroutine", p.Name, "type", fmt.Sprintf("%T", r))
			}
		}()
		after(p)
	}
}

func (m *Manager) countPanic(goroutine string) { m.tel.panics.With(goroutine).Inc() }

// RunInBackground is Run on a goroutine of its own that a panic in a sweep
// does not end: the sweep is started again with backoff, since without it no
// session expires and no pending seal is retried. Its panics count against
// the panic budget (gosafe.PanicBudget): a sweep that panics every time ends
// the service instead of leaving one that looks alive and does nothing.
func (m *Manager) RunInBackground(ctx context.Context) {
	gosafe.Go(gosafe.Options{Name: "sweep", Restarts: -1, Stop: ctx.Done(), Watch: m.watch, Budget: true, OnPanic: m.onPanic(nil)},
		func() { m.Run(ctx) })
}

// Health says why the control plane is not healthy, "" when it is: one of its
// goroutines waits for its restart after a panic, or its panic budget is
// spent. GET /healthz answers 503 with it.
func (m *Manager) Health() string { return m.watch.Degraded() }

// budgetSpent is called once when the panic budget is spent.
func (m *Manager) budgetSpent(reason string) {
	if m.cfg.OnPanicBudgetSpent == nil {
		m.log.Error("the control plane no longer does its background work and stays degraded", "reason", reason)
		return
	}
	m.cfg.OnPanicBudgetSpent(reason)
}

// drain polls the collector until the session is quiet or max_drain elapses,
// seals it, and records the outcome. Quiet also means that no SDK instance
// that sent delivery reports still owes the collector events it may yet
// deliver, and that the reports of every connected instance cover what the
// collector has from it (see deliverySettled). A seal that still fails when
// SealRetryBudget is spent ends the session SEALED with SealError and
// SealPending, and is retried in the background (see sealretry.go).
func (m *Manager) drain(ctx context.Context, d *drainRun) {
	id := d.id
	faultpoint.Hit("drain.top")
	s, ok := m.cfg.Store.Get(id)
	if !ok || m.cfg.Collector == nil {
		return
	}
	if d.restarted && s.State != StateDraining {
		// It panicked after it had stored the end of the session: what was
		// left to do is that the session leaves the active set and the SDKs
		// learn of it.
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.cancels, id)
		delete(m.delivery, id)
		if _, live := m.live[id]; live {
			m.indexLocked(s)
		}
		m.broadcastLocked()
		return
	}
	begin := d.begin
	quiet := time.Duration(s.QuiesceSeconds) * m.cfg.TimeUnit
	deadline := begin.Add(time.Duration(s.MaxDrainSeconds) * m.cfg.TimeUnit)

	// Quiescence is judged purely on the control plane's own monotonic clock:
	// "quiet" means drain-status has not changed (event count, last-event
	// marker, received marker, pending) for the quiesce window. Remote
	// timestamps are opaque change-detection tokens and are never compared
	// with local time, so collector clock skew cannot stall or short-circuit
	// a drain.
	var last, prev DrainStatus
	haveStatus := false
	changedAt := begin
	timedOut := false
	for {
		st, err := m.cfg.Collector.DrainStatus(ctx, id)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.log.Error("drain-status failed", "id", id, "err", err)
		} else {
			last = st
			if !haveStatus || st.signature() != prev.signature() {
				if haveStatus {
					changedAt = time.Now()
				}
				prev, haveStatus = st, true
			}
			// The reports are looked at last, and only once the collector is
			// quiet: a delivery it has just seen restarts the quiet window, so
			// the instance has that long to say so before the reports close.
			if st.Pending == 0 && time.Since(changedAt) >= quiet && m.deliverySettled(id, begin, m.countsFor(ctx, id, st, true), false) {
				break
			}
		}
		if !time.Now().Before(deadline) {
			// The reports close here too: one that arrived during the seal
			// would be stored and never judged.
			timedOut = true
			m.deliverySettled(id, begin, m.countsFor(ctx, id, last, haveStatus), true)
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.cfg.PollInterval):
		}
	}

	// The reports are closed. The collector is asked once more, so that the
	// verdict holds them against counts that are no older than they are: an
	// event that was delivered and reported between the last look and the
	// close must not read as missing.
	if st, err := m.cfg.Collector.DrainStatus(ctx, id); err == nil {
		last, haveStatus = st, true
	}
	counts := m.countsFor(ctx, id, last, haveStatus)
	if ctx.Err() != nil {
		return
	}
	res, err := m.sealWithRetry(ctx, id)
	if ctx.Err() != nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	faultpoint.Hit("drain.finish")
	cur, ok := m.cfg.Store.Get(id)
	if !ok || cur.State != StateDraining {
		delete(m.cancels, id)
		return // aborted meanwhile
	}
	cur.State, cur.EndedAt = StateSealed, time.Now().UTC()
	if cur.Reason == "ttl" && err == nil {
		cur.State = StateExpired
	}
	if err != nil {
		cur.SealError, cur.SealPending = err.Error(), true
		m.tel.sealFailures.Inc()
		m.armSealRetryLocked(id, time.Now().Add(m.cfg.PollInterval))
	}
	m.tel.finished.With(string(cur.State)).Inc()
	cur.DrainTimedOut = timedOut
	short := m.judgeDeliveryLocked(&cur, counts)
	cur.Incomplete = timedOut || err != nil || res.Incomplete || short
	cur.DroppedEvents = last.DroppedEvents
	cur.BundleURI = res.BundleURI
	faultpoint.Hit("drain.persist")
	if perr := m.putLocked(cur); perr != nil {
		m.log.Error("persist sealed session", "id", id, "err", perr)
	}
	// Only now: until the session is stored as finished, an Abort must find
	// the drain's cancel function, also while the drain waits for a restart.
	delete(m.cancels, id)
	m.broadcastLocked()
}

// sealWithRetry calls the collector seal, retrying with exponential backoff
// until cfg.SealRetryBudget is spent. The last error is returned on failure.
func (m *Manager) sealWithRetry(ctx context.Context, id string) (SealResult, error) {
	end := time.Now().Add(m.cfg.SealRetryBudget)
	delay := m.cfg.PollInterval
	for {
		// Bound each attempt by the remaining budget so a hung collector
		// cannot stretch the retry window.
		actx, cancel := context.WithDeadline(ctx, end)
		res, err := m.cfg.Collector.Seal(actx, id)
		cancel()
		if err == nil || ctx.Err() != nil {
			return res, err
		}
		m.log.Error("seal failed", "id", id, "err", err)
		if !time.Now().Add(delay).Before(end) {
			return res, fmt.Errorf("seal failed after retries: %w", err)
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > 2*time.Second {
			delay = 2 * time.Second
		}
	}
}
