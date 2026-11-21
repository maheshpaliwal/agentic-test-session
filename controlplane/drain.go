package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
)

// signature is the change-detection tuple of a drain-status response.
func (d DrainStatus) signature() [5]int64 {
	return [5]int64{int64(d.Pending), d.LastEventUnixNs, d.ReceivedUnixNs, d.EventCount, d.DroppedEvents}
}

func (m *Manager) startDrainLocked(id string) {
	ctx, cancel := context.WithCancel(m.rootCtx)
	m.cancels[id] = cancel
	m.wg.Add(1)
	// A drain that panicked is started again, for as long as it takes: it
	// reads everything it needs anew, as it does after a restart of the
	// process, and a session nobody drains would stay DRAINING, and in every
	// SDK's active set, for good. Its max_drain counts from the restart.
	gosafe.Go(gosafe.Options{Name: "drain", Restarts: -1, Stop: ctx.Done(), Done: m.wg.Done, OnPanic: m.onPanic(nil)},
		func() { m.drain(ctx, id) })
}

// onPanic is what the control plane's goroutines do with a recovered panic:
// log it at error level, count it (tsession_controlplane_goroutine_panics_total)
// and then run after, if any, to put right what the goroutine left undone.
func (m *Manager) onPanic(after func()) func(gosafe.Panic) {
	report := gosafe.Slog(m.log, func(name string) { m.tel.panics.With(name).Inc() })
	return func(p gosafe.Panic) {
		report(p)
		if after != nil {
			after()
		}
	}
}

// RunInBackground is Run on a goroutine of its own that a panic in a sweep
// does not end: the sweep is started again with backoff, without limit, since
// without it no session expires and no pending seal is retried.
func (m *Manager) RunInBackground(ctx context.Context) {
	gosafe.Go(gosafe.Options{Name: "sweep", Restarts: -1, Stop: ctx.Done(), OnPanic: m.onPanic(nil)}, func() { m.Run(ctx) })
}

// drain polls the collector until the session is quiet or max_drain elapses,
// seals it, and records the outcome. Quiet also means that no SDK instance
// that sent delivery reports still owes the collector events it may yet
// deliver, and that the reports of every connected instance cover what the
// collector has from it (see deliverySettled). A seal that still fails when
// SealRetryBudget is spent ends the session SEALED with SealError and
// SealPending, and is retried in the background (see sealretry.go).
func (m *Manager) drain(ctx context.Context, id string) {
	s, ok := m.cfg.Store.Get(id)
	if !ok || m.cfg.Collector == nil {
		return
	}
	begin := time.Now()
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
	delete(m.cancels, id)
	cur, ok := m.cfg.Store.Get(id)
	if !ok || cur.State != StateDraining {
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
	if perr := m.putLocked(cur); perr != nil {
		m.log.Error("persist sealed session", "id", id, "err", perr)
	}
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
