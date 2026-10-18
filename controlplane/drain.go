package controlplane

import (
	"context"
	"fmt"
	"time"
)

// signature is the change-detection tuple of a drain-status response.
func (d DrainStatus) signature() [5]int64 {
	return [5]int64{int64(d.Pending), d.LastEventUnixNs, d.ReceivedUnixNs, d.EventCount, d.DroppedEvents}
}

func (m *Manager) startDrain(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startDrainLocked(id)
}

func (m *Manager) startDrainLocked(id string) {
	ctx, cancel := context.WithCancel(m.rootCtx)
	m.cancels[id] = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.drain(ctx, id)
	}()
}

// drain polls the collector until the session is quiet or max_drain elapses,
// seals it, and records the outcome.
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
			m.log.Warn("drain-status failed", "id", id, "err", err)
		} else {
			last = st
			if !haveStatus || st.signature() != prev.signature() {
				if haveStatus {
					changedAt = time.Now()
				}
				prev, haveStatus = st, true
			}
			if st.Pending == 0 && time.Since(changedAt) >= quiet {
				break
			}
		}
		if !time.Now().Before(deadline) {
			timedOut = true
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.cfg.PollInterval):
		}
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
		cur.SealError = err.Error()
	}
	cur.Incomplete = timedOut || err != nil || res.Incomplete
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
		m.log.Warn("seal failed", "id", id, "err", err)
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
