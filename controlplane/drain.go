package controlplane

import (
	"context"
	"time"
)

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

	var last DrainStatus
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
			// Quiet is measured from the later of the last event and the
			// drain start, so in-flight exporter batches get a chance to land.
			ref := begin
			if st.LastEventUnixNs > 0 {
				if t := time.Unix(0, st.LastEventUnixNs); t.After(ref) {
					ref = t
				}
			}
			if st.Pending == 0 && time.Since(ref) >= quiet {
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

	res, err := m.cfg.Collector.Seal(ctx, id)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		m.log.Error("seal failed", "id", id, "err", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cancels, id)
	cur, ok := m.cfg.Store.Get(id)
	if !ok || cur.State != StateDraining {
		return // aborted meanwhile
	}
	cur.State, cur.EndedAt = StateSealed, time.Now().UTC()
	cur.Incomplete = timedOut || err != nil || res.Incomplete
	cur.DroppedEvents = last.DroppedEvents
	cur.BundleURI = res.BundleURI
	if perr := m.cfg.Store.Put(cur); perr != nil {
		m.log.Error("persist sealed session", "id", id, "err", perr)
	}
	m.broadcastLocked()
}
