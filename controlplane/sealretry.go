package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
)

// sealRetry is the in-memory backoff state of one pending seal: a session
// that is terminal with SealPending set. The marker itself lives on the
// Session, so it survives a restart; the backoff starts over then.
type sealRetry struct {
	delay  time.Duration      // wait after the next failed attempt
	next   time.Time          // earliest time of the next attempt (zero: now)
	cancel context.CancelFunc // non-nil while an attempt is in flight
	done   chan struct{}      // closed when the in-flight attempt has returned
}

// armSealRetryLocked schedules background seal retries for id, the first one
// at next. Caller holds m.mu.
func (m *Manager) armSealRetryLocked(id string, next time.Time) {
	m.pending[id] = &sealRetry{delay: min(m.cfg.PollInterval, m.cfg.SealRetryMaxInterval), next: next}
}

// disarmSealRetryLocked stops the seal retries of id and cancels an attempt in
// flight. It returns a channel that is closed once that attempt has returned
// (nil when none is running). Caller holds m.mu.
func (m *Manager) disarmSealRetryLocked(id string) <-chan struct{} {
	r := m.pending[id]
	if r == nil {
		return nil
	}
	delete(m.pending, id)
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	return r.done
}

// rearmSealRetry resumes the retries of a session that is still stored with
// SealPending (after a Delete that did not go through).
func (m *Manager) rearmSealRetry(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// A session that cannot be read is armed as well: the next attempt finds
	// out whether its seal is pending.
	if s, err := m.stored(id); (err == nil && s.SealPending || errors.Is(err, ErrStore)) && m.pending[id] == nil {
		m.armSealRetryLocked(id, time.Time{})
	}
}

// retrySealsLocked starts one seal attempt for every pending seal that is due
// at now and has none in flight. The collector is called from a goroutine,
// never under m.mu. Caller holds m.mu.
func (m *Manager) retrySealsLocked(now time.Time) {
	if m.cfg.Collector == nil || m.rootCtx.Err() != nil {
		return
	}
	for id, r := range m.pending {
		if r.cancel != nil || now.Before(r.next) {
			continue
		}
		// Like the seal attempts of a drain, one retry is bounded by
		// SealRetryBudget so a hung collector cannot hold it forever.
		ctx, cancel := context.WithTimeout(m.rootCtx, m.cfg.SealRetryBudget)
		r.cancel, r.done = cancel, make(chan struct{})
		m.wg.Add(1)
		// One attempt: not run again by the helper, the next sweep that finds
		// it due starts another.
		gosafe.Go(gosafe.Options{Name: "seal_retry", Done: m.wg.Done, OnPanic: m.onPanic(func() { m.sealRetryPanicked(r) })},
			func() { m.retrySeal(ctx, id, r) })
	}
}

// sealRetryPanicked puts right what a seal attempt that panicked left
// undone: the attempt is over (a Delete may be waiting for that) and the
// next one is due after the backoff, like after a failure. An attempt that
// panics every time is so retried for good, at SealRetryMaxInterval at most:
// the budget is looked at only once the collector has answered.
func (m *Manager) sealRetryPanicked(r *sealRetry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.cancel != nil { // it panicked before it took the lock
		r.cancel()
		r.cancel = nil
		close(r.done)
	}
	r.next = time.Now().Add(r.delay)
	r.delay = min(r.delay*2, m.cfg.SealRetryMaxInterval)
}

// retrySeal makes one seal attempt for a pending seal and records the
// outcome. On success the session gets the seal result and loses SealError
// and SealPending. On failure the next attempt is scheduled with doubled
// backoff (capped at SealRetryMaxInterval); once SealPendingBudget has passed
// since the session ended, the marker is cleared instead and SealError stays.
// The session's state does not change: it stays SEALED, also when it was
// ended by its TTL.
func (m *Manager) retrySeal(ctx context.Context, id string, r *sealRetry) {
	res, err := m.cfg.Collector.Seal(ctx, id) // idempotent on the collector

	m.mu.Lock()
	defer m.mu.Unlock()
	r.cancel()
	r.cancel = nil
	defer close(r.done)
	if m.pending[id] != r {
		return // deleted meanwhile: Delete owns the session now
	}
	cur, gerr := m.stored(id)
	now := time.Now()
	if errors.Is(gerr, ErrStore) {
		// The session is stored and cannot be read: the outcome of this
		// attempt cannot be recorded, and neither can it be known when to
		// give up. Keep the retry (the seal is idempotent) rather than drop
		// it for the life of the process.
		m.log.Error("seal retry: session could not be read, trying again", "id", id, "err", gerr, "next_in", r.delay)
		r.next = now.Add(r.delay)
		r.delay = min(r.delay*2, m.cfg.SealRetryMaxInterval)
		return
	}
	if gerr != nil || !cur.SealPending {
		delete(m.pending, id)
		return
	}
	deadline := cur.EndedAt.Add(m.cfg.SealPendingBudget)
	switch {
	case err == nil:
		cur.SealError, cur.SealPending = "", false
		cur.BundleURI = res.BundleURI
		// The seal knows nothing of events that never reached the collector.
		cur.Incomplete = cur.DrainTimedOut || res.Incomplete || len(cur.IncompleteReasons) > 0
		m.tel.sealRetries.With("success").Inc()
		m.log.Info("seal succeeded on retry", "id", id)
	case m.rootCtx.Err() != nil:
		return // shutting down: the marker stays for the next start
	case !now.Before(deadline):
		cur.SealPending = false
		m.tel.sealRetries.With("failure").Inc()
		m.tel.sealsAbandoned.Inc()
		m.log.Error("seal still failing, giving up", "id", id, "budget", m.cfg.SealPendingBudget, "err", err)
	default:
		m.tel.sealRetries.With("failure").Inc()
		m.log.Error("seal retry failed", "id", id, "err", err, "next_in", r.delay)
		// The last attempt is made at the deadline, not a full backoff later.
		if r.next = now.Add(r.delay); r.next.After(deadline) {
			r.next = deadline
		}
		r.delay = min(r.delay*2, m.cfg.SealRetryMaxInterval)
		return
	}
	delete(m.pending, id)
	if perr := m.putLocked(cur); perr != nil {
		m.log.Error("persist session after seal retry", "id", id, "err", perr)
	}
}
