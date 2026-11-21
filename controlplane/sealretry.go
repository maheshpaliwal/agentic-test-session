package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
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
	// disarmed: a Delete took the retry away (disarmSealRetryLocked) and owns
	// the session now. An attempt that panics must not arm it again.
	disarmed bool
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
	r.disarmed = true
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
		// it due starts another. So it has no Watch: the helper never waits
		// for its restart. sealRetryPanicked says that the control plane is
		// degraded while the attempt after a panic is not due yet.
		//
		// Its panics do not count against the panic budget (Budget), which
		// ends the service so that it starts again with clean state. That is
		// for the loops the service cannot do without. A seal retry is the
		// work of one finished session: it is bounded by SealPendingBudget,
		// the session says that its seal failed (seal_error), and ending the
		// service for it would cut short the drains of the sessions that run.
		gosafe.Go(gosafe.Options{Name: "seal_retry", Done: m.wg.Done, OnPanic: m.onPanic(func(gosafe.Panic) { m.sealRetryPanicked(id, r) })},
			func() { m.retrySeal(ctx, id, r) })
	}
}

// sealRetryPanicked puts right what a seal attempt that panicked left
// undone. The attempt is over (a Delete may be waiting for that). The next
// one is due after the backoff, like after a failure, also when the attempt
// had already taken the retry off the list (it panicked while it recorded
// its outcome): the session is still stored with its seal pending, and
// nothing else would look at it again before the next start. Like a failure,
// a panic ends the retries once SealPendingBudget has passed since the
// session ended: an attempt that panics every time is not retried for good.
func (m *Manager) sealRetryPanicked(id string, r *sealRetry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.cancel != nil { // it panicked before it took the lock
		r.cancel()
		r.cancel = nil
		close(r.done)
	}
	if r.disarmed || m.pending[id] != nil && m.pending[id] != r {
		return // a Delete owns the session, or it was armed anew
	}
	now := time.Now()
	r.next = now.Add(r.delay)
	r.delay = min(r.delay*2, m.cfg.SealRetryMaxInterval)
	cur, err := m.stored(id)
	if errors.Is(err, ErrStore) || err == nil && cur.SealPending && m.rootCtx.Err() == nil && now.Before(cur.EndedAt.Add(m.cfg.SealPendingBudget)) {
		// It is tried again, after the backoff: degraded until then, like a
		// loop that waits for its restart (see Health). Not for an attempt
		// that is not repeated.
		m.watch.Hold("seal_retry", r.next)
	}
	switch {
	case errors.Is(err, ErrStore): // stored and unreadable: keep trying, as retrySeal does
	case err != nil || !cur.SealPending:
		delete(m.pending, id) // gone, or the attempt had recorded its outcome
		return
	case m.rootCtx.Err() != nil:
		return // shutting down: the marker stays for the next start
	case !now.Before(cur.EndedAt.Add(m.cfg.SealPendingBudget)):
		delete(m.pending, id)
		cur.SealPending = false
		if cur.SealError == "" {
			cur.SealError = "the seal failed in the control plane"
		}
		m.tel.sealRetries.With("failure").Inc()
		m.tel.sealsAbandoned.Inc()
		m.log.Error("seal still failing (the attempt panicked), giving up", "id", id, "budget", m.cfg.SealPendingBudget)
		if perr := m.putLocked(cur); perr != nil {
			m.log.Error("persist session after seal retry", "id", id, "err", perr)
		}
		return
	}
	m.pending[id] = r
}

// retrySeal makes one seal attempt for a pending seal and records the
// outcome. On success the session gets the seal result and loses SealError
// and SealPending. On failure the next attempt is scheduled with doubled
// backoff (capped at SealRetryMaxInterval); once SealPendingBudget has passed
// since the session ended, the marker is cleared instead and SealError stays.
// The session's state does not change: it stays SEALED, also when it was
// ended by its TTL.
func (m *Manager) retrySeal(ctx context.Context, id string, r *sealRetry) {
	faultpoint.Hit("sealretry.top")
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
	faultpoint.Hit("sealretry.persist")
	if perr := m.putLocked(cur); perr != nil {
		m.log.Error("persist session after seal retry", "id", id, "err", perr)
	}
}
