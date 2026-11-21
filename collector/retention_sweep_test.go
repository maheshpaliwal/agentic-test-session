package collector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// sweepSpy counts what a retention sweep asks of the store and injects
// faults. It answers ListExpired from the store it wraps.
type sweepSpy struct {
	store.Store
	lists, listed     atomic.Int64 // ListSessions calls, sessions they returned
	pages, candidates atomic.Int64 // ListExpired calls, sessions they returned
	seals, deletes    atomic.Int64 // GetSeal and DeleteSession calls

	mu       sync.Mutex
	badDel   map[string]bool
	listErr  error
	maxDel   int64 // > 0: cancel is called at the maxDel-th DeleteSession of a sweep
	cancel   context.CancelFunc
	override func(cutoff time.Time, after string, limit int) ([]store.SessionInfo, error)
}

func (s *sweepSpy) ListSessions(ctx context.Context) ([]store.SessionInfo, error) {
	l, err := s.Store.ListSessions(ctx)
	s.lists.Add(1)
	s.listed.Add(int64(len(l)))
	return l, err
}

func (s *sweepSpy) ListExpired(ctx context.Context, cutoff time.Time, after string, limit int) ([]store.SessionInfo, error) {
	s.pages.Add(1)
	s.mu.Lock()
	err, override := s.listErr, s.override
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var l []store.SessionInfo
	if override != nil {
		l, err = override(cutoff, after, limit)
	} else {
		el, ok := s.Store.(interface {
			ListExpired(context.Context, time.Time, string, int) ([]store.SessionInfo, error)
		})
		if !ok {
			return nil, errors.New("wrapped store cannot list expired sessions")
		}
		l, err = el.ListExpired(ctx, cutoff, after, limit)
	}
	s.candidates.Add(int64(len(l)))
	return l, err
}

func (s *sweepSpy) GetSeal(ctx context.Context, id string) (*store.SealInfo, error) {
	s.seals.Add(1)
	return s.Store.GetSeal(ctx, id)
}

func (s *sweepSpy) DeleteSession(ctx context.Context, id string) error {
	n := s.deletes.Add(1)
	s.mu.Lock()
	bad, maxDel, cancel := s.badDel[id], s.maxDel, s.cancel
	s.mu.Unlock()
	if maxDel > 0 && n == maxDel {
		defer cancel()
	}
	if bad {
		return errors.New("injected delete failure")
	}
	return s.Store.DeleteSession(ctx, id)
}

func (s *sweepSpy) reset() {
	for _, c := range []*atomic.Int64{&s.lists, &s.listed, &s.pages, &s.candidates, &s.seals, &s.deletes} {
		c.Store(0)
	}
}

// listOnly hides ListExpired: the collector sweeps it from ListSessions.
type listOnly struct{ store.Store }

// seedSession stores a session of one event ending at lastNs, sealed at
// sealedAt unless that is zero.
func seedSession(t testing.TB, st store.Store, id string, lastNs int64, sealedAt time.Time) {
	t.Helper()
	ctx := context.Background()
	e := core.Event{SessionID: id, SpanID: "s", Instance: "i", LocalSeq: 1, Seq: 1, Kind: core.KindAPIIn, Service: "svc",
		EndHLC: 1, StartNs: lastNs, EndNs: lastNs}
	if err := st.AppendEvents(ctx, id, []core.Event{e}); err != nil {
		panic(err)
	}
	if sealedAt.IsZero() {
		return
	}
	if err := st.PutSeal(ctx, id, store.SealInfo{SealedAt: sealedAt}); err != nil {
		panic(err)
	}
}

func sweepCollector(t testing.TB, st store.Store) *Collector {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.Retention = time.Hour
	cfg.RetentionInterval = time.Hour // sweeps are driven by hand
	cfg.IdleEvict = -1
	c := New(st, cfg)
	t.Cleanup(func() { c.Close() })
	return c
}

func sessionIDs(t testing.TB, st store.Store) []string {
	t.Helper()
	l, err := st.ListSessions(context.Background())
	if err != nil {
		panic(err)
	}
	ids := make([]string, len(l))
	for i := range l {
		ids[i] = l[i].ID
	}
	return ids
}

// What one sweep reads with 10000 sealed sessions of which 3 are due: the
// measurement behind store.ExpiredLister.
func TestRetentionSweepCost(t *testing.T) {
	const total, due = 10000, 3
	now := time.Now()
	seed := func() *sweepSpy {
		spy := &sweepSpy{Store: store.NewMem()}
		for i := range total {
			at := now
			if i < due {
				at = now.Add(-2 * time.Hour)
			}
			seedSession(t, spy.Store, fmt.Sprintf("s%05d", i), now.UnixNano(), at)
		}
		return spy
	}

	t.Run("listed whole", func(t *testing.T) {
		spy := seed()
		c := sweepCollector(t, listOnly{spy})
		n, err := c.PurgeExpired(context.Background(), now)
		if err != nil || n != due {
			panic(fmt.Sprintf("purged %d, err %v", n, err))
		}
		t.Logf("ListSessions %d (%d sessions), GetSeal %d", spy.lists.Load(), spy.listed.Load(), spy.seals.Load())
		if spy.lists.Load() != 1 || spy.listed.Load() != total || spy.seals.Load() < total {
			panic(fmt.Sprintf("a store without ListExpired is read whole: lists %d, sessions %d, seals %d", spy.lists.Load(), spy.listed.Load(), spy.seals.Load()))
		}
	})

	t.Run("expired lister", func(t *testing.T) {
		spy := seed()
		c := sweepCollector(t, spy)
		n, err := c.PurgeExpired(context.Background(), now)
		if err != nil || n != due {
			panic(fmt.Sprintf("purged %d, err %v", n, err))
		}
		t.Logf("ListSessions %d, ListExpired %d (%d sessions), GetSeal %d", spy.lists.Load(), spy.pages.Load(), spy.candidates.Load(), spy.seals.Load())
		if spy.lists.Load() != 0 || spy.pages.Load() != 1 || spy.candidates.Load() != due {
			panic(fmt.Sprintf("lists %d, pages %d, candidates %d: want only the %d due sessions read", spy.lists.Load(), spy.pages.Load(), spy.candidates.Load(), due))
		}
		if got := spy.seals.Load(); got != due {
			panic(fmt.Sprintf("%d GetSeal calls for %d due sessions", got, due))
		}
		// With nothing due, a sweep is one empty page.
		spy.reset()
		if n, err := c.PurgeExpired(context.Background(), now); err != nil || n != 0 {
			panic(fmt.Sprintf("purged %d, err %v", n, err))
		}
		if spy.pages.Load() != 1 || spy.candidates.Load() != 0 || spy.seals.Load() != 0 || spy.lists.Load() != 0 {
			panic(fmt.Sprintf("idle sweep: pages %d, candidates %d, seals %d, lists %d", spy.pages.Load(), spy.candidates.Load(), spy.seals.Load(), spy.lists.Load()))
		}
	})
}

// A sweep deletes exactly the due sessions, however the store is listed, and
// a backlog of several pages goes in one sweep.
func TestRetentionSweepDeletesExactlyDue(t *testing.T) {
	now := time.Now()
	old, stale := now.Add(-2*time.Hour), now.Add(-48*time.Hour).UnixNano()
	for _, lister := range []bool{true, false} {
		t.Run(fmt.Sprintf("lister=%v", lister), func(t *testing.T) {
			spy := &sweepSpy{Store: store.NewMem()}
			var st store.Store = spy
			if !lister {
				st = listOnly{spy}
			}
			c := sweepCollector(t, st)
			var keep []string
			for i := range 3*retentionPage + 7 {
				seedSession(t, spy.Store, fmt.Sprintf("due_sealed_%04d", i), now.UnixNano(), old)
			}
			for i := range 20 {
				seedSession(t, spy.Store, fmt.Sprintf("due_open_%04d", i), stale, time.Time{})
				// Sealed in time, whatever the age of its events.
				keep = append(keep, fmt.Sprintf("keep_sealed_%04d", i), fmt.Sprintf("keep_open_%04d", i))
				seedSession(t, spy.Store, fmt.Sprintf("keep_sealed_%04d", i), stale, now.Add(-time.Minute))
				seedSession(t, spy.Store, fmt.Sprintf("keep_open_%04d", i), now.Add(-time.Minute).UnixNano(), time.Time{})
			}
			// A stale SDK clock, but the session's actor is alive.
			e := ev("a", core.KindAPIIn, 1, 1)
			e.SessionID, e.EndNs = "keep_live", stale
			if _, err := c.Ingest(context.Background(), "", []core.Event{e}); err != nil {
				panic(err)
			}
			keep = append(keep, "keep_live")
			slices.Sort(keep)

			n, err := c.PurgeExpired(context.Background(), now)
			if err != nil || n != 3*retentionPage+7+20 {
				panic(fmt.Sprintf("purged %d, err %v", n, err))
			}
			if got := sessionIDs(t, spy.Store); !slices.Equal(got, keep) {
				panic(fmt.Sprintf("survivors %v, want %v", got, keep))
			}
			if lister && (spy.lists.Load() != 0 || spy.pages.Load() < 4) {
				panic(fmt.Sprintf("lists %d, pages %d", spy.lists.Load(), spy.pages.Load()))
			}
			if n, err := c.PurgeExpired(context.Background(), now); err != nil || n != 0 {
				panic(fmt.Sprintf("second sweep purged %d, err %v", n, err))
			}
		})
	}
}

// Sweeps that are cut short drain a backlog over ticks, and sessions that
// cannot be deleted do not keep the ones behind them waiting: each sweep
// resumes after the last session the one before it tried.
func TestRetentionBacklogDrainsOverTicks(t *testing.T) {
	const due, stuck, perTick = 1000, 300, 100
	now := time.Now()
	spy := &sweepSpy{Store: store.NewMem(), badDel: map[string]bool{}, maxDel: perTick}
	c := sweepCollector(t, spy)
	for i := range due {
		id := fmt.Sprintf("due_%04d", i)
		seedSession(t, spy.Store, id, now.UnixNano(), now.Add(-2*time.Hour))
		// More than a page of undeletable sessions sort first.
		spy.badDel[id] = i < stuck
	}
	seedSession(t, spy.Store, "keep", now.UnixNano(), now)

	want := make([]string, 0, stuck+1)
	for i := range stuck {
		want = append(want, fmt.Sprintf("due_%04d", i))
	}
	want = append(want, "keep")
	// Every tick is cut off at its perTick-th delete, so the backlog needs at
	// least due/perTick of them.
	ticks, deleted := 0, 0
	for ; ticks < 4*due/perTick && !slices.Equal(sessionIDs(t, spy.Store), want); ticks++ {
		ctx, cancel := context.WithCancel(context.Background())
		spy.mu.Lock()
		spy.cancel = cancel
		spy.mu.Unlock()
		spy.deletes.Store(0)
		n, _ := c.PurgeExpired(ctx, now)
		cancel()
		if got := spy.deletes.Load(); got > perTick {
			panic(fmt.Sprintf("tick %d: %d deletes after the sweep was cut off at %d", ticks, got, perTick))
		}
		deleted += n
	}
	if got := sessionIDs(t, spy.Store); !slices.Equal(got, want) {
		panic(fmt.Sprintf("after %d ticks %d sessions are left, want the %d undeletable ones and keep", ticks, len(got), stuck))
	}
	if deleted != due-stuck {
		panic(fmt.Sprintf("deleted %d, want %d", deleted, due-stuck))
	}
	t.Logf("backlog of %d (%d undeletable) drained in %d ticks of %d deletes", due, stuck, ticks, perTick)

	// The undeletable ones are still tried, and go once the store recovers.
	spy.mu.Lock()
	spy.maxDel = 0
	clear(spy.badDel)
	spy.mu.Unlock()
	for range 2 { // the first may start mid-way and stop at the end
		if _, err := c.PurgeExpired(context.Background(), now); err != nil {
			panic(err)
		}
	}
	if got := sessionIDs(t, spy.Store); !slices.Equal(got, []string{"keep"}) {
		panic(fmt.Sprintf("left after the store recovered: %v", got))
	}
}

// A store that fails, or lists wrongly, costs one call a sweep and never a
// session that is not due.
func TestRetentionSweepFailingStore(t *testing.T) {
	now := time.Now()
	spy := &sweepSpy{Store: store.NewMem()}
	c := sweepCollector(t, spy)
	for i := range 2 * retentionPage {
		seedSession(t, spy.Store, fmt.Sprintf("due_%04d", i), now.UnixNano(), now.Add(-2*time.Hour))
		seedSession(t, spy.Store, fmt.Sprintf("keep_%04d", i), now.UnixNano(), now)
	}
	all := sessionIDs(t, spy.Store)
	sweep := func() (int, error) {
		spy.reset()
		return c.PurgeExpired(context.Background(), now)
	}

	spy.listErr = errors.New("store down")
	if n, err := sweep(); n != 0 || !errors.Is(err, spy.listErr) {
		panic(fmt.Sprintf("purged %d, err %v", n, err))
	}
	if spy.pages.Load() != 1 || spy.deletes.Load() != 0 {
		panic(fmt.Sprintf("failing list: %d list calls, %d deletes", spy.pages.Load(), spy.deletes.Load()))
	}
	spy.listErr = nil

	// A store that ignores the cursor would be asked for the same page forever.
	spy.override = func(time.Time, string, int) ([]store.SessionInfo, error) {
		l := make([]store.SessionInfo, retentionPage)
		for i := range l {
			l[i] = store.SessionInfo{ID: fmt.Sprintf("keep_%04d", i), Sealed: true}
		}
		return l, nil
	}
	if n, err := sweep(); n != 0 || err == nil {
		panic(fmt.Sprintf("purged %d, err %v", n, err))
	}
	if spy.pages.Load() > 2 || spy.deletes.Load() != 0 {
		panic(fmt.Sprintf("store ignoring the cursor: %d list calls, %d deletes", spy.pages.Load(), spy.deletes.Load()))
	}
	if got := sessionIDs(t, spy.Store); !slices.Equal(got, all) {
		panic(fmt.Sprintf("%d sessions left of %d", len(got), len(all)))
	}

	// A store that returns sessions that are not due: only the due ones go.
	// The sweep above stopped part of the way through, so this takes two.
	spy.override = func(_ time.Time, after string, limit int) ([]store.SessionInfo, error) {
		l, err := spy.Store.ListSessions(context.Background())
		i, _ := slices.BinarySearchFunc(l, after+"\x00", func(s store.SessionInfo, id string) int { return strings.Compare(s.ID, id) })
		return l[i:min(len(l), i+limit)], err
	}
	purged := 0
	for range 2 {
		n, err := sweep()
		if err != nil {
			panic(err)
		}
		purged += n
	}
	if purged != 2*retentionPage {
		panic(fmt.Sprintf("purged %d, want %d", purged, 2*retentionPage))
	}
	left := map[string]bool{}
	for _, id := range sessionIDs(t, spy.Store) {
		left[id] = true
	}
	for i := range 2 * retentionPage {
		delete(left, fmt.Sprintf("keep_%04d", i))
	}
	if len(left) != 0 || len(sessionIDs(t, spy.Store)) != 2*retentionPage {
		panic(fmt.Sprintf("wrong sessions survived: %v", slices.Collect(maps.Keys(left))))
	}
}

// A sweep that was cut short resumes behind the session it stopped at. The
// next sweep must still delete the due sessions that sort before that point,
// in the same call, not a tick later.
func TestRetentionSweepWrapsAfterResuming(t *testing.T) {
	now := time.Now()
	spy := &sweepSpy{Store: store.NewMem(), maxDel: 10}
	c := sweepCollector(t, spy)
	for i := range 40 {
		seedSession(t, spy.Store, fmt.Sprintf("m_%04d", i), now.UnixNano(), now.Add(-2*time.Hour))
	}
	ctx, cancel := context.WithCancel(context.Background())
	spy.cancel = cancel
	if n, _ := c.PurgeExpired(ctx, now); n != 10 {
		panic(fmt.Sprintf("test setup: the first sweep should stop after 10 deletes, deleted %d", n))
	}
	spy.mu.Lock()
	spy.maxDel = 0
	spy.mu.Unlock()
	for i := range 20 {
		seedSession(t, spy.Store, fmt.Sprintf("a_%04d", i), now.UnixNano(), now.Add(-2*time.Hour))
	}
	n, err := c.PurgeExpired(context.Background(), now)
	if left := sessionIDs(t, spy.Store); err != nil || n != 50 || len(left) != 0 {
		panic(fmt.Sprintf("a full sweep after a cut-short one deleted %d and left %v (%v), want 50 and none", n, left, err))
	}
}
