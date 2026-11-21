package collector

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func spansOf(evs []core.Event) []string {
	out := make([]string, len(evs))
	for i := range evs {
		out[i] = fmt.Sprintf("%d:%s", evs[i].Seq, evs[i].SpanID)
	}
	return out
}

// gateStore blocks the first full-session Events read (after it has read) once armed.
type gateStore struct {
	store.Store
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *gateStore) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	evs, err := g.Store.Events(ctx, id, from, limit)
	if g.armed.CompareAndSwap(true, false) {
		close(g.reached)
		<-g.release
	}
	return evs, err
}

// A read that races with Delete re-inserts the deleted session's view; the
// next view of the re-created session is "extended" from it.
func TestViewOfDeletedSessionIsNotReusedAfterRacingRead(t *testing.T) {
	g := &gateStore{Store: store.NewMem(), reached: make(chan struct{}), release: make(chan struct{})}
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(g, cfg)
	t.Cleanup(c.Close)
	ctx := context.Background()
	ingest(t, c, ev("old1", core.KindDBRead, 100, 1), ev("old2", core.KindDBRead, 101, 2), ev("old3", core.KindDBRead, 102, 3))

	g.armed.Store(true)
	done := make(chan struct{})
	go func() { defer close(done); c.viewOf(ctx, sid) }()
	<-g.reached // the read has the old events in hand
	if err := c.Delete(ctx, sid); err != nil {
		panic(err)
	}
	close(g.release)
	<-done

	// Late events for the id start a fresh session at Seq 1 (documented).
	ingest(t, c, ev("new1", core.KindDBRead, 200, 11), ev("new2", core.KindDBRead, 201, 12),
		ev("new3", core.KindDBRead, 202, 13), ev("new4", core.KindDBRead, 203, 14))
	stored, _ := g.Store.Events(ctx, sid, 0, 0)
	v, err := c.viewOf(ctx, sid)
	if err != nil {
		panic(err)
	}
	if fmt.Sprint(spansOf(v.evs)) != fmt.Sprint(spansOf(stored)) {
		panic(fmt.Sprintf("view  = %v\nstore = %v", spansOf(v.evs), spansOf(stored)))
	}
}

// Same outcome without any race when the session is deleted behind this
// collector's back (a second collector on the same store; the case the
// actor's resync handles).
func TestViewIsNotExtendedAcrossOutOfBandDelete(t *testing.T) {
	c, st := windowed(t, 0)
	ctx := context.Background()
	ingest(t, c, ev("old1", core.KindDBRead, 100, 1), ev("old2", core.KindDBRead, 101, 2), ev("old3", core.KindDBRead, 102, 3))
	if _, err := c.viewOf(ctx, sid); err != nil {
		panic(err)
	}
	if err := st.DeleteSession(ctx, sid); err != nil { // the other collector
		panic(err)
	}
	ingest(t, c, ev("new1", core.KindDBRead, 200, 11), ev("new2", core.KindDBRead, 201, 12),
		ev("new3", core.KindDBRead, 202, 13), ev("new4", core.KindDBRead, 203, 14))
	stored, _ := st.Events(ctx, sid, 0, 0)
	v, err := c.viewOf(ctx, sid)
	if err != nil {
		panic(err)
	}
	if fmt.Sprint(spansOf(v.evs)) != fmt.Sprint(spansOf(stored)) {
		panic(fmt.Sprintf("view  = %v\nstore = %v", spansOf(v.evs), spansOf(stored)))
	}
}

// flaky fails appends at random with a timeout-like error (never quarantined).
type flaky struct {
	store.Store
	mu   sync.Mutex
	rng  *rand.Rand
	rate int
	off  atomic.Bool
}

func (f *flaky) AppendEvents(ctx context.Context, id string, evs []core.Event) error {
	if !f.off.Load() {
		f.mu.Lock()
		fail := f.rng.Intn(100) < f.rate
		f.mu.Unlock()
		if fail {
			return fmt.Errorf("flaky: %w", context.DeadlineExceeded)
		}
	}
	return f.Store.AppendEvents(ctx, id, evs)
}

// Concurrent ingest, reads and a flaky store: nothing lost, nothing stored
// twice, Seq gapless, views always equal to a prefix of the store.
func TestWindowStress(t *testing.T) {
	f := &flaky{Store: store.NewMem(), rng: rand.New(rand.NewSource(1)), rate: 40}
	cfg := DefaultConfig()
	cfg.Window = 15 * time.Millisecond
	cfg.IdleEvict = 20 * time.Millisecond
	c := New(f, cfg)
	t.Cleanup(c.Close)
	ctx := context.Background()
	const writers, per = 6, 300
	var wg sync.WaitGroup
	var hlc atomic.Uint64
	stop := make(chan struct{})
	var bad atomic.Value
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, err := c.viewOf(ctx, sid)
				if err == nil {
					for i := range v.evs {
						if v.evs[i].Seq != uint64(i+1) {
							bad.Store(fmt.Sprintf("view index %d has seq %d (len %d head %d)", i, v.evs[i].Seq, len(v.evs), v.head))
							return
						}
					}
				}
				c.seq.Status(ctx, sid)
				time.Sleep(time.Millisecond)
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < writers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			inst := fmt.Sprintf("i%d", w)
			for i := 1; i <= per; {
				n := 1 + rand.Intn(7)
				var evs []core.Event
				for j := 0; j < n && i <= per; j, i = j+1, i+1 {
					e := ev(fmt.Sprintf("%s-%d", inst, i), core.KindDBRead, hlc.Add(1), uint64(i))
					e.Instance = inst
					evs = append(evs, e)
				}
				for {
					_, err := c.Ingest(ctx, inst, evs)
					if err == nil {
						break
					}
					if errors.Is(err, errBackpressure) {
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("ingest: %v", err)
					return
				}
				if rand.Intn(4) == 0 { // resend: must be deduplicated
					c.Ingest(ctx, inst, evs)
				}
				time.Sleep(time.Duration(rand.Intn(4)) * time.Millisecond)
			}
		}(w)
	}
	ww.Wait()
	f.off.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for {
		h, _ := f.Store.Head(ctx, sid)
		if h == writers*per && c.seq.buffered() == 0 {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("stored %d of %d, buffered %d", h, writers*per, c.seq.buffered()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if b := bad.Load(); b != nil {
		panic(b)
	}
	evs, _ := f.Store.Events(ctx, sid, 0, 0)
	seen := map[string]bool{}
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			panic(fmt.Sprintf("gap at %d: %d", i, e.Seq))
		}
		if seen[e.SpanID] {
			panic(fmt.Sprintf("duplicate %s", e.SpanID))
		}
		seen[e.SpanID] = true
	}
	if len(evs) != writers*per {
		panic(fmt.Sprintf("stored %d, want %d", len(evs), writers*per))
	}
	v, _ := c.viewOf(ctx, sid)
	if fmt.Sprint(spansOf(v.evs)) != fmt.Sprint(spansOf(evs)) {
		panic("final view differs from store")
	}
	// the actor must eventually be evicted (timer/goroutine not leaked)
	waitFor(t, "actor eviction", func() bool { return c.seq.actorCount() == 0 })
}
