package collector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A graceful stop must persist what is still inside the reorder window.
func TestCloseFlushesReorderWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = time.Hour // nothing would flush on its own
	st := store.NewMem()
	c := New(st, cfg)
	ctx := context.Background()
	for i, id := range []string{"s1", "s2"} {
		a, b := ev("b", core.KindAPIIn, 20, uint64(2+10*i)), ev("a", core.KindAPIIn, 10, uint64(1+10*i))
		a.SessionID, b.SessionID = id, id
		if r, err := c.Ingest(ctx, "", []core.Event{a, b}); err != nil || r.Accepted != 2 {
			t.Fatalf("ingest %s: %+v %v", id, r, err)
		}
		if h, _ := st.Head(ctx, id); h != 0 {
			t.Fatalf("%s flushed before Close (head %d)", id, h)
		}
	}
	c.Close()
	for _, id := range []string{"s1", "s2"} {
		evs, err := st.Events(ctx, id, 0, 0)
		if err != nil || len(evs) != 2 {
			t.Fatalf("%s after Close: %d events, err %v", id, len(evs), err)
		}
		if evs[0].SpanID != "a" || evs[0].Seq != 1 || evs[1].Seq != 2 {
			t.Fatalf("%s not sequenced in HLC order: %+v", id, evs)
		}
	}
	// A closed collector refuses work instead of panicking or hanging.
	if _, err := c.Ingest(ctx, "", []core.Event{ev("z", core.KindAPIIn, 99, 99)}); !errors.Is(err, errSeqClosed) {
		t.Fatalf("ingest after Close: %v", err)
	}
	if _, err := c.Seal(ctx, sid); !errors.Is(err, errSeqClosed) {
		t.Fatalf("seal after Close: %v", err)
	}
	c.Close() // idempotent
}

// Ingest racing with Close must never panic (it used to send on a closed
// channel) and every event reported accepted must be in the store.
func TestCloseRacesWithIngest(t *testing.T) {
	for round := 0; round < 20; round++ {
		cfg := DefaultConfig()
		cfg.Window = time.Millisecond
		st := store.NewMem()
		c := New(st, cfg)
		ctx := context.Background()
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			accepted = map[string]int{}
		)
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				id := fmt.Sprintf("s%d", w%4)
				for i := 0; i < 200; i++ {
					e := ev("x", core.KindAPIIn, uint64(i+1), uint64(i+1))
					e.SessionID, e.Instance = id, fmt.Sprintf("i%d", w)
					r, err := c.Ingest(ctx, "", []core.Event{e})
					if err != nil {
						if !errors.Is(err, errSeqClosed) {
							t.Errorf("ingest: %v", err)
						}
						return
					}
					mu.Lock()
					accepted[id] += r.Accepted
					mu.Unlock()
				}
			}(w)
		}
		time.Sleep(time.Duration(round%5) * time.Millisecond)
		c.Close()
		wg.Wait()
		for id, n := range accepted {
			if h, _ := st.Head(ctx, id); int(h) != n {
				t.Fatalf("round %d: session %s accepted %d events but stored %d", round, id, n, h)
			}
		}
	}
}

// A store that hangs must not wedge the session forever: the background
// append is bounded by StoreTimeout and the events stay buffered for a retry.
func TestHungStoreIsBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.StoreTimeout = 30 * time.Millisecond
	hs := &hangStore{Store: store.NewMem()}
	hs.hang.Store(true)
	c := New(hs, cfg)
	defer c.Close()
	done := make(chan error, 1)
	go func() {
		_, err := c.Ingest(context.Background(), "", []core.Event{ev("a", core.KindAPIIn, 1, 1)})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want deadline exceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ingest hung on a hung store")
	}
	hs.hang.Store(false)
	waitFor(t, "retry after the store recovers", func() bool { return len(events(t, c)) == 1 })
}
