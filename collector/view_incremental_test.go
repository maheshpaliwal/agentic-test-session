package collector

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// readCounter counts the events every Events call returns.
type readCounter struct {
	store.Store
	read atomic.Int64
}

func (r *readCounter) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	evs, err := r.Store.Events(ctx, id, from, limit)
	r.read.Add(int64(len(evs)))
	return evs, err
}

// Polling a growing session must read each stored event about once, not the
// whole session on every poll.
func TestViewGrowsIncrementally(t *testing.T) {
	st := &readCounter{Store: store.NewMem()}
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(st, cfg)
	t.Cleanup(c.Close)
	ctx := context.Background()

	const rounds, perRound = 20, 10
	local := uint64(0)
	for r := 0; r < rounds; r++ {
		evs := make([]core.Event, perRound)
		for i := range evs {
			local++
			evs[i] = ev(fmt.Sprintf("s%d", local), core.KindDBRead, 100+local, local)
		}
		if _, err := c.Ingest(ctx, "i1", evs); err != nil {
			t.Fatal(err)
		}
		v, err := c.viewOf(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if want := (r + 1) * perRound; len(v.evs) != want || v.head != uint64(want) {
			t.Fatalf("round %d: view has %d events, head %d, want %d", r, len(v.evs), v.head, want)
		}
		for i, e := range v.evs {
			if e.Seq != uint64(i+1) {
				t.Fatalf("round %d: event %d has seq %d", r, i, e.Seq)
			}
		}
	}
	total := int64(rounds * perRound)
	// One initial load by the actor plus one pass by the view; a full re-read
	// per poll would be rounds*(rounds+1)/2*perRound = 2100.
	if got := st.read.Load(); got > 2*total+perRound {
		t.Fatalf("read %d events from the store for a session of %d", got, total)
	}
}

// A transaction that commits after its writes were already in the cached view
// must still mark those earlier writes effective.
func TestIncrementalViewResolvesEarlierEvents(t *testing.T) {
	c, _ := windowed(t, 0)
	ctx := context.Background()
	w := ev("w", core.KindDBWrite, 100, 1)
	w.TxID = "tx1"
	w.Mutations = []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert"}}
	ingest(t, c, w)
	v, err := c.viewOf(ctx, sid)
	if err != nil || v.evs[0].Mutations[0].Effective != nil {
		t.Fatalf("open transaction must be unresolved: %+v %v", v.evs, err)
	}
	commit := ev("c", core.KindDBTxCommit, 200, 2)
	commit.TxID = "tx1"
	ingest(t, c, commit)
	full, err := c.st.Events(ctx, sid, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := resolve(full, nil, false, nil)[0].Mutations[0].Effective
	v, err = c.viewOf(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	got := v.evs[0].Mutations[0].Effective
	if (got == nil) != (want == nil) || (got != nil && *got != *want) {
		t.Fatalf("incremental view resolved the write differently from a full read: got %v want %v", got, want)
	}
	if want == nil {
		t.Fatal("test setup: the commit event did not resolve the transaction")
	}
}

// The status of a session without a live actor is answered from memory while
// the store is unchanged, and refreshed when it grows or the session is deleted.
func TestStatusOfEvictedSessionIsMemoised(t *testing.T) {
	st := &readCounter{Store: store.NewMem()}
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(st, cfg)
	t.Cleanup(c.Close)
	ctx := context.Background()
	pub := ev("p", core.KindQueuePub, 100, 1)
	if _, err := c.Ingest(ctx, "i1", []core.Event{pub, ev("r", core.KindDBRead, 101, 2)}); err != nil {
		t.Fatal(err)
	}
	c.seq.evictAll()

	base := st.read.Load()
	for i := 0; i < 10; i++ {
		s, err := c.seq.Status(ctx, sid)
		if err != nil || s.EventCount != 2 || len(s.Open) != 1 || s.Open[0] != "p" {
			t.Fatalf("status: %+v %v", s, err)
		}
	}
	if got := st.read.Load() - base; got != 2 {
		t.Fatalf("10 status polls read %d events, want 2 (one load)", got)
	}

	// Growth behind the memo (written straight to the store) is noticed.
	if err := st.AppendEvents(ctx, sid, []core.Event{func() core.Event { e := ev("x", core.KindDBRead, 102, 3); e.Seq = 3; return e }()}); err != nil {
		t.Fatal(err)
	}
	if s, err := c.seq.Status(ctx, sid); err != nil || s.EventCount != 3 {
		t.Fatalf("status after growth: %+v %v", s, err)
	}
	if err := c.Delete(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if s, err := c.seq.Status(ctx, sid); err != nil || s.EventCount != 0 || len(s.Open) != 0 {
		t.Fatalf("status after delete: %+v %v", s, err)
	}
}
