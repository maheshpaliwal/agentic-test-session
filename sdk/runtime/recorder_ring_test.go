package runtime

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// heldSessions returns the sessions whose events a full ring drops first.
func heldSessions(r *Recorder) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(maps.Keys(r.held))
}

// bufferedOf counts the session's events in the ring.
func bufferedOf(r *Recorder, sid string) (n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.n {
		if r.at(i).ev.SessionID == sid {
			n++
		}
	}
	return n
}

// take and settle against a plain slice: events leave in any combination and
// the rest stays in the order it was recorded, through wrap-around.
func TestRecorderTakeSettleMatchesModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	r := NewRecorder(16)
	var model []uint64 // local_seq in the ring, oldest first
	sids := []string{"a", "b", "c"}
	seq := uint64(0)
	for step := range 5000 {
		switch rng.Intn(3) {
		case 0:
			for range rng.Intn(6) {
				seq++
				r.Record(core.Event{SessionID: sids[rng.Intn(3)], LocalSeq: seq})
				if len(model) < 16 {
					model = append(model, seq)
				}
			}
		default:
			skip := sids[rng.Intn(3)]
			evs, ids, _ := r.take(0, 1+rng.Intn(8), func(sid string) bool { return sid != skip })
			var gone []uint64
			for i, ev := range evs {
				if ev.SessionID == skip {
					panic(fmt.Sprintf("step %d: take returned an event of a session it was to skip", step))
				}
				if rng.Intn(3) != 0 {
					gone = append(gone, ids[i])
					model = slices.DeleteFunc(model, func(s uint64) bool { return s == ev.LocalSeq })
				}
			}
			r.settle(ids, gone)
		}
		got := []uint64{}
		for _, ev := range r.Peek(16) {
			got = append(got, ev.LocalSeq)
		}
		if !slices.Equal(got, model) && len(got)+len(model) > 0 {
			panic(fmt.Sprintf("step %d: ring holds %v, want %v", step, got, model))
		}
	}
}

// What a full ring drops: the newest event of a held session that was never
// sent, for an event of another session; the new event itself for a held
// session, with nothing held, and when no such event is near the end.
func TestRecorderEviction(t *testing.T) {
	ev := func(sid string, seq uint64) core.Event { return core.Event{SessionID: sid, LocalSeq: seq} }
	seqs := func(r *Recorder) (out []uint64) {
		for _, e := range r.Peek(len(r.buf)) {
			out = append(out, e.LocalSeq)
		}
		return out
	}
	drops := func(r *Recorder) map[string]uint64 {
		r.mu.Lock()
		defer r.mu.Unlock()
		return maps.Clone(r.drops)
	}
	r := NewRecorder(4)
	for i, sid := range []string{"held", "ok", "held", "held"} {
		r.Record(ev(sid, uint64(i+1)))
	}
	r.Record(ev("ok", 5)) // nothing is held: the new event is dropped
	if got := seqs(r); !slices.Equal(got, []uint64{1, 2, 3, 4}) || drops(r)["ok"] != 1 {
		panic(fmt.Sprintf("nothing held: ring %v drops %v", got, drops(r)))
	}
	r.setHeld(map[string]struct{}{"held": {}})
	r.Record(ev("held", 6)) // a held session never evicts
	if got := seqs(r); !slices.Equal(got, []uint64{1, 2, 3, 4}) || drops(r)["held"] != 1 {
		panic(fmt.Sprintf("event of a held session: ring %v drops %v", got, drops(r)))
	}
	r.Record(ev("ok", 7)) // evicts 4, the newest held event
	if got := seqs(r); !slices.Equal(got, []uint64{1, 2, 3, 7}) || drops(r)["held"] != 2 {
		panic(fmt.Sprintf("eviction: ring %v drops %v", got, drops(r)))
	}
	// 3 was sent, so the collector may have it: the next eviction takes 1
	// instead, and with only 3 left the new event is dropped, also after the
	// request is over.
	_, ids, _ := r.take(3, 1, func(sid string) bool { return sid == "held" })
	r.Record(ev("ok", 8))
	if got := seqs(r); !slices.Equal(got, []uint64{2, 3, 7, 8}) || drops(r)["held"] != 3 {
		panic(fmt.Sprintf("eviction around a sent event: ring %v drops %v", got, drops(r)))
	}
	r.settle(ids, nil)
	r.Record(ev("ok", 9))
	if got := seqs(r); !slices.Equal(got, []uint64{2, 3, 7, 8}) || drops(r)["ok"] != 2 {
		panic(fmt.Sprintf("only a sent event left: ring %v drops %v", got, drops(r)))
	}

	// A held event further from the end than evictScan is not looked for.
	big := NewRecorder(evictScan + 8)
	big.setHeld(map[string]struct{}{"held": {}})
	big.Record(ev("held", 1))
	for i := range evictScan + 7 {
		big.Record(ev("ok", uint64(i+2)))
	}
	big.Record(ev("ok", 1<<20))
	if d := drops(big); d["ok"] != 1 || d["held"] != 0 {
		panic(fmt.Sprintf("held event out of reach: drops %v", d))
	}
}
