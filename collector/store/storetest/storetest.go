// Package storetest holds the conformance suite shared by every store.Store backend.
package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Run is the backend conformance suite: every store.Store implementation must pass it.
func Run(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()
	sid := "ts_store_" + time.Now().Format("150405.000000")
	mk := func(seq uint64, kind core.Kind, tx string) core.Event {
		return core.Event{SessionID: sid, SpanID: "s", Instance: "i", LocalSeq: seq, Seq: seq, Kind: kind, TxID: tx,
			Service: "svc", EndHLC: seq, StartNs: int64(seq), EndNs: int64(seq) + 1,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "t", PK: map[string]any{"id": 1}, Op: "update"}}}
	}
	if err := s.AppendEvents(ctx, sid, []core.Event{mk(1, core.KindDBWrite, "t1"), mk(2, core.KindDBTxCommit, "t1")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents(ctx, sid, []core.Event{mk(4, core.KindDBWrite, "")}); err == nil {
		t.Fatal("expected seq conflict")
	}
	got, err := s.Events(ctx, sid, 2, 0)
	if err != nil || len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("events: %v %v", got, err)
	}
	if o, _ := s.TxOutcomes(ctx, sid); !o["t1"] {
		t.Fatalf("outcomes %v", o)
	}
	if h, err := s.Head(ctx, sid); err != nil || h != 2 {
		t.Fatalf("head %d %v", h, err)
	}
	if h, err := s.Head(ctx, sid+"_none"); err != nil || h != 0 {
		t.Fatalf("head of unknown %d %v", h, err)
	}
	// failed commits (Err / commit_failed) and rollbacks are not effective
	bad := mk(3, core.KindDBTxCommit, "t2")
	bad.Err = "commit failed"
	cf := mk(4, core.KindDBTxCommit, "t3")
	cf.Attrs = map[string]string{"commit_failed": "true"}
	rb := mk(5, core.KindDBTxRollbck, "t4")
	rb.Attrs = map[string]string{"commit_failed": "true"}
	// a NUL escape in a body/column must not break the backend
	nul := mk(6, core.KindDBRead, "")
	nul.Name = "a\x00b"
	nul.Req = core.PayloadRef{Inline: []byte(`{"k":"v\u0000w"}`)}
	if err := s.AppendEvents(ctx, sid, []core.Event{bad, cf, rb, nul}); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.TxOutcomes(ctx, sid); !o["t1"] || o["t2"] || o["t3"] || o["t4"] || len(o) != 4 {
		t.Fatalf("outcomes %v", o)
	}
	if si, _ := s.GetSeal(ctx, sid); si != nil {
		t.Fatal("unexpected seal")
	}
	if err := s.PutSeal(ctx, sid, store.SealInfo{Orphaned: []string{"x"}, BundleURI: "u", SealedAt: time.Now(), Quarantined: 3}); err != nil {
		t.Fatal(err)
	}
	if si, _ := s.GetSeal(ctx, sid); si == nil || si.BundleURI != "u" || len(si.Orphaned) != 1 || si.Quarantined != 3 {
		t.Fatalf("seal %+v", si)
	}
	l, _ := s.ListSessions(ctx)
	found := false
	for _, x := range l {
		if x.ID == sid && x.EventCount == 6 && x.Sealed {
			found = true
		}
	}
	if !found {
		t.Fatalf("list %+v", l)
	}

	// DeleteSession removes events and seal, leaves other sessions alone, and
	// is idempotent. The id is then free for a fresh run starting at Seq 1.
	other := sid + "_other"
	oe := mk(1, core.KindDBWrite, "")
	oe.SessionID = other
	if err := s.AppendEvents(ctx, other, []core.Event{oe}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.DeleteSession(ctx, sid); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	if evs, err := s.Events(ctx, sid, 0, 0); err != nil || len(evs) != 0 {
		t.Fatalf("events after delete: %d %v", len(evs), err)
	}
	if h, err := s.Head(ctx, sid); err != nil || h != 0 {
		t.Fatalf("head after delete %d %v", h, err)
	}
	if si, err := s.GetSeal(ctx, sid); err != nil || si != nil {
		t.Fatalf("seal after delete %+v %v", si, err)
	}
	if o, err := s.TxOutcomes(ctx, sid); err != nil || len(o) != 0 {
		t.Fatalf("outcomes after delete %v %v", o, err)
	}
	l, _ = s.ListSessions(ctx)
	sawOther := false
	for _, x := range l {
		if x.ID == sid {
			t.Fatalf("deleted session still listed: %+v", x)
		}
		sawOther = sawOther || x.ID == other
	}
	if !sawOther {
		t.Fatalf("delete removed an unrelated session: %+v", l)
	}
	if err := s.AppendEvents(ctx, sid, []core.Event{mk(1, core.KindDBWrite, "")}); err != nil {
		t.Fatalf("append after delete: %v", err)
	}
	for _, id := range []string{sid, other} {
		if err := s.DeleteSession(ctx, id); err != nil {
			t.Fatalf("cleanup %s: %v", id, err)
		}
	}
}
