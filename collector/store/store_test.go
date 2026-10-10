package store

import (
	"context"
	"os"
	"testing"
	"time"

	"tsession/core"
)

func exercise(t *testing.T, s Store) {
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
	if si, _ := s.GetSeal(ctx, sid); si != nil {
		t.Fatal("unexpected seal")
	}
	if err := s.PutSeal(ctx, sid, SealInfo{Orphaned: []string{"x"}, BundleURI: "u", SealedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if si, _ := s.GetSeal(ctx, sid); si == nil || si.BundleURI != "u" || len(si.Orphaned) != 1 {
		t.Fatalf("seal %+v", si)
	}
	l, _ := s.ListSessions(ctx)
	found := false
	for _, x := range l {
		if x.ID == sid && x.EventCount == 2 && x.Sealed {
			found = true
		}
	}
	if !found {
		t.Fatalf("list %+v", l)
	}
}

func TestMem(t *testing.T) { exercise(t, NewMem()) }

func TestPG(t *testing.T) {
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	pg, err := OpenPG(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	exercise(t, pg)
}
