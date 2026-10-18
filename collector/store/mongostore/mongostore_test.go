package mongostore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/collector/store/storetest"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	uri := os.Getenv("TSESSION_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("TSESSION_TEST_MONGO_URI not set")
	}
	// A database per test keeps ListSessions assertions exact.
	db := fmt.Sprintf("tstest_%d_%d", os.Getpid(), time.Now().UnixNano())
	s, err := Open(context.Background(), Config{URI: uri, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.events.Database().Drop(context.Background()) //nolint:errcheck
		s.Close()
	})
	return s
}

func mk(sid string, seq uint64) core.Event {
	return core.Event{SessionID: sid, SpanID: fmt.Sprintf("sp%d", seq), Instance: "i", LocalSeq: seq, Seq: seq,
		Kind: core.KindDBWrite, Service: "svc", EndHLC: seq, StartNs: int64(seq) * 10, EndNs: int64(seq)*10 + 5}
}

func TestMongoStoreConformance(t *testing.T) {
	storetest.Run(t, openTest(t))
}

func TestMongoStoreSeqContinuity(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.AppendEvents(ctx, "s", []core.Event{mk("s", 2)}); !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("first seq must be 1: %v", err)
	}
	if err := s.AppendEvents(ctx, "s", []core.Event{mk("s", 1), mk("s", 2), mk("s", 3)}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents(ctx, "s", []core.Event{mk("s", 3)}); !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("replay: %v", err)
	}
	if err := s.AppendEvents(ctx, "s", []core.Event{mk("s", 4), mk("s", 6)}); !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("gap in batch: %v", err)
	}
	if h, _ := s.Head(ctx, "s"); h != 3 {
		t.Fatalf("rejected batch must store nothing, head=%d", h)
	}
	if err := s.AppendEvents(ctx, "s", nil); err != nil {
		t.Fatal(err)
	}
	// reopen keeps continuity and index creation is idempotent
	s2, err := Open(ctx, Config{URI: os.Getenv("TSESSION_TEST_MONGO_URI"), Database: s.events.Database().Name()})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.AppendEvents(ctx, "s", []core.Event{mk("s", 4), mk("s", 5)}); err != nil {
		t.Fatal(err)
	}
}

func TestMongoStoreFromSeqAndLimit(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	var evs []core.Event
	for i := uint64(1); i <= 10; i++ {
		evs = append(evs, mk("s", i))
	}
	if err := s.AppendEvents(ctx, "s", evs); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		from  uint64
		limit int
		first uint64
		n     int
	}{{0, 0, 1, 10}, {1, 0, 1, 10}, {4, 0, 4, 7}, {4, 3, 4, 3}, {9, 5, 9, 2}, {11, 0, 0, 0}, {0, 2, 1, 2}} {
		got, err := s.Events(ctx, "s", c.from, c.limit)
		if err != nil || len(got) != c.n || (c.n > 0 && got[0].Seq != c.first) {
			t.Fatalf("from=%d limit=%d: n=%d err=%v", c.from, c.limit, len(got), err)
		}
		for i := 1; i < len(got); i++ {
			if got[i].Seq != got[i-1].Seq+1 {
				t.Fatalf("not ascending: %v", got)
			}
		}
	}
	if got, err := s.Events(ctx, "nope", 0, 0); err != nil || len(got) != 0 {
		t.Fatalf("unknown session: %v %v", got, err)
	}
}

func TestMongoStoreConcurrentSameSeq(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	for round := uint64(1); round <= 20; round++ {
		const writers = 8
		var wins, conflicts atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				e := mk("c", round)
				e.Instance = fmt.Sprintf("w%d", w) // distinct, so only the Seq index can arbitrate
				<-start
				switch err := s.AppendEvents(ctx, "c", []core.Event{e}); {
				case err == nil:
					wins.Add(1)
				case errors.Is(err, store.ErrSeqConflict):
					conflicts.Add(1)
				default:
					t.Errorf("append: %v", err)
				}
			}(w)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 || conflicts.Load() != writers-1 {
			t.Fatalf("round %d: wins=%d conflicts=%d", round, wins.Load(), conflicts.Load())
		}
	}
	got, _ := s.Events(ctx, "c", 0, 0)
	if len(got) != 20 {
		t.Fatalf("events=%d", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("gap at %d", i)
		}
	}
}

// A duplicate (instance, local_seq) midway leaves the earlier events stored:
// the prefix is a valid run and the loser reports ErrSeqConflict.
func TestMongoStorePartialBatchIsGaplessPrefix(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.AppendEvents(ctx, "p", []core.Event{mk("p", 1)}); err != nil {
		t.Fatal(err)
	}
	dup := mk("p", 3)
	dup.LocalSeq = 1 // collides with seq 1's (instance, local_seq)
	err := s.AppendEvents(ctx, "p", []core.Event{mk("p", 2), dup, mk("p", 4)})
	if !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	h, _ := s.Head(ctx, "p")
	got, _ := s.Events(ctx, "p", 0, 0)
	if h != 2 || len(got) != 2 || got[1].Seq != 2 {
		t.Fatalf("head=%d events=%d", h, len(got))
	}
	if err := s.AppendEvents(ctx, "p", []core.Event{mk("p", 3)}); err != nil {
		t.Fatalf("continuing after the prefix: %v", err)
	}
}

func TestMongoStoreUnsafeKeysRoundTrip(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	e := mk("k", 1)
	e.Attrs = map[string]string{"a.b": "1", "$x": "2", "": "3", "ünï": "4"}
	e.Mutations = []core.Mutation{{Store: "mongo", Entity: "t", Op: "update",
		PK: map[string]any{"a.b": "v", "$set": float64(7), "nested": map[string]any{"$gt": "x", "c.d": true}}}}
	e.Req = core.PayloadRef{Inline: []byte(`{"$where":"1","a.b":{"$x":null}}`)}
	if err := s.AppendEvents(ctx, "k", []core.Event{e}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Events(ctx, "k", 0, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %v", got, err)
	}
	g := got[0]
	if g.Attrs["a.b"] != "1" || g.Attrs["$x"] != "2" || g.Attrs[""] != "3" || g.Attrs["ünï"] != "4" {
		t.Fatalf("attrs %v", g.Attrs)
	}
	pk := g.Mutations[0].PK
	if pk["a.b"] != "v" || pk["$set"] != float64(7) || pk["nested"].(map[string]any)["c.d"] != true {
		t.Fatalf("pk %v", pk)
	}
	if !bytes.Equal(g.Req.Inline, e.Req.Inline) {
		t.Fatalf("payload %s", g.Req.Inline)
	}
}

func TestMongoStoreLargeEvent(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	big := mk("l", 1)
	big.Req = core.PayloadRef{Inline: []byte(`"` + strings.Repeat("x", 3<<20) + `"`)}
	if err := s.AppendEvents(ctx, "l", []core.Event{big, mk("l", 2)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Events(ctx, "l", 1, 1)
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Req.Inline, big.Req.Inline) {
		t.Fatalf("3MB round trip failed: %v", err)
	}
	huge := mk("l", 3)
	huge.Req = core.PayloadRef{Inline: []byte(`"` + strings.Repeat("x", MaxEventBytes) + `"`)}
	err = s.AppendEvents(ctx, "l", []core.Event{mk("l", 3), huge})
	if !errors.Is(err, ErrEventTooLarge) || errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("want ErrEventTooLarge, got %v", err)
	}
	if h, _ := s.Head(ctx, "l"); h != 2 {
		t.Fatalf("oversize batch must store nothing, head=%d", h)
	}
}

func TestMongoStoreListSessionsAndSeal(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	for _, sid := range []string{"b", "a"} {
		var evs []core.Event
		for i := uint64(1); i <= 3; i++ {
			evs = append(evs, mk(sid, i))
		}
		if err := s.AppendEvents(ctx, sid, evs); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	for i := 0; i < 3; i++ { // idempotent
		if err := s.PutSeal(ctx, "a", store.SealInfo{BundleURI: "u", SealedAt: at, Quarantined: 2}); err != nil {
			t.Fatal(err)
		}
	}
	l, err := s.ListSessions(ctx)
	if err != nil || len(l) != 2 {
		t.Fatalf("%+v %v", l, err)
	}
	want := []store.SessionInfo{{ID: "a", EventCount: 3, FirstNs: 10, LastNs: 35, Sealed: true}, {ID: "b", EventCount: 3, FirstNs: 10, LastNs: 35}}
	for i := range want {
		if l[i] != want[i] {
			t.Fatalf("session %d: %+v want %+v", i, l[i], want[i])
		}
	}
	si, err := s.GetSeal(ctx, "a")
	if err != nil || si == nil || !si.SealedAt.Equal(at) || si.Quarantined != 2 {
		t.Fatalf("seal %+v %v", si, err)
	}
	if si, err := s.GetSeal(ctx, "b"); si != nil || err != nil {
		t.Fatalf("unsealed: %+v %v", si, err)
	}
	// overwrite
	if err := s.PutSeal(ctx, "a", store.SealInfo{Incomplete: true, Orphaned: []string{"x", "y"}, SealedAt: at}); err != nil {
		t.Fatal(err)
	}
	if si, _ := s.GetSeal(ctx, "a"); !si.Incomplete || len(si.Orphaned) != 2 || si.Quarantined != 0 {
		t.Fatalf("overwrite %+v", si)
	}
}

func TestMongoStoreTxOutcomesSticky(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	rb := mk("t", 1)
	rb.Kind, rb.TxID = core.KindDBTxRollbck, "x"
	cm := mk("t", 2)
	cm.Kind, cm.TxID = core.KindDBTxCommit, "x"
	ok := mk("t", 3)
	ok.Kind, ok.TxID = core.KindDBTxCommit, "y"
	if err := s.AppendEvents(ctx, "t", []core.Event{rb, cm, ok}); err != nil {
		t.Fatal(err)
	}
	o, err := s.TxOutcomes(ctx, "t")
	if err != nil || o["x"] || !o["y"] || len(o) != 2 {
		t.Fatalf("%v %v", o, err)
	}
}

func TestMongoStoreContextDeadline(t *testing.T) {
	s := openTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Head(ctx, "s"); err == nil {
		t.Fatal("cancelled context must fail")
	}
}

func TestMongoStoreOpenErrorsHideURI(t *testing.T) {
	uri := "mongodb://user:s3cretpw@127.0.0.1:1/?serverSelectionTimeoutMS=200"
	_, err := Open(context.Background(), Config{URI: uri, ConnectTimeout: 300 * time.Millisecond})
	if err == nil || strings.Contains(err.Error(), "s3cretpw") {
		t.Fatalf("err=%v", err)
	}
	if _, err := Open(context.Background(), Config{URI: "mongodb://user:s3cretpw@[bad", ConnectTimeout: 300 * time.Millisecond}); err == nil || strings.Contains(err.Error(), "s3cretpw") {
		t.Fatalf("err=%v", err)
	}
}
