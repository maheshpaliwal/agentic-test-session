package dynamostore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/collector/store/storetest"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

const endpointEnv = "TSESSION_TEST_DYNAMO_ENDPOINT"

var tableSeq atomic.Int64

// backend is one Store under test: the fake client always, and DynamoDB
// Local when TSESSION_TEST_DYNAMO_ENDPOINT is set.
type backend struct {
	name string
	open func(t *testing.T) *Store
}

func backends() []backend {
	bs := []backend{{"fake", func(t *testing.T) *Store {
		t.Helper()
		s, err := newStore(context.Background(), newFakeDB(), Config{CreateTable: true})
		if err != nil {
			panic(err)
		}
		return s
	}}}
	if ep := os.Getenv(endpointEnv); ep != "" {
		bs = append(bs, backend{"live", func(t *testing.T) *Store {
			t.Helper()
			region := os.Getenv("AWS_REGION")
			if region == "" {
				region = "us-east-1"
			}
			s, err := Open(context.Background(), Config{
				TableName: fmt.Sprintf("tsession_test_%d_%d", os.Getpid(), tableSeq.Add(1)),
				Region:    region, Endpoint: ep, CreateTable: true,
			})
			if err != nil {
				panic(err)
			}
			t.Cleanup(func() {
				s.db.(*dynamodb.Client).DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &s.table}) //nolint:errcheck
			})
			return s
		}})
	}
	return bs
}

func eachBackend(t *testing.T, fn func(t *testing.T, s *Store)) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			s := b.open(t)
			defer s.Close()
			fn(t, s)
		})
	}
}

func TestLiveEndpointGate(t *testing.T) {
	if os.Getenv(endpointEnv) == "" {
		t.Skip(endpointEnv + " not set; live DynamoDB Local tests skipped")
	}
}

func ev(sid string, seq uint64) core.Event {
	return core.Event{SessionID: sid, SpanID: fmt.Sprintf("s%d", seq), Instance: "i", LocalSeq: seq, Seq: seq,
		Kind: core.KindDBWrite, Service: "svc", EndHLC: seq, StartNs: int64(seq), EndNs: int64(seq) + 1}
}

func run(sid string, from uint64, n int) []core.Event {
	out := make([]core.Event, n)
	for i := range out {
		out[i] = ev(sid, from+uint64(i))
	}
	return out
}

// bigEvent is an event whose JSON is about n bytes and does not compress well.
func bigEvent(sid string, seq uint64, n int) core.Event {
	raw := make([]byte, n*3/4)
	rand.Read(raw) //nolint:errcheck
	e := ev(sid, seq)
	e.Req = core.PayloadRef{Inline: []byte(`"` + base64.StdEncoding.EncodeToString(raw) + `"`)}
	return e
}

func TestConformance(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) { storetest.Run(t, s) })
}

func TestConcurrentSameSeq(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const writers = 8
		for round := uint64(1); round <= 3; round++ {
			var wins, conflicts atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					e := ev("race", round)
					e.Instance = fmt.Sprintf("w%d", w)
					<-start
					switch err := s.AppendEvents(ctx, "race", []core.Event{e}); {
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
				panic(fmt.Sprintf("round %d: wins=%d conflicts=%d", round, wins.Load(), conflicts.Load()))
			}
		}
		got, err := s.Events(ctx, "race", 0, 0)
		if err != nil || len(got) != 3 {
			panic(fmt.Sprintf("events=%d err=%v", len(got), err))
		}
		for i, e := range got {
			if e.Seq != uint64(i+1) {
				panic(fmt.Sprintf("gap at %d: seq %d", i, e.Seq))
			}
		}
	})
}

func TestSeqContinuity(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		if err := s.AppendEvents(ctx, "c", run("c", 2, 1)); !errors.Is(err, store.ErrSeqConflict) {
			panic(fmt.Sprintf("first seq must be 1: %v", err))
		}
		if err := s.AppendEvents(ctx, "c", run("c", 1, 3)); err != nil {
			panic(err)
		}
		if err := s.AppendEvents(ctx, "c", run("c", 3, 1)); !errors.Is(err, store.ErrSeqConflict) {
			panic(fmt.Sprintf("replayed seq: %v", err))
		}
		gap := []core.Event{ev("c", 4), ev("c", 6)}
		if err := s.AppendEvents(ctx, "c", gap); !errors.Is(err, store.ErrSeqConflict) {
			panic(fmt.Sprintf("gap inside batch: %v", err))
		}
		if h, _ := s.Head(ctx, "c"); h != 3 {
			panic(fmt.Sprintf("head %d after rejected batches", h))
		}
		if err := s.AppendEvents(ctx, "c", nil); err != nil {
			panic(fmt.Sprintf("empty append: %v", err))
		}
		if err := s.AppendEvents(ctx, "c", run("c", 4, 2)); err != nil {
			panic(err)
		}
	})
}

func TestBatchCrossesTransactionLimits(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		// 250 events > 100 actions per transaction.
		if err := s.AppendEvents(ctx, "big", run("big", 1, 250)); err != nil {
			panic(err)
		}
		// Byte-bound split: 12 events of ~700KB gzip-resistant data (~4 items
		// each) exceed the 4MB transaction size.
		var evs []core.Event
		for i := 0; i < 12; i++ {
			evs = append(evs, bigEvent("big", uint64(251+i), 700*1024))
		}
		if err := s.AppendEvents(ctx, "big", evs); err != nil {
			panic(err)
		}
		if h, _ := s.Head(ctx, "big"); h != 262 {
			panic(fmt.Sprintf("head %d", h))
		}
		got, err := s.Events(ctx, "big", 0, 0)
		if err != nil || len(got) != 262 {
			panic(fmt.Sprintf("events=%d err=%v", len(got), err))
		}
		for i, e := range got {
			if e.Seq != uint64(i+1) {
				panic(fmt.Sprintf("order: index %d has seq %d", i, e.Seq))
			}
		}
		if string(got[261].Req.Inline) != string(evs[11].Req.Inline) {
			panic("payload mismatch")
		}
		l, _ := s.ListSessions(ctx)
		if len(l) != 1 || l[0].EventCount != 262 || l[0].FirstNs != 1 {
			panic(fmt.Sprintf("list %+v", l))
		}
	})
}

func TestLargeEventChunking(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		small := ev("m", 1)
		huge := bigEvent("m", 2, 3<<20) // 3MB of JSON, ~2.3MB gzipped, 9+ items
		if err := s.AppendEvents(ctx, "m", []core.Event{small, huge, ev("m", 3)}); err != nil {
			panic(err)
		}
		got, err := s.Events(ctx, "m", 0, 0)
		if err != nil || len(got) != 3 {
			panic(fmt.Sprintf("events=%d err=%v", len(got), err))
		}
		if string(got[1].Req.Inline) != string(huge.Req.Inline) || got[2].Seq != 3 {
			panic("3MB event did not round trip")
		}
		// from_seq/limit landing on a chunked event.
		got, _ = s.Events(ctx, "m", 2, 1)
		if len(got) != 1 || got[0].Seq != 2 || string(got[0].Req.Inline) != string(huge.Req.Inline) {
			panic(fmt.Sprintf("limit 1 from 2: %d events", len(got)))
		}
		// Beyond the hard maximum: a clear error and nothing written.
		over := bigEvent("m", 5, 5<<20)
		err = s.AppendEvents(ctx, "m", []core.Event{ev("m", 4), over})
		if err == nil || !strings.Contains(err.Error(), "too large") {
			panic(fmt.Sprintf("oversized event: %v", err))
		}
		if h, _ := s.Head(ctx, "m"); h != 3 {
			panic(fmt.Sprintf("head %d after rejected batch", h))
		}
	})
}

func TestPaginationFromSeqLimit(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		const n = 1200
		for from := uint64(1); from <= n; from += 100 {
			if err := s.AppendEvents(ctx, "p", run("p", from, 100)); err != nil {
				panic(err)
			}
		}
		for _, tc := range []struct {
			from      uint64
			limit     int
			first, ln int
		}{
			{0, 0, 1, n}, {1, 0, 1, n}, {600, 0, 600, 601}, {1200, 0, 1200, 1},
			{1201, 0, 0, 0}, {5, 3, 5, 3}, {1195, 100, 1195, 6}, {0, 1500, 1, n}, {1, 1001, 1, 1001},
		} {
			got, err := s.Events(ctx, "p", tc.from, tc.limit)
			if err != nil || len(got) != tc.ln {
				panic(fmt.Sprintf("from=%d limit=%d: got %d err=%v", tc.from, tc.limit, len(got), err))
			}
			for i, e := range got {
				if e.Seq != uint64(tc.first+i) {
					panic(fmt.Sprintf("from=%d limit=%d: index %d seq %d", tc.from, tc.limit, i, e.Seq))
				}
			}
		}
		if got, err := s.Events(ctx, "nobody", 0, 0); err != nil || len(got) != 0 {
			panic(fmt.Sprintf("unknown session: %v %v", got, err))
		}
	})
}

func TestTxOutcomesSticky(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		mk := func(seq uint64, k core.Kind, tx string) core.Event {
			e := ev("t", seq)
			e.Kind, e.TxID = k, tx
			return e
		}
		bad := mk(3, core.KindDBTxCommit, "a")
		bad.Err = "boom"
		evs := []core.Event{mk(1, core.KindDBTxRollbck, "a"), mk(2, core.KindDBTxCommit, "a"), bad, mk(4, core.KindDBTxCommit, "b"), mk(5, core.KindDBWrite, "c")}
		if err := s.AppendEvents(ctx, "t", evs); err != nil {
			panic(err)
		}
		o, err := s.TxOutcomes(ctx, "t")
		if err != nil || len(o) != 2 || o["a"] || !o["b"] {
			panic(fmt.Sprintf("outcomes %v %v", o, err))
		}
	})
}

func TestSealAndListSessions(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		for _, id := range []string{"b", "a"} {
			e := run(id, 1, 2)
			e[0].StartNs, e[1].EndNs = 50, 90
			if err := s.AppendEvents(ctx, id, e); err != nil {
				panic(err)
			}
		}
		if si, err := s.GetSeal(ctx, "a"); si != nil || err != nil {
			panic(fmt.Sprintf("unsealed: %v %v", si, err))
		}
		at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
		want := store.SealInfo{Incomplete: true, Orphaned: []string{"x", "y"}, BundleURI: "u", SealedAt: at, Quarantined: 2}
		for i := 0; i < 2; i++ { // idempotent
			if err := s.PutSeal(ctx, "a", want); err != nil {
				panic(err)
			}
			si, err := s.GetSeal(ctx, "a")
			if err != nil || si == nil || !si.Incomplete || si.BundleURI != "u" || len(si.Orphaned) != 2 || si.Quarantined != 2 || !si.SealedAt.Equal(at) {
				panic(fmt.Sprintf("seal %+v %v", si, err))
			}
		}
		if err := s.PutSeal(ctx, "a", store.SealInfo{BundleURI: "v", SealedAt: at}); err != nil {
			panic(err)
		}
		if si, _ := s.GetSeal(ctx, "a"); si.BundleURI != "v" || si.Incomplete || len(si.Orphaned) != 0 {
			panic(fmt.Sprintf("overwrite %+v", si))
		}
		if err := s.PutSeal(ctx, "ghost", want); err != nil { // sealed with no events
			panic(err)
		}
		l, err := s.ListSessions(ctx)
		if err != nil || len(l) != 2 {
			panic(fmt.Sprintf("list %+v %v", l, err))
		}
		if l[0] != (store.SessionInfo{ID: "a", EventCount: 2, FirstNs: 2, LastNs: 90, Sealed: true}) ||
			l[1] != (store.SessionInfo{ID: "b", EventCount: 2, FirstNs: 2, LastNs: 90}) {
			panic(fmt.Sprintf("list %+v", l))
		}
	})
}

func TestContextAndClose(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.AppendEvents(ctx, "x", run("x", 1, 1)); err == nil {
			panic("cancelled context must fail")
		}
		if _, err := s.Events(ctx, "x", 0, 0); err == nil {
			panic("cancelled context must fail")
		}
		if err := s.AppendEvents(context.Background(), "", run("", 1, 1)); err == nil {
			panic("empty session id must fail")
		}
		for range 2 { // repeatable
			if err := s.Close(); err != nil {
				panic(fmt.Sprintf("Close must be a no-op: %v", err))
			}
		}
	})
}

func TestEnsureTable(t *testing.T) {
	ctx := context.Background()
	f := newFakeDB()
	if _, err := newStore(ctx, f, Config{TableName: "t"}); err == nil || !strings.Contains(err.Error(), "not found") {
		panic(fmt.Sprintf("missing table without CreateTable: %v", err))
	}
	if _, err := newStore(ctx, f, Config{TableName: "t", CreateTable: true}); err != nil || f.creates != 1 {
		panic(fmt.Sprintf("create: %v creates=%d", err, f.creates))
	}
	if _, err := newStore(ctx, f, Config{TableName: "t", CreateTable: true}); err != nil || f.creates != 1 {
		panic(fmt.Sprintf("existing table: %v creates=%d", err, f.creates))
	}
}

func TestEncodeEventScalars(t *testing.T) {
	e := ev("s", 7)
	e.Kind, e.TxID, e.Err = core.KindDBTxCommit, "tx1", "failed"
	enc, err := encodeEvent("s", &e)
	if err != nil || len(enc.items) != 1 {
		panic(fmt.Sprintf("%v %d", err, len(enc.items)))
	}
	it := enc.items[0]
	if getS(it, aSK) != "E#00000000000000000007" || getS(it, aKind) != "db.tx.commit" || getS(it, aTxID) != "tx1" ||
		getS(it, aService) != "svc" || getI(it, aStartNs) != 7 || getI(it, aEndNs) != 8 {
		panic(fmt.Sprintf("item %v", it))
	}
	if v, ok := it[aTxEff]; !ok || fmt.Sprint(v) == "" {
		panic("tx_eff missing")
	}
	if !(contSK(7, 1) > eventSK(7) && contSK(7, 1) < eventSK(8) && contSK(7, 12) < eventSK(8)) {
		panic("sort key order")
	}
}

// DeleteSession has to page through the partition, chunk its BatchWriteItem
// calls at 25 and resend what the service hands back as unprocessed.
func TestDeleteSessionPagesAndRetries(t *testing.T) {
	ctx := context.Background()
	db := newFakeDB()
	db.unprocessed = 3
	s, err := newStore(ctx, db, Config{CreateTable: true})
	if err != nil {
		panic(err)
	}
	const sid, keep = "ts_del", "ts_keep"
	// 60 small events plus one chunked event: well over one page (7) and one batch (25).
	if err := s.AppendEvents(ctx, sid, run(sid, 1, 60)); err != nil {
		panic(err)
	}
	if err := s.AppendEvents(ctx, sid, []core.Event{bigEvent(sid, 61, 700*1024)}); err != nil {
		panic(err)
	}
	if err := s.PutSeal(ctx, sid, store.SealInfo{SealedAt: time.Now()}); err != nil {
		panic(err)
	}
	if err := s.AppendEvents(ctx, keep, run(keep, 1, 3)); err != nil {
		panic(err)
	}
	if err := s.DeleteSession(ctx, sid); err != nil {
		panic(err)
	}
	db.mu.Lock()
	for k := range db.items {
		if strings.HasPrefix(k, sid+"\x00") {
			t.Errorf("item survived delete: %q", k)
		}
	}
	calls := db.batchCalls
	db.mu.Unlock()
	if calls < 4 {
		panic(fmt.Sprintf("expected several BatchWriteItem calls (chunking + retries), got %d", calls))
	}
	if evs, err := s.Events(ctx, keep, 0, 0); err != nil || len(evs) != 3 {
		panic(fmt.Sprintf("unrelated session damaged: %d %v", len(evs), err))
	}
	// A cancelled context stops the delete instead of spinning.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.DeleteSession(cctx, keep); err == nil {
		panic("expected an error from a cancelled delete")
	}
}
