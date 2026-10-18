package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestValidateSchemaName(t *testing.T) {
	for _, ok := range []string{"tsession", "_x", "a1_b2", strings.Repeat("a", 63)} {
		if err := ValidateSchemaName(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "1abc", "Ts", "a-b", "a b", `a"b`, "a;drop table x", "a.b", "pg_catalog; --", strings.Repeat("a", 64), "é"} {
		if err := ValidateSchemaName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := OpenPGConfig(context.Background(), PGConfig{DSN: "postgres://127.0.0.1:1/x", Schema: `x"; drop`}); err == nil || !strings.Contains(err.Error(), "invalid pg schema") {
		t.Fatalf("expected validation error before connecting: %v", err)
	}
}

func pgDSN(t *testing.T) string {
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	return dsn
}

func openTestPG(t *testing.T, schema string) *PG {
	t.Helper()
	pg, err := OpenPGConfig(context.Background(), PGConfig{DSN: pgDSN(t), Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close() })
	return pg
}

func TestPGSchemaIsolation(t *testing.T) {
	pgDSN(t)
	ctx := context.Background()
	schema := fmt.Sprintf("tstest_%d", os.Getpid())
	pg := openTestPG(t, schema)
	t.Cleanup(func() { dropTestSchema(schema) })
	var n int
	if err := pg.pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('sessions','session_events','entity_mutations') AND table_schema=$1`, schema).Scan(&n); err != nil || n != 3 {
		t.Fatalf("tables in %s: %d %v", schema, n, err)
	}
	if err := pg.pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_name IN ('sessions','session_events','entity_mutations') AND table_schema='public'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("trace tables leaked into public: %d %v", n, err)
	}
	// every pooled connection must be pinned
	var sp string
	if err := pg.pool.QueryRow(ctx, `SHOW search_path`).Scan(&sp); err != nil || !strings.Contains(sp, schema) {
		t.Fatalf("search_path=%q %v", sp, err)
	}
	// re-open is idempotent
	pg2 := openTestPG(t, schema)
	_ = pg2
}

func TestPGReopenKeepsSeqContinuity(t *testing.T) {
	pgDSN(t)
	ctx := context.Background()
	schema := fmt.Sprintf("tstest_reopen_%d", os.Getpid())
	pg := openTestPG(t, schema)
	t.Cleanup(func() { dropTestSchema(schema) })
	mk := func(seq uint64) core.Event {
		return core.Event{SessionID: "s", SpanID: "x", Instance: "i", LocalSeq: seq, Seq: seq, Kind: core.KindDBWrite, Service: "svc", EndHLC: seq}
	}
	if err := pg.AppendEvents(ctx, "s", []core.Event{mk(1), mk(2), mk(3)}); err != nil {
		t.Fatal(err)
	}
	pg.Close()
	pg = openTestPG(t, schema)
	if err := pg.AppendEvents(ctx, "s", []core.Event{mk(3)}); err != ErrSeqConflict {
		t.Fatalf("replayed seq must conflict: %v", err)
	}
	if err := pg.AppendEvents(ctx, "s", []core.Event{mk(4), mk(6)}); err != ErrSeqConflict {
		t.Fatalf("gap inside batch must conflict: %v", err)
	}
	if err := pg.AppendEvents(ctx, "s", []core.Event{mk(4), mk(5)}); err != nil {
		t.Fatal(err)
	}
	if got, _ := pg.Events(ctx, "s", 0, 0); len(got) != 5 || got[4].Seq != 5 {
		t.Fatalf("got %d events", len(got))
	}
}

func TestPGConcurrentAppend(t *testing.T) {
	pgDSN(t)
	ctx := context.Background()
	schema := fmt.Sprintf("tstest_conc_%d", os.Getpid())
	pg := openTestPG(t, schema)
	t.Cleanup(func() { dropTestSchema(schema) })
	const writers, per = 8, 25
	var ok, conflicts atomic.Int64
	var wg sync.WaitGroup
	// Writers race to append the next Seq; exactly one wins each round, the
	// rest must see ErrSeqConflict (never a corrupt/gapped run or a raw error).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for done := 0; done < per; {
				cur, _ := pg.Events(ctx, "c", 0, 0)
				seq := uint64(len(cur) + 1)
				e := core.Event{SessionID: "c", SpanID: fmt.Sprintf("w%d-%d", w, done), Instance: fmt.Sprintf("i%d", w), LocalSeq: uint64(done + 1), Seq: seq,
					Kind: core.KindDBWrite, Service: "svc", EndHLC: seq,
					Mutations: []core.Mutation{{Store: "pg", Entity: "t", PK: map[string]any{"id": w}, Op: "update"}}}
				switch err := pg.AppendEvents(ctx, "c", []core.Event{e}); err {
				case nil:
					ok.Add(1)
					done++
				case ErrSeqConflict:
					conflicts.Add(1)
				default:
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	got, err := pg.Events(ctx, "c", 0, 0)
	if err != nil || len(got) != writers*per || ok.Load() != writers*per {
		t.Fatalf("events=%d ok=%d err=%v", len(got), ok.Load(), err)
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("gap at %d: seq %d", i, e.Seq)
		}
	}
	t.Logf("ok=%d conflicts=%d", ok.Load(), conflicts.Load())
}

// dropTestSchema uses a fresh connection so it works after pools are closed.
func dropTestSchema(schema string) {
	pg, err := OpenPGConfig(context.Background(), PGConfig{DSN: os.Getenv("TSESSION_TEST_PG_DSN"), Schema: schema})
	if err != nil {
		return
	}
	defer pg.Close()
	pg.pool.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) //nolint:errcheck
}
