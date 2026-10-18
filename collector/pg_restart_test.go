package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"tsession/collector/store"
	"tsession/core"
)

func pgCollector(t *testing.T, schema string) (*Collector, *httptest.Server, *store.PG) {
	t.Helper()
	pg, err := store.OpenPGConfig(context.Background(), store.PGConfig{DSN: os.Getenv("TSESSION_TEST_PG_DSN"), Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(pg, cfg)
	srv := httptest.NewServer(c.Handler())
	return c, srv, pg
}

func dropSchema(t *testing.T, schema string) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), os.Getenv("TSESSION_TEST_PG_DSN"))
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`); err != nil {
		t.Error(err)
	}
}

// A restarted collector must dedupe replayed events and continue Seq.
func TestPGRestartRecovery(t *testing.T) {
	if os.Getenv("TSESSION_TEST_PG_DSN") == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	schema := fmt.Sprintf("tstest_restart_%d", os.Getpid())
	c, srv, pg := pgCollector(t, schema)
	t.Cleanup(func() { pg.Close() })
	post(t, srv.URL, ev("a", core.KindDBWrite, 1, 1), ev("b", core.KindDBWrite, 2, 2), ev("c", core.KindDBWrite, 3, 3))
	srv.Close()
	c.Close()
	pg.Close() // "crash"

	c2, srv2, pg2 := pgCollector(t, schema)
	defer func() {
		srv2.Close()
		c2.Close()
		dropSchema(t, schema)
		pg2.Close()
	}()
	r := post(t, srv2.URL, ev("b", core.KindDBWrite, 2, 2), ev("c", core.KindDBWrite, 3, 3), ev("d", core.KindDBWrite, 4, 4), ev("e", core.KindDBWrite, 5, 5))
	if r.Accepted != 2 || r.Duplicates != 2 {
		t.Fatalf("after restart: %+v", r)
	}
	got := events(t, c2)
	if len(got) != 5 {
		t.Fatalf("len=%d", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("seq gap: %d at %d", e.Seq, i)
		}
	}
	// concurrent ingest through the collector with overlapping duplicates
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(6); i < 36; i++ {
				post(t, srv2.URL, ev(fmt.Sprintf("s%d", i), core.KindDBWrite, i, i))
			}
		}()
	}
	wg.Wait()
	got = events(t, c2)
	if len(got) != 35 {
		t.Fatalf("after concurrent dedupe len=%d", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("seq gap: %d at %d", e.Seq, i)
		}
	}
}

// NUL escapes must not wedge a PG-backed session; failed commits are not
// effective via the store's TxOutcomes; a second collector writing the same
// session triggers a reload instead of endless seq conflicts.
func TestPGHardening(t *testing.T) {
	if os.Getenv("TSESSION_TEST_PG_DSN") == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	schema := fmt.Sprintf("tstest_hard_%d", os.Getpid())
	c, srv, pg := pgCollector(t, schema)
	defer func() { srv.Close(); c.Close(); pg.Close(); dropSchema(t, schema) }()

	nul := `{"session_id":"` + sid + `","span_id":"n","kind":"api.in","name":"a\u0000b","instance":"i1","local_seq":1,"end_hlc":1,"req":{"inline":{"x":"\u0000"}}}` + "\n"
	resp, err := http.Post(srv.URL+"/v1/ingest", "application/x-ndjson", strings.NewReader(nul))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("nul ingest: %v %v", err, resp)
	}
	resp.Body.Close()
	if n := len(events(t, c)); n != 1 {
		t.Fatalf("stored %d", n)
	}

	w := ev("w", core.KindDBWrite, 2, 2)
	w.TxID = "tx"
	w.Mutations = []core.Mutation{{Store: "mongo", Entity: "o", PK: map[string]any{"id": 1}, Op: "insert"}}
	cm := ev("cm", core.KindDBTxCommit, 3, 3)
	cm.TxID, cm.Err = "tx", "commit failed"
	post(t, srv.URL, w, cm)
	var out struct{ Events []core.Event }
	get(t, srv.URL+"/v1/sessions/"+sid+"/events", &out)
	for _, e := range out.Events {
		if e.SpanID == "w" && (e.Mutations[0].Effective == nil || *e.Mutations[0].Effective) {
			t.Fatal("failed commit reported effective")
		}
	}

	// second collector (separate process stand-in) appends behind our back
	c2, srv2, pg2 := pgCollector(t, schema)
	defer func() { srv2.Close(); c2.Close(); pg2.Close() }()
	post(t, srv2.URL, ev("x1", core.KindAPIIn, 10, 10))
	r := post(t, srv.URL, ev("x2", core.KindAPIIn, 11, 11), ev("x1", core.KindAPIIn, 10, 10))
	if r.Accepted != 2 { // x1 was not yet known to this collector; the store dedupes it on conflict
		t.Logf("ingest result %+v", r)
	}
	evs := events(t, c)
	seen := map[string]int{}
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			t.Fatalf("gap at %d", i)
		}
		seen[e.SpanID]++
	}
	if seen["x1"] != 1 || seen["x2"] != 1 || len(evs) != 5 {
		t.Fatalf("%d events, %v", len(evs), seen)
	}
}
