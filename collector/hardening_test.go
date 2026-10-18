package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tsession/collector/store"
	"tsession/core"
)

// spyStore wraps a Store with fault injection and call counters.
type spyStore struct {
	store.Store
	poison   func(core.Event) bool
	down     atomic.Bool
	eventsN  atomic.Int64
	putSeals atomic.Int64
}

var errInjected = errors.New("injected store failure")

func (s *spyStore) AppendEvents(ctx context.Context, id string, evs []core.Event) error {
	if s.down.Load() {
		return errInjected
	}
	if s.poison != nil {
		for _, e := range evs {
			if s.poison(e) {
				return errors.New("invalid input syntax")
			}
		}
	}
	return s.Store.AppendEvents(ctx, id, evs)
}

func (s *spyStore) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	if s.down.Load() {
		return nil, errInjected
	}
	s.eventsN.Add(1)
	return s.Store.Events(ctx, id, from, limit)
}

func (s *spyStore) PutSeal(ctx context.Context, id string, si store.SealInfo) error {
	s.putSeals.Add(1)
	return s.Store.PutSeal(ctx, id, si)
}

func newSpyC(t *testing.T, cfg Config, sp *spyStore) (*Collector, *httptest.Server) {
	t.Helper()
	if sp.Store == nil {
		sp.Store = store.NewMem()
	}
	c := New(sp, cfg)
	srv := httptest.NewServer(c.Handler())
	t.Cleanup(func() { srv.Close(); c.Close() })
	return c, srv
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestStripNUL(t *testing.T) {
	for in, want := range map[string]string{
		`{"a":"x\u0000y"}`:     `{"a":"x\ufffdy"}`,
		`{"a":"x\\u0000y"}`:    `{"a":"x\\u0000y"}`, // escaped backslash + literal text
		`{"a":"\\\u0000"}`:     `{"a":"\\\ufffd"}`,
		`{"a":"\u0000\u0000"}`: `{"a":"\ufffd\ufffd"}`,
		`{"a":"no nul here"}`:  `{"a":"no nul here"}`,
		`{"a":{"b":"é ok"}}`:   `{"a":{"b":"é ok"}}`,
	} {
		if got := string(store.StripNUL([]byte(in))); got != want {
			t.Errorf("%s -> %s want %s", in, got, want)
		}
	}
}

func TestIngestSanitizesNUL(t *testing.T) {
	c, srv := newC(t, 0, "")
	line := `{"session_id":"` + sid + `","span_id":"a","kind":"api.in","name":"bad\u0000name","instance":"i1","local_seq":1,"end_hlc":1,` +
		`"req":{"inline":{"k":"v\u0000w"}},"attrs":{"k\u0000":"v"}}` + "\n"
	resp, err := http.Post(srv.URL+"/v1/ingest", "application/x-ndjson", strings.NewReader(line))
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(err, resp)
	}
	resp.Body.Close()
	got := events(t, c)
	if len(got) != 1 || strings.ContainsRune(got[0].Name, 0) || !strings.Contains(got[0].Name, "\ufffd") ||
		bytes.Contains(got[0].Req.Inline, []byte(`\u0000`)) {
		t.Fatalf("%+v", got)
	}
}

func TestPoisonEventQuarantined(t *testing.T) {
	sp := &spyStore{poison: func(e core.Event) bool { return e.Name == "POISON" }}
	cfg := DefaultConfig()
	cfg.Window = 20 * time.Millisecond
	cfg.BundleDir = t.TempDir()
	c, srv := newSpyC(t, cfg, sp)
	var batch []core.Event
	for i := 1; i <= 9; i++ {
		e := ev(fmt.Sprintf("s%d", i), core.KindDBRead, uint64(i), uint64(i))
		if i == 5 {
			e.Name = "POISON"
		}
		batch = append(batch, e)
	}
	if r := post(t, srv.URL, batch...); r.Accepted != 9 {
		t.Fatalf("%+v", r)
	}
	var ds DrainStatus
	waitFor(t, "quarantine", func() bool {
		get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
		return ds.Quarantined == 1 && ds.Pending == 0
	})
	got := events(t, c)
	if len(got) != 8 {
		t.Fatalf("stored %d", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) || e.Name == "POISON" {
			t.Fatalf("pos %d: %+v", i, e)
		}
	}
	// ingest keeps working and the retried poison is a duplicate, not a new failure
	if r := post(t, srv.URL, batch[4], ev("later", core.KindDBRead, 20, 20)); r.Duplicates != 1 || r.Accepted != 1 {
		t.Fatalf("%+v", r)
	}
	res, err := c.Seal(context.Background(), sid)
	if err != nil || !res.Incomplete || res.Quarantined != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	var s Summary
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &s)
	if s.Quarantined != 1 || s.Flags["quarantined"] != 1 || !s.Incomplete {
		t.Fatalf("%+v", s)
	}
}

func TestPoisonWindowZeroDoesNotReturn500Forever(t *testing.T) {
	sp := &spyStore{poison: func(e core.Event) bool { return e.Name == "POISON" }}
	cfg := DefaultConfig()
	cfg.Window = 0
	c, srv := newSpyC(t, cfg, sp)
	e := ev("p", core.KindDBRead, 1, 1)
	e.Name = "POISON"
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(e)
	resp, err := http.Post(srv.URL+"/v1/ingest", "application/x-ndjson", &buf)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("%v %v", err, resp)
	}
	resp.Body.Close()
	if st, _ := c.seq.Status(context.Background(), sid); st.Quarantined != 1 || st.Pending != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestStoreOutageDoesNotQuarantine(t *testing.T) {
	sp := &spyStore{}
	cfg := DefaultConfig()
	cfg.Window = 10 * time.Millisecond
	c, srv := newSpyC(t, cfg, sp)
	post(t, srv.URL, ev("a", core.KindDBRead, 1, 1), ev("b", core.KindDBRead, 2, 2), ev("c", core.KindDBRead, 3, 3))
	sp.down.Store(true)
	time.Sleep(300 * time.Millisecond) // several retries incl. isolation attempts
	sp.down.Store(false)
	waitFor(t, "recovery", func() bool { return len(events(t, c)) == 3 })
	if q := c.seq.quarantined(sid); q != 0 {
		t.Fatalf("quarantined during outage: %d", q)
	}
}

func TestSeqConflictReloadsFromStore(t *testing.T) {
	shared := store.NewMem()
	cfg := DefaultConfig()
	cfg.Window = 0
	a, b := New(shared, cfg), New(shared, cfg)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	mustIngest := func(c *Collector, evs ...core.Event) {
		t.Helper()
		if _, err := c.Ingest(ctx, "", evs); err != nil {
			t.Fatal(err)
		}
	}
	mustIngest(b, ev("b0", core.KindAPIIn, 1, 1))                                 // B loads (next=2)
	mustIngest(a, ev("a1", core.KindAPIIn, 2, 2))                                 // A writes seq 2 behind B's back
	mustIngest(a, ev("a2", core.KindAPIIn, 3, 3))                                 // seq 3
	mustIngest(b, ev("b3", core.KindAPIIn, 4, 4), ev("a2", core.KindAPIIn, 3, 3)) // stale next; a2 duplicates what A stored
	got, _ := shared.Events(ctx, sid, 0, 0)
	if len(got) != 4 {
		t.Fatalf("stored %d: %+v", len(got), got)
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("gap at %d", i)
		}
	}
}

func TestDrainStatusReceiveTimeAndBuffered(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 200 * time.Millisecond
	c, srv := newC2(t, cfg)
	_ = c
	old := ev("a", core.KindAPIIn, 1, 1)
	old.EndNs = 1_000 // wildly skewed client clock
	before := time.Now().UnixNano()
	post(t, srv.URL, old)
	var ds DrainStatus
	get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
	if ds.LastEventUnixNs < before || ds.ReceivedUnixNs != ds.LastEventUnixNs {
		t.Fatalf("receive time not used: %+v", ds)
	}
	if ds.Pending != 1 || ds.EventCount != 0 {
		t.Fatalf("buffered event must be pending: %+v", ds)
	}
	waitFor(t, "flush", func() bool {
		get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
		return ds.EventCount == 1 && ds.Pending == 0
	})
	if ds.LastEventUnixNs < before {
		t.Fatalf("%+v", ds)
	}
}

func newC2(t *testing.T, cfg Config) (*Collector, *httptest.Server) {
	t.Helper()
	c := New(store.NewMem(), cfg)
	srv := httptest.NewServer(c.Handler())
	t.Cleanup(func() { srv.Close(); c.Close() })
	return c, srv
}

func TestConcurrentSealWritesBundleOnce(t *testing.T) {
	sp := &spyStore{}
	cfg := DefaultConfig()
	cfg.Window = 10 * time.Millisecond
	cfg.BundleDir = t.TempDir()
	c, srv := newSpyC(t, cfg, sp)
	post(t, srv.URL, ev("a", core.KindAPIIn, 1, 1), ev("b", core.KindAPIIn, 2, 2))
	var wg sync.WaitGroup
	uris := make([]string, 16)
	for i := range uris {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Seal(context.Background(), sid)
			if err != nil {
				t.Error(err)
			}
			uris[i] = r.BundleURI
		}()
	}
	wg.Wait()
	if n := sp.putSeals.Load(); n != 1 {
		t.Fatalf("sealed %d times", n)
	}
	for _, u := range uris {
		if u == "" || u != uris[0] {
			t.Fatalf("uris differ: %v", uris)
		}
	}
	ents, _ := os.ReadDir(strings.TrimPrefix(uris[0], "file://"))
	if len(ents) != 2 {
		t.Fatalf("bundle dir has %d entries (temp files left?)", len(ents))
	}
}

func txEff(t *testing.T, c *Collector, span string) string {
	t.Helper()
	for _, e := range mustView(t, c) {
		if e.SpanID == span {
			if e.Mutations[0].Effective == nil {
				return "nil"
			}
			return fmt.Sprint(*e.Mutations[0].Effective)
		}
	}
	t.Fatalf("span %s missing", span)
	return ""
}

func TestTxFailedCommitNotEffective(t *testing.T) {
	c, srv := newC(t, 0, "")
	mk := func(span, tx string, hlc uint64) core.Event {
		e := ev(span, core.KindDBWrite, hlc, hlc)
		e.TxID = tx
		e.Mutations = []core.Mutation{{Store: "mongo", Entity: "orders", PK: map[string]any{"id": hlc}, Op: "insert"}}
		return e
	}
	okC := ev("c1", core.KindDBTxCommit, 10, 10)
	okC.TxID = "t1"
	errC := ev("c2", core.KindDBTxCommit, 11, 11)
	errC.TxID, errC.Err = "t2", "commit failed: network"
	attrC := ev("c3", core.KindDBTxCommit, 12, 12)
	attrC.TxID, attrC.Attrs = "t3", map[string]string{"commit_failed": "true"}
	rbC := ev("c4", core.KindDBTxRollbck, 13, 13) // tsql: failed commit reported as rollback
	rbC.TxID, rbC.Attrs = "t4", map[string]string{"commit_failed": "true"}
	// failed commit arriving BEFORE a (bogus) later success must stay negative
	errC5 := ev("c5a", core.KindDBTxCommit, 14, 14)
	errC5.TxID, errC5.Err = "t5", "boom"
	okC5 := ev("c5b", core.KindDBTxCommit, 15, 15)
	okC5.TxID = "t5"
	post(t, srv.URL, mk("w1", "t1", 1), mk("w2", "t2", 2), mk("w3", "t3", 3), mk("w4", "t4", 4), mk("w5", "t5", 5),
		okC, errC, attrC, rbC, errC5, okC5)
	for span, want := range map[string]string{"w1": "true", "w2": "false", "w3": "false", "w4": "false", "w5": "false"} {
		if got := txEff(t, c, span); got != want {
			t.Errorf("%s: %s want %s", span, got, want)
		}
	}
	// the paged /events path (store.TxOutcomes) must agree
	var out struct{ Events []core.Event }
	get(t, srv.URL+"/v1/sessions/"+sid+"/events", &out)
	for _, e := range out.Events {
		if e.SpanID == "w2" && *e.Mutations[0].Effective {
			t.Error("events endpoint: failed commit effective")
		}
	}
	// store-level outcomes
	o, _ := c.st.TxOutcomes(context.Background(), sid)
	if !o["t1"] || o["t2"] || o["t3"] || o["t4"] || o["t5"] {
		t.Fatalf("%v", o)
	}
}

func worker(span, kind string, hlc uint64, link string, attrs map[string]string) core.Event {
	e := ev(span, core.Kind(kind), hlc, hlc)
	e.LinkIDs, e.Attrs = []string{link}, attrs
	return e
}

func TestWorkerRetryLedger(t *testing.T) {
	retry := map[string]string{"retry_pending": "true"}
	t.Run("retry_pending end does not close; final end does", func(t *testing.T) {
		c, srv := newC(t, 0, "")
		post(t, srv.URL, ev("pub", core.KindQueuePub, 1, 1), worker("e1", "worker.end", 2, "pub", retry))
		var ds DrainStatus
		get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
		if ds.Pending != 1 {
			t.Fatalf("closed by retry_pending end: %+v", ds)
		}
		post(t, srv.URL, worker("r1", "worker.retry", 3, "pub", nil))
		get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
		if ds.Pending != 1 {
			t.Fatalf("closed by worker.retry: %+v", ds)
		}
		post(t, srv.URL, worker("e2", "worker.end", 4, "pub", nil))
		get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
		if ds.Pending != 0 {
			t.Fatalf("final end did not close: %+v", ds)
		}
		if res, _ := c.Seal(context.Background(), sid); res.Incomplete {
			t.Fatalf("%+v", res)
		}
	})
	t.Run("dlq closes", func(t *testing.T) {
		c, srv := newC(t, 0, "")
		post(t, srv.URL, ev("pub", core.KindQueuePub, 1, 1), worker("e1", "worker.end", 2, "pub", retry), worker("d", "worker.dlq", 3, "pub", nil))
		if res, _ := c.Seal(context.Background(), sid); res.Incomplete || len(res.Orphaned) != 0 {
			t.Fatalf("%+v", res)
		}
	})
	t.Run("out of order: final end, retry end, then publisher", func(t *testing.T) {
		c, _ := newC(t, 0, "")
		ctx := context.Background()
		for _, e := range []core.Event{worker("e2", "worker.end", 4, "pub", nil), worker("e1", "worker.end", 2, "pub", retry), ev("pub", core.KindQueuePub, 1, 1)} {
			if _, err := c.Ingest(ctx, "", []core.Event{e}); err != nil {
				t.Fatal(err)
			}
		}
		if res, _ := c.Seal(ctx, sid); res.Incomplete || len(res.Orphaned) != 0 {
			t.Fatalf("%+v", res)
		}
	})
	t.Run("out of order: retry end before publisher, no final", func(t *testing.T) {
		c, _ := newC(t, 0, "")
		ctx := context.Background()
		for _, e := range []core.Event{worker("e1", "worker.end", 2, "pub", retry), ev("pub", core.KindQueuePub, 1, 1)} {
			if _, err := c.Ingest(ctx, "", []core.Event{e}); err != nil {
				t.Fatal(err)
			}
		}
		res, _ := c.Seal(ctx, sid)
		if !res.Incomplete || len(res.Orphaned) != 1 || len(res.RetryPending) != 1 || res.RetryPending[0] != "pub" {
			t.Fatalf("%+v", res)
		}
		for _, e := range mustView(t, c) {
			if e.SpanID == "pub" && (len(e.Flags) != 2 || e.Flags[1] != "retry_pending") {
				t.Fatalf("flags %v", e.Flags)
			}
		}
		// idempotent re-seal keeps the report
		if r2, _ := c.Seal(ctx, sid); len(r2.RetryPending) != 1 {
			t.Fatalf("%+v", r2)
		}
	})
	t.Run("plain orphan is not retry_pending", func(t *testing.T) {
		c, _ := newC(t, 0, "")
		_, _ = c.Ingest(context.Background(), "", []core.Event{ev("pub", core.KindQueuePub, 1, 1)})
		res, _ := c.Seal(context.Background(), sid)
		if len(res.Orphaned) != 1 || len(res.RetryPending) != 0 {
			t.Fatalf("%+v", res)
		}
	})
}

func TestGuardAndInternalAccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.AllowedHosts = []string{"collector"}
	cfg.InternalAllow = []string{"10.0.0.0/8", "bogus"}
	c := New(store.NewMem(), cfg)
	defer c.Close()
	h := c.Handler()
	do := func(method, url, host, remote, ct, body string) int {
		req := httptest.NewRequest(method, url, strings.NewReader(body))
		if host != "" {
			req.Host = host
		}
		req.RemoteAddr = remote
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	ds := "/v1/internal/sessions/" + sid + "/drain-status"
	for _, tc := range []struct {
		name                                string
		method, url, host, remote, ct, body string
		want                                int
	}{
		{"loopback ok", "GET", "/healthz", "127.0.0.1:7071", "127.0.0.1:5", "", "", 200},
		{"healthz exempt from host guard", "GET", "/healthz", "collector:7071", "203.0.113.9:5", "", "", 200},
		{"healthz POST still guarded", "POST", "/healthz", "evil.example.com", "127.0.0.1:5", "", "", 403},
		{"rebinding host", "GET", "/v1/sessions", "evil.example.com", "127.0.0.1:5", "", "", 403},
		{"service name allowed", "GET", "/v1/sessions", "collector:7071", "127.0.0.1:5", "", "", 200},
		{"internal loopback", "GET", ds, "127.0.0.1:7071", "127.0.0.1:5", "", "", 200},
		{"internal ipv6 loopback", "GET", ds, "localhost:7071", "[::1]:5", "", "", 200},
		{"internal foreign peer", "GET", ds, "collector:7071", "203.0.113.9:5", "", "", 403},
		{"internal allowed cidr", "GET", ds, "collector:7071", "10.1.2.3:5", "", "", 200},
		{"ingest ndjson", "POST", "/v1/ingest", "127.0.0.1:7071", "203.0.113.9:5", "application/x-ndjson", "{}\n", 200},
		{"ingest json", "POST", "/v1/ingest", "127.0.0.1:7071", "127.0.0.1:5", "application/json", "{}\n", 200},
		{"ingest text/plain", "POST", "/v1/ingest", "127.0.0.1:7071", "127.0.0.1:5", "text/plain", "{}\n", 415},
		{"seal foreign peer", "POST", "/v1/internal/sessions/" + sid + "/seal", "collector", "203.0.113.9:5", "", "", 403},
		{"browser origin", "GET", "/v1/sessions", "127.0.0.1:7071", "127.0.0.1:5", "", "", 200},
	} {
		if got := do(tc.method, tc.url, tc.host, tc.remote, tc.ct, tc.body); got != tc.want {
			t.Errorf("%s: %d want %d", tc.name, got, tc.want)
		}
	}
}

func TestViewCacheAvoidsRedecode(t *testing.T) {
	sp := &spyStore{}
	cfg := DefaultConfig()
	cfg.Window = 0
	c, srv := newSpyC(t, cfg, sp)
	post(t, srv.URL, ev("a", core.KindAPIIn, 1, 1), ev("b", core.KindAPIIn, 2, 2))
	base := sp.eventsN.Load()
	var tr Trace
	var s Summary
	var ents []Entity
	for i := 0; i < 10; i++ {
		get(t, srv.URL+"/v1/sessions/"+sid+"/trace", &tr)
		get(t, srv.URL+"/v1/sessions/"+sid+"/entities", &ents)
		get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &s)
	}
	// summary also asks the sequencer for status (one load per actor lifetime, already loaded)
	if n := sp.eventsN.Load() - base; n != 1 {
		t.Fatalf("store.Events called %d times for 30 polls, want 1", n)
	}
	if len(tr.Roots) != 2 || s.EventCount != 2 {
		t.Fatalf("%+v %+v", tr, s)
	}
	post(t, srv.URL, ev("c", core.KindAPIIn, 3, 3))
	get(t, srv.URL+"/v1/sessions/"+sid+"/trace", &tr)
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &s)
	if len(tr.Roots) != 3 || s.EventCount != 3 {
		t.Fatalf("cache not invalidated: %+v %+v", tr, s)
	}
	if _, err := c.Seal(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &s)
	if !s.Sealed {
		t.Fatal("seal not reflected")
	}
}

func rawReq(h http.Handler, path string) int {
	req := httptest.NewRequest("GET", "http://127.0.0.1:7071"+path, nil)
	req.RemoteAddr = "127.0.0.1:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestNoGoroutineGrowthOnRandomIDs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(store.NewMem(), cfg)
	defer c.Close()
	h := c.Handler()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("ts_%016x", rng.Uint64())
		base := "/v1/sessions/" + id
		var path string
		switch i % 5 {
		case 0:
			path = base + "/summary"
		case 1:
			path = "/v1/internal/sessions/" + id + "/drain-status"
		case 2:
			path = base + "/events"
		case 3:
			path = base + "/trace"
		default:
			path = base + "/entities"
		}
		if code := rawReq(h, path); code != 200 && code != 404 {
			t.Fatalf("%s: %d", path, code)
		}
	}
	if n := c.seq.actorCount(); n != 0 {
		t.Fatalf("%d actors spawned by reads", n)
	}
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutines grew %d -> %d", before, after)
	}
	t.Logf("goroutines before=%d after=%d actors=%d", before, after, c.seq.actorCount())
}

func TestSealedIdleActorEvicted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.IdleEvict = 40 * time.Millisecond
	c := New(store.NewMem(), cfg)
	defer c.Close()
	ctx := context.Background()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()
	const n = 200
	for i := 0; i < n; i++ {
		e := ev("a", core.KindAPIIn, 1, 1)
		e.SessionID = fmt.Sprintf("s%d", i)
		if _, err := c.Ingest(ctx, "", []core.Event{e}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Seal(ctx, e.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	if c.seq.actorCount() == 0 {
		t.Fatal("expected live actors right after sealing")
	}
	waitFor(t, "eviction", func() bool { return c.seq.actorCount() == 0 })
	if g := runtime.NumGoroutine(); g > before+2 {
		t.Fatalf("goroutines %d -> %d after eviction", before, g)
	}
	// state reloads on demand: dedupe and gapless Seq survive eviction
	e := ev("a", core.KindAPIIn, 1, 1)
	e.SessionID = "s7"
	r, err := c.Ingest(ctx, "", []core.Event{e})
	if err != nil || r.Duplicates != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	e2 := ev("b", core.KindAPIIn, 2, 2)
	e2.SessionID = "s7"
	if _, err := c.Ingest(ctx, "", []core.Event{e2}); err != nil {
		t.Fatal(err)
	}
	evs, _ := c.st.Events(ctx, "s7", 0, 0)
	if len(evs) != 2 || evs[1].Seq != 2 {
		t.Fatalf("%+v", evs)
	}
	// an unsealed session with a buffered event is never evicted
	cfg.Window = time.Hour
	c2 := New(store.NewMem(), cfg)
	defer c2.Close()
	_, _ = c2.Ingest(ctx, "", []core.Event{ev("a", core.KindAPIIn, 1, 1)})
	time.Sleep(150 * time.Millisecond)
	if c2.seq.actorCount() != 1 {
		t.Fatal("actor with buffered events was evicted")
	}
}
