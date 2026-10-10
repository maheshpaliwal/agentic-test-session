package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"tsession/collector/store"
	"tsession/core"
)

const sid = "ts_test1"

func ev(span string, kind core.Kind, hlc, local uint64) core.Event {
	return core.Event{SessionID: sid, TraceID: "tr1", SpanID: span, Service: "svc", Instance: "i1",
		Kind: kind, EndHLC: hlc, LocalSeq: local, StartNs: int64(hlc) * 10, EndNs: int64(hlc)*10 + 5}
}

func newC(t *testing.T, window int, dir string) (*Collector, *httptest.Server) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Window = 0
	if window > 0 {
		cfg.Window = 20 * 1e6
	}
	cfg.BundleDir = dir
	c := New(store.NewMem(), cfg)
	srv := httptest.NewServer(c.Handler())
	t.Cleanup(func() { srv.Close(); c.Close() })
	return c, srv
}

func post(t *testing.T, url string, evs ...core.Event) IngestResult {
	t.Helper()
	var b bytes.Buffer
	for _, e := range evs {
		_ = json.NewEncoder(&b).Encode(e)
	}
	resp, err := http.Post(url+"/v1/ingest", "application/x-ndjson", &b)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r IngestResult
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	_ = json.NewDecoder(resp.Body).Decode(&r)
	return r
}

func get(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func events(t *testing.T, c *Collector) []core.Event {
	t.Helper()
	evs, err := c.st.Events(context.Background(), sid, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

func TestOutOfOrderSorted(t *testing.T) {
	c, srv := newC(t, 20, "")
	// arrive in scrambled order across two requests, within the window
	post(t, srv.URL, ev("c", core.KindAPIIn, 30, 3), ev("a", core.KindAPIIn, 10, 1))
	post(t, srv.URL, ev("b", core.KindAPIIn, 20, 2))
	if _, err := c.seq.Flush(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	got := events(t, c)
	if len(got) != 3 {
		t.Fatalf("got %d events", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].SpanID != want || got[i].Seq != uint64(i+1) {
			t.Fatalf("pos %d: span=%s seq=%d", i, got[i].SpanID, got[i].Seq)
		}
	}
}

func TestTieBreakByService(t *testing.T) {
	c, _ := newC(t, 0, "")
	a, b := ev("a", core.KindAPIIn, 5, 9), ev("b", core.KindAPIIn, 5, 1)
	a.Service, b.Service = "alpha", "beta"
	if _, err := c.Ingest(context.Background(), "", []core.Event{b, a}); err != nil {
		t.Fatal(err)
	}
	if got := events(t, c); got[0].SpanID != "a" {
		t.Fatalf("service tiebreak failed: %v", got[0].SpanID)
	}
}

func TestDuplicateIngestIgnored(t *testing.T) {
	c, srv := newC(t, 0, "")
	e := ev("a", core.KindAPIIn, 1, 1)
	if r := post(t, srv.URL, e, e); r.Accepted != 1 || r.Duplicates != 1 {
		t.Fatalf("first: %+v", r)
	}
	if r := post(t, srv.URL, e); r.Accepted != 0 || r.Duplicates != 1 {
		t.Fatalf("second: %+v", r)
	}
	if n := len(events(t, c)); n != 1 {
		t.Fatalf("stored %d", n)
	}
	// dedupe survives a sequencer restart (state reloaded from the store)
	c.seq.Close()
	if r := post(t, srv.URL, e); r.Duplicates != 1 {
		t.Fatalf("after restart: %+v", r)
	}
}

func TestGaplessSeqConcurrent(t *testing.T) {
	c, srv := newC(t, 20, "")
	const workers, per = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				e := ev(fmt.Sprintf("s%d-%d", w, i), core.KindDBRead, uint64(w*1000+i), uint64(i+1))
				e.Instance = fmt.Sprintf("inst%d", w)
				post(t, srv.URL, e)
			}
		}()
	}
	wg.Wait()
	if _, err := c.seq.Flush(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	got := events(t, c)
	if len(got) != workers*per {
		t.Fatalf("got %d", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Fatalf("gap at %d: seq %d", i, e.Seq)
		}
	}
}

func TestLedgerAndOrphans(t *testing.T) {
	c, srv := newC(t, 0, "")
	post(t, srv.URL, ev("pub1", core.KindQueuePub, 1, 1), ev("pub2", core.KindQueuePub, 2, 2))
	var ds DrainStatus
	get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
	if ds.Pending != 2 || ds.EventCount != 2 {
		t.Fatalf("drain %+v", ds)
	}
	w := ev("w1", core.KindWorkerEnd, 3, 3)
	w.LinkIDs = []string{"pub1"}
	post(t, srv.URL, w)
	get(t, srv.URL+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
	if ds.Pending != 1 {
		t.Fatalf("drain %+v", ds)
	}
	res, err := c.Seal(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Incomplete || len(res.Orphaned) != 1 || res.Orphaned[0] != "pub2" {
		t.Fatalf("seal %+v", res)
	}
	var out struct {
		Events []core.Event `json:"events"`
		Sealed bool         `json:"sealed"`
	}
	get(t, srv.URL+"/v1/sessions/"+sid+"/events", &out)
	if !out.Sealed || len(out.Events[1].Flags) != 1 || out.Events[1].Flags[0] != "orphaned" {
		t.Fatalf("orphan flag missing: %+v", out)
	}
}

func TestWorkerBeforePublisher(t *testing.T) {
	c, _ := newC(t, 0, "")
	w := ev("w1", core.KindWorkerEnd, 1, 1)
	w.LinkIDs = []string{"pub1"}
	_, _ = c.Ingest(context.Background(), "", []core.Event{w})
	_, _ = c.Ingest(context.Background(), "", []core.Event{ev("pub1", core.KindQueuePub, 2, 2)})
	res, _ := c.Seal(context.Background(), sid)
	if res.Incomplete || len(res.Orphaned) != 0 {
		t.Fatalf("%+v", res)
	}
}

func TestTxEffective(t *testing.T) {
	c, srv := newC(t, 0, "")
	mk := func(span, tx string, hlc uint64) core.Event {
		e := ev(span, core.KindDBWrite, hlc, hlc)
		e.TxID = tx
		e.Mutations = []core.Mutation{{Store: "postgres", Entity: "orders", PK: map[string]any{"id": hlc}, Op: "insert"}}
		return e
	}
	cm, rb := ev("c", core.KindDBTxCommit, 3, 3), ev("r", core.KindDBTxRollbck, 6, 6)
	cm.TxID, rb.TxID = "tx1", "tx2"
	free := ev("f", core.KindDBWrite, 7, 7)
	free.Mutations = []core.Mutation{{Store: "redis", Entity: "k", Op: "set"}}
	post(t, srv.URL, mk("w1", "tx1", 1), cm, mk("w2", "tx2", 4), rb, free, mk("w3", "tx3", 8))

	eff := func() map[string]string {
		m := map[string]string{}
		for _, e := range mustView(t, c) {
			for _, mu := range e.Mutations {
				s := "nil"
				if mu.Effective != nil {
					s = fmt.Sprint(*mu.Effective)
				}
				m[e.SpanID] = s
			}
		}
		return m
	}
	m := eff()
	if m["w1"] != "true" || m["w2"] != "false" || m["f"] != "true" || m["w3"] != "nil" {
		t.Fatalf("live: %v", m)
	}
	if _, err := c.Seal(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	if m = eff(); m["w3"] != "false" {
		t.Fatalf("sealed: %v", m)
	}
	// stored events are untouched
	for _, e := range events(t, c) {
		for _, mu := range e.Mutations {
			if mu.Effective != nil {
				t.Fatal("store was mutated")
			}
		}
	}
}

func mustView(t *testing.T, c *Collector) []core.Event {
	t.Helper()
	v, err := c.view(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestTraceTreeWithLinkedWorker(t *testing.T) {
	c, srv := newC(t, 0, "")
	api := ev("api", core.KindAPIIn, 10, 1)
	db := ev("db", core.KindDBWrite, 5, 2)
	db.ParentID = "api"
	pub := ev("pub", core.KindQueuePub, 6, 3)
	pub.ParentID = "api"
	ws := ev("ws", core.KindWorkerStart, 7, 4)
	ws.ParentID, ws.LinkIDs = "pub", []string{"pub"} // BeginIngress sets both
	we := ev("we", core.KindWorkerEnd, 8, 5)
	we.ParentID, we.LinkIDs = "ws", []string{"pub"}
	post(t, srv.URL, api, db, pub, ws, we)
	_ = c

	var tr Trace
	get(t, srv.URL+"/v1/sessions/"+sid+"/trace", &tr)
	if len(tr.Roots) != 1 || tr.Roots[0].Event.SpanID != "api" {
		t.Fatalf("roots: %+v", tr.Roots)
	}
	kids := tr.Roots[0].Children
	if len(kids) != 2 || kids[0].Event.SpanID != "db" || kids[1].Event.SpanID != "pub" {
		t.Fatalf("children: %+v", kids)
	}
	pubN := kids[1]
	if len(pubN.Children) != 0 || len(pubN.Linked) != 1 || pubN.Linked[0].Event.SpanID != "ws" {
		t.Fatalf("pub node: children=%d linked=%d", len(pubN.Children), len(pubN.Linked))
	}
	if ch := pubN.Linked[0].Children; len(ch) != 1 || ch[0].Event.SpanID != "we" {
		t.Fatalf("worker nesting: %+v", ch)
	}
}

func TestEntitiesAndSummary(t *testing.T) {
	_, srv := newC(t, 0, "")
	e1 := ev("w1", core.KindDBWrite, 1, 1)
	e1.Mutations = []core.Mutation{{Store: "postgres", Entity: "orders", PK: map[string]any{"id": 42}, Op: "insert",
		After: core.PayloadRef{Inline: json.RawMessage(`{"s":"new"}`)}}}
	e2 := ev("w2", core.KindDBWrite, 2, 2)
	e2.Err = "boom"
	e2.Mutations = []core.Mutation{{Store: "postgres", Entity: "orders", PK: map[string]any{"id": 42}, Op: "update",
		Diff: []core.FieldChange{{Field: "s", Old: "new", New: "paid"}}}}
	post(t, srv.URL, e1, e2)

	var ents []Entity
	get(t, srv.URL+"/v1/sessions/"+sid+"/entities", &ents)
	if len(ents) != 1 || ents[0].MutationCount != 2 || ents[0].PK != `{"id":42}` {
		t.Fatalf("%+v", ents)
	}
	var h EntityHistory
	get(t, srv.URL+"/v1/sessions/"+sid+"/entities/postgres/orders/42", &h)
	if len(h.Mutations) != 2 || h.Mutations[0].Seq != 1 || h.Mutations[1].Op != "update" || len(h.Mutations[1].Diff) != 1 {
		t.Fatalf("%+v", h)
	}
	var s Summary
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &s)
	if s.EventCount != 2 || s.ByKind["db.write"] != 2 || s.Mutations["postgres"]["orders"] != 2 || len(s.Failures) != 1 {
		t.Fatalf("%+v", s)
	}
	var list []store.SessionInfo
	get(t, srv.URL+"/v1/sessions", &list)
	if len(list) != 1 || list[0].ID != sid || list[0].EventCount != 2 {
		t.Fatalf("%+v", list)
	}
}

func TestSealBundleHash(t *testing.T) {
	dir := t.TempDir()
	c, srv := newC(t, 20, dir)
	post(t, srv.URL, ev("b", core.KindAPIIn, 2, 2), ev("a", core.KindAPIIn, 1, 1))
	resp, err := http.Post(srv.URL+"/v1/internal/sessions/"+sid+"/seal", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var res SealResult
	_ = json.NewDecoder(resp.Body).Decode(&res)
	resp.Body.Close()
	if res.Incomplete || res.Orphaned == nil || !strings.HasPrefix(res.BundleURI, "file://") {
		t.Fatalf("%+v", res)
	}
	bdir := strings.TrimPrefix(res.BundleURI, "file://")
	data, err := os.ReadFile(filepath.Join(bdir, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	var man Manifest
	mb, _ := os.ReadFile(filepath.Join(bdir, "manifest.json"))
	if err := json.Unmarshal(mb, &man); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if man.Files[0].SHA256 != hex.EncodeToString(sum[:]) || man.EventCount != 2 || bytes.Count(data, []byte("\n")) != 2 {
		t.Fatalf("manifest mismatch: %+v", man)
	}
	if !strings.Contains(strings.SplitN(string(data), "\n", 2)[0], `"span_id":"a"`) {
		t.Fatal("bundle not in seq order")
	}
	// idempotent
	if r2, _ := c.Seal(context.Background(), sid); r2.BundleURI != res.BundleURI {
		t.Fatalf("re-seal differs: %+v", r2)
	}
}

func TestIngestLimitsAndValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window, cfg.MaxBodyBytes, cfg.MaxLineBytes = 0, 2048, 512
	c := New(store.NewMem(), cfg)
	defer c.Close()
	srv := httptest.NewServer(c.Handler())
	defer srv.Close()

	big := ev("x", core.KindAPIIn, 1, 1)
	big.Name = strings.Repeat("a", 1000)
	b, _ := json.Marshal(big)
	resp, _ := http.Post(srv.URL+"/v1/ingest", "", bytes.NewReader(append(b, '\n')))
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("long line: %d", resp.StatusCode)
	}
	resp, _ = http.Post(srv.URL+"/v1/ingest", "", strings.NewReader(strings.Repeat("{}\n", 2000)))
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("big body: %d", resp.StatusCode)
	}
	bad := ev("y", core.KindAPIIn, 1, 1)
	bad.SessionID = "../etc"
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(bad)
	buf.WriteString("not json\n")
	resp, _ = http.Post(srv.URL+"/v1/ingest", "", &buf)
	var r IngestResult
	_ = json.NewDecoder(resp.Body).Decode(&r)
	resp.Body.Close()
	if r.Accepted != 0 || r.Rejected != 2 {
		t.Fatalf("%+v", r)
	}
}
