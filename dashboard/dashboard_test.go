package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func get(t *testing.T, base, path string) (*http.Response, []byte) {
	t.Helper()
	r, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r, b
}

func TestProxyRoutingAndHeaderStripping(t *testing.T) {
	type seen struct {
		path, rawPath, query string
		hdr                  http.Header
	}
	var cpSeen, colSeen seen
	mk := func(dst *seen) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*dst = seen{r.URL.Path, r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Clone()}
			w.Header().Set("Set-Cookie", "sid=1")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true}`)
		}))
	}
	cp, col := mk(&cpSeen), mk(&colSeen)
	defer cp.Close()
	defer col.Close()
	h, err := New(Config{ControlPlaneURL: cp.URL, CollectorURL: col.URL})
	if err != nil {
		t.Fatal(err)
	}
	d := httptest.NewServer(h)
	defer d.Close()

	req, _ := http.NewRequest("GET", d.URL+"/api/cp/v1/sessions?state=ACTIVE", nil)
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("Authorization", "Bearer synthetic")
	req.Header.Set("X-Custom", "keep")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if cpSeen.path != "/v1/sessions" || cpSeen.query != "state=ACTIVE" {
		t.Fatalf("cp routing: %+v", cpSeen)
	}
	if cpSeen.hdr.Get("Cookie") != "" || cpSeen.hdr.Get("Authorization") != "" {
		t.Fatalf("credentials forwarded: %v", cpSeen.hdr)
	}
	if cpSeen.hdr.Get("X-Custom") != "keep" {
		t.Fatal("ordinary headers should pass")
	}
	if resp.Header.Get("Set-Cookie") != "" || resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("response headers leaked: %v", resp.Header)
	}

	// collector + escaped path segments preserved
	r, _ := get(t, d.URL, "/api/col/v1/sessions/ts_1/entities/redis/order:%2A/a%2Fb")
	if r.StatusCode != 200 || colSeen.path != "/v1/sessions/ts_1/entities/redis/order:*/a/b" || !strings.HasSuffix(colSeen.rawPath, "/a%2Fb") {
		t.Fatalf("col routing: %+v", colSeen)
	}
	cpSeen = seen{}
	// blocked endpoints
	for _, p := range []string{"/api/cp/v1/kill", "/api/col/v1/internal/sessions/x/seal", "/api/cp/v1/internal/x"} {
		if r, _ := get(t, d.URL, p); r.StatusCode != 403 {
			t.Fatalf("%s should be 403, got %d", p, r.StatusCode)
		}
	}
	if cpSeen.path != "" {
		t.Fatal("blocked request reached upstream")
	}
	// cross-origin POST refused; DELETE refused
	req, _ = http.NewRequest("POST", d.URL+"/api/cp/v1/sessions", strings.NewReader("{}"))
	req.Header.Set("Origin", "http://evil.example")
	if resp, _ = http.DefaultClient.Do(req); resp.StatusCode != 403 {
		t.Fatalf("cross-origin POST: %d", resp.StatusCode)
	}
	// DELETE is refused everywhere except a single session on the control plane
	// (see TestProxyDeleteOnlyDeletesSession).
	for _, p := range []string{"/api/cp/v1/sessions/x/stop", "/api/col/v1/sessions/x", "/api/cp/v1/active"} {
		req, _ = http.NewRequest("DELETE", d.URL+p, nil)
		if resp, _ = http.DefaultClient.Do(req); resp.StatusCode != 405 {
			t.Fatalf("DELETE %s: %d", p, resp.StatusCode)
		}
	}
	// bad Host (DNS rebinding)
	req, _ = http.NewRequest("GET", d.URL+"/", nil)
	req.Host = "attacker.example"
	if resp, _ = http.DefaultClient.Do(req); resp.StatusCode != 403 {
		t.Fatalf("foreign Host: %d", resp.StatusCode)
	}
}

func TestProxyUpstreamDown(t *testing.T) {
	h, _ := New(Config{ControlPlaneURL: "http://127.0.0.1:1", CollectorURL: "http://127.0.0.1:1"})
	d := httptest.NewServer(h)
	defer d.Close()
	if r, _ := get(t, d.URL, "/api/cp/v1/sessions"); r.StatusCode != 502 {
		t.Fatalf("want 502, got %d", r.StatusCode)
	}
	if _, err := New(Config{ControlPlaneURL: "not a url"}); err == nil {
		t.Fatal("expected URL validation error")
	}
}

func TestSPAServed(t *testing.T) {
	h, _ := New(Config{Demo: true})
	d := httptest.NewServer(h)
	defer d.Close()
	for path, ct := range map[string]string{"/": "text/html", "/app.js": "text/javascript", "/style.css": "text/css"} {
		r, b := get(t, d.URL, path)
		if r.StatusCode != 200 || !strings.HasPrefix(r.Header.Get("Content-Type"), ct) || len(b) == 0 {
			t.Fatalf("%s: %d %q", path, r.StatusCode, r.Header.Get("Content-Type"))
		}
		if r.Header.Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(r.Header.Get("Content-Security-Policy"), "default-src 'none'") {
			t.Fatalf("%s: missing security headers", path)
		}
	}
}

var urlRe = regexp.MustCompile(`(?i)(?:https?:)?//[^\s"'<>)]+`)

func TestNoExternalURLsInWebAssets(t *testing.T) {
	n := 0
	err := fs.WalkDir(WebFS(), ".", func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		n++
		b, _ := fs.ReadFile(WebFS(), p)
		src := string(b)
		for _, u := range urlRe.FindAllString(src, -1) {
			l := strings.ToLower(u)
			if strings.Contains(l, "localhost") || strings.Contains(l, "127.0.0.1") {
				continue
			}
			t.Errorf("%s: external URL %q", p, u)
		}
		if strings.HasSuffix(p, ".js") {
			for _, bad := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function"} {
				if strings.Contains(src, bad) {
					t.Errorf("%s: forbidden construct %s", p, bad)
				}
			}
		}
		return nil
	})
	if err != nil || n < 3 {
		t.Fatalf("walk: %v (%d files)", err, n)
	}
}

func demoServer(t *testing.T, now func() time.Time) *httptest.Server {
	d := newDemoAt(now)
	mux := http.NewServeMux()
	mux.Handle("/cp/", http.StripPrefix("/cp", d.ControlPlane()))
	mux.Handle("/col/", http.StripPrefix("/col", d.Collector()))
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func decode(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode: %v\n%s", err, b)
	}
}

type eventsResp = client.EventPage

// Node is one span of the trace tree, as the collector answers it.
type Node = client.TraceNode

func TestDemoEndpointShapes(t *testing.T) {
	s := demoServer(t, time.Now)
	var sessions []Session
	_, b := get(t, s.URL, "/cp/v1/sessions")
	decode(t, b, &sessions)
	if len(sessions) != 5 {
		t.Fatalf("sessions: %d", len(sessions))
	}
	states := map[string]bool{}
	for _, x := range sessions {
		states[string(x.State)] = true
	}
	if !states["ACTIVE"] || !states["SEALED"] {
		t.Fatalf("states: %v", states)
	}
	var one Session
	_, b = get(t, s.URL, "/cp/v1/sessions/ts_demo0001")
	decode(t, b, &one)
	if one.ID != "ts_demo0001" {
		t.Fatal(one)
	}
	if r, _ := get(t, s.URL, "/cp/v1/sessions/nope"); r.StatusCode != 404 {
		t.Fatal("want 404")
	}
	var col []map[string]any
	_, b = get(t, s.URL, "/col/v1/sessions")
	decode(t, b, &col)
	if len(col) != 5 || col[0]["event_count"] == nil {
		t.Fatalf("collector list: %v", col)
	}

	var er eventsResp
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/events?from_seq=0&limit=1000")
	decode(t, b, &er)
	// from_seq is inclusive (0 reads from the first event) and next_seq is the
	// from_seq of the page after it, as on the collector.
	if len(er.Events) < 30 || !er.Sealed || er.Events[0].Seq != 1 || er.NextSeq != er.Events[len(er.Events)-1].Seq+1 {
		t.Fatalf("events: n=%d next=%d sealed=%v", len(er.Events), er.NextSeq, er.Sealed)
	}
	var tail eventsResp
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/events?from_seq=10&limit=5")
	decode(t, b, &tail)
	if len(tail.Events) != 5 || tail.Events[0].Seq != 10 || tail.NextSeq != 15 {
		t.Fatalf("paging: first %d next %d", tail.Events[0].Seq, tail.NextSeq)
	}
	// Past the end: no events, and next_seq stays where the request was.
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/events?from_seq=9999")
	if strings.TrimSpace(string(b)) != `{"events":[],"next_seq":9999,"sealed":true}` {
		t.Fatalf("page past the end: %s", b)
	}
	// A session the collector does not hold reads as an empty one, not as 404.
	if r, b := get(t, s.URL, "/col/v1/sessions/ts_unknown/events"); r.StatusCode != 200 || strings.TrimSpace(string(b)) != `{"events":[],"next_seq":1,"sealed":false}` {
		t.Fatalf("events of an unknown session: %d %s", r.StatusCode, b)
	}

	var trace struct {
		SessionID string  `json:"session_id"`
		Roots     []*Node `json:"roots"`
	}
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/trace")
	decode(t, b, &trace)
	if trace.SessionID != "ts_demo0001" || len(trace.Roots) == 0 {
		t.Fatal("trace shape")
	}
	var ents []client.Entity
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities")
	decode(t, b, &ents)
	if len(ents) == 0 {
		t.Fatal("no entities")
	}
	// The history is the collector's object, with the key in canonical form.
	var hist client.EntityHistory
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities/postgres/orders/ord_1001")
	decode(t, b, &hist)
	if hm := hist.Mutations; hist.PK != `{"id":"ord_1001"}` || hist.Store != "postgres" || hist.Entity != "orders" || len(hm) != 2 ||
		hm[0].Seq >= hm[1].Seq || hm[0].Op != "insert" || hm[1].Op != "update" || hm[0].Kind != core.KindDBWrite || hm[0].EndNs == 0 {
		t.Fatalf("orders history: %s", b)
	}
	// The summary is the collector's: failures are objects, mutations a map
	// of store to entity to count.
	var typed client.Summary
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/summary")
	strict := json.NewDecoder(bytes.NewReader(b))
	strict.DisallowUnknownFields()
	if err := strict.Decode(&typed); err != nil {
		t.Fatalf("summary is not the collector's: %v\n%s", err, b)
	}
	if !typed.Sealed || len(typed.Failures) == 0 || typed.Failures[0].Err == "" || typed.Mutations["postgres"]["orders"] != 2 {
		t.Fatalf("summary: %s", b)
	}
	var sum map[string]any
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/summary")
	decode(t, b, &sum)
	for _, k := range []string{"by_kind", "by_service", "mutations", "failures", "pending_async", "dropped_events", "duration_ns"} {
		if _, ok := sum[k]; !ok {
			t.Errorf("summary missing %s", k)
		}
	}
	// The collector reports a count plus the ids; the demo must use the same shape.
	if n, ok := sum["pending_async"].(float64); !ok || n != 0 {
		t.Errorf("pending_async must be a number, 0 for the sealed demo session: %#v", sum["pending_async"])
	}
	if ids, ok := sum["pending_async_ids"].([]any); !ok || len(ids) != 0 {
		t.Errorf("pending_async_ids must be a list, empty for the sealed demo session: %#v", sum["pending_async_ids"])
	}
}

func TestDemoStartStop(t *testing.T) {
	cur := time.Now()
	s := demoServer(t, func() time.Time { return cur })
	post := func(path, body string) (*http.Response, []byte) {
		r, err := http.Post(s.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r, b
	}
	if r, _ := post("/cp/v1/sessions", `{"name":"x","mode":"exclusive"}`); r.StatusCode != 409 {
		t.Fatalf("exclusive conflict expected, got %d", r.StatusCode)
	}
	r, b := post("/cp/v1/sessions", `{"name":"mine","mode":"header"}`)
	var ns Session
	decode(t, b, &ns)
	if r.StatusCode != 201 || ns.State != "ACTIVE" || ns.InitiatedBy != "dashboard" {
		t.Fatalf("start: %d %+v", r.StatusCode, ns)
	}
	post("/cp/v1/sessions/"+ns.ID+"/stop", "")
	var got Session
	_, b = get(t, s.URL, "/cp/v1/sessions/"+ns.ID)
	decode(t, b, &got)
	if got.State != "DRAINING" {
		t.Fatalf("state %s", got.State)
	}
	cur = cur.Add(5 * time.Second)
	_, b = get(t, s.URL, "/cp/v1/sessions/"+ns.ID)
	decode(t, b, &got)
	if got.State != "SEALED" || got.EndedAt.IsZero() {
		t.Fatalf("state %s", got.State)
	}
}

func TestDemoLiveTailGrows(t *testing.T) {
	cur := time.Now()
	s := demoServer(t, func() time.Time { return cur })
	n := func() int {
		var er eventsResp
		_, b := get(t, s.URL, "/col/v1/sessions/ts_demo0002/events?from_seq=0")
		decode(t, b, &er)
		return len(er.Events)
	}
	a := n()
	cur = cur.Add(6 * time.Second)
	if b := n(); b <= a {
		t.Fatalf("live session did not grow: %d -> %d", a, b)
	}
}

func flatten(ns []*Node, out *[]*Node) {
	for _, n := range ns {
		*out = append(*out, n)
		flatten(n.Children, out)
		flatten(n.Linked, out)
	}
}

func TestDemoDataInvariants(t *testing.T) {
	base := time.Now()
	evs := demoEvents("ts_x", base)
	if len(evs) < 30 {
		t.Fatalf("expected a rich session, got %d events", len(evs))
	}
	spans := map[string]core.Event{}
	locals := map[string]bool{}
	kinds := map[core.Kind]int{}
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			t.Fatalf("seq not gapless at %d: %d", i, e.Seq)
		}
		if i > 0 && e.EndHLC < evs[i-1].EndHLC {
			t.Fatalf("seq %d not in completion order", e.Seq)
		}
		if e.EndNs < e.StartNs {
			t.Fatalf("seq %d ends before it starts", e.Seq)
		}
		if _, dup := spans[e.SpanID]; dup {
			t.Fatalf("duplicate span %s", e.SpanID)
		}
		spans[e.SpanID] = e
		k := e.Instance + "/" + string(rune(e.LocalSeq))
		if locals[k] {
			t.Fatalf("dup idempotency key %s", k)
		}
		locals[k] = true
		kinds[e.Kind]++
	}
	for _, k := range []core.Kind{core.KindAPIIn, core.KindAPIOut, core.KindDBRead, core.KindDBWrite, core.KindDBTxBegin, core.KindDBTxCommit,
		core.KindDBTxRollbck, core.KindCacheRead, core.KindCacheWrite, core.KindQueuePub, core.KindWorkerStart, core.KindWorkerEnd,
		core.KindWorkerRetry, core.KindWorkerDLQ} {
		if kinds[k] == 0 {
			t.Errorf("demo lacks kind %s", k)
		}
	}
	if kinds[core.KindWorkerRetry] != 2 || kinds[core.KindWorkerDLQ] != 1 {
		t.Errorf("retry/dlq counts: %d/%d", kinds[core.KindWorkerRetry], kinds[core.KindWorkerDLQ])
	}

	evs = resolve(evs, true, nil)
	roots := buildTrace("ts_x", evs).Roots
	var all []*Node
	flatten(roots, &all)
	if len(all) != len(evs) {
		t.Fatalf("tree has %d nodes for %d events (cycle or loss)", len(all), len(evs))
	}
	seen := map[string]bool{}
	var check func(n *Node, parent *Node, linked bool)
	check = func(n *Node, parent *Node, linked bool) {
		e := n.Event
		if seen[e.SpanID] {
			t.Fatalf("span %s appears twice in tree", e.SpanID)
		}
		seen[e.SpanID] = true
		if parent != nil {
			p := parent.Event
			if linked {
				found := false
				for _, l := range e.LinkIDs {
					found = found || l == p.SpanID
				}
				if !found || e.Kind != core.KindWorkerStart || p.Kind != core.KindQueuePub {
					t.Errorf("bad linked node %s under %s", e.SpanID, p.SpanID)
				}
			} else {
				if e.ParentID != p.SpanID {
					t.Errorf("child %s has parent %s, nested under %s", e.SpanID, e.ParentID, p.SpanID)
				}
				if e.StartNs < p.StartNs || e.EndNs > p.EndNs {
					t.Errorf("child %s [%d,%d] not within parent %s [%d,%d]", e.SpanID, e.StartNs, e.EndNs, p.SpanID, p.StartNs, p.EndNs)
				}
			}
		} else if e.ParentID != "" {
			t.Errorf("root %s has parent %s", e.SpanID, e.ParentID)
		}
		for i := 1; i < len(n.Children); i++ {
			if n.Children[i].Event.StartNs < n.Children[i-1].Event.StartNs {
				t.Errorf("children of %s not ordered by start", e.SpanID)
			}
		}
		for _, c := range n.Children {
			check(c, n, false)
		}
		for _, c := range n.Linked {
			check(c, n, true)
		}
	}
	for _, r := range roots {
		check(r, nil, false)
	}
	// 1 api root + 4 worker attempts
	if len(roots) != 1 || roots[0].Event.Kind != core.KindAPIIn {
		t.Fatalf("expected a single api.in root, got %d", len(roots))
	}
	linked := 0
	for _, n := range all {
		if n.Event.Kind == core.KindQueuePub {
			if len(n.Linked) != 2 {
				t.Errorf("publish %s should link 2 worker attempts, has %d", n.Event.Name, len(n.Linked))
			}
			linked += len(n.Linked)
		}
	}
	if linked != 4 {
		t.Errorf("linked worker spans: %d", linked)
	}

	// tx + effective invariants; every publish closed by a worker.end
	begun, ended := map[string]bool{}, map[string]core.Kind{}
	closed := map[string]bool{}
	for _, e := range evs {
		switch e.Kind {
		case core.KindDBTxBegin:
			begun[e.TxID] = true
		case core.KindDBTxCommit, core.KindDBTxRollbck:
			ended[e.TxID] = e.Kind
		case core.KindWorkerEnd:
			for _, l := range e.LinkIDs {
				closed[l] = true
			}
		}
	}
	for id := range ended {
		if !begun[id] {
			t.Errorf("tx %s ended without begin", id)
		}
	}
	rolled := 0
	for _, e := range evs {
		if e.Kind == core.KindQueuePub && !closed[e.SpanID] {
			t.Errorf("publish %s not closed", e.SpanID)
		}
		for _, m := range e.Mutations {
			if m.Effective == nil {
				t.Errorf("seq %d mutation unresolved", e.Seq)
				continue
			}
			want := e.TxID == "" || ended[e.TxID] == core.KindDBTxCommit
			if *m.Effective != want {
				t.Errorf("seq %d effective=%v want %v", e.Seq, *m.Effective, want)
			}
			if !*m.Effective {
				rolled++
			}
		}
	}
	if rolled != 1 {
		t.Errorf("expected exactly one rolled-back mutation, got %d", rolled)
	}
}

func TestPrefixSessionsAreConsistent(t *testing.T) {
	d := newDemoAt(time.Now)
	for _, s := range d.sess {
		_, cv := d.view(s)
		evs := cv.evs
		for i, e := range evs {
			if e.Seq != uint64(i+1) {
				t.Fatalf("%s: seq gap at %d", s.ID, i)
			}
		}
		var all []*Node
		flatten(buildTrace(s.ID, evs).Roots, &all)
		if len(all) != len(evs) {
			t.Fatalf("%s: tree drops events", s.ID)
		}
	}
}

func TestProxySendsUpstreamHostNoOriginAndJSONContentType(t *testing.T) {
	type seen struct {
		host string
		hdr  http.Header
	}
	ch := make(chan seen, 4)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch <- seen{r.Host, r.Header.Clone()}
		io.WriteString(w, `{}`)
	}))
	defer up.Close()
	h, err := New(Config{ControlPlaneURL: up.URL, CollectorURL: up.URL})
	if err != nil {
		t.Fatal(err)
	}
	d := httptest.NewServer(h)
	defer d.Close()
	upHost := strings.TrimPrefix(up.URL, "http://")

	do := func(method, path, ct, origin string) seen {
		req, _ := http.NewRequest(method, d.URL+path, nil)
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return <-ch
	}
	// empty-body POST without a content type, same-origin Origin sent by the browser
	s := do("POST", "/api/cp/v1/sessions/ts_x/stop", "", d.URL)
	if s.host != upHost {
		t.Errorf("Host = %q, want upstream %q", s.host, upHost)
	}
	if s.hdr.Get("Origin") != "" || s.hdr.Get("Referer") != "" {
		t.Errorf("Origin/Referer forwarded: %v", s.hdr)
	}
	if got := s.hdr.Get("Content-Type"); got != "application/json" {
		t.Errorf("POST Content-Type = %q", got)
	}
	// a wrong content type is normalised too
	if s = do("POST", "/api/col/v1/x", "text/plain", ""); s.hdr.Get("Content-Type") != "application/json" {
		t.Errorf("POST text/plain not normalised: %q", s.hdr.Get("Content-Type"))
	}
	// GET must not gain a content type
	if s = do("GET", "/api/col/v1/sessions", "", ""); s.hdr.Get("Content-Type") != "" || s.host != upHost {
		t.Errorf("GET: host=%q ct=%q", s.host, s.hdr.Get("Content-Type"))
	}
}

func TestDemoEntityPKIsCanonicalJSONString(t *testing.T) {
	s := demoServer(t, time.Now)
	defer s.Close()
	_, b := get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities")
	var ents []struct {
		Store, Entity, PK string
	}
	if err := json.Unmarshal(b, &ents); err != nil {
		var w struct {
			Entities []struct{ Store, Entity, PK string }
		}
		decode(t, b, &w)
		ents = w.Entities
	}
	if len(ents) == 0 {
		t.Fatal("no entities")
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.PK, "{") {
			t.Fatalf("pk %q is not canonical JSON", e.PK)
		}
		resp, hb := get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities/"+e.Store+"/"+e.Entity+"/"+url.PathEscape(e.PK))
		if resp.StatusCode != 200 || len(hb) < 3 || string(hb) == "[]" {
			t.Fatalf("history for %s: %d %s", e.PK, resp.StatusCode, hb)
		}
	}
}

func TestJSHelpers(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	if out, err := exec.Command(node, "jstest/util_test.js").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if out, err := exec.Command(node, "--check", "web/app.js").CombinedOutput(); err != nil {
		t.Fatalf("app.js syntax: %v\n%s", err, out)
	}
}

// The demo's HTTP events carry what the HTTP wrapper records (thttp's message:
// method, url, headers, body, body_size; status on the response), which is
// also what curlFor in web/util.js needs to offer "Copy as curl".
func TestDemoHTTPEventsHaveRecordedShape(t *testing.T) {
	evs := append(demoEvents("ts_x", time.Now()), activityEvents("ts_y", time.Now())...)
	decode := func(e core.Event, what string, p core.PayloadRef) (msg httpMsg) {
		t.Helper()
		dec := json.NewDecoder(strings.NewReader(string(p.Inline)))
		dec.DisallowUnknownFields() // a field thttp does not record, such as the old "path"
		if err := dec.Decode(&msg); err != nil {
			t.Fatalf("%s %s: %s is not a recorded HTTP message: %v\n%s", e.Kind, e.Name, what, err, p.Inline)
		}
		if p.Redacted != strings.Contains(string(p.Inline), core.RedactedValue) {
			t.Errorf("%s %s: %s redacted=%v does not match its content", e.Kind, e.Name, what, p.Redacted)
		}
		if msg.Body != nil {
			if b, _ := json.Marshal(msg.Body); msg.BodySize != len(b) {
				t.Errorf("%s %s: %s body_size %d, body is %d bytes", e.Kind, e.Name, what, msg.BodySize, len(b))
			}
		}
		return msg
	}
	method := regexp.MustCompile(`^[A-Za-z]+$`) // as curlFor requires
	var httpEvs []core.Event
	for _, e := range evs {
		if e.Kind != core.KindAPIIn && e.Kind != core.KindAPIOut {
			continue
		}
		httpEvs = append(httpEvs, e)
		req, resp := decode(e, "request", e.Req), decode(e, "response", e.Resp)
		u, err := url.Parse(req.URL)
		if err != nil || strings.ContainsFunc(req.URL, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
			t.Errorf("%s %s: bad url %q: %v", e.Kind, e.Name, req.URL, err)
			continue
		}
		if !method.MatchString(req.Method) || !strings.HasPrefix(e.Name, req.Method+" ") || !strings.HasSuffix(e.Name, u.Path) {
			t.Errorf("%s %s: the request is %q %q", e.Kind, e.Name, req.Method, req.URL)
		}
		masked := false
		for name, vs := range req.Headers {
			masked = masked || (len(vs) == 1 && vs[0] == core.RedactedValue)
			if http.CanonicalHeaderKey(name) != name || len(vs) == 0 {
				t.Errorf("%s %s: header %q %v is not as net/http holds it", e.Kind, e.Name, name, vs)
			}
		}
		if !masked {
			t.Errorf("%s %s: no request header is redacted: %v", e.Kind, e.Name, req.Headers)
		}
		if e.Kind == core.KindAPIOut {
			// An outbound call records the full URL, and the wrapper sets the host attribute.
			if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || e.Attrs["host"] != u.Host || req.Body == nil {
				t.Errorf("api.out %s: url %q, host attr %q, body %v", e.Name, req.URL, e.Attrs["host"], req.Body)
			}
		} else if u.IsAbs() || !strings.HasPrefix(req.URL, "/") {
			t.Errorf("api.in %s: url %q, want a path", e.Name, req.URL) // a server sees no scheme or host
		}
		if resp.Status < 100 || e.Attrs["http.status"] != strconv.Itoa(resp.Status) || len(resp.Headers) == 0 || resp.Body == nil {
			t.Errorf("%s %s: response status %d (attr %q), headers %v, body %v", e.Kind, e.Name, resp.Status, e.Attrs["http.status"], resp.Headers, resp.Body)
		}
		if resp.Method != "" || resp.URL != "" || req.Status != 0 {
			t.Errorf("%s %s: request and response fields are mixed up", e.Kind, e.Name)
		}
	}
	if len(httpEvs) != 8 {
		t.Fatalf("the demo has %d api.in/api.out events, want 8", len(httpEvs))
	}

	// The real curlFor accepts every one of them.
	node, err := exec.LookPath("node")
	if err != nil {
		t.Log("node not installed: curlFor itself was not run")
		return
	}
	const script = `
const U = require('./web/util.js');
const evs = JSON.parse(require('fs').readFileSync(0, 'utf8'));
for (const e of evs) {
  const c = U.curlFor(e), p = e.req.inline;
  const base = e.kind === 'api.out' ? '' : '"$BASE_URL"';
  if (!c || !c.command.startsWith('curl -X ' + p.method + ' ' + base + "'" + p.url + "'")) {
    console.error('curlFor(' + e.kind + ' ' + e.name + ') = ' + JSON.stringify(c));
    process.exit(1);
  }
  if (!c.notes.some((n) => n.includes('[REDACTED]'))) {
    console.error(e.kind + ' ' + e.name + ': no note about the redacted header: ' + JSON.stringify(c.notes));
    process.exit(1);
  }
}
console.log(evs.length);`
	in, _ := json.Marshal(httpEvs)
	cmd := exec.Command(node, "-e", script)
	cmd.Stdin = strings.NewReader(string(in))
	if out, err := cmd.CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "8" {
		t.Fatalf("curlFor on the demo events: %v\n%s", err, out)
	}
}

func TestDemoBindings(t *testing.T) {
	cur := time.Now()
	s := demoServer(t, func() time.Time { return cur })
	post := func(path, body string) (*http.Response, []byte) {
		r, err := http.Post(s.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r, b
	}
	// the seeded bound demo session is listed with its bindings and in /v1/active
	var act struct {
		Bindings []map[string]string `json:"bindings"`
	}
	_, b := get(t, s.URL, "/cp/v1/active")
	decode(t, b, &act)
	if len(act.Bindings) != 2 || act.Bindings[0]["session_id"] != "ts_demo0004" {
		t.Fatalf("active bindings: %+v", act)
	}
	in := `{"name":"b","mode":"header","bindings":[{"source":"header","name":"X-User-Id","value":"u1"}]}`
	r, b := post("/cp/v1/sessions", in)
	var ns Session
	decode(t, b, &ns)
	if r.StatusCode != 201 || len(ns.Bindings) != 1 || ns.Bindings[0].Value != "u1" {
		t.Fatalf("start: %d %s", r.StatusCode, b)
	}
	// same (source,name,value) with different header case => 409 naming the holder
	r, b = post("/cp/v1/sessions", `{"bindings":[{"source":"header","name":"x-user-id","value":"u1"}]}`)
	if r.StatusCode != 409 || !strings.Contains(string(b), ns.ID) {
		t.Fatalf("dup: %d %s", r.StatusCode, b)
	}
	for _, bad := range []string{
		`{"bindings":[{"source":"nope","name":"a","value":"b"}]}`,
		`{"bindings":[{"source":"header","name":"","value":"b"}]}`,
		`{"mode":"exclusive","bindings":[{"source":"header","name":"a","value":"b"}]}`,
	} {
		if r, _ := post("/cp/v1/sessions", bad); r.StatusCode != 400 {
			t.Fatalf("%s: %d", bad, r.StatusCode)
		}
	}
	// after stop the binding is released
	post("/cp/v1/sessions/"+ns.ID+"/stop", "")
	if r, _ := post("/cp/v1/sessions", in); r.StatusCode != 201 {
		t.Fatalf("rebind after stop: %d", r.StatusCode)
	}
}

func TestActivityDemoInvariants(t *testing.T) {
	evs := resolve(activityEvents("ts_x", time.Now()), true, nil)
	spans := map[string]core.Event{}
	for i, e := range evs {
		if e.Seq != uint64(i+1) || (i > 0 && e.EndHLC < evs[i-1].EndHLC) {
			t.Fatalf("seq %d not gapless/completion ordered", e.Seq)
		}
		spans[e.SpanID] = e
	}
	apiRoots, workers, orphans := 0, 0, 0
	for _, e := range evs {
		_, parentOK := spans[e.ParentID]
		switch {
		case e.Kind == core.KindAPIIn && !parentOK:
			apiRoots++
		case e.Kind == core.KindWorkerStart:
			workers++
			if _, ok := spans[e.LinkIDs[0]]; !ok {
				t.Errorf("worker link %v missing", e.LinkIDs)
			}
		case !parentOK:
			orphans++
		}
	}
	if apiRoots != 3 || workers != 1 || orphans != 1 {
		t.Fatalf("api triggers %d, workers %d, orphan roots %d", apiRoots, workers, orphans)
	}
	// the trigger completes after its children: the point of the grouped view
	if spans["sp_c_api"].Seq <= spans["sp_c_ins"].Seq {
		t.Error("api.in should complete after its db children")
	}
	rolled := 0
	for _, e := range evs {
		for _, m := range e.Mutations {
			if m.Effective != nil && !*m.Effective {
				rolled++
			}
		}
	}
	if rolled != 1 {
		t.Errorf("rolled back mutations: %d", rolled)
	}
	var all []*Node
	flatten(buildTrace("ts_x", evs).Roots, &all)
	if len(all) != len(evs) {
		t.Fatalf("tree has %d of %d", len(all), len(evs))
	}
}

func TestProxyPatchOnlyRenamesSession(t *testing.T) {
	var got []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path+" "+r.Header.Get("Content-Type")+" origin="+r.Header.Get("Origin"))
		io.WriteString(w, `{}`)
	}))
	defer up.Close()
	h, _ := New(Config{ControlPlaneURL: up.URL, CollectorURL: up.URL})
	d := httptest.NewServer(h)
	defer d.Close()
	patch := func(path, origin string) int {
		req, _ := http.NewRequest("PATCH", d.URL+path, strings.NewReader(`{"name":"x"}`))
		req.Header.Set("Content-Type", "text/plain")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if c := patch("/api/cp/v1/sessions/ts_1", d.URL); c != 200 || len(got) != 1 || got[0] != "PATCH /v1/sessions/ts_1 application/json origin=" {
		t.Fatalf("rename: %d %v", c, got)
	}
	for _, bad := range []string{"/api/cp/v1/sessions", "/api/cp/v1/sessions/", "/api/cp/v1/sessions/a/stop", "/api/cp/v1/internal/x", "/api/cp/v1/kill",
		"/api/cp/v1/sessions/..", "/api/col/v1/sessions/ts_1"} {
		if c := patch(bad, ""); c != 405 {
			t.Errorf("PATCH %s = %d, want 405", bad, c)
		}
	}
	if c := patch("/api/cp/v1/sessions/ts_1", "http://evil.example"); c != 403 {
		t.Errorf("cross-origin PATCH = %d", c)
	}
	if len(got) != 1 {
		t.Errorf("unexpected upstream calls: %v", got)
	}
}

func TestDemoRename(t *testing.T) {
	s := demoServer(t, time.Now)
	defer s.Close()
	patch := func(id, body string) (int, Session) {
		req, _ := http.NewRequest("PATCH", s.URL+"/cp/v1/sessions/"+id, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var out Session
		_ = json.NewDecoder(r.Body).Decode(&out)
		return r.StatusCode, out
	}
	c, got := patch("ts_demo0002", `{"name":"  my live run "}`)
	if c != 200 || got.Name != "my live run" || got.NameAuto {
		t.Fatalf("rename: %d %+v", c, got)
	}
	for _, bad := range []string{`{"name":""}`, `{"name":"   "}`, `{"name":"` + strings.Repeat("x", 121) + `"}`, `nope`} {
		if c, _ := patch("ts_demo0001", bad); c != 400 {
			t.Errorf("%q: %d", bad, c)
		}
	}
	if c, _ := patch("nope", `{"name":"a"}`); c != 404 {
		t.Errorf("unknown id: %d", c)
	}
	_, b := get(t, s.URL, "/cp/v1/sessions/ts_demo0002")
	var one Session
	decode(t, b, &one)
	if one.Name != "my live run" {
		t.Errorf("not persisted: %q", one.Name)
	}
}

func TestProxyDeleteOnlyDeletesSession(t *testing.T) {
	var got []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.Path+" origin="+r.Header.Get("Origin"))
		io.WriteString(w, `{"deleted":true}`)
	}))
	defer up.Close()
	h, _ := New(Config{ControlPlaneURL: up.URL, CollectorURL: up.URL})
	d := httptest.NewServer(h)
	defer d.Close()
	del := func(path, origin string) int {
		req, _ := http.NewRequest(http.MethodDelete, d.URL+path, nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if c := del("/api/cp/v1/sessions/ts_1", d.URL); c != 200 || len(got) != 1 || got[0] != "DELETE /v1/sessions/ts_1 origin=" {
		t.Fatalf("delete: %d %v", c, got)
	}
	// Nothing else is deletable through the dashboard: not the collector's
	// data directly, not internal endpoints, not collections.
	for _, bad := range []string{"/api/cp/v1/sessions", "/api/cp/v1/sessions/", "/api/cp/v1/sessions/a/stop", "/api/cp/v1/internal/sessions/x",
		"/api/cp/v1/kill", "/api/cp/v1/sessions/..", "/api/col/v1/sessions/ts_1", "/api/col/v1/internal/sessions/ts_1"} {
		if c := del(bad, ""); c != 405 {
			t.Errorf("DELETE %s = %d, want 405", bad, c)
		}
	}
	if c := del("/api/cp/v1/sessions/ts_1", "http://evil.example"); c != 403 {
		t.Errorf("cross-origin DELETE = %d", c)
	}
	if len(got) != 1 {
		t.Errorf("unexpected upstream calls: %v", got)
	}
}

func TestDemoDelete(t *testing.T) {
	s := demoServer(t, time.Now)
	defer s.Close()
	del := func(id string) int {
		req, _ := http.NewRequest(http.MethodDelete, s.URL+"/cp/v1/sessions/"+id, nil)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	list := func() map[string]string {
		r, err := http.Get(s.URL + "/cp/v1/sessions")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		var ss []Session
		_ = json.NewDecoder(r.Body).Decode(&ss)
		out := map[string]string{}
		for _, x := range ss {
			out[x.ID] = string(x.State)
		}
		return out
	}
	before := list()
	if before["ts_demo0001"] != "SEALED" || before["ts_demo0002"] != "ACTIVE" {
		t.Fatalf("demo fixture changed: %v", before)
	}
	if c := del("ts_demo0002"); c != 409 {
		t.Errorf("deleting a running session: %d, want 409", c)
	}
	if c := del("nope"); c != 404 {
		t.Errorf("unknown id: %d", c)
	}
	if c := del("ts_demo0001"); c != 200 {
		t.Fatalf("delete: %d", c)
	}
	after := list()
	if _, still := after["ts_demo0001"]; still || len(after) != len(before)-1 {
		t.Fatalf("after delete: %v", after)
	}
	if c := del("ts_demo0001"); c != 404 {
		t.Errorf("second delete: %d", c)
	}
}
