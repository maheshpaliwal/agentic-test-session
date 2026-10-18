package dashboard

import (
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
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
	req, _ = http.NewRequest("DELETE", d.URL+"/api/cp/v1/sessions/x", nil)
	if resp, _ = http.DefaultClient.Do(req); resp.StatusCode != 405 {
		t.Fatalf("DELETE: %d", resp.StatusCode)
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

type eventsResp struct {
	Events  []core.Event `json:"events"`
	NextSeq uint64       `json:"next_seq"`
	Sealed  bool         `json:"sealed"`
}

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
		states[x.State] = true
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
	if len(er.Events) < 30 || !er.Sealed || er.NextSeq != er.Events[len(er.Events)-1].Seq {
		t.Fatalf("events: n=%d next=%d sealed=%v", len(er.Events), er.NextSeq, er.Sealed)
	}
	var tail eventsResp
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/events?from_seq=10&limit=5")
	decode(t, b, &tail)
	if len(tail.Events) != 5 || tail.Events[0].Seq != 11 {
		t.Fatalf("paging: %+v", tail.NextSeq)
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
	var ents []EntityRef
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities")
	decode(t, b, &ents)
	if len(ents) == 0 {
		t.Fatal("no entities")
	}
	var hist []EntityMutation
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities/postgres/orders/ord_1001")
	decode(t, b, &hist)
	if len(hist) != 2 || hist[0].Seq >= hist[1].Seq || hist[0].Op != "insert" || hist[1].Op != "update" {
		t.Fatalf("orders history: %+v", hist)
	}
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/entities/postgres/loyalty_ledger/cus_synth_001,order_id=ord_1001")
	_ = b
	var sum map[string]any
	_, b = get(t, s.URL, "/col/v1/sessions/ts_demo0001/summary")
	decode(t, b, &sum)
	for _, k := range []string{"by_kind", "by_service", "mutations", "failures", "pending_async", "dropped_events", "duration_ns"} {
		if _, ok := sum[k]; !ok {
			t.Errorf("summary missing %s", k)
		}
	}
	if p, _ := sum["pending_async"].([]any); len(p) != 0 {
		t.Errorf("sealed demo session should have no pending async: %v", p)
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
	if got.State != "SEALED" || got.EndedAt == "" {
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

	evs = resolveTx(evs)
	roots := BuildTrace(evs)
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
		_, evs := d.view(s)
		for i, e := range evs {
			if e.Seq != uint64(i+1) {
				t.Fatalf("%s: seq gap at %d", s.ID, i)
			}
		}
		var all []*Node
		flatten(BuildTrace(evs), &all)
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
	evs := resolveTx(activityEvents("ts_x", time.Now()))
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
	flatten(BuildTrace(evs), &all)
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
