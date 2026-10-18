package thttp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"tsession/core"
)

func tok(payload string) string {
	e := base64.RawURLEncoding
	return e.EncodeToString([]byte(`{"alg":"none"}`)) + "." + e.EncodeToString([]byte(payload)) + ".sig"
}

func bound(bs ...core.Binding) core.StaticSessions {
	m := map[string]bool{}
	for _, b := range bs {
		m[b.SessionID] = true
	}
	return core.StaticSessions{Active: m, Bindings: bs}
}

func echoSession(seen *[]string, mu *sync.Mutex) http.Handler {
	return Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc, _ := core.FromContext(r.Context())
		mu.Lock()
		*seen = append(*seen, sc.SessionID)
		mu.Unlock()
	}))
}

func TestBindingTwoUsersConcurrentIsolated(t *testing.T) {
	rec := setup(t, bound(
		core.Binding{SessionID: "sA", Source: "jwt_claim", Name: "sub", Value: "alice"},
		core.Binding{SessionID: "sB", Source: "header", Name: "x-user-id", Value: "bob"},
	))
	var wrong atomic.Int32
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc, _ := core.FromContext(r.Context())
		if sc.SessionID != r.Header.Get("X-Expect") {
			wrong.Add(1)
		}
	}))
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/orders", nil)
			r.Header.Set("Authorization", "Bearer "+tok(`{"sub":"alice"}`))
			r.Header.Set("X-Expect", "sA")
			h.ServeHTTP(httptest.NewRecorder(), r)
		}()
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/orders", nil)
			r.Header.Set("X-User-Id", "bob")
			r.Header.Set("X-Expect", "sB")
			h.ServeHTTP(httptest.NewRecorder(), r)
		}()
	}
	wg.Wait()
	if wrong.Load() != 0 {
		t.Fatalf("%d crossover", wrong.Load())
	}
	count := map[string]int{}
	for _, e := range rec.Events() {
		count[e.SessionID]++
	}
	if count["sA"] != n || count["sB"] != n || len(count) != 2 {
		t.Fatalf("%v", count)
	}
}

func TestBindingSourcesAndBoundByAttr(t *testing.T) {
	rec := setup(t, bound(
		core.Binding{SessionID: "h", Source: "header", Name: "X-Dev", Value: "d1"},
		core.Binding{SessionID: "c", Source: "cookie", Name: "uid", Value: "c1"},
		core.Binding{SessionID: "q", Source: "query", Name: "uid", Value: "q1"},
		core.Binding{SessionID: "j", Source: "jwt_claim", Name: "sub", Value: "j1"},
	))
	var seen []string
	var mu sync.Mutex
	h := echoSession(&seen, &mu)
	do := func(mod func(r *http.Request)) {
		r := httptest.NewRequest("GET", "/x", nil)
		mod(r)
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	do(func(r *http.Request) { r.Header.Set("x-dev", "d1") })
	do(func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "uid", Value: "c1"}) })
	do(func(r *http.Request) { r.URL.RawQuery = "uid=q1" })
	do(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+tok(`{"sub":"j1"}`)) })
	if strings.Join(seen, ",") != "h,c,q,j" {
		t.Fatalf("%v", seen)
	}
	want := map[string]string{"h": "header:x-dev", "c": "cookie:uid", "q": "query:uid", "j": "jwt_claim:sub"}
	for _, e := range rec.Events() {
		if e.Attrs["bound_by"] != want[e.SessionID] {
			t.Fatalf("%s: %v", e.SessionID, e.Attrs)
		}
		for _, v := range e.Attrs {
			if v == "j1" || v == "c1" || v == "q1" || v == "d1" {
				t.Fatal("binding value leaked into attrs")
			}
		}
		if strings.Contains(string(e.Req.Inline), "eyJ") && !strings.Contains(string(e.Req.Inline), "REDACTED") {
			t.Fatalf("raw JWT recorded: %s", e.Req.Inline)
		}
	}
}

func TestBindingNoAttrAndJWTEdgeCases(t *testing.T) {
	rec := setup(t, bound(core.Binding{SessionID: "j", Source: "jwt_claim", Name: "sub", Value: "u1"}))
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	for name, authz := range map[string]string{
		"none":      "",
		"malformed": "Bearer not.a.jwt!",
		"huge":      "Bearer " + tok(`{"sub":"`+strings.Repeat("u", 20000)+`"}`),
		"missing":   "Bearer " + tok(`{"other":"u1"}`),
		"wrong":     "Bearer " + tok(`{"sub":"u2"}`),
		"basic":     "Basic " + tok(`{"sub":"u1"}`),
	} {
		r := httptest.NewRequest("GET", "/x", nil)
		if authz != "" {
			r.Header.Set("Authorization", authz)
		}
		h.ServeHTTP(httptest.NewRecorder(), r)
		if n := len(rec.Events()); n != 0 {
			t.Fatalf("%s: traced (%d events)", name, n)
		}
	}
	// alg:none with garbage signature is accepted: signature is not verified.
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("Authorization", "Bearer "+tok(`{"sub":"u1"}`))
	h.ServeHTTP(httptest.NewRecorder(), r)
	if evs := rec.Events(); len(evs) != 1 || evs[0].SessionID != "j" {
		t.Fatalf("%+v", evs)
	}
}

func TestBindingHeaderBeatsBinding(t *testing.T) {
	rec := setup(t, core.StaticSessions{
		Active:   map[string]bool{"hdr": true, "b": true},
		Bindings: []core.Binding{{SessionID: "b", Source: "header", Name: "x-user-id", Value: "u1"}},
	})
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("X-User-Id", "u1")
	r.Header.Set(core.KeySession, "hdr")
	h.ServeHTTP(httptest.NewRecorder(), r)
	evs := rec.Events()
	if len(evs) != 1 || evs[0].SessionID != "hdr" || evs[0].Attrs["bound_by"] != "" {
		t.Fatalf("%+v", evs)
	}
}

func TestBindingAfterStopUntracedInflightContinues(t *testing.T) {
	rec := setup(t, bound(core.Binding{SessionID: "b", Source: "header", Name: "x-user-id", Value: "u1"}))
	var ctxIn context.Context
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := core.FromContext(r.Context()); ok {
			ctxIn = r.Context()
		}
	}))
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("X-User-Id", "u1")
	h.ServeHTTP(httptest.NewRecorder(), r)
	// Stop: binding removed; session stays active (DRAINING) for propagated work.
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"b": true}})
	r = httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("X-User-Id", "u1")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if len(rec.Events()) != 1 {
		t.Fatalf("post-stop request attributed: %d", len(rec.Events()))
	}
	// in-flight work with the stamped context still records.
	_, sp := core.Begin(ctxIn, core.KindWorkerStart, "w")
	if sp == nil {
		t.Fatal("in-flight work must continue into the session")
	}
	sp.End()
	if len(rec.Events()) != 2 {
		t.Fatalf("%d", len(rec.Events()))
	}
}

func TestDownstreamWithoutBindingConfigJoinsViaPropagation(t *testing.T) {
	rec := setup(t, bound(core.Binding{SessionID: "b", Source: "header", Name: "x-user-id", Value: "u1"}))
	// Downstream service: sees only propagated headers, never the user attribute.
	down := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})))
	defer down.Close()
	front := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", down.URL+"/inner", nil)
		core.Inject(r.Context(), core.HTTPHeaderCarrier(req.Header))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}))
	r := httptest.NewRequest("GET", "/outer", nil)
	r.Header.Set("X-User-Id", "u1")
	front.ServeHTTP(httptest.NewRecorder(), r)
	evs := rec.Events()
	if len(evs) != 2 {
		t.Fatalf("%+v", evs)
	}
	var outer, inner core.Event
	for _, e := range evs {
		if strings.HasSuffix(e.Name, "/outer") {
			outer = e
		} else {
			inner = e
		}
	}
	if inner.SessionID != "b" || inner.TraceID != outer.TraceID || inner.ParentID != outer.SpanID {
		t.Fatalf("downstream did not join: outer=%+v inner=%+v", outer, inner)
	}
	if inner.Attrs["bound_by"] != "" {
		t.Fatal("downstream must not claim binding")
	}
}

func TestBoundValueNeverRecorded(t *testing.T) {
	rec := setup(t, bound(
		core.Binding{SessionID: "sA", Source: "header", Name: "x-user-id", Value: "u_secret_hdr"},
		core.Binding{SessionID: "sB", Source: "query", Name: "uid", Value: "u_secret_qry"},
		core.Binding{SessionID: "sC", Source: "cookie", Name: "uc", Value: "u_secret_cki"},
	))
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	// One bound attribute per request (two different sessions would be ambiguous
	// and untraced); the query value still appears in the URL of every request.
	for _, mk := range []func(*http.Request) *http.Request{
		func(r *http.Request) *http.Request {
			r.URL.RawQuery = "keep=1"
			r.Header.Set("X-User-Id", "u_secret_hdr")
			return r
		},
		func(r *http.Request) *http.Request { return r },
		func(r *http.Request) *http.Request {
			r.URL.RawQuery = "keep=1"
			r.AddCookie(&http.Cookie{Name: "uc", Value: "u_secret_cki"})
			return r
		},
	} {
		r := httptest.NewRequest("GET", "/p?uid=u_secret_qry&keep=1", nil)
		r.Header.Set("Referer", "http://x/y?uid=u_secret_qry")
		r = mk(r)
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	h2 := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", srv.URL+"/o?uid=u_secret_qry&keep=1", nil)
		req.Header.Set("X-User-Id", "u_secret_hdr")
		resp, err := Client(nil).Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
	}))
	r := httptest.NewRequest("GET", "/q", nil)
	r.Header.Set("X-User-Id", "u_secret_hdr")
	h2.ServeHTTP(httptest.NewRecorder(), r)

	evs := rec.Events()
	if len(evs) < 4 {
		t.Fatalf("events=%d", len(evs))
	}
	var out int
	for _, e := range evs {
		b, _ := json.Marshal(e)
		if strings.Contains(string(b), "u_secret_") {
			t.Fatalf("bound value leaked: %s", b)
		}
		if e.Kind == core.KindAPIOut {
			out++
		}
	}
	if out == 0 {
		t.Fatal("no api.out recorded")
	}
	if !strings.Contains(string(mustJSON(evs[0])), "keep=1") {
		t.Fatalf("non-bound query param lost")
	}
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func TestBindPriorityHTTP(t *testing.T) {
	rec := setup(t, bound(
		core.Binding{SessionID: "sU", Source: "header", Name: "x-wms-uid", Value: "u_prio_100"},
		core.Binding{SessionID: "sN", Source: "header", Name: "name", Value: "alice"},
	))
	core.Configure(core.Config{Service: "svc", Enabled: true, BindPriority: core.ParseBindPriority("header:x-wms-uid,header:name")})
	t.Cleanup(func() { core.Configure(core.Config{}) })
	h := Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	do := func(kv ...string) {
		r := httptest.NewRequest("GET", "/p", nil)
		for i := 0; i < len(kv); i += 2 {
			r.Header.Set(kv[i], kv[i+1])
		}
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	do("X-Wms-Uid", "u_prio_100", "Name", "alice") // decided by uid
	do("X-Wms-Uid", "u_prio_999", "Name", "alice") // unbound uid: untraced
	do("Name", "alice")                            // fallback to name
	evs := rec.Events()
	var in []core.Event
	for _, e := range evs {
		if e.Kind == core.KindAPIIn {
			in = append(in, e)
		}
	}
	if len(in) != 2 || in[0].SessionID != "sU" || in[0].Attrs["bound_by"] != "header:x-wms-uid" ||
		in[1].SessionID != "sN" || in[1].Attrs["bound_fallback"] != "true" {
		t.Fatalf("%+v", in)
	}
	for _, e := range evs {
		if strings.Contains(string(mustJSON(e)), "u_prio_") {
			t.Fatalf("priority value leaked: %s", mustJSON(e))
		}
	}
}
