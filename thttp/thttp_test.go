package thttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func setup(t *testing.T, s core.StaticSessions) *core.MemRecorder {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(s)
	t.Cleanup(func() {
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})
	return rec
}

func active(ids ...string) core.StaticSessions {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return core.StaticSessions{Active: m}
}

func kind(evs []core.Event, k core.Kind) []core.Event {
	var out []core.Event
	for _, e := range evs {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

func TestHeaderModeAndBodyRestoration(t *testing.T) {
	rec := setup(t, active("s1"))
	var got string
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.Header().Set("Set-Cookie", "sid=abc")
		w.WriteHeader(201)
		w.Write([]byte(`{"ok":true}`))
	}))
	req := httptest.NewRequest("POST", "/orders?x=1", strings.NewReader(`{"a":1}`))
	req.Header.Set(core.KeySession, "s1")
	req.Header.Set("Authorization", "Bearer synthetic")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got != `{"a":1}` || rr.Code != 201 || rr.Body.String() != `{"ok":true}` {
		t.Fatalf("behavior altered: %q %d %q", got, rr.Code, rr.Body.String())
	}
	evs := rec.Events()
	if len(evs) != 1 || evs[0].Kind != core.KindAPIIn || evs[0].SessionID != "s1" || evs[0].Name != "POST /orders" {
		t.Fatalf("events: %+v", evs)
	}
	req0 := string(evs[0].Req.Inline)
	if strings.Contains(req0, "synthetic") || !strings.Contains(req0, "REDACTED") || !strings.Contains(req0, `"body":{"a":1}`) {
		t.Fatalf("req payload: %s", req0)
	}
	resp := string(evs[0].Resp.Inline)
	if strings.Contains(resp, "sid=abc") || !strings.Contains(resp, `"status":201`) || !strings.Contains(resp, `"ok":true`) {
		t.Fatalf("resp payload: %s", resp)
	}
}

func TestExclusiveAndUnknown(t *testing.T) {
	rec := setup(t, core.StaticSessions{ExclusiveID: "ex"})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"ex": true}, ExclusiveID: "ex"})
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := core.FromContext(r.Context()); !ok {
			t.Error("no span ctx")
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/a", nil))
	if evs := rec.Events(); len(evs) != 1 || evs[0].SessionID != "ex" {
		t.Fatalf("exclusive: %+v", evs)
	}

	rec2 := setup(t, active("s1"))
	req := httptest.NewRequest("GET", "/a", nil)
	req.Header.Set(core.KeySession, "unknown")
	Handler(http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), req)
	if len(rec2.Events()) != 0 {
		t.Fatal("unknown session recorded")
	}
}

func TestPropagationAcrossServers(t *testing.T) {
	rec := setup(t, active("s1"))
	back := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "backend")
	})))
	defer back.Close()
	client := Client(nil)
	front := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", back.URL+"/inner", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		w.Write(b)
	})))
	defer front.Close()

	req, _ := http.NewRequest("GET", front.URL+"/outer", nil)
	req.Header.Set(core.KeySession, "s1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "backend" {
		t.Fatalf("body %q", b)
	}

	evs := rec.Events()
	ins, outs := kind(evs, core.KindAPIIn), kind(evs, core.KindAPIOut)
	if len(ins) != 2 || len(outs) != 1 {
		t.Fatalf("events: %+v", evs)
	}
	var outer, inner core.Event
	for _, e := range ins {
		if e.Name == "GET /outer" {
			outer = e
		} else {
			inner = e
		}
	}
	out := outs[0]
	if out.ParentID != outer.SpanID || inner.ParentID != out.SpanID {
		t.Fatalf("parent links: outer=%s out.parent=%s inner.parent=%s out=%s", outer.SpanID, out.ParentID, inner.ParentID, out.SpanID)
	}
	if outer.TraceID != out.TraceID || out.TraceID != inner.TraceID || outer.TraceID == "" {
		t.Fatal("trace ids differ")
	}
	if !strings.Contains(string(out.Resp.Inline), "backend") {
		t.Fatalf("out resp: %s", out.Resp.Inline)
	}
}

func TestPatternName(t *testing.T) {
	rec := setup(t, active("s1"))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {})
	req := httptest.NewRequest("GET", "/items/42", nil)
	req.Header.Set(core.KeySession, "s1")
	WrapMux(mux).ServeHTTP(httptest.NewRecorder(), req)
	if evs := rec.Events(); len(evs) != 1 || evs[0].Name != "GET /items/{id}" {
		t.Fatalf("%+v", evs)
	}
}

func TestPanicAnd5xx(t *testing.T) {
	rec := setup(t, active("s1"))
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("boom") }))
	req := httptest.NewRequest("GET", "/p", nil)
	req.Header.Set(core.KeySession, "s1")
	func() {
		defer func() {
			if recover() != "boom" {
				t.Error("not re-panicked")
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()
	evs := rec.Events()
	if len(evs) != 1 || !strings.Contains(evs[0].Err, "boom") {
		t.Fatalf("%+v", evs)
	}

	rec2 := setup(t, active("s1"))
	Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })).ServeHTTP(httptest.NewRecorder(), req)
	if evs := rec2.Events(); len(evs) != 1 || evs[0].Err != "HTTP 503" {
		t.Fatalf("%+v", evs)
	}
}

func TestIdempotentAndDisabled(t *testing.T) {
	setup(t, active("s1"))
	h := Handler(http.NotFoundHandler())
	if Handler(h) != h {
		t.Fatal("handler double wrap")
	}
	tr := Transport(nil)
	if Transport(tr) != tr {
		t.Fatal("transport double wrap")
	}

	core.Configure(core.Config{Enabled: false})
	orig := http.NotFoundHandler()
	if _, ok := Handler(orig).(*tracedHandler); ok {
		t.Fatal("disabled handler wrapped")
	}
	if Transport(http.DefaultTransport) != http.DefaultTransport {
		t.Fatal("disabled transport wrapped")
	}
	c := &http.Client{}
	if Client(c) != c {
		t.Fatal("disabled client copied")
	}
}

func TestFlusherPreserved(t *testing.T) {
	rec := setup(t, active("s1"))
	srv := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("no flusher")
			return
		}
		io.WriteString(w, "a")
		f.Flush()
		io.WriteString(w, "b")
	})))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set(core.KeySession, "s1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "ab" || resp.StatusCode != 200 {
		t.Fatalf("%q %d", b, resp.StatusCode)
	}
	_ = rec
}
