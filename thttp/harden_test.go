package thttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tsession/core"
)

func TestQueryTokenRedactedInReq(t *testing.T) {
	rec := setup(t, active("s1"))
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	req := httptest.NewRequest("GET", "/cb?access_token=SYNTH123&a=1", nil)
	req.Header.Set(core.KeySession, "s1")
	req.Header.Set("Referer", "http://x.test/p?token=SYNTHREF&b=2")
	h.ServeHTTP(httptest.NewRecorder(), req)
	e := rec.Events()[0]
	all := e.Name + string(e.Req.Inline)
	if strings.Contains(all, "SYNTH123") || strings.Contains(all, "SYNTHREF") || !strings.Contains(all, "a=1") {
		t.Fatalf("leak: %s", all)
	}
}

func TestClientURLRedacted(t *testing.T) {
	rec := setup(t, active("s1"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	ctx := core.ContextWith(t.Context(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/x?api_key=SYNTHKEY&q=1", nil)
	resp, err := Client(nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, e := range kind(rec.Events(), core.KindAPIOut) {
		if strings.Contains(string(e.Req.Inline), "SYNTHKEY") {
			t.Fatalf("leak: %s", e.Req.Inline)
		}
	}
}

func TestOversizeAndFormBodiesRedacted(t *testing.T) {
	rec := setup(t, active("s1"))
	core.Configure(core.Config{Service: "svc", Enabled: true, MaxInlineBytes: 200})
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json password=SYNTHPW " + strings.Repeat("z", 500)))
	}))
	do := func(ct, body string) {
		req := httptest.NewRequest("POST", "/f", strings.NewReader(body))
		req.Header.Set(core.KeySession, "s1")
		req.Header.Set("Content-Type", ct)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	do("application/x-www-form-urlencoded", "user=a&password=SYNTHFORM&x=1")
	do("application/json", `{"pad":"`+strings.Repeat("p", 300)+`","password":"SYNTHBIG"}`)
	do("application/json", `{"password":"SYNTHTRUNC","pad":"`+strings.Repeat("p", 300)+`"`) // invalid/truncated
	for _, e := range rec.Events() {
		s := string(e.Req.Inline) + string(e.Resp.Inline)
		for _, bad := range []string{"SYNTHFORM", "SYNTHBIG", "SYNTHTRUNC", "SYNTHPW"} {
			if strings.Contains(s, bad) {
				t.Fatalf("leak %s: %s", bad, s)
			}
		}
	}
}

func TestSSEEndsSpanEarly(t *testing.T) {
	rec := setup(t, active("s1"))
	release := make(chan struct{})
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: hi\n\n"))
		w.(http.Flusher).Flush()
		<-release
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/events", nil)
	req.Header.Set(core.KeySession, "s1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	resp.Body.Read(buf)
	evs := rec.Events() // handler still blocked
	close(release)
	resp.Body.Close()
	if len(evs) != 1 || evs[0].Attrs["stream"] != "true" {
		t.Fatalf("span not ended early: %+v", evs)
	}
}

func TestHijackEndsSpanEarly(t *testing.T) {
	rec := setup(t, active("s1"))
	release := make(chan struct{})
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		c.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n\r\n"))
		<-release
		c.Close()
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/ws", nil)
	req.Header.Set(core.KeySession, "s1")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	evs := rec.Events()
	close(release)
	resp.Body.Close()
	if len(evs) != 1 || evs[0].Attrs["stream"] != "true" {
		t.Fatalf("span not ended early: %+v", evs)
	}
}
