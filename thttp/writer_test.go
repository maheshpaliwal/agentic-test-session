package thttp

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// unwrapOnly is a middleware writer in the modern style: it exposes the
// underlying writer's optional interfaces only through Unwrap.
type unwrapOnly struct{ http.ResponseWriter }

func (u unwrapOnly) Unwrap() http.ResponseWriter { return u.ResponseWriter }

// capWriter records which optional ResponseWriter features were reached.
type capWriter struct {
	http.ResponseWriter
	flushed, hijacked bool
	pushed            string
}

func (c *capWriter) Flush() { c.flushed = true }
func (c *capWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.hijacked = true
	return nil, nil, nil
}
func (c *capWriter) Push(target string, _ *http.PushOptions) error {
	c.pushed = target
	return nil
}

// serveTraced runs fn as a traced handler whose ResponseWriter is wrap(base).
func serveTraced(t *testing.T, base http.ResponseWriter, wrap func(http.ResponseWriter) http.ResponseWriter, fn http.HandlerFunc) {
	t.Helper()
	h := Handler(fn)
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(core.KeySession, "s1")
	h.ServeHTTP(wrap(base), req)
}

func TestWriterReachesFeaturesBehindUnwrap(t *testing.T) {
	rec := setup(t, active("s1"))
	base := &capWriter{ResponseWriter: httptest.NewRecorder()}
	wrap := func(w http.ResponseWriter) http.ResponseWriter { return unwrapOnly{w} }
	serveTraced(t, base, wrap, func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
		if err := w.(http.Pusher).Push("/style.css", nil); err != nil {
			t.Errorf("Push: %v", err)
		}
		if _, _, err := rc.Hijack(); err != nil {
			t.Errorf("Hijack: %v", err)
		}
	})
	if !base.flushed || !base.hijacked || base.pushed != "/style.css" {
		panic(fmt.Sprintf("not forwarded through Unwrap: flushed=%v hijacked=%v pushed=%q", base.flushed, base.hijacked, base.pushed))
	}
	evs := kind(rec.Events(), core.KindAPIIn)
	if len(evs) != 1 || evs[0].Attrs["stream"] != "true" {
		panic(fmt.Sprintf("hijack through Unwrap must end the span as a stream: %+v", evs))
	}
}

func TestWriterWithoutFeaturesDegrades(t *testing.T) {
	setup(t, active("s1"))
	// plain has none of Flusher/Hijacker/Pusher, directly or via Unwrap.
	plain := struct{ http.ResponseWriter }{httptest.NewRecorder()}
	same := func(w http.ResponseWriter) http.ResponseWriter { return w }
	serveTraced(t, plain, same, func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush() // must not panic
		if _, _, err := w.(http.Hijacker).Hijack(); !errors.Is(err, http.ErrNotSupported) {
			t.Errorf("Hijack err = %v, want ErrNotSupported", err)
		}
		if err := w.(http.Pusher).Push("/x", nil); !errors.Is(err, http.ErrNotSupported) {
			t.Errorf("Push err = %v, want ErrNotSupported", err)
		}
	})
}

func TestNestedHandlersRecordOneSpan(t *testing.T) {
	rec := setup(t, core.StaticSessions{Active: map[string]bool{"s1": true}, ExclusiveID: "s1"})
	sub := http.NewServeMux()
	sub.HandleFunc("/api/x", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	root := http.NewServeMux()
	root.Handle("/api/", WrapMux(sub))
	// A middleware between the two defeats the type-based idempotency check.
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { WrapMux(root).ServeHTTP(w, r) }))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/x", nil))
	if rr.Body.String() != "ok" {
		panic(fmt.Sprintf("body %q", rr.Body.String()))
	}
	evs := kind(rec.Events(), core.KindAPIIn)
	if len(evs) != 1 {
		panic(fmt.Sprintf("want one api.in for one request, got %d: %+v", len(evs), evs))
	}
	if evs[0].Name != "GET /api/x" {
		panic(fmt.Sprintf("name = %q, want the innermost matched route", evs[0].Name))
	}
}

func TestSpanNameSetForTracedRequests(t *testing.T) {
	rec := setup(t, active("s1"))
	srv := httptest.NewServer(Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	defer srv.Close()
	ctx := core.ContextWith(t.Context(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/orders/7", nil)
	resp, err := Client(nil).Do(req)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	for _, k := range []core.Kind{core.KindAPIIn, core.KindAPIOut} {
		if evs := kind(rec.Events(), k); len(evs) != 1 || evs[0].Name != "GET /orders/7" {
			panic(fmt.Sprintf("%s: %+v", k, evs))
		}
	}
}
