package thttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// The application's trace context, as its own tracing would send it.
const appTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// otelLike stands in for OpenTelemetry's HTTP client instrumentation: a
// RoundTripper that sets traceparent on a copy of the request (replacing any
// value already there, as a propagator's Inject does) and calls the next one.
type otelLike struct {
	next http.RoundTripper
	tp   string
}

func (o otelLike) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("traceparent", o.tp)
	return o.next.RoundTrip(r)
}

// With the application's tracing on the same client, in either wrapping
// order, the downstream service receives the application's traceparent
// untouched and its api.in span is a child of the caller's api.out span.
func TestClientKeepsApplicationTraceparent(t *testing.T) {
	for name, build := range map[string]func() http.RoundTripper{
		// The application's transport is the outer one: traceparent is
		// already on the request when tsession injects.
		"application tracing runs first": func() http.RoundTripper {
			return otelLike{next: Transport(&http.Transport{}), tp: appTraceparent}
		},
		// The application's transport is the inner one: it sets traceparent
		// after tsession injected, replacing what tsession wrote there.
		"application tracing runs last": func() http.RoundTripper {
			return Transport(otelLike{next: &http.Transport{}, tp: appTraceparent})
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := setup(t, active("s1"))
			var gotApp []string
			var gotOwn, gotHLC string
			down := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotApp = r.Header.Values("Traceparent")
				gotOwn, gotHLC = r.Header.Get(core.KeyTSessionTraceparent), r.Header.Get(core.KeyHLC)
			})))
			defer down.Close()

			root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
			req, _ := http.NewRequestWithContext(core.ContextWith(t.Context(), root), "GET", down.URL+"/next", nil)
			resp, err := (&http.Client{Transport: build()}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if len(gotApp) != 1 || gotApp[0] != appTraceparent {
				t.Fatalf("application traceparent arrived as %q, want exactly %q", gotApp, appTraceparent)
			}
			out, in := kind(rec.Events(), core.KindAPIOut), kind(rec.Events(), core.KindAPIIn)
			if len(out) != 1 || len(in) != 1 {
				t.Fatalf("api.out/api.in = %d/%d", len(out), len(in))
			}
			if want := "00-" + root.TraceID + "-" + out[0].SpanID + "-01;" + gotHLC; gotOwn != want {
				t.Fatalf("%s = %q, want %q", core.KeyTSessionTraceparent, gotOwn, want)
			}
			if in[0].ParentID != out[0].SpanID || in[0].TraceID != root.TraceID {
				t.Fatalf("downstream span parent %q trace %q, want parent %q trace %q",
					in[0].ParentID, in[0].TraceID, out[0].SpanID, root.TraceID)
			}
			if len(req.Header) != 0 {
				t.Fatalf("caller's request headers mutated: %v", req.Header)
			}
		})
	}
}

// Without application tracing the wire is what it was, plus the own key.
func TestClientWritesBothKeysWithoutApplicationTracing(t *testing.T) {
	rec := setup(t, active("s1"))
	var got http.Header
	down := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	defer down.Close()
	root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
	req, _ := http.NewRequestWithContext(core.ContextWith(t.Context(), root), "GET", down.URL, nil)
	resp, err := Client(nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	out := kind(rec.Events(), core.KindAPIOut)
	if len(out) != 1 {
		t.Fatalf("api.out = %d", len(out))
	}
	want := "00-" + root.TraceID + "-" + out[0].SpanID + "-01"
	if got.Get("Traceparent") != want || got.Get(core.KeyTSessionTraceparent) != want+";"+got.Get(core.KeyHLC) {
		t.Fatalf("traceparent %q, %s %q, want %q and that with the clock", got.Get("Traceparent"), core.KeyTSessionTraceparent,
			got.Get(core.KeyTSessionTraceparent), want)
	}
}

// A request from a service whose SDK predates x-tsession-traceparent carries
// the parent in traceparent only.
func TestHandlerReadsTraceparentFromOlderSDK(t *testing.T) {
	rec := setup(t, active("s1"))
	trace, span := core.NewTraceID(), core.NewSpanID()
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(core.KeySession, "s1")
	req.Header.Set("Traceparent", "00-"+trace+"-"+span+"-01")
	Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(httptest.NewRecorder(), req)
	in := kind(rec.Events(), core.KindAPIIn)
	if len(in) != 1 || in[0].ParentID != span || in[0].TraceID != trace {
		t.Fatalf("api.in = %+v", in)
	}
}

// A request that a service on an older SDK forwarded with the headers it
// received from a new one (a reverse proxy): it wrote its span into
// traceparent and a new x-tsession-hlc, and copied the x-tsession-traceparent
// it does not know. The parent is the forwarder's span, not the first
// sender's.
func TestHandlerIgnoresOwnKeyCopiedByOlderSDK(t *testing.T) {
	rec := setup(t, active("s1"))
	trace, sender, forwarder := core.NewTraceID(), core.NewSpanID(), core.NewSpanID()
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(core.KeySession, "s1")
	req.Header.Set(core.KeyTSessionTraceparent, "00-"+trace+"-"+sender+"-01;1000")
	req.Header.Set("Traceparent", "00-"+trace+"-"+forwarder+"-01")
	req.Header.Set(core.KeyHLC, "2000")
	Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(httptest.NewRecorder(), req)
	in := kind(rec.Events(), core.KindAPIIn)
	if len(in) != 1 || in[0].ParentID != forwarder || in[0].TraceID != trace {
		t.Fatalf("api.in = %+v, want parent %s (the forwarder; the first sender is %s)", in, forwarder, sender)
	}
}
