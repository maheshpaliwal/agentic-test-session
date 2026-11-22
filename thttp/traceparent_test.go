package thttp

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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
				panic(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if len(gotApp) != 1 || gotApp[0] != appTraceparent {
				panic(fmt.Sprintf("application traceparent arrived as %q, want exactly %q", gotApp, appTraceparent))
			}
			out, in := kind(rec.Events(), core.KindAPIOut), kind(rec.Events(), core.KindAPIIn)
			if len(out) != 1 || len(in) != 1 {
				panic(fmt.Sprintf("api.out/api.in = %d/%d", len(out), len(in)))
			}
			if want := "00-" + root.TraceID + "-" + out[0].SpanID + "-01;" + gotHLC; gotOwn != want {
				panic(fmt.Sprintf("%s = %q, want %q", core.KeyTSessionTraceparent, gotOwn, want))
			}
			if in[0].ParentID != out[0].SpanID || in[0].TraceID != root.TraceID {
				panic(fmt.Sprintf("downstream span parent %q trace %q, want parent %q trace %q",
					in[0].ParentID, in[0].TraceID, out[0].SpanID, root.TraceID))
			}
			if len(req.Header) != 0 {
				panic(fmt.Sprintf("caller's request headers mutated: %v", req.Header))
			}
		})
	}
}

// An application that sets its header with req.Header["traceparent"] = ...
// stores it under a key http.Header.Get does not find. The receiver must get
// that one value, not tsession's in front of it.
func TestClientSeesTraceparentUnderNonCanonicalKey(t *testing.T) {
	rec := setup(t, active("s1"))
	var got http.Header
	down := httptest.NewServer(Handler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() })))
	defer down.Close()
	root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
	req, _ := http.NewRequestWithContext(core.ContextWith(t.Context(), root), "GET", down.URL, nil)
	req.Header["traceparent"] = []string{appTraceparent}
	resp, err := Client(nil).Do(req)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if tp := got.Values("Traceparent"); len(tp) != 1 || tp[0] != appTraceparent {
		panic(fmt.Sprintf("receiver got traceparent %q, want exactly the application's", tp))
	}
	out, in := kind(rec.Events(), core.KindAPIOut), kind(rec.Events(), core.KindAPIIn)
	if len(out) != 1 || len(in) != 1 || in[0].ParentID != out[0].SpanID || in[0].TraceID != root.TraceID {
		panic(fmt.Sprintf("api.out %+v, api.in %+v", out, in))
	}
	if len(req.Header) != 1 {
		panic(fmt.Sprintf("caller's request headers mutated: %v", req.Header))
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
		panic(err)
	}
	resp.Body.Close()
	out := kind(rec.Events(), core.KindAPIOut)
	if len(out) != 1 {
		panic(fmt.Sprintf("api.out = %d", len(out)))
	}
	want := "00-" + root.TraceID + "-" + out[0].SpanID + "-01"
	if got.Get("Traceparent") != want || got.Get(core.KeyTSessionTraceparent) != want+";"+got.Get(core.KeyHLC) {
		panic(fmt.Sprintf("traceparent %q, %s %q, want %q and that with the clock", got.Get("Traceparent"), core.KeyTSessionTraceparent,
			got.Get(core.KeyTSessionTraceparent), want))
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
		panic(fmt.Sprintf("api.in = %+v", in))
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
		panic(fmt.Sprintf("api.in = %+v, want parent %s (the forwarder; the first sender is %s)", in, forwarder, sender))
	}
}

// A request that was refused or failed carries its status as an attribute,
// also when the response is too large to be recorded whole; one that went
// well carries none.
func TestStatusAttrOfARefusedRequest(t *testing.T) {
	rec := setup(t, active("s1"))
	big := strings.Repeat("x", 64<<10)
	back := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(big))
		case "/broken":
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})))
	defer back.Close()
	client := Client(nil)
	// The front service passes the request on and answers 200 whatever comes back: the statuses under test
	// are those of the back service (api.in there) and of the call to it (api.out here).
	front := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", back.URL+r.URL.Path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	})))
	defer front.Close()
	for _, path := range []string{"/ok", "/missing", "/broken"} {
		req, _ := http.NewRequest("GET", front.URL+path, nil)
		req.Header.Set(core.KeySession, "s1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	want := []string{"", "404", "503"}
	evs := rec.Events()
	backHost := strings.TrimPrefix(back.URL, "http://")
	// api.out: the three calls of the front service. api.in: the three requests the back service got.
	outs := kind(evs, core.KindAPIOut)
	var ins []core.Event
	for _, e := range kind(evs, core.KindAPIIn) {
		if e.ParentID != "" { // reached through the front service
			ins = append(ins, e)
		}
	}
	for label, list := range map[string][]core.Event{"api.out to " + backHost: outs, "api.in of the back service": ins} {
		if len(list) != 3 {
			panic(fmt.Sprintf("%s: %d events, want 3", label, len(list)))
		}
		slices.SortFunc(list, func(a, b core.Event) int { return strings.Compare(statusOrder(a.Name), statusOrder(b.Name)) })
		for i, e := range list {
			if got := e.Attrs[StatusAttr]; got != want[i] {
				t.Errorf("%s, %s: %s = %q, want %q", label, e.Name, StatusAttr, got, want[i])
			}
		}
	}
}

// statusOrder sorts the three requests of the test as ok, missing, broken.
func statusOrder(name string) string {
	switch {
	case strings.Contains(name, "/ok"):
		return "0"
	case strings.Contains(name, "/missing"):
		return "1"
	}
	return "2"
}
