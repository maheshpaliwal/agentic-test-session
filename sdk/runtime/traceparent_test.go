package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

const appTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// otelLike stands in for OpenTelemetry's HTTP client instrumentation: it sets
// traceparent on a copy of the request, replacing any value already there.
type otelLike struct {
	next http.RoundTripper
	tp   string
}

func (o otelLike) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("traceparent", o.tp)
	return o.next.RoundTrip(r)
}

func traceparentSetup(t *testing.T, cfg core.Config) (*core.MemRecorder, core.SpanContext) {
	t.Helper()
	rec := &core.MemRecorder{}
	cfg.Service, cfg.Enabled = "svc", true
	core.Configure(cfg)
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	return rec, core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
}

// roundTrip sends one request in root's session through rt and returns the
// headers the server received and the api.out span recorded for it.
func roundTrip(t *testing.T, rec *core.MemRecorder, root core.SpanContext, rt http.RoundTripper) (http.Header, core.Event) {
	t.Helper()
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	defer srv.Close()
	req, _ := http.NewRequestWithContext(core.ContextWith(t.Context(), root), "GET", srv.URL+"/x", nil)
	resp, err := (&http.Client{Transport: rt}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var out []core.Event
	for _, e := range rec.Events() {
		if e.Kind == core.KindAPIOut {
			out = append(out, e)
		}
	}
	if len(out) != 1 {
		t.Fatalf("api.out spans: %d", len(out))
	}
	return got, out[0]
}

// The patched default transport and the application's tracing transport can
// be stacked either way round. In both orders the application's traceparent
// reaches the server untouched and tsession's parent is read from its own key.
func TestTransportKeepsApplicationTraceparent(t *testing.T) {
	for name, build := range map[string]func() http.RoundTripper{
		"application tracing runs first": func() http.RoundTripper {
			return otelLike{next: &Transport{Base: &http.Transport{}}, tp: appTraceparent}
		},
		"application tracing runs last": func() http.RoundTripper {
			return &Transport{Base: otelLike{next: &http.Transport{}, tp: appTraceparent}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec, root := traceparentSetup(t, core.Config{})
			got, out := roundTrip(t, rec, root, build())
			if v := got.Values("Traceparent"); len(v) != 1 || v[0] != appTraceparent {
				t.Fatalf("application traceparent arrived as %q", v)
			}
			sc, ok := core.ExtractSpanContext(core.HTTPHeaderCarrier(got))
			if !ok || sc.SpanID != out.SpanID || sc.TraceID != root.TraceID {
				t.Fatalf("server would link to %+v, want span %s trace %s", sc, out.SpanID, root.TraceID)
			}
		})
	}
}

func TestTransportWritesBothKeysWithoutApplicationTracing(t *testing.T) {
	rec, root := traceparentSetup(t, core.Config{})
	got, out := roundTrip(t, rec, root, &Transport{Base: &http.Transport{}})
	want := "00-" + root.TraceID + "-" + out.SpanID + "-01"
	if got.Get("Traceparent") != want || got.Get(core.KeyTSessionTraceparent) != want+";"+got.Get(core.KeyHLC) {
		t.Fatalf("traceparent %q, own key %q, want %q and that with the clock", got.Get("Traceparent"), got.Get(core.KeyTSessionTraceparent), want)
	}
}

// The opt-out restores the old wire: tsession's ids replace the application's
// in traceparent. It can only do so when tsession injects after the
// application's transport ran.
func TestTransportOverwriteTraceparentSetting(t *testing.T) {
	rec, root := traceparentSetup(t, core.Config{OverwriteTraceparent: true})
	got, out := roundTrip(t, rec, root, otelLike{next: &Transport{Base: &http.Transport{}}, tp: appTraceparent})
	want := "00-" + root.TraceID + "-" + out.SpanID + "-01"
	if got.Get("Traceparent") != want || got.Get(core.KeyTSessionTraceparent) != want+";"+got.Get(core.KeyHLC) {
		t.Fatalf("traceparent %q, own key %q, want %q and that with the clock", got.Get("Traceparent"), got.Get(core.KeyTSessionTraceparent), want)
	}
}

func TestStartPassesOverwriteTraceparent(t *testing.T) {
	// Nothing listens on these ports: Start does its network work in the
	// background and only retries.
	cfg := Config{Service: "s", ControlPlaneURL: "http://127.0.0.1:1", CollectorURL: "http://127.0.0.1:1", OverwriteTraceparent: true}
	if err := Start(cfg); err != nil {
		t.Fatal(err)
	}
	got := core.GetConfig().OverwriteTraceparent
	Stop()
	if !got {
		t.Fatal("Config.OverwriteTraceparent did not reach core")
	}
	if core.GetConfig().OverwriteTraceparent {
		t.Fatal("setting survived Stop")
	}
}
