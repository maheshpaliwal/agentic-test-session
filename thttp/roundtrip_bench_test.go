package thttp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type benchNop struct{}

func (benchNop) Record(core.Event) {}

func benchRoundTrip(b *testing.B, traced bool) {
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(benchNop{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	b.Cleanup(func() { core.Configure(core.Config{}); core.SetSessions(nil); core.SetRecorder(nil) })
	srv := httptest.NewServer(Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })))
	b.Cleanup(srv.Close)
	cl := &http.Client{Transport: Transport(&http.Transport{MaxIdleConnsPerHost: 4})}
	ctx := context.Background()
	if traced {
		ctx = core.ContextWith(ctx, core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/x", nil)
		req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		resp, err := cl.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func BenchmarkRoundTripTraced(b *testing.B)   { benchRoundTrip(b, true) }
func BenchmarkRoundTripUntraced(b *testing.B) { benchRoundTrip(b, false) }
