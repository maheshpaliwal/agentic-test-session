package core

import (
	"context"
	"net/http"
	"testing"
)

type benchNop struct{}

func (benchNop) Record(Event) {}

func propBenchSetup(b *testing.B) context.Context {
	Configure(Config{Service: "svc", Enabled: true})
	SetRecorder(benchNop{})
	SetSessions(StaticSessions{Active: map[string]bool{"s1": true}})
	b.Cleanup(func() { Configure(Config{}); SetSessions(nil); SetRecorder(nil) })
	return ContextWith(context.Background(), SpanContext{SessionID: "s1", TraceID: NewTraceID(), SpanID: NewSpanID()})
}

func BenchmarkInjectMap(b *testing.B) {
	ctx := propBenchSetup(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Inject(ctx, MapCarrier{})
	}
}

func BenchmarkInjectHTTP(b *testing.B) {
	ctx := propBenchSetup(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Inject(ctx, HTTPHeaderCarrier(http.Header{}))
	}
}

func BenchmarkInjectHTTPAppTP(b *testing.B) {
	ctx := propBenchSetup(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := http.Header{"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}
		Inject(ctx, HTTPHeaderCarrier(h))
	}
}

func BenchmarkInjectNoSession(b *testing.B) {
	propBenchSetup(b)
	ctx := context.Background()
	c := MapCarrier{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Inject(ctx, c)
	}
}

func BenchmarkExtract(b *testing.B) {
	ctx := propBenchSetup(b)
	h := http.Header{}
	Inject(ctx, HTTPHeaderCarrier(h))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ExtractSpanContext(HTTPHeaderCarrier(h))
	}
}

func BenchmarkExtractOldSender(b *testing.B) {
	propBenchSetup(b)
	h := http.Header{}
	h.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.Set("x-tsession-id", "s1")
	h.Set("x-tsession-hlc", "1")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ExtractSpanContext(HTTPHeaderCarrier(h))
	}
}

func BenchmarkExtractNoSession(b *testing.B) {
	propBenchSetup(b)
	h := http.Header{"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ExtractSpanContext(HTTPHeaderCarrier(h))
	}
}
