package tgrpc

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// An interceptor decides at construction whether it traces: one built before
// the SDK starts records nothing, so each constructor reports it.
func TestInterceptorsBeforeStartAreNoted(t *testing.T) {
	us, uc := UnaryServerInterceptor(), UnaryClientInterceptor()
	StreamServerInterceptor()
	StreamClientInterceptor()
	noted := core.WrappersWhileDisabled()
	for _, w := range []string{"tgrpc.UnaryServerInterceptor", "tgrpc.StreamServerInterceptor",
		"tgrpc.UnaryClientInterceptor", "tgrpc.StreamClientInterceptor"} {
		if !slices.Contains(noted, w) {
			t.Errorf("%s is not noted: %q", w, noted)
		}
	}

	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()

	in := metadata.NewIncomingContext(context.Background(), metadata.Pairs(core.KeySession, "s1"))
	if _, err := us(in, "req", &grpc.UnaryServerInfo{FullMethod: "/svc/M"},
		func(context.Context, any) (any, error) { return "resp", nil }); err != nil {
		panic(err)
	}
	out := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	if err := uc(out, "/svc/M", "req", nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }); err != nil {
		panic(err)
	}
	if n := len(rec.Events()); n != 0 {
		panic(fmt.Sprintf("%d events from interceptors built before the start: the note would be wrong", n))
	}
}
