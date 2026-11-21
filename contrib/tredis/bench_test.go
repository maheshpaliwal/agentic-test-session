package tredis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type discard struct{}

func (discard) Record(core.Event) {}

// benchHook measures what the hook adds to one command, images off: the
// command itself is a no-op.
func benchHook(b *testing.B, session bool, args ...any) {
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(discard{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()
	process := Hook().ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	ctx := context.Background()
	if session {
		ctx = core.ContextWith(ctx, core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	}
	cmd := redis.NewStatusCmd(ctx, args...)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = process(ctx, cmd)
	}
}

func BenchmarkOutsideSessionSet(b *testing.B) { benchHook(b, false, "set", "user:42:profile", "v") }
func BenchmarkSet(b *testing.B)               { benchHook(b, true, "set", "user:42:profile", "v") }
func BenchmarkGet(b *testing.B)               { benchHook(b, true, "get", "user:42:profile") }
func BenchmarkSetSecretKey(b *testing.B) {
	benchHook(b, true, "set", "session:Ab3dE9Ab3dE9Ab3dE9", "v")
}
func BenchmarkMGet8(b *testing.B) {
	benchHook(b, true, "mget", "user:1:a", "user:2:a", "user:3:a", "user:4:a", "user:5:a", "user:6:a", "user:7:a", "user:8:a")
}
