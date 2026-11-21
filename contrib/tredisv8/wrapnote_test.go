package tredisv8

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// start enables the SDK the way the runtime does, after the wrappers under
// test were created.
func start(t *testing.T) (*core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	return rec, core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
}

// A client wrapped before the SDK starts records nothing: Wrap reports it.
func TestWrapBeforeStartIsNoted(t *testing.T) {
	c := Wrap(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()}))
	if !slices.Contains(core.WrappersWhileDisabled(), "tredisv8.Wrap") {
		t.Errorf("noted %q, want tredis.Wrap", core.WrappersWhileDisabled())
	}
	rec, ctx := start(t)
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("%d events from a client wrapped before the start: the note would be wrong", n)
	}
}

// The bare hook looks the SDK up on every command, so it has nothing to report.
func TestHookBeforeStartRecords(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	c.AddHook(Hook())
	if slices.Contains(core.WrappersWhileDisabled(), "tredisv8.Hook") {
		t.Error("Hook is noted, but it records once the SDK runs")
	}
	rec, ctx := start(t)
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events()); n != 1 {
		t.Fatalf("%d events from a hook installed before the start, want 1", n)
	}
}

func TestWrapAnyBeforeStartIsNoted(t *testing.T) {
	WrapAny(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()}))
	if !slices.Contains(core.WrappersWhileDisabled(), "tredisv8.WrapAny") {
		t.Errorf("noted %q, want tredisv8.WrapAny", core.WrappersWhileDisabled())
	}
}
