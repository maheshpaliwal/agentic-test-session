package tredisv8

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// wrapperClient mimics an internal wrapper that hides the concrete client
// behind an embedded interface.
type wrapperClient struct {
	redis.Cmdable
}

func enable(t *testing.T) (*core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	return rec, core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
}

func TestWrapAnyThroughWrapper(t *testing.T) {
	rec, ctx := enable(t)
	mr := miniredis.RunT(t)
	w := &wrapperClient{Cmdable: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	if !WrapAny(w.Cmdable, WithBeforeImage("user:*")) {
		t.Fatal("WrapAny must hook the concrete client")
	}
	if WrapAny(w.Cmdable) {
		t.Fatal("second WrapAny must be a no-op")
	}
	w.HSet(ctx, "user:1", "name", "a")
	w.HSet(ctx, "user:1", "name", "b")
	ev := rec.Events()
	if len(ev) != 2 {
		t.Fatalf("events=%d (double hook or traced image reads)", len(ev))
	}
	if m := ev[1].Mutations[0]; string(m.Before.Inline) != `{"name":"a"}` || string(m.After.Inline) != `{"name":"b"}` {
		t.Fatalf("%s -> %s", m.Before.Inline, m.After.Inline)
	}
	if WrapAny(w) || WrapAny(nil) { // wrapper itself has no AddHook
		t.Fatal("non-hookable must report false")
	}
}

func TestBareHookAndTxPipeline(t *testing.T) {
	rec, ctx := enable(t)
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c.AddHook(Hook())
	if _, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "a", "1", 0)
		p.Incr(ctx, "n")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ev := rec.Events()
	if len(ev) != 2 { // multi/exec are not spans
		t.Fatalf("events=%d", len(ev))
	}
	if ev[0].Attrs["pipeline"] != "true" || ev[1].Attrs["pipeline_size"] == "" {
		t.Fatalf("%+v", ev)
	}
	c.Get(context.Background(), "a") // no session
	if len(rec.Events()) != 2 {
		t.Fatal("untraced call produced events")
	}
}

func TestDisabledPassthrough(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	if WrapAny(c) {
		t.Fatal("disabled WrapAny must not hook")
	}
	if Wrap(c) != c {
		t.Fatal("disabled Wrap must return the same client")
	}
	if WrapAny(c) {
		t.Fatal("still disabled")
	}
}
