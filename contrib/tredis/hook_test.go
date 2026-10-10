package tredis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"tsession/core"
)

func setup(t *testing.T, opts ...Option) (redis.UniversalClient, *core.MemRecorder, context.Context) {
	t.Helper()
	mr := miniredis.RunT(t)
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	c := Wrap(redis.NewClient(&redis.Options{Addr: mr.Addr()}), opts...)
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	return c, rec, ctx
}

func TestReadWriteAndNil(t *testing.T) {
	c, rec, ctx := setup(t)
	if err := c.Set(ctx, "user:123", "bob", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if v := c.Get(ctx, "user:123").Val(); v != "bob" {
		t.Fatal(v)
	}
	if err := c.Get(ctx, "missing").Err(); err != redis.Nil {
		t.Fatal(err)
	}
	ev := rec.Events()
	if len(ev) != 3 {
		t.Fatalf("events=%d", len(ev))
	}
	w := ev[0]
	if w.Kind != core.KindCacheWrite || w.Name != "SET user:123" || len(w.Mutations) != 1 {
		t.Fatalf("%+v", w)
	}
	m := w.Mutations[0]
	if m.Store != "redis" || m.Entity != "user:*" || m.PK["key"] != "user:123" || m.Op != "set" || string(m.After.Inline) != `"bob"` {
		t.Fatalf("%+v", m)
	}
	if ev[1].Kind != core.KindCacheRead || string(ev[1].Resp.Inline) != `"bob"` {
		t.Fatalf("%+v", ev[1])
	}
	if ev[2].Err != "" {
		t.Fatalf("redis.Nil must not be an error: %q", ev[2].Err)
	}
}

func TestIdempotentAndDisabled(t *testing.T) {
	c, rec, ctx := setup(t)
	Wrap(c)
	c.Set(ctx, "k", "v", 0)
	if n := len(rec.Events()); n != 1 {
		t.Fatalf("double wrap produced %d events", n)
	}
	core.Configure(core.Config{})
	c2 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	if Wrap(c2) != redis.UniversalClient(c2) {
		t.Fatal("disabled Wrap must return the same client")
	}
}

func TestNoSessionNoEvents(t *testing.T) {
	c, rec, _ := setup(t)
	c.Set(context.Background(), "k", "v", 0)
	if len(rec.Events()) != 0 {
		t.Fatal("unexpected events")
	}
}

func TestBeforeImage(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("user:*"))
	c.HSet(ctx, "user:1", "name", "a")
	c.HSet(ctx, "user:1", "name", "b")
	c.Set(ctx, "other", "x", 0)
	ev := rec.Events()
	if len(ev) != 3 { // image reads must not be traced
		t.Fatalf("events=%d", len(ev))
	}
	m := ev[1].Mutations[0]
	if string(m.Before.Inline) != `{"name":"a"}` || string(m.After.Inline) != `{"name":"b"}` {
		t.Fatalf("%s -> %s", m.Before.Inline, m.After.Inline)
	}
	if !ev[2].Mutations[0].Before.IsZero() {
		t.Fatal("non-matching key must have no before image")
	}
}

func TestMultiKeyAndPipeline(t *testing.T) {
	c, rec, ctx := setup(t)
	c.MSet(ctx, "a", "1", "b", "2")
	ev := rec.Events()
	if len(ev[0].Mutations) != 2 || string(ev[0].Mutations[1].After.Inline) != `"2"` {
		t.Fatalf("%+v", ev[0].Mutations)
	}
	_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "p1", "x", 0)
		p.Get(ctx, "nope")
		return nil
	})
	if err != redis.Nil {
		t.Fatal(err)
	}
	ev = rec.Events()[1:]
	if len(ev) != 2 || ev[0].Attrs["pipeline"] != "true" || ev[1].Attrs["pipeline_index"] != "1" {
		t.Fatalf("%+v", ev)
	}
	if ev[1].Err != "" {
		t.Fatal("nil in pipeline is not an error")
	}
}

func TestErrorAndAuthRedaction(t *testing.T) {
	c, rec, ctx := setup(t)
	c.HSet(ctx, "k", "f", "v")
	if err := c.Incr(ctx, "k").Err(); err == nil {
		t.Fatal("expected error")
	}
	ev := rec.Events()
	if ev[1].Err == "" || len(ev[1].Mutations) != 0 {
		t.Fatalf("%+v", ev[1])
	}
	got := reqArgs([]any{"AUTH", "user", "hunter2"})
	if got[2] != "[REDACTED]" {
		t.Fatal(got)
	}
}
