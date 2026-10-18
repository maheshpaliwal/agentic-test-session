package tqueue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func setup(t *testing.T, enabled bool) (*core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: enabled})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() {
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})
	root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
	return rec, core.ContextWith(context.Background(), root)
}

func byKind(evs []core.Event, k core.Kind) []core.Event {
	var out []core.Event
	for _, e := range evs {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func TestRoundTripSameTraceAndLinks(t *testing.T) {
	rec, ctx := setup(t, true)
	root, _ := core.FromContext(ctx)
	carrier := core.MapCarrier{}
	err := Publish(ctx, Message{System: "mem", Queue: "q", JobName: "send_email", Params: map[string]any{"to": "a@example.test"}}, carrier,
		func(sctx context.Context) error {
			return Consume(context.Background(), carrier, Delivery{System: "mem", Queue: "q", JobName: "send_email", Attempt: 1, MessageID: "m1"},
				func(hctx context.Context) error {
					_, a := core.Begin(hctx, core.KindAPIOut, "POST /mail")
					a.End()
					_, d := core.Begin(hctx, core.KindDBRead, "select")
					d.End()
					return nil
				})
		})
	if err != nil {
		t.Fatal(err)
	}
	evs := rec.Events()
	pub, st, en := byKind(evs, core.KindQueuePub), byKind(evs, core.KindWorkerStart), byKind(evs, core.KindWorkerEnd)
	if len(pub) != 1 || len(st) != 1 || len(en) != 1 {
		t.Fatalf("want 1/1/1 got %d/%d/%d", len(pub), len(st), len(en))
	}
	if pub[0].ParentID != root.SpanID || pub[0].Req.IsZero() {
		t.Fatalf("publish parent/req wrong: %+v", pub[0])
	}
	for _, e := range evs {
		if e.TraceID != root.TraceID {
			t.Fatalf("trace mismatch on %s", e.Kind)
		}
	}
	if len(st[0].LinkIDs) != 1 || st[0].LinkIDs[0] != pub[0].SpanID {
		t.Fatalf("start link: %v", st[0].LinkIDs)
	}
	if !contains(en[0].LinkIDs, pub[0].SpanID) || en[0].ParentID != st[0].SpanID || en[0].Attempt != 1 {
		t.Fatalf("end wrong: %+v", en[0])
	}
	for _, k := range []core.Kind{core.KindAPIOut, core.KindDBRead} {
		e := byKind(evs, k)
		if len(e) != 1 || e[0].ParentID != st[0].SpanID {
			t.Fatalf("%s not nested under worker.start", k)
		}
	}
}

func TestRetryThenDLQ(t *testing.T) {
	rec, ctx := setup(t, true)
	carrier := core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "q", JobName: "j"}, carrier, func(context.Context) error { return nil })
	boom := errors.New("boom")
	for a := 1; a <= 3; a++ {
		err := Consume(context.Background(), carrier, Delivery{Queue: "q", JobName: "j", Attempt: a, MaxAttempts: 3},
			func(context.Context) error { return boom })
		if err != boom {
			t.Fatalf("error not propagated: %v", err)
		}
	}
	evs := rec.Events()
	st, en := byKind(evs, core.KindWorkerStart), byKind(evs, core.KindWorkerEnd)
	if len(st) != 3 || len(en) != 3 {
		t.Fatalf("start/end = %d/%d", len(st), len(en))
	}
	seen := map[string]bool{}
	for i := range st {
		if st[i].Attempt != i+1 || en[i].Attempt != i+1 || en[i].Err != "boom" || seen[st[i].SpanID] {
			t.Fatalf("attempt %d bad: %+v %+v", i+1, st[i], en[i])
		}
		seen[st[i].SpanID] = true
	}
	for i, e := range en {
		want := i < 2
		if (e.Attrs["retry_pending"] == "true") != want {
			t.Fatalf("attempt %d retry_pending=%q want %v", i+1, e.Attrs["retry_pending"], want)
		}
	}
	if r, d := byKind(evs, core.KindWorkerRetry), byKind(evs, core.KindWorkerDLQ); len(r) != 2 || len(d) != 1 || d[0].Attempt != 3 {
		t.Fatalf("retry/dlq = %d/%d", len(r), len(d))
	}
}

func TestFanOut(t *testing.T) {
	rec, ctx := setup(t, true)
	root, _ := core.FromContext(ctx)
	c1, c2 := core.MapCarrier{}, core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "a", JobName: "a"}, c1, func(context.Context) error { return nil })
	err := Consume(context.Background(), c1, Delivery{Queue: "a", JobName: "a"}, func(h context.Context) error {
		return Publish(h, Message{Queue: "b", JobName: "b"}, c2, func(context.Context) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Consume(context.Background(), c2, Delivery{Queue: "b", JobName: "b"}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	evs := rec.Events()
	pubs, st := byKind(evs, core.KindQueuePub), byKind(evs, core.KindWorkerStart)
	if len(pubs) != 2 || len(st) != 2 {
		t.Fatalf("pubs/starts %d/%d", len(pubs), len(st))
	}
	if pubs[1].ParentID != st[0].SpanID {
		t.Fatal("second publish should be child of first worker.start")
	}
	if st[1].LinkIDs[0] != pubs[1].SpanID {
		t.Fatal("second worker should link second publish")
	}
	for _, e := range evs {
		if e.TraceID != root.TraceID {
			t.Fatal("trace changed across fan-out")
		}
	}
}

func TestSideChannelHitMissTTL(t *testing.T) {
	rec, ctx := setup(t, true)
	now := time.Now()
	sc := &MemSideChannel{Now: func() time.Time { return now }}
	_ = PublishVia(ctx, sc, "m1", Message{Queue: "q", JobName: "j", Params: "x"}, func(context.Context) error { return nil })
	if _, err := sc.Get(ctx, "ts:ctx:m1"); err != nil {
		t.Fatalf("carrier not stored: %v", err)
	}
	ran := 0
	h := func(context.Context) error { ran++; return nil }
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "m1"}, h)
	if n := len(byKind(rec.Events(), core.KindWorkerStart)); n != 1 {
		t.Fatalf("hit should trace, starts=%d", n)
	}
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "nope"}, h)
	now = now.Add(2 * DefaultTTL)
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "m1"}, h)
	if ran != 3 {
		t.Fatalf("handler must always run, ran=%d", ran)
	}
	if n := len(byKind(rec.Events(), core.KindWorkerStart)); n != 1 {
		t.Fatalf("miss/expired must be untraced, starts=%d", n)
	}
}

type fakeRedis struct {
	m   map[string]string
	ttl time.Duration
}

func (f *fakeRedis) SetEX(_ context.Context, k, v string, ttl time.Duration) error {
	f.m[k], f.ttl = v, ttl
	return nil
}
func (f *fakeRedis) Get(_ context.Context, k string) (string, bool, error) {
	v, ok := f.m[k]
	return v, ok, nil
}

func TestRedisSideChannel(t *testing.T) {
	r := RedisSideChannel{Client: &fakeRedis{m: map[string]string{}}}
	if _, err := r.Get(context.Background(), "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	_ = r.Put(context.Background(), "k", map[string]string{"a": "b"}, time.Minute)
	m, err := r.Get(context.Background(), "k")
	if err != nil || m["a"] != "b" || r.Client.(*fakeRedis).ttl != time.Minute {
		t.Fatalf("roundtrip failed: %v %v", m, err)
	}
}

func TestWrapHandlerAsynqStyle(t *testing.T) {
	rec, ctx := setup(t, true)
	sc := &MemSideChannel{}
	_ = PublishVia(ctx, sc, "id1", Message{Queue: "q", JobName: "j"}, func(context.Context) error { return nil })
	h := WrapHandler(sc, "asynq", "q", "j", 0, func(context.Context, string, int, []byte) error { return nil })
	_ = h(context.Background(), "id1", 1, []byte(`{"a":1}`))
	if len(byKind(rec.Events(), core.KindWorkerEnd)) != 1 {
		t.Fatal("expected worker.end")
	}
}

func TestDisabledPassthrough(t *testing.T) {
	rec, _ := setup(t, false)
	sent, handled := false, false
	c := core.MapCarrier{}
	_ = Publish(context.Background(), Message{Queue: "q"}, c, func(context.Context) error { sent = true; return nil })
	_ = Consume(context.Background(), c, Delivery{Queue: "q"}, func(context.Context) error { handled = true; return nil })
	if !sent || !handled || len(rec.Events()) != 0 || len(c) != 0 {
		t.Fatal("disabled must be a pure passthrough")
	}
}

func TestNoSessionRunsUntraced(t *testing.T) {
	rec, _ := setup(t, true)
	sent, handled := false, false
	c := core.MapCarrier{}
	_ = Publish(context.Background(), Message{Queue: "q"}, c, func(context.Context) error { sent = true; return nil })
	_ = Consume(context.Background(), c, Delivery{Queue: "q"}, func(context.Context) error { handled = true; return nil })
	if !sent || !handled || len(rec.Events()) != 0 {
		t.Fatal("no session => untraced but still runs")
	}
}

func TestIdempotentDoubleWrap(t *testing.T) {
	rec, ctx := setup(t, true)
	c := core.MapCarrier{}
	m := Message{System: "s", Queue: "q", JobName: "j"}
	_ = Publish(ctx, m, c, func(sctx context.Context) error {
		return Publish(sctx, m, c, func(context.Context) error { return nil })
	})
	if n := len(byKind(rec.Events(), core.KindQueuePub)); n != 1 {
		t.Fatalf("double publish wrap emitted %d spans", n)
	}
	d := Delivery{Queue: "q", JobName: "j", MessageID: "x", Attempt: 1}
	_ = Consume(context.Background(), c, d, func(h context.Context) error {
		return Consume(h, c, d, func(context.Context) error { return nil })
	})
	if n := len(byKind(rec.Events(), core.KindWorkerStart)); n != 1 {
		t.Fatalf("double consume wrap emitted %d starts", n)
	}
}

func TestPanicRecordedAndRepanicked(t *testing.T) {
	rec, ctx := setup(t, true)
	c := core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "q"}, c, func(context.Context) error { return nil })
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic swallowed")
			}
		}()
		_ = Consume(context.Background(), c, Delivery{Queue: "q"}, func(context.Context) error { panic("kaboom") })
	}()
	en := byKind(rec.Events(), core.KindWorkerEnd)
	if len(en) != 1 || en[0].Err == "" {
		t.Fatalf("panic not recorded: %+v", en)
	}
}

func TestPublishSendErrorRecorded(t *testing.T) {
	rec, ctx := setup(t, true)
	boom := errors.New("down")
	if err := Publish(ctx, Message{Queue: "q"}, core.MapCarrier{}, func(context.Context) error { return boom }); err != boom {
		t.Fatal("error lost")
	}
	if p := byKind(rec.Events(), core.KindQueuePub); len(p) != 1 || p[0].Err != "down" {
		t.Fatal("send error not on span")
	}
}

func TestSuccessAndUnknownPolicyCloseEachAttempt(t *testing.T) {
	rec, ctx := setup(t, true)
	c := core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "q", JobName: "j"}, c, func(context.Context) error { return nil })
	boom := errors.New("boom")
	_ = Consume(context.Background(), c, Delivery{Queue: "q", JobName: "j", Attempt: 1}, func(context.Context) error { return boom })
	_ = Consume(context.Background(), c, Delivery{Queue: "q", JobName: "j", Attempt: 2, MaxAttempts: 3}, func(context.Context) error { return nil })
	for _, e := range byKind(rec.Events(), core.KindWorkerEnd) {
		if e.Attrs["retry_pending"] != "" {
			t.Fatalf("unexpected retry_pending: %+v", e)
		}
	}
	if n := len(byKind(rec.Events(), core.KindWorkerRetry)) + len(byKind(rec.Events(), core.KindWorkerDLQ)); n != 0 {
		t.Fatalf("retry/dlq = %d", n)
	}
}

func TestRetryExpectedCallback(t *testing.T) {
	rec, ctx := setup(t, true)
	c := core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "q", JobName: "j"}, c, func(context.Context) error { return nil })
	boom := errors.New("boom")
	re := func(a int, err error) bool { return a < 2 && err == boom }
	for a := 1; a <= 2; a++ {
		_ = Consume(context.Background(), c, Delivery{Queue: "q", JobName: "j", Attempt: a, RetryExpected: re},
			func(context.Context) error { return boom })
	}
	en := byKind(rec.Events(), core.KindWorkerEnd)
	if en[0].Attrs["retry_pending"] != "true" || en[1].Attrs["retry_pending"] != "" {
		t.Fatalf("retry_pending wrong: %v %v", en[0].Attrs, en[1].Attrs)
	}
	if len(byKind(rec.Events(), core.KindWorkerRetry)) != 1 {
		t.Fatal("want one worker.retry")
	}
}

func TestFanOutRetryOutOfOrder(t *testing.T) {
	rec, ctx := setup(t, true)
	c1, c2 := core.MapCarrier{}, core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "a", JobName: "a"}, c1, func(context.Context) error { return nil })
	_ = Publish(ctx, Message{Queue: "b", JobName: "b"}, c2, func(context.Context) error { return nil })
	boom := errors.New("boom")
	// b's worker runs first (out of order) and fails with a retry pending, a succeeds, then b's retry succeeds.
	_ = Consume(context.Background(), c2, Delivery{Queue: "b", JobName: "b", Attempt: 1, MaxAttempts: 2}, func(context.Context) error { return boom })
	_ = Consume(context.Background(), c1, Delivery{Queue: "a", JobName: "a", Attempt: 1, MaxAttempts: 2}, func(context.Context) error { return nil })
	_ = Consume(context.Background(), c2, Delivery{Queue: "b", JobName: "b", Attempt: 2, MaxAttempts: 2}, func(context.Context) error { return nil })
	pubs := byKind(rec.Events(), core.KindQueuePub)
	closing := map[string]int{}
	for _, e := range byKind(rec.Events(), core.KindWorkerEnd) {
		if e.Attrs["retry_pending"] != "true" {
			for _, l := range e.LinkIDs {
				closing[l]++
			}
		}
	}
	for _, p := range pubs {
		if closing[p.SpanID] != 1 {
			t.Fatalf("publish %s closed %d times, want exactly 1", p.Name, closing[p.SpanID])
		}
	}
}

func TestBodyRedaction(t *testing.T) {
	rec, ctx := setup(t, true)
	core.Redactor = func(b []byte) ([]byte, bool) { return b, false } // key-based redaction must not depend on it
	c := core.MapCarrier{}
	_ = Publish(ctx, Message{Queue: "q", Params: bodyParams([]byte(`{"user":"u","password":"hunter2"}`))}, c, func(context.Context) error { return nil })
	_ = Publish(ctx, Message{Queue: "q2", Params: bodyParams([]byte(`password=hunter2&a=1`))}, core.MapCarrier{}, func(context.Context) error { return nil })
	for _, e := range byKind(rec.Events(), core.KindQueuePub) {
		if strings.Contains(string(e.Req.Inline), "hunter2") {
			t.Fatalf("secret leaked: %s", e.Req.Inline)
		}
	}
}

type slowSide struct{ block chan struct{} }

func (s slowSide) Put(ctx context.Context, _ string, _ map[string]string, _ time.Duration) error {
	<-s.block
	return nil
}
func (s slowSide) Get(context.Context, string) (map[string]string, error) { return nil, ErrNotFound }

func TestPublishViaTimeoutDoesNotBlock(t *testing.T) {
	rec, ctx := setup(t, true)
	sc := slowSide{block: make(chan struct{})}
	defer close(sc.block)
	sent := false
	start := time.Now()
	err := PublishVia(ctx, sc, "m1", Message{Queue: "q", JobName: "j"}, func(context.Context) error { sent = true; return nil }, WithSideTimeout(30*time.Millisecond))
	if err != nil || !sent || time.Since(start) > 2*time.Second {
		t.Fatalf("send blocked/failed: %v %v %v", err, sent, time.Since(start))
	}
	if p := byKind(rec.Events(), core.KindQueuePub); len(p) != 1 || !contains(p[0].Flags, FlagSideChannel) {
		t.Fatalf("flag missing: %+v", p)
	}
}

func TestMemSideChannelSweepAndCap(t *testing.T) {
	now := time.Now()
	sc := &MemSideChannel{Now: func() time.Time { return now }, MaxEntries: 3}
	bg := context.Background()
	_ = sc.Put(bg, "a", map[string]string{"x": "1"}, time.Second)
	_ = sc.Put(bg, "b", map[string]string{"x": "1"}, time.Second)
	now = now.Add(5 * time.Second)
	_ = sc.Put(bg, "c", map[string]string{"x": "1"}, time.Hour)
	if sc.Len() != 1 {
		t.Fatalf("expired not swept on Put: len=%d", sc.Len())
	}
	for _, k := range []string{"d", "e", "f", "g"} {
		_ = sc.Put(bg, k, map[string]string{"x": "1"}, time.Hour)
	}
	if sc.Len() > 3 {
		t.Fatalf("cap exceeded: %d", sc.Len())
	}
}
