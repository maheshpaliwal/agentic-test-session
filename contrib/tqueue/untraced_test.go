package tqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// hangSide is a side-channel store that does not answer: every call blocks
// until its context ends (or five seconds pass). It counts the calls it gets.
type hangSide struct{ puts, gets atomic.Int64 }

func (h *hangSide) wait(ctx context.Context) error {
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrNotFound
	}
}

func (h *hangSide) Put(ctx context.Context, _ string, _ map[string]string, _ time.Duration) error {
	h.puts.Add(1)
	return h.wait(ctx)
}

func (h *hangSide) Get(ctx context.Context, _ string) (map[string]string, error) {
	h.gets.Add(1)
	return nil, h.wait(ctx)
}

// countSide counts the calls that reach a working side channel.
type countSide struct {
	MemSideChannel
	puts, gets atomic.Int64
}

func (c *countSide) Put(ctx context.Context, k string, m map[string]string, ttl time.Duration) error {
	c.puts.Add(1)
	return c.MemSideChannel.Put(ctx, k, m, ttl)
}

func (c *countSide) Get(ctx context.Context, k string) (map[string]string, error) {
	c.gets.Add(1)
	return c.MemSideChannel.Get(ctx, k)
}

// wellUnderBound is the most an untraced job may wait before its handler runs:
// far below DefaultSideTimeout, far above scheduler noise.
const wellUnderBound = DefaultSideTimeout / 4

// plainSet is a session set that cannot say whether anything is active.
type plainSet struct{}

func (plainSet) IsActive(string) bool      { return false }
func (plainSet) Exclusive() (string, bool) { return "", false }

func sqsMsg(body string) types.Message {
	return types.Message{MessageId: aws.String("m"), Body: aws.String(body)}
}

func TestConsumeViaSkipsLookupWhenNothingActive(t *testing.T) {
	rec, _ := setup(t, true)
	core.SetSessions(core.StaticSessions{})
	sc := &hangSide{}
	ran := 0
	handler := func(context.Context) error { ran++; return nil }

	start := time.Now()
	if err := ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "id1"}, handler); err != nil {
		t.Fatal(err)
	}
	h := WrapHandler(sc, "asynq", "q", "j", 0, func(c context.Context, _ string, _ int, _ []byte) error { return handler(c) })
	if err := h(context.Background(), "id2", 1, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeSQSMessageOpts(context.Background(), "q", sqsMsg(`{"a":1}`), 0, SQSOptions{SideChannel: sc}, handler); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > wellUnderBound {
		t.Fatalf("three untraced jobs waited %v on a hanging side channel", el)
	}
	if ran != 3 {
		t.Fatalf("handlers ran %d times, want 3", ran)
	}
	if g, p := sc.gets.Load(), sc.puts.Load(); g != 0 || p != 0 {
		t.Fatalf("side channel saw %d gets and %d puts, want none", g, p)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("recorded %d events with no session", n)
	}
}

// With no session set installed at all (the SDK's state before Start, and
// after Stop) nothing can be active either.
func TestConsumeViaSkipsLookupWithoutSessionSet(t *testing.T) {
	setup(t, true)
	core.SetSessions(nil)
	sc := &hangSide{}
	start := time.Now()
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", MessageID: "id1"}, func(context.Context) error { return nil })
	if el := time.Since(start); el > wellUnderBound || sc.gets.Load() != 0 {
		t.Fatalf("waited %v, gets=%d", el, sc.gets.Load())
	}
}

func TestConsumeViaLooksUpWhenSessionActive(t *testing.T) {
	rec, ctx := setup(t, true) // session s1 is active
	sc := &countSide{}
	if err := PublishVia(ctx, sc, "id1", Message{Queue: "q", JobName: "j"}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "id1"},
		func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if p, g := sc.puts.Load(), sc.gets.Load(); p != 1 || g != 1 {
		t.Fatalf("puts=%d gets=%d, want 1 and 1", p, g)
	}
	assertLinked(t, rec)
}

// An exclusive session attributes every request, so the lookup must still
// happen: a carrier in the side channel links the worker to its publisher.
func TestConsumeViaLooksUpForExclusiveSession(t *testing.T) {
	rec, _ := setup(t, true)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"ex": true}, ExclusiveID: "ex"})
	sc := &countSide{}
	pubCtx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "ex", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()})
	_ = PublishVia(pubCtx, sc, "id1", Message{Queue: "q", JobName: "j"}, func(context.Context) error { return nil })
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "id1"}, func(context.Context) error { return nil })
	if g := sc.gets.Load(); g != 1 {
		t.Fatalf("gets=%d, want 1", g)
	}
	assertLinked(t, rec)

	// Exclusive id alone (no entry in the active list) still counts.
	core.SetSessions(core.StaticSessions{ExclusiveID: "ex"})
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "other"}, func(context.Context) error { return nil })
	if g := sc.gets.Load(); g != 2 {
		t.Fatalf("gets=%d, want 2", g)
	}
}

// A session set that cannot say whether anything is active keeps the old
// behaviour: the lookup happens.
func TestConsumeViaLooksUpWhenSetCannotTell(t *testing.T) {
	setup(t, true)
	core.SetSessions(plainSet{})
	sc := &countSide{}
	_ = ConsumeVia(context.Background(), sc, Delivery{Queue: "q", MessageID: "id1"}, func(context.Context) error { return nil })
	_ = ConsumeSQSMessageOpts(context.Background(), "q", sqsMsg(`{}`), 0, SQSOptions{SideChannel: sc}, func(context.Context) error { return nil })
	if g := sc.gets.Load(); g != 2 {
		t.Fatalf("gets=%d, want 2", g)
	}
}

func TestSQSSideChannelLookupWhenSessionActive(t *testing.T) {
	sc := &countSide{}
	rec, _, _ := sqsRound(t, 10, SQSOptions{SideChannel: sc}) // no free attribute slot
	if p, g := sc.puts.Load(), sc.gets.Load(); p != 1 || g != 1 {
		t.Fatalf("puts=%d gets=%d, want 1 and 1", p, g)
	}
	assertLinked(t, rec)
}

// A publish outside a session never writes to the side channel, whether or not
// another session is active.
func TestUntracedPublishDoesNotWriteSideChannel(t *testing.T) {
	rec, _ := setup(t, true) // s1 active, but these publishes carry no session
	sc := &hangSide{}
	sent := 0
	start := time.Now()
	if err := PublishVia(context.Background(), sc, "id1", Message{Queue: "q", JobName: "j"},
		func(context.Context) error { sent++; return nil }); err != nil {
		t.Fatal(err)
	}
	c := &fakeSQS{}
	in := &sqs.SendMessageInput{QueueUrl: aws.String("q"), MessageBody: aws.String(`{}`), MessageAttributes: fill(10)}
	if _, err := SendMessageOpts(context.Background(), c, SQSOptions{SideChannel: sc}, in); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > wellUnderBound {
		t.Fatalf("untraced publishes waited %v", el)
	}
	if sent != 1 || len(c.sent) != 1 || len(c.sent[0].MessageAttributes) != 10 {
		t.Fatalf("sent=%d sqs=%d", sent, len(c.sent))
	}
	if p := sc.puts.Load(); p != 0 {
		t.Fatalf("side channel saw %d puts, want none", p)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("recorded %d events", n)
	}
}

// ---- benchmarks: consumers while no session is active ----

func benchNoSession(b *testing.B) {
	b.Helper()
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetSessions(core.StaticSessions{})
	b.Cleanup(func() { core.Configure(core.Config{}); core.SetSessions(nil) })
	b.ReportAllocs()
}

var benchHandler = func(context.Context) error { return nil }

// A healthy in-memory store: the cost is the bounded-lookup machinery.
func BenchmarkConsumeViaUntraced(b *testing.B) {
	benchNoSession(b)
	sc := &MemSideChannel{}
	d := Delivery{System: "asynq", Queue: "q", JobName: "j", MessageID: "id1"}
	ctx := context.Background()
	for b.Loop() {
		_ = ConsumeVia(ctx, sc, d, benchHandler)
	}
}

// A store that does not answer: the cost is the wait for the bound.
func BenchmarkConsumeViaUntracedSlowStore(b *testing.B) {
	benchNoSession(b)
	sc := &hangSide{}
	d := Delivery{System: "asynq", Queue: "q", JobName: "j", MessageID: "id1"}
	ctx := context.Background()
	for b.Loop() {
		_ = ConsumeVia(ctx, sc, d, benchHandler)
	}
}

func BenchmarkWrapHandlerUntraced(b *testing.B) {
	benchNoSession(b)
	h := WrapHandler(&MemSideChannel{}, "asynq", "q", "j", 3, func(context.Context, string, int, []byte) error { return nil })
	ctx, body := context.Background(), []byte(`{"order_id":"o-1","qty":2}`)
	for b.Loop() {
		_ = h(ctx, "id1", 1, body)
	}
}

func BenchmarkConsumeSQSSideChannelUntraced(b *testing.B) {
	benchNoSession(b)
	opts := SQSOptions{SideChannel: &MemSideChannel{}}
	msg := types.Message{MessageId: aws.String("m"), Body: aws.String(`{"order_id":"o-1","qty":2}`),
		MD5OfBody: aws.String("0123456789abcdef0123456789abcdef")}
	ctx := context.Background()
	for b.Loop() {
		_ = ConsumeSQSMessageOpts(ctx, "q", msg, 0, opts, benchHandler)
	}
}

func BenchmarkConsumeSQSSideChannelUntracedSlowStore(b *testing.B) {
	benchNoSession(b)
	opts := SQSOptions{SideChannel: &hangSide{}}
	msg := types.Message{MessageId: aws.String("m"), Body: aws.String(`{"order_id":"o-1","qty":2}`),
		MD5OfBody: aws.String("0123456789abcdef0123456789abcdef")}
	ctx := context.Background()
	for b.Loop() {
		_ = ConsumeSQSMessageOpts(ctx, "q", msg, 0, opts, benchHandler)
	}
}
