package tkafkaconfluent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"tsession/core"
)

type fakeProducer struct {
	got   []*kafka.Message
	calls int
	err   error
}

func (f *fakeProducer) Produce(m *kafka.Message, _ chan kafka.Event) error {
	f.calls++
	if m != nil {
		cp := *m
		cp.Headers = append([]kafka.Header(nil), m.Headers...)
		f.got = append(f.got, &cp)
	}
	return f.err
}

func setup(t *testing.T) (*core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	return rec, core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
}

func str(s string) *string { return &s }

func hdr(h []kafka.Header, k string) (string, int) {
	n, v := 0, ""
	for _, x := range h {
		if strings.EqualFold(x.Key, k) {
			n++
			v = string(x.Value)
		}
	}
	return v, n
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: str("orders"), Partition: 3},
		Key:            []byte("k1"),
		Value:          []byte(`{"id":1,"password":"hunter2","qty":2}`),
		Headers:        []kafka.Header{{Key: "app", Value: []byte("x")}},
	}
	if err := Produce(ctx, p, msg, nil); err != nil {
		t.Fatal(err)
	}
	sent := p.got[0]
	if v, _ := hdr(sent.Headers, "app"); v != "x" {
		t.Fatalf("existing header clobbered: %+v", sent.Headers)
	}
	if len(sent.Headers) < 2 {
		t.Fatalf("carrier not injected: %+v", sent.Headers)
	}
	ev := rec.Events()
	if len(ev) != 1 || ev[0].Kind != core.KindQueuePub || ev[0].Name != "orders" || ev[0].Attrs["system"] != "kafka" {
		t.Fatalf("%+v", ev)
	}
	req := string(ev[0].Req.Inline)
	if strings.Contains(req, "hunter2") || !strings.Contains(req, `"qty":2`) || !strings.Contains(req, `"topic":"orders"`) ||
		!strings.Contains(req, `"partition":3`) || !strings.Contains(req, `"key":"k1"`) {
		t.Fatalf("req=%s", req)
	}

	// consumer in a fresh context: trace comes only from headers
	cons := *sent
	cons.TopicPartition.Offset = 7
	ran := 0
	if err := Consume(context.Background(), &cons, func(context.Context) error { ran++; return nil }); err != nil || ran != 1 {
		t.Fatal(err, ran)
	}
	ev = rec.Events()
	if len(ev) != 3 || ev[1].Kind != core.KindWorkerStart || ev[2].Kind != core.KindWorkerEnd {
		t.Fatalf("%+v", ev)
	}
	if ev[1].TraceID != ev[0].TraceID {
		t.Fatal("trace not continued")
	}
	if ev[1].Attrs["message_id"] != "orders/3/7" {
		t.Fatalf("attrs=%v", ev[1].Attrs)
	}
	if len(ev[2].LinkIDs) == 0 || ev[2].LinkIDs[0] != ev[0].SpanID {
		t.Fatalf("worker.end not linked to publish: %+v", ev[2].LinkIDs)
	}
}

func TestProduceReplacesNoDuplicateHeaders(t *testing.T) {
	_, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("t"), Partition: kafka.PartitionAny}, Value: []byte("hi")}
	Produce(ctx, p, msg, nil)
	Produce(ctx, p, msg, nil) // reuse of the same message (retry)
	for _, h := range p.got[1].Headers {
		if _, n := hdr(p.got[1].Headers, h.Key); n != 1 {
			t.Fatalf("duplicate %q: %+v", h.Key, p.got[1].Headers)
		}
	}
}

func TestProducePassthrough(t *testing.T) {
	// disabled
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("t")}, Value: []byte("v")}
	if err := Produce(context.Background(), p, msg, nil); err != nil || p.calls != 1 || len(p.got[0].Headers) != 0 {
		t.Fatal("disabled must be untouched", err, p.got)
	}
	// enabled, no session in ctx
	rec, _ := setup(t)
	if err := Produce(context.Background(), p, msg, nil); err != nil || p.calls != 2 || len(p.got[1].Headers) != 0 || len(rec.Events()) != 0 {
		t.Fatal("no-session must be untouched")
	}
	// nil message passes through to the producer
	if err := Produce(context.Background(), p, nil, nil); err != nil || p.calls != 3 {
		t.Fatal(err)
	}
}

func TestProduceErrorPropagates(t *testing.T) {
	rec, ctx := setup(t)
	want := errors.New("queue full")
	p := &fakeProducer{err: want}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("t")}, Value: []byte("v")}
	if err := Produce(ctx, p, msg, nil); err != want {
		t.Fatal(err)
	}
	if ev := rec.Events(); len(ev) != 1 || ev[0].Err == "" {
		t.Fatalf("%+v", ev)
	}
}

func TestHeaderLimit(t *testing.T) {
	var hs []kafka.Header
	c := newCarrier(&hs, true)
	c.Set("big", strings.Repeat("x", maxInjectBytes+1))
	if len(hs) != 0 {
		t.Fatal("oversized header must be dropped")
	}
	for i := 0; i < 20; i++ {
		c.Set("k"+string(rune('a'+i)), "v")
	}
	if len(hs) != maxInjectHeaders {
		t.Fatalf("headers=%d", len(hs))
	}
	var un []kafka.Header
	newCarrier(&un, false).Set("big", strings.Repeat("x", 5000))
	if len(un) != 1 {
		t.Fatal("unlimited carrier must accept")
	}
}

func TestConsumeRetrySemantics(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("jobs"), Partition: 0}, Value: []byte(`{"a":1}`)}
	Produce(ctx, p, msg, nil)
	in := p.got[0]
	boom := errors.New("boom")

	err := Consume(context.Background(), in, func(context.Context) error { return boom },
		WithRetryExpected(func(a int, e error) bool { return true }))
	if err != boom {
		t.Fatal(err)
	}
	var kinds []string
	var pending string
	for _, e := range rec.Events()[1:] {
		kinds = append(kinds, string(e.Kind))
		if e.Kind == core.KindWorkerEnd {
			pending = e.Attrs["retry_pending"]
		}
	}
	if !contains(kinds, string(core.KindWorkerRetry)) || pending != "true" {
		t.Fatalf("kinds=%v pending=%q", kinds, pending)
	}

	// final attempt -> dlq, no retry_pending
	n := len(rec.Events())
	Consume(context.Background(), in, func(context.Context) error { return boom },
		WithMaxAttempts(3), WithAttempt(func(*kafka.Message) int { return 3 }))
	kinds = nil
	for _, e := range rec.Events()[n:] {
		kinds = append(kinds, string(e.Kind))
		if e.Kind == core.KindWorkerEnd && e.Attrs["retry_pending"] != "" {
			t.Fatal("last attempt must not be retry_pending")
		}
	}
	if !contains(kinds, string(core.KindWorkerDLQ)) {
		t.Fatalf("kinds=%v", kinds)
	}
}

func TestConsumePassthrough(t *testing.T) {
	ran := 0
	h := func(context.Context) error { ran++; return nil }
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("t")}}
	if Consume(context.Background(), msg, h) != nil || ran != 1 { // disabled
		t.Fatal(ran)
	}
	rec, _ := setup(t)
	if Consume(context.Background(), msg, h) != nil || ran != 2 || len(rec.Events()) != 0 { // no session in headers
		t.Fatal(ran, rec.Events())
	}
	if Consume(context.Background(), nil, h) != nil || ran != 3 {
		t.Fatal(ran)
	}
	if Consume(context.Background(), msg, nil) != nil {
		t.Fatal("nil handler")
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
