package tqueue

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func assertLinked(t *testing.T, rec *core.MemRecorder) {
	t.Helper()
	evs := rec.Events()
	pub, st, en := byKind(evs, core.KindQueuePub), byKind(evs, core.KindWorkerStart), byKind(evs, core.KindWorkerEnd)
	if len(pub) != 1 || len(st) != 1 || len(en) != 1 {
		t.Fatalf("want 1/1/1 got %d/%d/%d", len(pub), len(st), len(en))
	}
	if st[0].TraceID != pub[0].TraceID || !contains(st[0].LinkIDs, pub[0].SpanID) || !contains(en[0].LinkIDs, pub[0].SpanID) {
		t.Fatal("worker not linked to publish")
	}
}

type fakeKafka struct{ got []kafka.Message }

func (f *fakeKafka) WriteMessages(_ context.Context, m ...kafka.Message) error {
	f.got = append(f.got, m...)
	return nil
}

func TestKafkaRoundTrip(t *testing.T) {
	rec, ctx := setup(t, true)
	w := &fakeKafka{}
	orig := kafka.Message{Topic: "orders", Value: []byte(`{"id":1}`), Headers: []kafka.Header{{Key: "x", Value: []byte("1")}}}
	if err := ProduceMessages(ctx, w, orig); err != nil || len(w.got) != 1 {
		t.Fatal(err)
	}
	if len(orig.Headers) != 1 {
		t.Fatal("caller headers mutated")
	}
	ran := false
	if err := ConsumeMessage(context.Background(), w.got[0], func(context.Context) error { ran = true; return nil }); err != nil || !ran {
		t.Fatal("handler not run")
	}
	assertLinked(t, rec)
}

type fakeSQS struct{ sent []*sqs.SendMessageInput }

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.sent = append(f.sent, in)
	return &sqs.SendMessageOutput{MessageId: aws.String("m1")}, nil
}
func (f *fakeSQS) ReceiveMessage(_ context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func TestSQSRoundTrip(t *testing.T) {
	rec, ctx := setup(t, true)
	c := &fakeSQS{}
	in := &sqs.SendMessageInput{QueueUrl: aws.String("q-url"), MessageBody: aws.String(`{"a":1}`)}
	if _, err := SendMessage(ctx, c, in); err != nil {
		t.Fatal(err)
	}
	if in.MessageAttributes != nil {
		t.Fatal("input mutated")
	}
	msg := types.Message{MessageId: aws.String("m1"), Body: in.MessageBody, MessageAttributes: c.sent[0].MessageAttributes,
		Attributes: map[string]string{"ApproximateReceiveCount": "2"}}
	if err := ConsumeSQSMessage(context.Background(), "q-url", msg, 0, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertLinked(t, rec)
	if st := byKind(rec.Events(), core.KindWorkerStart); st[0].Attempt != 2 {
		t.Fatalf("attempt = %d", st[0].Attempt)
	}
}

type fakeAMQP struct{ got []amqp.Publishing }

func (f *fakeAMQP) PublishWithContext(_ context.Context, _, _ string, _, _ bool, m amqp.Publishing) error {
	f.got = append(f.got, m)
	return nil
}

func TestAMQPRoundTrip(t *testing.T) {
	rec, ctx := setup(t, true)
	ch := &fakeAMQP{}
	if err := PublishAMQP(ctx, ch, "ex", "job.run", false, false, amqp.Publishing{Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	dl := amqp.Delivery{Headers: ch.got[0].Headers, RoutingKey: "job.run", Body: ch.got[0].Body, Redelivered: true, DeliveryTag: 7}
	if err := ConsumeDelivery(context.Background(), "q", dl, 0, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertLinked(t, rec)
}

func TestKafkaNoSessionSingleBatch(t *testing.T) {
	rec, _ := setup(t, true)
	calls := 0
	w := kw{func(m ...kafka.Message) { calls++; _ = m }}
	if err := ProduceMessages(context.Background(), w, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"}); err != nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	if len(rec.Events()) != 0 {
		t.Fatal("no session must be untraced")
	}
}

type kw struct{ f func(...kafka.Message) }

func (k kw) WriteMessages(_ context.Context, m ...kafka.Message) error { k.f(m...); return nil }

func TestKafkaPerMessageSpansOption(t *testing.T) {
	rec, ctx := setup(t, true)
	calls := 0
	w := kw{func(...kafka.Message) { calls++ }}
	_ = ProduceMessages(ctx, w, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"})
	if calls != 1 || len(byKind(rec.Events(), core.KindQueuePub)) != 2 {
		t.Fatalf("per-message spans must not split the batch: calls=%d", calls)
	}
	calls = 0
	off := false
	var got []kafka.Message
	w2 := kw{func(m ...kafka.Message) { calls++; got = m }}
	_ = ProduceMessagesOpts(ctx, w2, KafkaOptions{PerMessageSpans: &off}, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"})
	if calls != 1 || len(got) != 2 || len(got[0].Headers) == 0 || len(got[1].Headers) == 0 {
		t.Fatalf("batch mode: calls=%d got=%+v", calls, got)
	}
}

func fill(n int) map[string]types.MessageAttributeValue {
	m := map[string]types.MessageAttributeValue{}
	for i := 0; i < n; i++ {
		m["a"+string(rune('a'+i))] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("v")}
	}
	return m
}

func sqsRound(t *testing.T, existing int, opts SQSOptions) (*core.MemRecorder, *sqs.SendMessageInput, int) {
	rec, ctx := setup(t, true)
	c := &fakeSQS{}
	in := &sqs.SendMessageInput{QueueUrl: aws.String("q"), MessageBody: aws.String(`{"password":"hunter2"}`), MessageAttributes: fill(existing)}
	if _, err := SendMessageOpts(ctx, c, opts, in); err != nil {
		t.Fatal(err)
	}
	sent := c.sent[0]
	if len(sent.MessageAttributes) > 10 {
		t.Fatalf("attr limit exceeded: %d", len(sent.MessageAttributes))
	}
	msg := types.Message{MessageId: aws.String("m"), Body: in.MessageBody, MessageAttributes: sent.MessageAttributes}
	_ = ConsumeSQSMessageOpts(context.Background(), "q", msg, 0, opts, func(context.Context) error { return nil })
	return rec, sent, len(sent.MessageAttributes)
}

func TestSQSAttrLimit(t *testing.T) {
	rec, _, n := sqsRound(t, 7, SQSOptions{})
	if n != 10 {
		t.Fatalf("7+3 attrs = %d", n)
	}
	assertLinked(t, rec)

	rec, sent, n := sqsRound(t, 9, SQSOptions{})
	if n != 10 || sent.MessageAttributes[ctxAttr].StringValue == nil {
		t.Fatalf("expected compact %s attr, n=%d", ctxAttr, n)
	}
	assertLinked(t, rec)

	sc := &MemSideChannel{}
	rec, _, n = sqsRound(t, 10, SQSOptions{SideChannel: sc})
	if n != 10 || sc.Len() != 1 {
		t.Fatalf("side channel: n=%d len=%d", n, sc.Len())
	}
	assertLinked(t, rec)

	rec, _, n = sqsRound(t, 10, SQSOptions{})
	if n != 10 {
		t.Fatalf("n=%d", n)
	}
	if p := byKind(rec.Events(), core.KindQueuePub); len(p) != 1 || !contains(p[0].Flags, FlagAttrLimit) {
		t.Fatalf("flag missing: %+v", p)
	}
	if len(byKind(rec.Events(), core.KindWorkerStart)) != 0 {
		t.Fatal("skipped propagation must leave consumer untraced")
	}
	for _, e := range byKind(rec.Events(), core.KindQueuePub) {
		if strings.Contains(string(e.Req.Inline), "hunter2") {
			t.Fatal("sqs body secret leaked")
		}
	}
}

func strAttr(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
}

// The compact carrier must not take over an attribute the application owns:
// neither the old "ts" name nor, if the application happens to use it, the
// namespaced one.
func TestSQSCompactCarrierNeverOverwritesAppAttr(t *testing.T) {
	for _, name := range []string{"ts", ctxAttr} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			c := &fakeSQS{}
			attrs := fill(8)
			attrs[name] = strAttr("1700000000")
			in := &sqs.SendMessageInput{QueueUrl: aws.String("q"), MessageBody: aws.String(`{}`), MessageAttributes: attrs}
			if _, err := SendMessage(ctx, c, in); err != nil {
				t.Fatal(err)
			}
			sent := c.sent[0].MessageAttributes
			if got := aws.ToString(sent[name].StringValue); got != "1700000000" {
				t.Fatalf("application attribute %q overwritten with %q", name, got)
			}
			if len(sent) > maxSQSAttrs {
				t.Fatalf("%d attributes sent", len(sent))
			}
			pub := byKind(rec.Events(), core.KindQueuePub)
			if len(pub) != 1 {
				t.Fatalf("publish spans: %d", len(pub))
			}
			// "ts" leaves one slot for the namespaced carrier; a taken
			// namespaced name is treated as no room.
			if skipped := contains(pub[0].Flags, FlagAttrLimit); skipped != (name == ctxAttr) {
				t.Fatalf("flags = %v", pub[0].Flags)
			}
		})
	}
}

// Messages from producers that still write the compact carrier as "ts" keep
// their trace; an application "ts" attribute is ignored.
func TestSQSConsumeAcceptsLegacyCompactAttr(t *testing.T) {
	rec, _ := setup(t, true)
	trace, span := core.NewTraceID(), core.NewSpanID()
	legacy := encodeTS(core.MapCarrier{core.KeySession: "s1", core.KeyTraceparent: "00-" + trace + "-" + span + "-01"})
	msg := types.Message{MessageId: aws.String("m1"), Body: aws.String(`{}`),
		MessageAttributes: map[string]types.MessageAttributeValue{legacyCtxAttr: strAttr(legacy)}}
	if err := ConsumeSQSMessage(context.Background(), "q", msg, 0, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	st := byKind(rec.Events(), core.KindWorkerStart)
	if len(st) != 1 || st[0].TraceID != trace || !contains(st[0].LinkIDs, span) {
		t.Fatalf("legacy carrier not honoured: %+v", st)
	}

	msg.MessageAttributes = map[string]types.MessageAttributeValue{legacyCtxAttr: strAttr("1700000000")}
	if err := ConsumeSQSMessage(context.Background(), "q", msg, 0, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if n := len(byKind(rec.Events(), core.KindWorkerStart)); n != 1 {
		t.Fatalf("application ts attribute started a worker span (%d)", n)
	}
}

func TestSQSReceiveRequestsBothCompactAttrs(t *testing.T) {
	setup(t, true)
	var got *sqs.ReceiveMessageInput
	c := recvSQS{func(in *sqs.ReceiveMessageInput) { got = in }}
	in := &sqs.ReceiveMessageInput{QueueUrl: aws.String("q"), MessageAttributeNames: []string{"app"}}
	if _, err := ReceiveMessage(context.Background(), c, in); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"app", ctxAttr, legacyCtxAttr, core.KeySession} {
		if !contains(got.MessageAttributeNames, want) {
			t.Fatalf("%q not requested: %v", want, got.MessageAttributeNames)
		}
	}
	if len(in.MessageAttributeNames) != 1 {
		t.Fatal("input mutated")
	}
}

type recvSQS struct {
	f func(*sqs.ReceiveMessageInput)
}

func (recvSQS) SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{}, nil
}

func (r recvSQS) ReceiveMessage(_ context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	r.f(in)
	return &sqs.ReceiveMessageOutput{}, nil
}

// batchWriter records every WriteMessages call and returns err.
type batchWriter struct {
	calls [][]kafka.Message
	err   error
}

func (b *batchWriter) WriteMessages(_ context.Context, m ...kafka.Message) error {
	b.calls = append(b.calls, m)
	return b.err
}

func header(m kafka.Message, key string) string { return kafkaCarrier{&m.Headers}.Get(key) }

// Per-message spans must not change how the application batches: one
// WriteMessages call, each message carrying its own publish span.
func TestKafkaPerMessageSpansSingleWrite(t *testing.T) {
	rec, ctx := setup(t, true)
	w := &batchWriter{}
	in := []kafka.Message{
		{Topic: "a", Value: []byte(`{"n":1}`), Headers: []kafka.Header{{Key: "app", Value: []byte("1")}}},
		{Topic: "a", Value: []byte(`{"n":2}`)},
		{Topic: "b", Value: []byte(`{"n":3}`)},
	}
	if err := ProduceMessages(ctx, w, in...); err != nil {
		t.Fatal(err)
	}
	if len(w.calls) != 1 || len(w.calls[0]) != len(in) {
		t.Fatalf("want one WriteMessages call with the whole batch, got %d calls", len(w.calls))
	}
	if len(in[0].Headers) != 1 || len(in[1].Headers) != 0 {
		t.Fatal("caller's messages mutated")
	}
	pubs := byKind(rec.Events(), core.KindQueuePub)
	if len(pubs) != len(in) {
		t.Fatalf("publish spans: %d", len(pubs))
	}
	for i, m := range w.calls[0] {
		if m.Topic != in[i].Topic || string(m.Value) != string(in[i].Value) {
			t.Fatalf("message %d reordered or altered", i)
		}
		// Each message carries the context of its own span, in order.
		tp := header(m, core.KeyTraceparent)
		if header(m, core.KeySession) != "s1" || !strings.Contains(tp, pubs[i].SpanID) {
			t.Fatalf("message %d: traceparent %q does not name span %s", i, tp, pubs[i].SpanID)
		}
		if pubs[i].Name != m.Topic || pubs[i].Attrs["queue"] != m.Topic || pubs[i].Attrs["system"] != "kafka" {
			t.Fatalf("span %d: %+v", i, pubs[i])
		}
		if !strings.Contains(string(pubs[i].Req.Inline), `"n":`+string(rune('1'+i))) {
			t.Fatalf("span %d req = %s", i, pubs[i].Req.Inline)
		}
	}
	if header(w.calls[0][0], "app") != "1" {
		t.Fatal("application header dropped")
	}

	// Every message links to exactly one worker.
	for _, m := range w.calls[0] {
		if err := ConsumeMessage(context.Background(), m, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for i, st := range byKind(rec.Events(), core.KindWorkerStart) {
		if !contains(st.LinkIDs, pubs[i].SpanID) {
			t.Fatalf("worker %d not linked to its publish span", i)
		}
	}
}

func TestKafkaPerMessageSpansErrors(t *testing.T) {
	rec, ctx := setup(t, true)
	boom := errors.New("broker down")
	w := &batchWriter{err: boom}
	if err := ProduceMessages(ctx, w, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	for _, p := range byKind(rec.Events(), core.KindQueuePub) {
		if p.Err != "broker down" {
			t.Fatalf("batch error not on span: %q", p.Err)
		}
	}

	// kafka.WriteErrors attributes the failure to the message it belongs to.
	rec, ctx = setup(t, true)
	w = &batchWriter{err: kafka.WriteErrors{nil, boom}}
	if err := ProduceMessages(ctx, w, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"}); err == nil {
		t.Fatal("error swallowed")
	}
	pubs := byKind(rec.Events(), core.KindQueuePub)
	if len(pubs) != 2 || pubs[0].Err != "" || pubs[1].Err != "broker down" {
		t.Fatalf("per-message errors: %+v", pubs)
	}
}

type panicWriter struct{}

func (panicWriter) WriteMessages(context.Context, ...kafka.Message) error { panic("boom") }

func TestKafkaPerMessageSpansEndOnPanic(t *testing.T) {
	rec, ctx := setup(t, true)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic swallowed")
			}
		}()
		_ = ProduceMessages(ctx, panicWriter{}, kafka.Message{Topic: "a"}, kafka.Message{Topic: "a"})
	}()
	pubs := byKind(rec.Events(), core.KindQueuePub)
	if len(pubs) != 2 || !strings.Contains(pubs[0].Err, "panic: boom") || !strings.Contains(pubs[1].Err, "panic: boom") {
		t.Fatalf("spans not ended on panic: %+v", pubs)
	}
}
