package tqueue

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"tsession/core"
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
	if calls != 2 || len(byKind(rec.Events(), core.KindQueuePub)) != 2 {
		t.Fatalf("per-message: calls=%d", calls)
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
	if n != 10 || sent.MessageAttributes["ts"].StringValue == nil {
		t.Fatalf("expected compact ts attr, n=%d", n)
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
