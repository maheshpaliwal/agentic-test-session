package tqueue

import (
	"context"
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
