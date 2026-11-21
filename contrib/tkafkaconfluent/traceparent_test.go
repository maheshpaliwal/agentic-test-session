package tkafkaconfluent

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// The application's trace context, as its own tracing would put it on a record.
const appTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func pubs(rec *core.MemRecorder) []core.Event {
	var out []core.Event
	for _, e := range rec.Events() {
		if e.Kind == core.KindQueuePub {
			out = append(out, e)
		}
	}
	return out
}

func tpFor(pub core.Event) string { return "00-" + pub.TraceID + "-" + pub.SpanID + "-01" }

// assertWorker checks that the latest worker.start is a child of pub.
func assertWorker(t *testing.T, rec *core.MemRecorder, pub core.Event) {
	t.Helper()
	var w *core.Event
	evs := rec.Events()
	for i := range evs {
		if evs[i].Kind == core.KindWorkerStart {
			w = &evs[i]
		}
	}
	if w == nil {
		t.Fatal("no worker.start")
	}
	if w.ParentID != pub.SpanID || w.TraceID != pub.TraceID {
		t.Fatalf("worker parent %q trace %q, want publish span %q trace %q", w.ParentID, w.TraceID, pub.SpanID, pub.TraceID)
	}
}

func TestProduceKeepsApplicationTraceparent(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("orders")}, Value: []byte(`{}`),
		Headers: []kafka.Header{{Key: "traceparent", Value: []byte(appTP)}}}
	if err := Produce(ctx, p, msg, nil); err != nil {
		t.Fatal(err)
	}
	sent, pub := p.got[0], pubs(rec)[0]
	if v, n := hdr(sent.Headers, "traceparent"); n != 1 || v != appTP {
		t.Fatalf("application traceparent is now %q (%d headers)", v, n)
	}
	if v, n := hdr(sent.Headers, core.KeyTSessionTraceparent); n != 1 || v != tpFor(pub) {
		t.Fatalf("%s = %q, want %q", core.KeyTSessionTraceparent, v, tpFor(pub))
	}
	if err := Consume(context.Background(), sent, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertWorker(t, rec, pub)
}

func TestProduceWritesBothKeysWithoutApplicationTracing(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("orders")}, Value: []byte(`{}`)}
	if err := Produce(ctx, p, msg, nil); err != nil {
		t.Fatal(err)
	}
	want := tpFor(pubs(rec)[0])
	own, _ := hdr(p.got[0].Headers, core.KeyTSessionTraceparent)
	tp, _ := hdr(p.got[0].Headers, "traceparent")
	if own != want || tp != want {
		t.Fatalf("traceparent %q, %s %q, want both %q", tp, core.KeyTSessionTraceparent, own, want)
	}
}

// A consumer that produces the record it received, headers included, and a
// producer that sends one message value twice: the context of the earlier
// publish must not travel on.
func TestProduceReplacesStaleContext(t *testing.T) {
	for name, app := range map[string]string{"no application traceparent": "", "application traceparent": appTP} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t)
			p := &fakeProducer{}
			first := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a")}, Value: []byte(`{}`)}
			if app != "" {
				first.Headers = []kafka.Header{{Key: "traceparent", Value: []byte(app)}}
			}
			if err := Produce(ctx, p, first, nil); err != nil {
				t.Fatal(err)
			}
			received := p.got[0]
			err := Consume(context.Background(), received, func(hctx context.Context) error {
				return Produce(hctx, p, &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("b")}, Headers: received.Headers}, nil)
			})
			if err != nil {
				t.Fatal(err)
			}
			second, pub2 := p.got[1], pubs(rec)[1]
			if v, n := hdr(second.Headers, core.KeyTSessionTraceparent); n != 1 || v != tpFor(pub2) {
				t.Fatalf("%s = %q (%d), want the second publish %q", core.KeyTSessionTraceparent, v, n, tpFor(pub2))
			}
			want := tpFor(pub2)
			if app != "" {
				want = app
			}
			if v, n := hdr(second.Headers, "traceparent"); n != 1 || v != want {
				t.Fatalf("traceparent = %q (%d), want %q", v, n, want)
			}
			if err := Consume(context.Background(), second, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			assertWorker(t, rec, pub2)
		})
	}
}

// A record from a producer whose SDK predates x-tsession-traceparent.
func TestConsumeReadsTraceparentFromOlderSDK(t *testing.T) {
	rec, _ := setup(t)
	trace, span := core.NewTraceID(), core.NewSpanID()
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a")}, Headers: []kafka.Header{
		{Key: core.KeySession, Value: []byte("s1")},
		{Key: core.KeyTraceparent, Value: []byte("00-" + trace + "-" + span + "-01")},
	}}
	if err := Consume(context.Background(), msg, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertWorker(t, rec, core.Event{TraceID: trace, SpanID: span})
}

// The injection budget (8 headers, 1 KiB) still holds the whole carrier.
func TestCarrierFitsInjectionBudget(t *testing.T) {
	_, ctx := setup(t)
	p := &fakeProducer{}
	if err := Produce(ctx, p, &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a")}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{core.KeyTSessionTraceparent, core.KeyTraceparent, core.KeySession, core.KeyHLC} {
		if _, n := hdr(p.got[0].Headers, k); n != 1 {
			t.Fatalf("%s: %d headers", k, n)
		}
	}
	if n := len(p.got[0].Headers); n > maxInjectHeaders {
		t.Fatalf("%d headers injected", n)
	}
}
