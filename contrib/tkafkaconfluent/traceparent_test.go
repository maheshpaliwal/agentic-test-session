package tkafkaconfluent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

// ownFor is the x-tsession-traceparent value for that span among headers h:
// the trace context bound to the x-tsession-hlc written with it.
func ownFor(pub core.Event, h []kafka.Header) string {
	hlc, _ := hdr(h, core.KeyHLC)
	return tpFor(pub) + ";" + hlc
}

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
		panic("no worker.start")
	}
	if w.ParentID != pub.SpanID || w.TraceID != pub.TraceID {
		panic(fmt.Sprintf("worker parent %q trace %q, want publish span %q trace %q", w.ParentID, w.TraceID, pub.SpanID, pub.TraceID))
	}
}

func TestProduceKeepsApplicationTraceparent(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("orders")}, Value: []byte(`{}`),
		Headers: []kafka.Header{{Key: "traceparent", Value: []byte(appTP)}}}
	if err := Produce(ctx, p, msg, nil); err != nil {
		panic(err)
	}
	sent, pub := p.got[0], pubs(rec)[0]
	if v, n := hdr(sent.Headers, "traceparent"); n != 1 || v != appTP {
		panic(fmt.Sprintf("application traceparent is now %q (%d headers)", v, n))
	}
	if v, n := hdr(sent.Headers, core.KeyTSessionTraceparent); n != 1 || v != ownFor(pub, sent.Headers) {
		panic(fmt.Sprintf("%s = %q, want %q", core.KeyTSessionTraceparent, v, ownFor(pub, sent.Headers)))
	}
	if err := Consume(context.Background(), sent, func(context.Context) error { return nil }); err != nil {
		panic(err)
	}
	assertWorker(t, rec, pub)
}

// A traceparent header the application sent with no value is still its own.
func TestProduceLeavesEmptyApplicationTraceparent(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("orders")}, Value: []byte(`{}`),
		Headers: []kafka.Header{{Key: "traceparent"}}}
	if err := Produce(ctx, p, msg, nil); err != nil {
		panic(err)
	}
	sent := p.got[0]
	if v, n := hdr(sent.Headers, "traceparent"); n != 1 || v != "" {
		panic(fmt.Sprintf("empty application traceparent is now %q (%d headers)", v, n))
	}
	if err := Consume(context.Background(), sent, func(context.Context) error { return nil }); err != nil {
		panic(err)
	}
	assertWorker(t, rec, pubs(rec)[0])
}

func TestProduceWritesBothKeysWithoutApplicationTracing(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("orders")}, Value: []byte(`{}`)}
	if err := Produce(ctx, p, msg, nil); err != nil {
		panic(err)
	}
	want := tpFor(pubs(rec)[0])
	own, _ := hdr(p.got[0].Headers, core.KeyTSessionTraceparent)
	tp, _ := hdr(p.got[0].Headers, "traceparent")
	if hlc, n := hdr(p.got[0].Headers, core.KeyHLC); n != 1 || own != want+";"+hlc || tp != want {
		panic(fmt.Sprintf("traceparent %q, %s %q, want %q and that with the clock %q", tp, core.KeyTSessionTraceparent, own, want, hlc))
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
				panic(err)
			}
			received := p.got[0]
			err := Consume(context.Background(), received, func(hctx context.Context) error {
				return Produce(hctx, p, &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("b")}, Headers: received.Headers}, nil)
			})
			if err != nil {
				panic(err)
			}
			second, pub2 := p.got[1], pubs(rec)[1]
			if v, n := hdr(second.Headers, core.KeyTSessionTraceparent); n != 1 || v != ownFor(pub2, second.Headers) {
				panic(fmt.Sprintf("%s = %q (%d), want the second publish %q", core.KeyTSessionTraceparent, v, n, ownFor(pub2, second.Headers)))
			}
			want := tpFor(pub2)
			if app != "" {
				want = app
			}
			if v, n := hdr(second.Headers, "traceparent"); n != 1 || v != want {
				panic(fmt.Sprintf("traceparent = %q (%d), want %q", v, n, want))
			}
			if err := Consume(context.Background(), second, func(context.Context) error { return nil }); err != nil {
				panic(err)
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
		panic(err)
	}
	assertWorker(t, rec, core.Event{TraceID: trace, SpanID: span})
}

// A record produced by a new SDK, then produced again to a retry topic by a
// service on an older SDK with the headers copied: that SDK replaces
// traceparent and x-tsession-hlc and leaves the x-tsession-traceparent it does
// not know. The worker's parent is the republisher's span.
func TestConsumeIgnoresOwnKeyCopiedByOlderSDK(t *testing.T) {
	rec, ctx := setup(t)
	p := &fakeProducer{}
	if err := Produce(ctx, p, &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a")}}, nil); err != nil {
		panic(err)
	}
	first := pubs(rec)[0]
	republisher := core.Event{TraceID: first.TraceID, SpanID: core.NewSpanID()}
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a.retry")}}
	for _, h := range p.got[0].Headers {
		switch h.Key {
		case core.KeyTraceparent:
			h.Value = []byte(tpFor(republisher))
		case core.KeyHLC:
			h.Value = []byte(strconv.FormatUint(core.HLCNow(), 10))
		}
		msg.Headers = append(msg.Headers, h)
	}
	if v, _ := hdr(msg.Headers, core.KeyTSessionTraceparent); !strings.HasPrefix(v, tpFor(first)) {
		panic(fmt.Sprintf("the copied %s is %q, want the first publish", core.KeyTSessionTraceparent, v))
	}
	if err := Consume(context.Background(), msg, func(context.Context) error { return nil }); err != nil {
		panic(err)
	}
	assertWorker(t, rec, republisher)
}

// The injection budget (8 headers, 1 KiB) still holds the whole carrier.
func TestCarrierFitsInjectionBudget(t *testing.T) {
	_, ctx := setup(t)
	p := &fakeProducer{}
	if err := Produce(ctx, p, &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: str("a")}}, nil); err != nil {
		panic(err)
	}
	for _, k := range []string{core.KeyTSessionTraceparent, core.KeyTraceparent, core.KeySession, core.KeyHLC} {
		if _, n := hdr(p.got[0].Headers, k); n != 1 {
			panic(fmt.Sprintf("%s: %d headers", k, n))
		}
	}
	if n := len(p.got[0].Headers); n > maxInjectHeaders {
		panic(fmt.Sprintf("%d headers injected", n))
	}
}
