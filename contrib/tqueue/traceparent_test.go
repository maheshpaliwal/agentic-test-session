package tqueue

import (
	"context"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// The application's trace context, as its own tracing (OpenTelemetry, say)
// would put it on a message.
const appTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func noop(context.Context) error { return nil }

// tpFor is the trace context tsession sends for a recorded publish span.
func tpFor(pub core.Event) string { return "00-" + pub.TraceID + "-" + pub.SpanID + "-01" }

// ownFor is the x-tsession-traceparent value for that span on a carrier whose
// x-tsession-hlc is hlc: the trace context bound to the clock written with it.
func ownFor(pub core.Event, hlc string) string { return tpFor(pub) + ";" + hlc }

// lastPub returns the most recent queue.publish span.
func lastPub(t *testing.T, rec *core.MemRecorder) core.Event {
	t.Helper()
	pubs := byKind(rec.Events(), core.KindQueuePub)
	if len(pubs) == 0 {
		t.Fatal("no publish span")
	}
	return pubs[len(pubs)-1]
}

// assertLastWorker checks that the most recent worker.start is a child of pub.
func assertLastWorker(t *testing.T, rec *core.MemRecorder, pub core.Event) {
	t.Helper()
	st := byKind(rec.Events(), core.KindWorkerStart)
	if len(st) == 0 {
		t.Fatal("no worker.start: the consumer did not join the session")
	}
	w := st[len(st)-1]
	if w.ParentID != pub.SpanID || w.TraceID != pub.TraceID || !contains(w.LinkIDs, pub.SpanID) {
		t.Fatalf("worker parent %q trace %q links %v, want publish span %q trace %q",
			w.ParentID, w.TraceID, w.LinkIDs, pub.SpanID, pub.TraceID)
	}
}

// oldExtract is the trace-context half of ExtractSpanContext as SDKs before
// x-tsession-traceparent implemented it: read from traceparent only. get is
// the old carrier's lookup.
func oldExtract(get func(string) string) (session, trace, span string) {
	parts := strings.Split(get(core.KeyTraceparent), "-")
	if len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		trace, span = parts[1], parts[2]
	}
	return get(core.KeySession), trace, span
}

// oldSQSGet is sqsCarrier.Get as those SDKs implemented it: the attribute
// first, then the compact attribute.
func oldSQSGet(attrs map[string]types.MessageAttributeValue) func(string) string {
	return func(key string) string {
		if v, ok := attrs[key]; ok && v.StringValue != nil {
			return *v.StringValue
		}
		for _, name := range [...]string{ctxAttr, legacyCtxAttr} {
			if v, ok := attrs[name]; ok && v.StringValue != nil {
				if q, err := url.ParseQuery(*v.StringValue); err == nil && q.Get(key) != "" {
					return q.Get(key)
				}
			}
		}
		return ""
	}
}

// A message published by a new SDK and then published again by a service on
// an older SDK with its headers copied (a retry or dead-letter queue). That
// SDK writes its own span into traceparent and a new x-tsession-hlc, and
// copies the x-tsession-traceparent it does not know. The worker's parent is
// the republisher's span, not the first publisher's.
func TestConsumeIgnoresOwnKeyCopiedByOlderSDK(t *testing.T) {
	newClock := func() string { return strconv.FormatUint(core.HLCNow(), 10) }
	for name, run := range map[string]func(t *testing.T, ctx context.Context, republisher core.Event){
		"kafka": func(t *testing.T, ctx context.Context, republisher core.Event) {
			w := &batchWriter{}
			if err := ProduceMessages(ctx, w, kafka.Message{Topic: "a", Value: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			msg := w.calls[0][0]
			c := kafkaCarrier{&msg.Headers}
			c.Set(core.KeyTraceparent, tpFor(republisher))
			c.Set(core.KeySession, "s1")
			c.Set(core.KeyHLC, newClock())
			if err := ConsumeMessage(context.Background(), msg, noop); err != nil {
				t.Fatal(err)
			}
		},
		"amqp": func(t *testing.T, ctx context.Context, republisher core.Event) {
			ch := &fakeAMQP{}
			if err := PublishAMQP(ctx, ch, "ex", "k", false, false, amqp.Publishing{Body: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			h := ch.got[0].Headers
			h[core.KeyTraceparent], h[core.KeySession], h[core.KeyHLC] = tpFor(republisher), "s1", newClock()
			if err := ConsumeDelivery(context.Background(), "q", amqp.Delivery{Headers: h, RoutingKey: "k", DeliveryTag: 1}, 0, noop); err != nil {
				t.Fatal(err)
			}
		},
		"sqs": func(t *testing.T, ctx context.Context, republisher core.Event) {
			attrs := sqsSend(t, ctx, fill(2), SQSOptions{})
			if sqsForm(attrs) != "full" {
				t.Fatalf("form %q", sqsForm(attrs))
			}
			attrs[core.KeyTraceparent], attrs[core.KeySession], attrs[core.KeyHLC] = strAttr(tpFor(republisher)), strAttr("s1"), strAttr(newClock())
			sqsConsume(t, attrs, SQSOptions{})
		},
		"side channel": func(t *testing.T, ctx context.Context, republisher core.Event) {
			sc := &MemSideChannel{}
			if err := PublishVia(ctx, sc, "m1", Message{Queue: "q", JobName: "j"}, noop); err != nil {
				t.Fatal(err)
			}
			m, _ := sc.Get(ctx, SideKey("m1"))
			m[core.KeyTraceparent], m[core.KeySession], m[core.KeyHLC] = tpFor(republisher), "s1", newClock()
			if err := sc.Put(ctx, SideKey("m1"), m, 0); err != nil {
				t.Fatal(err)
			}
			if err := ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "m1"}, noop); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			sc, _ := core.FromContext(ctx)
			republisher := core.Event{TraceID: sc.TraceID, SpanID: core.NewSpanID()}
			run(t, ctx, republisher)
			if first := lastPub(t, rec); first.SpanID == republisher.SpanID {
				t.Fatal("test setup: the two publishers have one span id")
			}
			assertLastWorker(t, rec, republisher)
		})
	}
}

// ---- Kafka ----

func kafkaHeaders(m kafka.Message, key string) []string {
	var out []string
	for _, h := range m.Headers {
		if strings.EqualFold(h.Key, key) {
			out = append(out, string(h.Value))
		}
	}
	return out
}

func TestKafkaKeepsApplicationTraceparent(t *testing.T) {
	off := false
	for name, opts := range map[string]KafkaOptions{"span per message": {}, "one span per batch": {PerMessageSpans: &off}} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			w := &batchWriter{}
			in := []kafka.Message{
				{Topic: "a", Value: []byte(`{}`), Headers: []kafka.Header{{Key: "traceparent", Value: []byte(appTP)}}},
				{Topic: "a", Value: []byte(`{}`)}, // no application tracing on this one
			}
			if err := ProduceMessagesOpts(ctx, w, opts, in...); err != nil {
				t.Fatal(err)
			}
			got := w.calls[0]
			pubs := byKind(rec.Events(), core.KindQueuePub)
			pubOf := func(i int) core.Event { return pubs[min(i, len(pubs)-1)] }

			if tp := kafkaHeaders(got[0], "traceparent"); len(tp) != 1 || tp[0] != appTP {
				t.Fatalf("application traceparent is now %q", tp)
			}
			if own, want := kafkaHeaders(got[0], core.KeyTSessionTraceparent), ownFor(pubOf(0), header(got[0], core.KeyHLC)); len(own) != 1 || own[0] != want {
				t.Fatalf("%s = %q, want %q", core.KeyTSessionTraceparent, own, want)
			}
			// Without an application value both keys carry tsession's.
			if header(got[1], "traceparent") != tpFor(pubOf(1)) || header(got[1], core.KeyTSessionTraceparent) != ownFor(pubOf(1), header(got[1], core.KeyHLC)) {
				t.Fatalf("message without application tracing: %v", got[1].Headers)
			}
			for i, m := range got {
				if err := ConsumeMessage(context.Background(), m, noop); err != nil {
					t.Fatal(err)
				}
				assertLastWorker(t, rec, pubOf(i))
			}
			if len(in[0].Headers) != 1 || len(in[1].Headers) != 0 {
				t.Fatal("caller's messages mutated")
			}
		})
	}
}

// A consumer that publishes the message it received, headers included, must
// not pass on the context of the first publish.
func TestKafkaRepublishWithCopiedHeaders(t *testing.T) {
	for name, app := range map[string]string{"no application traceparent": "", "application traceparent": appTP} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			w := &batchWriter{}
			first := kafka.Message{Topic: "a", Value: []byte(`{}`)}
			if app != "" {
				first.Headers = []kafka.Header{{Key: "traceparent", Value: []byte(app)}}
			}
			if err := ProduceMessages(ctx, w, first); err != nil {
				t.Fatal(err)
			}
			received := w.calls[0][0]
			pub1 := lastPub(t, rec)
			err := ConsumeMessage(context.Background(), received, func(hctx context.Context) error {
				return ProduceMessages(hctx, w, kafka.Message{Topic: "b", Value: received.Value, Headers: received.Headers})
			})
			if err != nil {
				t.Fatal(err)
			}
			second := w.calls[1][0]
			pub2 := lastPub(t, rec)
			if pub2.SpanID == pub1.SpanID {
				t.Fatal("second publish not recorded")
			}
			if own, want := kafkaHeaders(second, core.KeyTSessionTraceparent), ownFor(pub2, header(second, core.KeyHLC)); len(own) != 1 || own[0] != want {
				t.Fatalf("%s = %q, want the second publish %q", core.KeyTSessionTraceparent, own, want)
			}
			if hlc := kafkaHeaders(second, core.KeyHLC); len(hlc) != 1 {
				t.Fatalf("%s = %q, want one", core.KeyHLC, hlc)
			}
			want := tpFor(pub2) // tsession's own stale copy is replaced
			if app != "" {
				want = app
			}
			if tp := kafkaHeaders(second, "traceparent"); len(tp) != 1 || tp[0] != want {
				t.Fatalf("traceparent = %q, want %q", tp, want)
			}
			if err := ConsumeMessage(context.Background(), second, noop); err != nil {
				t.Fatal(err)
			}
			assertLastWorker(t, rec, pub2)
		})
	}
}

func TestKafkaOverwriteTraceparentSetting(t *testing.T) {
	rec, ctx := setup(t, true)
	core.Configure(core.Config{Service: "svc", Enabled: true, OverwriteTraceparent: true})
	w := &batchWriter{}
	msg := kafka.Message{Topic: "a", Headers: []kafka.Header{{Key: "traceparent", Value: []byte(appTP)}}}
	if err := ProduceMessages(ctx, w, msg); err != nil {
		t.Fatal(err)
	}
	pub := lastPub(t, rec)
	if tp := kafkaHeaders(w.calls[0][0], "traceparent"); len(tp) != 1 || tp[0] != tpFor(pub) {
		t.Fatalf("traceparent = %q, want tsession's %q", tp, tpFor(pub))
	}
	// What the setting is for: a consumer on an older SDK links correctly.
	if _, _, span := oldExtract(func(k string) string { return header(w.calls[0][0], k) }); span != pub.SpanID {
		t.Fatalf("older consumer would link to %q", span)
	}
}

// A traceparent header the application sent with no value is still its own.
func TestKafkaLeavesEmptyApplicationTraceparent(t *testing.T) {
	off := false
	for name, opts := range map[string]KafkaOptions{"span per message": {}, "one span per batch": {PerMessageSpans: &off}} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			w := &batchWriter{}
			msg := kafka.Message{Topic: "a", Value: []byte(`{}`), Headers: []kafka.Header{{Key: "traceparent"}}}
			if err := ProduceMessagesOpts(ctx, w, opts, msg); err != nil {
				t.Fatal(err)
			}
			got := w.calls[0][0]
			if tp := kafkaHeaders(got, "traceparent"); len(tp) != 1 || tp[0] != "" {
				t.Fatalf("empty application traceparent is now %q", tp)
			}
			if err := ConsumeMessage(context.Background(), got, noop); err != nil {
				t.Fatal(err)
			}
			assertLastWorker(t, rec, lastPub(t, rec))
		})
	}
}

// ---- AMQP ----

// An AMQP header can hold any field type. A traceparent that is neither a
// string nor bytes is the application's all the same.
func TestAMQPLeavesTraceparentThatIsNotText(t *testing.T) {
	for name, app := range map[string]any{"int32": int32(7), "table": amqp.Table{"v": appTP}, "nil": nil, "empty string": ""} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			ch := &fakeAMQP{}
			in := amqp.Publishing{Body: []byte(`{}`), Headers: amqp.Table{"traceparent": app}}
			if err := PublishAMQP(ctx, ch, "ex", "k", false, false, in); err != nil {
				t.Fatal(err)
			}
			got := ch.got[0].Headers
			if !reflect.DeepEqual(got["traceparent"], app) {
				t.Fatalf("application traceparent %v (%T) is now %v (%T)", app, app, got["traceparent"], got["traceparent"])
			}
			if err := ConsumeDelivery(context.Background(), "q", amqp.Delivery{Headers: got, RoutingKey: "k", DeliveryTag: 1}, 0, noop); err != nil {
				t.Fatal(err)
			}
			assertLastWorker(t, rec, lastPub(t, rec))
		})
	}
}

func TestAMQPKeepsApplicationTraceparent(t *testing.T) {
	for name, app := range map[string]any{"string": appTP, "bytes": []byte(appTP)} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			ch := &fakeAMQP{}
			in := amqp.Publishing{Body: []byte(`{}`), Headers: amqp.Table{"traceparent": app, "app": "1"}}
			if err := PublishAMQP(ctx, ch, "ex", "job.run", false, false, in); err != nil {
				t.Fatal(err)
			}
			got := ch.got[0].Headers
			pub := lastPub(t, rec)
			switch v := got["traceparent"].(type) {
			case string:
				if v != appTP || name != "string" {
					t.Fatalf("application traceparent is now %q", v)
				}
			case []byte:
				if string(v) != appTP || name != "bytes" {
					t.Fatalf("application traceparent is now %q", v)
				}
			default:
				t.Fatalf("application traceparent is now %T", v)
			}
			if hlc, _ := got[core.KeyHLC].(string); got[core.KeyTSessionTraceparent] != ownFor(pub, hlc) || got["app"] != "1" {
				t.Fatalf("headers: %v", got)
			}
			if len(in.Headers) != 2 {
				t.Fatal("caller's headers mutated")
			}
			dl := amqp.Delivery{Headers: got, RoutingKey: "job.run", Body: ch.got[0].Body, DeliveryTag: 1}
			if err := ConsumeDelivery(context.Background(), "q", dl, 0, noop); err != nil {
				t.Fatal(err)
			}
			assertLastWorker(t, rec, pub)
		})
	}
}

func TestAMQPRepublishWithCopiedHeaders(t *testing.T) {
	rec, ctx := setup(t, true)
	ch := &fakeAMQP{}
	if err := PublishAMQP(ctx, ch, "ex", "k", false, false, amqp.Publishing{Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	received := amqp.Delivery{Headers: ch.got[0].Headers, RoutingKey: "k", DeliveryTag: 1}
	err := ConsumeDelivery(context.Background(), "q", received, 0, func(hctx context.Context) error {
		return PublishAMQP(hctx, ch, "ex", "k2", false, false, amqp.Publishing{Headers: received.Headers})
	})
	if err != nil {
		t.Fatal(err)
	}
	pub2 := lastPub(t, rec)
	got := ch.got[1].Headers
	if hlc, _ := got[core.KeyHLC].(string); got[core.KeyTSessionTraceparent] != ownFor(pub2, hlc) || got["traceparent"] != tpFor(pub2) {
		t.Fatalf("stale context passed on: %v, want %q", got, tpFor(pub2))
	}
	if err := ConsumeDelivery(context.Background(), "q", amqp.Delivery{Headers: got, RoutingKey: "k2", DeliveryTag: 2}, 0, noop); err != nil {
		t.Fatal(err)
	}
	assertLastWorker(t, rec, pub2)
}

// ---- SQS ----

// sqsSend publishes a message with the given attributes in session s1.
func sqsSend(t *testing.T, ctx context.Context, attrs map[string]types.MessageAttributeValue, opts SQSOptions) map[string]types.MessageAttributeValue {
	t.Helper()
	c := &fakeSQS{}
	in := &sqs.SendMessageInput{QueueUrl: aws.String("q"), MessageBody: aws.String(`{"n":1}`), MessageAttributes: attrs}
	before := len(attrs)
	if _, err := SendMessageOpts(ctx, c, opts, in); err != nil {
		t.Fatal(err)
	}
	if len(attrs) != before {
		t.Fatal("caller's attributes mutated")
	}
	sent := c.sent[0].MessageAttributes
	if len(sent) > maxSQSAttrs {
		t.Fatalf("%d attributes sent, the limit is %d", len(sent), maxSQSAttrs)
	}
	return sent
}

func sqsConsume(t *testing.T, attrs map[string]types.MessageAttributeValue, opts SQSOptions) {
	t.Helper()
	msg := types.Message{MessageId: aws.String("m"), Body: aws.String(`{"n":1}`), MessageAttributes: attrs}
	if err := ConsumeSQSMessageOpts(context.Background(), "q", msg, 0, opts, noop); err != nil {
		t.Fatal(err)
	}
}

// withApp is n application attributes, one of which is its traceparent.
func withApp(n int) map[string]types.MessageAttributeValue {
	m := fill(n - 1)
	m["traceparent"] = strAttr(appTP)
	return m
}

func sqsForm(sent map[string]types.MessageAttributeValue) string {
	switch {
	case has(sent, ctxAttr):
		return "compact"
	case has(sent, core.KeySession):
		return "full"
	}
	return "none"
}

// The form chosen at every attribute count, without and with an application
// traceparent among the attributes. In every form the application's attribute
// is sent as it was and the consumer links to the publish span.
func TestSQSFormsAndAttributeLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attrs    func() map[string]types.MessageAttributeValue
		side     bool
		form     string
		sent     int // attributes on the wire
		untraced bool
	}{
		{"0 attrs", func() map[string]types.MessageAttributeValue { return nil }, false, "full", 4, false},
		{"6 attrs: the full form just fits", func() map[string]types.MessageAttributeValue { return fill(6) }, false, "full", 10, false},
		{"7 attrs: one short of the full form", func() map[string]types.MessageAttributeValue { return fill(7) }, false, "compact", 8, false},
		{"9 attrs: last slot", func() map[string]types.MessageAttributeValue { return fill(9) }, false, "compact", 10, false},
		{"10 attrs, side channel", func() map[string]types.MessageAttributeValue { return fill(10) }, true, "none", 10, false},
		{"10 attrs, no side channel", func() map[string]types.MessageAttributeValue { return fill(10) }, false, "none", 10, true},

		{"app traceparent, 1 attr", func() map[string]types.MessageAttributeValue { return withApp(1) }, false, "full", 4, false},
		{"app traceparent, 7 attrs: three own keys just fit", func() map[string]types.MessageAttributeValue { return withApp(7) }, false, "full", 10, false},
		{"app traceparent, 8 attrs", func() map[string]types.MessageAttributeValue { return withApp(8) }, false, "compact", 9, false},
		{"app traceparent, 9 attrs", func() map[string]types.MessageAttributeValue { return withApp(9) }, false, "compact", 10, false},
		{"app traceparent, 10 attrs, side channel", func() map[string]types.MessageAttributeValue { return withApp(10) }, true, "none", 10, false},
		{"app traceparent, 10 attrs, no side channel", func() map[string]types.MessageAttributeValue { return withApp(10) }, false, "none", 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			var opts SQSOptions
			if tc.side {
				opts.SideChannel = &MemSideChannel{}
			}
			in := tc.attrs()
			sent := sqsSend(t, ctx, in, opts)
			pub := lastPub(t, rec)
			if got := sqsForm(sent); got != tc.form || len(sent) != tc.sent {
				t.Fatalf("form %q with %d attributes, want %q with %d", got, len(sent), tc.form, tc.sent)
			}
			for k, v := range in { // every application attribute, traceparent included, is as it was
				if aws.ToString(sent[k].StringValue) != aws.ToString(v.StringValue) {
					t.Fatalf("application attribute %q changed to %q", k, aws.ToString(sent[k].StringValue))
				}
			}
			switch tc.form {
			case "full":
				want := ownFor(pub, aws.ToString(sent[core.KeyHLC].StringValue))
				if got := aws.ToString(sent[core.KeyTSessionTraceparent].StringValue); got != want {
					t.Fatalf("%s = %q, want %q", core.KeyTSessionTraceparent, got, want)
				}
				if !has(in, "traceparent") && aws.ToString(sent["traceparent"].StringValue) != tpFor(pub) {
					t.Fatalf("traceparent = %q, want tsession's", aws.ToString(sent["traceparent"].StringValue))
				}
			case "compact":
				q, err := url.ParseQuery(aws.ToString(sent[ctxAttr].StringValue))
				if err != nil || q.Get(core.KeyTSessionTraceparent) != ownFor(pub, q.Get(core.KeyHLC)) || q.Get(core.KeyTraceparent) != tpFor(pub) ||
					q.Get(core.KeySession) != "s1" || q.Get(core.KeyHLC) == "" {
					t.Fatalf("compact attribute = %v (%v)", q, err)
				}
			}
			sqsConsume(t, sent, opts)
			if tc.untraced {
				if !contains(pub.Flags, FlagAttrLimit) || len(byKind(rec.Events(), core.KindWorkerStart)) != 0 {
					t.Fatalf("want propagation skipped and flagged; flags %v", pub.Flags)
				}
				return
			}
			assertLastWorker(t, rec, pub)
		})
	}
}

// What a consumer on an older SDK makes of the messages this version sends.
func TestSQSReadByOlderConsumer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attrs   map[string]types.MessageAttributeValue
		correct bool // links to the publish span
	}{
		{"full form", fill(2), true},
		{"compact form", fill(8), true},
		{"full form, application traceparent", withApp(3), false},
		{"compact form, application traceparent", withApp(8), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			sent := sqsSend(t, ctx, tc.attrs, SQSOptions{})
			pub := lastPub(t, rec)
			session, trace, span := oldExtract(oldSQSGet(sent))
			if session != "s1" {
				t.Fatalf("session = %q: the event would not be recorded at all", session)
			}
			if tc.correct {
				if span != pub.SpanID || trace != pub.TraceID {
					t.Fatalf("older consumer links to %s/%s, want %s/%s", trace, span, pub.TraceID, pub.SpanID)
				}
			} else if !strings.Contains(appTP, trace+"-"+span) {
				// The compatibility note: it reads the application's ids.
				t.Fatalf("older consumer links to %s/%s; the note says the application's ids", trace, span)
			}
		})
	}
}

// An application attribute named traceparent that is not a String (SQS also
// has Binary and Number) is left alone in every form, and counts as the
// message's traceparent when the form is chosen.
func TestSQSLeavesTraceparentThatIsNotAString(t *testing.T) {
	for name, app := range map[string]types.MessageAttributeValue{
		"binary": {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2, 3}},
		"number": {DataType: aws.String("Number"), StringValue: aws.String("7")},
		"empty":  {DataType: aws.String("String")},
	} {
		for n := 1; n <= maxSQSAttrs; n++ { // application attributes, traceparent among them
			form := map[bool]string{true: "full", false: "compact"}[n <= 7]
			if n == maxSQSAttrs {
				form = "none"
			}
			t.Run(name+", "+strconv.Itoa(n)+" attrs", func(t *testing.T) {
				rec, ctx := setup(t, true)
				in := fill(n - 1)
				in["traceparent"] = app
				sent := sqsSend(t, ctx, in, SQSOptions{})
				if !reflect.DeepEqual(sent["traceparent"], app) {
					t.Fatalf("application traceparent is now %s %q", aws.ToString(sent["traceparent"].DataType), aws.ToString(sent["traceparent"].StringValue))
				}
				if got := sqsForm(sent); got != form {
					t.Fatalf("form %q, want %q", got, form)
				}
				if form == "none" {
					return
				}
				sqsConsume(t, sent, SQSOptions{})
				assertLastWorker(t, rec, lastPub(t, rec))
			})
		}
	}
}

// A service on an older SDK republishes a message that a new SDK sent in the
// compact form, attributes copied. With room for them it writes its context
// as three attributes and leaves the compact attribute, which it would only
// replace when writing the compact form itself. The fresh context is the one
// in the attributes.
func TestSQSConsumerPrefersAttributesToLeftoverCompact(t *testing.T) {
	const trace = "11111111111111111111111111111111"
	fresh, stale := "00-"+trace+"-aaaaaaaaaaaaaaaa-01", "00-"+trace+"-bbbbbbbbbbbbbbbb-01"
	for name, compact := range map[string]string{
		"from a new SDK": encodeTS(core.MapCarrier{core.KeyTraceparent: stale, core.KeyTSessionTraceparent: stale + ";1",
			core.KeySession: "s1", core.KeyHLC: "1"}),
		// The clock values happen to be equal: only where the session id is
		// tells the two contexts apart.
		"from a new SDK, same clock value": encodeTS(core.MapCarrier{core.KeyTraceparent: stale, core.KeyTSessionTraceparent: stale + ";2",
			core.KeySession: "s1", core.KeyHLC: "2"}),
		"from an older SDK": encodeTS(core.MapCarrier{core.KeyTraceparent: stale, core.KeySession: "s1", core.KeyHLC: "1"}),
	} {
		for _, attr := range []string{ctxAttr, legacyCtxAttr} {
			t.Run(name+", "+attr, func(t *testing.T) {
				rec, _ := setup(t, true)
				attrs := fill(3)
				attrs[attr] = strAttr(compact)
				attrs[core.KeyTraceparent], attrs[core.KeySession], attrs[core.KeyHLC] = strAttr(fresh), strAttr("s1"), strAttr("2")
				if _, _, span := oldExtract(oldSQSGet(attrs)); span != "aaaaaaaaaaaaaaaa" {
					t.Fatalf("test setup: an older consumer reads %s", span)
				}
				sqsConsume(t, attrs, SQSOptions{})
				assertLastWorker(t, rec, core.Event{TraceID: trace, SpanID: "aaaaaaaaaaaaaaaa"})
			})
		}
	}
}

// The overwrite setting exists so that a consumer on an older SDK links to
// the publish span although the application set traceparent. It must hold in
// every form that carries the context in the message: at every attribute
// count, the application's traceparent attribute is replaced.
func TestSQSOverwriteTraceparentSetting(t *testing.T) {
	for n := 1; n <= maxSQSAttrs; n++ { // application attributes, its traceparent among them
		form := map[bool]string{true: "full", false: "compact"}[n <= 7]
		if n == maxSQSAttrs {
			form = "none" // no slot: nothing is propagated, nothing is replaced
		}
		t.Run(strconv.Itoa(n)+" attrs, "+form, func(t *testing.T) {
			rec, ctx := setup(t, true)
			core.Configure(core.Config{Service: "svc", Enabled: true, OverwriteTraceparent: true})
			in := withApp(n)
			sent := sqsSend(t, ctx, in, SQSOptions{})
			pub := lastPub(t, rec)
			if got := sqsForm(sent); got != form {
				t.Fatalf("form %q, want %q", got, form)
			}
			for k, v := range in {
				if k != "traceparent" && aws.ToString(sent[k].StringValue) != aws.ToString(v.StringValue) {
					t.Fatalf("application attribute %q changed", k)
				}
			}
			tp := aws.ToString(sent["traceparent"].StringValue)
			if form == "none" {
				if tp != appTP {
					t.Fatalf("nothing was propagated, yet traceparent is now %q", tp)
				}
				return
			}
			if tp != tpFor(pub) || aws.ToString(sent["traceparent"].DataType) != "String" {
				t.Fatalf("traceparent = %q, want tsession's %q", tp, tpFor(pub))
			}
			if session, trace, span := oldExtract(oldSQSGet(sent)); session != "s1" || trace != pub.TraceID || span != pub.SpanID {
				t.Fatalf("older consumer links to %s/%s in session %q, want %s/%s", trace, span, session, pub.TraceID, pub.SpanID)
			}
			sqsConsume(t, sent, SQSOptions{})
			assertLastWorker(t, rec, pub)
		})
	}

	// With the side channel the context is not in the message, an older
	// consumer reads all of it from the record, and the attribute is left.
	rec, ctx := setup(t, true)
	core.Configure(core.Config{Service: "svc", Enabled: true, OverwriteTraceparent: true})
	opts := SQSOptions{SideChannel: &MemSideChannel{}}
	sent := sqsSend(t, ctx, withApp(maxSQSAttrs), opts)
	if aws.ToString(sent["traceparent"].StringValue) != appTP || len(sent) != maxSQSAttrs {
		t.Fatalf("side channel: attributes changed: %v", sent)
	}
	sqsConsume(t, sent, opts)
	assertLastWorker(t, rec, lastPub(t, rec))
}

// Messages from a producer on an older SDK: the context is in traceparent
// (full form) or in the compact attribute, where an attribute named
// traceparent is the application's.
func TestSQSFromOlderProducer(t *testing.T) {
	rec, _ := setup(t, true)
	trace, span := core.NewTraceID(), core.NewSpanID()
	tp := "00-" + trace + "-" + span + "-01"
	check := func(name string) {
		t.Helper()
		st := byKind(rec.Events(), core.KindWorkerStart)
		if w := st[len(st)-1]; w.ParentID != span || w.TraceID != trace {
			t.Fatalf("%s: worker parent %q trace %q, want %q %q", name, w.ParentID, w.TraceID, span, trace)
		}
	}
	sqsConsume(t, map[string]types.MessageAttributeValue{
		core.KeyTraceparent: strAttr(tp), core.KeySession: strAttr("s1"), core.KeyHLC: strAttr("1")}, SQSOptions{})
	check("full form")

	old := encodeTS(core.MapCarrier{core.KeySession: "s1", core.KeyTraceparent: tp, core.KeyHLC: "1"})
	sqsConsume(t, map[string]types.MessageAttributeValue{ctxAttr: strAttr(old)}, SQSOptions{})
	check("compact form")

	sqsConsume(t, map[string]types.MessageAttributeValue{ctxAttr: strAttr(old), "traceparent": strAttr(appTP)}, SQSOptions{})
	check("compact form next to an application traceparent")
}

// A received message sent on with its attributes: the carrier it arrived with
// is replaced, in whichever form the new publish uses.
func TestSQSRepublishWithCopiedAttributes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first map[string]types.MessageAttributeValue
		form  string
	}{
		{"full form, attributes at the limit", fill(6), "full"},
		{"full form, application traceparent", withApp(7), "full"},
		{"compact form, attributes at the limit", fill(9), "compact"},
		{"compact form, application traceparent", withApp(9), "compact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t, true)
			received := sqsSend(t, ctx, tc.first, SQSOptions{})
			pub1 := lastPub(t, rec)
			var second map[string]types.MessageAttributeValue
			msg := types.Message{MessageId: aws.String("m"), Body: aws.String(`{}`), MessageAttributes: received}
			if err := ConsumeSQSMessage(context.Background(), "q", msg, 0, func(hctx context.Context) error {
				second = sqsSend(t, hctx, received, SQSOptions{})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			pub2 := lastPub(t, rec)
			if pub2.SpanID == pub1.SpanID {
				t.Fatal("second publish not recorded")
			}
			if got := sqsForm(second); got != tc.form || len(second) != len(received) {
				t.Fatalf("form %q with %d attributes, want %q with %d", got, len(second), tc.form, len(received))
			}
			if has(tc.first, "traceparent") && aws.ToString(second["traceparent"].StringValue) != appTP {
				t.Fatalf("application traceparent is now %q", aws.ToString(second["traceparent"].StringValue))
			}
			sqsConsume(t, second, SQSOptions{})
			assertLastWorker(t, rec, pub2)
			if !has(tc.first, "traceparent") {
				if _, _, span := oldExtract(oldSQSGet(second)); span != pub2.SpanID {
					t.Fatalf("older consumer would link to %q, want the second publish %q", span, pub2.SpanID)
				}
			}
		})
	}
}

// A message that is not part of a session is sent exactly as the application
// built it, even when it carries attributes copied from a traced message.
func TestSQSUntracedSendLeavesCopiedCarrier(t *testing.T) {
	rec, ctx := setup(t, true)
	received := sqsSend(t, ctx, fill(9), SQSOptions{}) // compact form
	n := len(rec.Events())
	sent := sqsSend(t, context.Background(), received, SQSOptions{})
	if len(rec.Events()) != n {
		t.Fatal("publish outside a session recorded")
	}
	if len(sent) != len(received) || aws.ToString(sent[ctxAttr].StringValue) != aws.ToString(received[ctxAttr].StringValue) {
		t.Fatalf("untraced message changed: %v", sent)
	}
}

func TestSQSReceiveRequestsOwnTraceparent(t *testing.T) {
	setup(t, true)
	var got *sqs.ReceiveMessageInput
	c := recvSQS{func(in *sqs.ReceiveMessageInput) { got = in }}
	if _, err := ReceiveMessage(context.Background(), c, &sqs.ReceiveMessageInput{QueueUrl: aws.String("q")}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{core.KeyTSessionTraceparent, core.KeyTraceparent} {
		if !contains(got.MessageAttributeNames, want) {
			t.Fatalf("%q not requested: %v", want, got.MessageAttributeNames)
		}
	}
}

// ---- side channel ----

// The side-channel record is tsession's alone: it holds both keys with the
// same value, and nothing the application wrote is involved.
func TestSideChannelRecord(t *testing.T) {
	rec, ctx := setup(t, true)
	sc := &MemSideChannel{}
	if err := PublishVia(ctx, sc, "m1", Message{Queue: "q", JobName: "j"}, noop); err != nil {
		t.Fatal(err)
	}
	pub := lastPub(t, rec)
	m, err := sc.Get(ctx, SideKey("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if m[core.KeyTSessionTraceparent] != ownFor(pub, m[core.KeyHLC]) || m[core.KeyHLC] == "" || m[core.KeyTraceparent] != tpFor(pub) || m[core.KeySession] != "s1" {
		t.Fatalf("record = %v", m)
	}
	if err := ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "m1"}, noop); err != nil {
		t.Fatal(err)
	}
	assertLastWorker(t, rec, pub)

	// The record alone decides the parent, whichever key holds it: a record
	// from an older SDK has traceparent only, and a consumer on an older SDK
	// reads traceparent from a record written now.
	if _, _, span := oldExtract(core.MapCarrier(m).Get); span != pub.SpanID {
		t.Fatalf("older consumer would link to %q", span)
	}
	trace, span := core.NewTraceID(), core.NewSpanID()
	old := map[string]string{core.KeySession: "s1", core.KeyTraceparent: "00-" + trace + "-" + span + "-01"}
	if err := sc.Put(ctx, SideKey("m2"), old, 0); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeVia(context.Background(), sc, Delivery{Queue: "q", JobName: "j", MessageID: "m2"}, noop); err != nil {
		t.Fatal(err)
	}
	st := byKind(rec.Events(), core.KindWorkerStart)
	if w := st[len(st)-1]; w.ParentID != span || w.TraceID != trace {
		t.Fatalf("record from an older SDK: worker parent %q trace %q", w.ParentID, w.TraceID)
	}
}

// The SQS side channel is used when no attribute slot is free; an application
// traceparent among the ten attributes is not touched and not consulted.
func TestSQSSideChannelIgnoresApplicationTraceparent(t *testing.T) {
	rec, ctx := setup(t, true)
	opts := SQSOptions{SideChannel: &MemSideChannel{}}
	sent := sqsSend(t, ctx, withApp(10), opts)
	pub := lastPub(t, rec)
	if aws.ToString(sent["traceparent"].StringValue) != appTP || has(sent, core.KeyTSessionTraceparent) {
		t.Fatalf("attributes: %v", sent)
	}
	sqsConsume(t, sent, opts)
	assertLastWorker(t, rec, pub)
}
