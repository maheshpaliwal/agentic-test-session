package tqueue

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// wrapped is a writer decorator that itself goes through tqueue (the double
// wrapping that pubMarker exists for), in one-span-per-batch mode.
type wrapped struct{ inner KafkaWriter }

func (w wrapped) WriteMessages(ctx context.Context, m ...kafka.Message) error {
	f := false
	return ProduceMessagesOpts(ctx, w.inner, KafkaOptions{PerMessageSpans: &f}, m...)
}

func hdrs(m kafka.Message) map[string]string {
	out := map[string]string{}
	for _, h := range m.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}

// A publish that is not recorded (here the inner of two wrapped writers) must not write empty
// trace keys over the context the outer publish put on the message.
func TestKafkaBatchUnderAWrappedWriterKeepsItsContext(t *testing.T) {
	for _, outerBatch := range []bool{true, false} {
		rec, ctx := setup(t, true)
		w := &fakeKafka{}
		pm := !outerBatch
		if err := ProduceMessagesOpts(ctx, wrapped{w}, KafkaOptions{PerMessageSpans: &pm},
			kafka.Message{Topic: "orders", Value: []byte(`{"id":1}`)}); err != nil {
			panic(err)
		}
		pub := lastPub(t, rec)
		h := hdrs(w.got[0])
		if h[core.KeyTraceparent] != tpFor(pub) {
			t.Errorf("outerBatch=%v: traceparent = %q, want %q", outerBatch, h[core.KeyTraceparent], tpFor(pub))
		}
		if err := ConsumeMessage(context.Background(), w.got[0], noop); err != nil {
			panic(err)
		}
		st := byKind(rec.Events(), core.KindWorkerStart)
		if len(st) != 1 || st[0].ParentID != pub.SpanID || st[0].TraceID != pub.TraceID {
			t.Errorf("outerBatch=%v: worker %+v not a child of publish span %s trace %s", outerBatch, st, pub.SpanID, pub.TraceID)
		}
	}
}

// flip is a session set whose session ends after n IsActive calls.
type flip struct{ n *int }

func (f flip) IsActive(string) bool      { *f.n--; return *f.n >= 0 }
func (f flip) Exclusive() (string, bool) { return "", false }

// A message whose session ended between the check and the publish goes out untouched.
func TestKafkaBatchWritesNothingWhenThePublishIsNotRecorded(t *testing.T) {
	_, ctx := setup(t, true)
	n := 1 // sessionActive sees it active, Publish does not
	core.SetSessions(flip{&n})
	w := &fakeKafka{}
	f := false
	if err := ProduceMessagesOpts(ctx, w, KafkaOptions{PerMessageSpans: &f}, kafka.Message{Topic: "orders", Value: []byte(`1`)}); err != nil {
		panic(err)
	}
	if len(w.got[0].Headers) != 0 {
		t.Errorf("untraced message got headers: %q", hdrs(w.got[0]))
	}
}
