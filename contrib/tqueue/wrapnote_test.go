package tqueue

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// tqueue looks the SDK up on every publish and delivery, so a handler wrapped
// before the SDK starts records once it runs: nothing is reported.
func TestWrapHandlerBeforeStartRecords(t *testing.T) {
	sc := &MemSideChannel{}
	h := WrapHandler(sc, "asynq", "q", "j", 0, func(context.Context, string, int, []byte) error { return nil })
	c := core.MapCarrier{}
	_ = Publish(context.Background(), Message{Queue: "q"}, c, func(context.Context) error { return nil })
	for _, w := range core.WrappersWhileDisabled() {
		if strings.HasPrefix(w, "tqueue.") {
			t.Errorf("%s is noted, but it records once the SDK runs", w)
		}
	}

	rec, ctx := setup(t, true)
	_ = PublishVia(ctx, sc, "id1", Message{Queue: "q", JobName: "j"}, func(context.Context) error { return nil })
	_ = h(context.Background(), "id1", 1, []byte(`{"a":1}`))
	if len(byKind(rec.Events(), core.KindQueuePub)) != 1 || len(byKind(rec.Events(), core.KindWorkerEnd)) != 1 {
		panic(fmt.Sprintf("a handler wrapped before the start must record: %d events", len(rec.Events())))
	}
}
