package collector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// After a resync (store changed behind the actor) the events still held in
// their window must stay marked as seen, or a resend of one is stored twice.
func TestResyncKeepsHeldEventsDeduplicated(t *testing.T) {
	c, st := windowed(t, 200*time.Millisecond)
	ctx := context.Background()
	ingest(t, c, ev("x", core.KindDBRead, 10, 1))
	waitStored(t, st, 1, 2*time.Second)
	if err := st.DeleteSession(ctx, sid); err != nil { // another writer deleted the session
		panic(err)
	}
	ingest(t, c, ev("a", core.KindDBRead, 100, 2))
	time.Sleep(150 * time.Millisecond)
	b := ev("b", core.KindDBRead, 200, 3)
	ingest(t, c, b)                                  // held: its window ends 150ms after a's
	waitStored(t, st, 1, 2*time.Second)              // a flushed alone (conflict -> resync)
	res, err := c.Ingest(ctx, "i1", []core.Event{b}) // the exporter resends b
	if err != nil {
		panic(err)
	}
	if _, err := c.seq.Flush(ctx, sid); err != nil {
		panic(err)
	}
	spans := storedSpans(t, st)
	if res.Duplicates != 1 || len(spans) != 2 {
		panic(fmt.Sprintf("resend: %+v; stored %v (want duplicates=1 and [a b])", res, spans))
	}
}
