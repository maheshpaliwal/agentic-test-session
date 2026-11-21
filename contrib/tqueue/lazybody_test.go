package tqueue

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A body is rendered only when a span records it, but what is recorded must
// be what an eagerly rendered body gave: text (including a body that is a
// JSON string literal) is scrubbed as text and flagged as redacted, and the
// size is the text's, not its JSON-escaped form's.
func TestLazyBodyRecordsLikeAnEagerOne(t *testing.T) {
	rec, ctx := setup(t, true)
	pub := func(body string) core.Event {
		t.Helper()
		before := len(byKind(rec.Events(), core.KindQueuePub))
		_ = Publish(ctx, Message{Queue: "q", Params: bodyParams([]byte(body))}, core.MapCarrier{}, func(context.Context) error { return nil })
		evs := byKind(rec.Events(), core.KindQueuePub)
		if len(evs) != before+1 {
			panic(fmt.Sprintf("publish of %q recorded %d events", body, len(evs)-before))
		}
		return evs[len(evs)-1]
	}
	for _, body := range []string{
		`"password=hunter2&a=1"`, // a JSON string literal holding form text
		`"Authorization: Bearer abcdefghijklmnopqrstuvwxyz012345"`,
		`password=hunter2&a=1`,
		`note password=hunter2 end`,
	} {
		e := pub(body)
		if s := string(e.Req.Inline); strings.Contains(s, "hunter2") || strings.Contains(s, "abcdefghijklmnop") {
			t.Errorf("body %q: secret recorded: %s", body, s)
		}
		if !e.Req.Redacted {
			t.Errorf("body %q: Redacted=false although a value was masked: %s", body, e.Req.Inline)
		}
	}
	// Control characters cost six bytes each when JSON-escaped; the size and the
	// truncation decision must be about the text itself.
	body := "plain text " + strings.Repeat("\x01", 3000)
	e := pub(body)
	if e.Req.Size != len(body) || e.Req.Truncated {
		t.Errorf("3011-byte text body: Size=%d Truncated=%v", e.Req.Size, e.Req.Truncated)
	}
	// Scrubbed exactly once: a second pass used to leave "[REDACTED]]" behind.
	if e := pub(`password=hunter2&a=1`); string(e.Req.Inline) != `"password=[REDACTED]\u0026a=1"` {
		t.Errorf("form body recorded as %s", e.Req.Inline)
	}
	// Bodies nested in a batch are scrubbed too, including one that is a JSON string literal.
	_ = Publish(ctx, Message{Queue: "batch", Params: []any{bodyParams([]byte(`"password=hunter2"`)), bodyParams([]byte(`note password=hunter2`)),
		bodyParams([]byte(`{"password":"hunter2","ok":1}`))}}, core.MapCarrier{}, func(context.Context) error { return nil })
	evs := byKind(rec.Events(), core.KindQueuePub)
	if batch := string(evs[len(evs)-1].Req.Inline); strings.Contains(batch, "hunter2") || !strings.Contains(batch, `"ok":1`) {
		t.Errorf("batch recorded as %s", batch)
	}
	if e := pub(""); !e.Req.IsZero() {
		t.Errorf("empty body recorded a payload: %+v", e.Req)
	}
}
