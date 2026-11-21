package collector

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// An entity's history carries the flags of the event behind each change, and
// the entities list counts the changes recorded with an uncertain key or
// image: two rows merged under one non-unique key must not read as one clean
// history.
func TestHistoryAndEntitiesCarryUncertainty(t *testing.T) {
	stock := func(pk map[string]any) []core.Mutation {
		return []core.Mutation{{Store: "postgres", Entity: "stock", Op: "update", PK: pk}}
	}
	// The second event is decoded from the shape events had before flags
	// mattered here (and that old bundles hold): no "flags" member at all.
	var old core.Event
	if err := json.Unmarshal([]byte(`{"session_id":"s","seq":2,"span_id":"b","service":"api","kind":"db.write",
		"mutations":[{"store":"postgres","entity":"stock","op":"update","pk":{"sku":"w"}}]}`), &old); err != nil {
		panic(err)
	}
	evs := []core.Event{
		{Seq: 1, SpanID: "a", Service: "api", Kind: core.KindDBWrite, Flags: []string{"pk_not_unique", "after_unavailable"}, Mutations: stock(map[string]any{"sku": "w"})},
		old,
		{Seq: 3, SpanID: "c", Service: "api", Kind: core.KindDBWrite, Flags: []string{"image_skipped:deadline"}, Mutations: stock(map[string]any{"sku": "w"})},
		{Seq: 4, SpanID: "d", Service: "api", Kind: core.KindDBWrite, Flags: []string{"redacted"}, Mutations: stock(map[string]any{"sku": "w"})},
		{Seq: 5, SpanID: "e", Service: "api", Kind: core.KindDBWrite, Flags: []string{"pk_unknown"}, Mutations: stock(nil)},
		{Seq: 6, SpanID: "f", Service: "api", Kind: core.KindDBWrite, Mutations: stock(map[string]any{"sku": "clean"})},
		// One flagged statement that changed two rows: the flag is the event's, so both count.
		{Seq: 7, SpanID: "g", Service: "api", Kind: core.KindDBWrite, Flags: []string{"after_from_args"},
			Mutations: append(stock(map[string]any{"sku": "m1"}), stock(map[string]any{"sku": "m2"})...)},
	}

	h := History(evs, "postgres", "stock", `{"sku":"w"}`)
	if len(h.Mutations) != 4 {
		panic(fmt.Sprintf("history: %+v", h))
	}
	for i, want := range []string{"pk_not_unique,after_unavailable", "", "image_skipped:deadline", "redacted"} {
		if got := strings.Join(h.Mutations[i].Flags, ","); got != want {
			t.Errorf("mutation %d: flags %q, want %q", i, got, want)
		}
	}
	b, err := json.Marshal(h)
	if err != nil {
		panic(err)
	}
	// Optional on the wire: an entry without flags has the shape it always had.
	if got := strings.Count(string(b), `"flags"`); got != 3 {
		panic(fmt.Sprintf("%d entries carry flags, want 3: %s", got, b))
	}
	var wire struct {
		Mutations []map[string]json.RawMessage `json:"mutations"`
	}
	if err := json.Unmarshal(b, &wire); err != nil {
		panic(err)
	}
	if _, has := wire.Mutations[1]["flags"]; has || string(wire.Mutations[0]["flags"]) != `["pk_not_unique","after_unavailable"]` {
		panic(fmt.Sprintf("wire shape: %s", b))
	}

	want := map[string][2]int{ // key -> mutations, uncertain
		`{"sku":"w"}`: {4, 2}, `{}`: {1, 1}, `{"sku":"clean"}`: {1, 0}, `{"sku":"m1"}`: {1, 1}, `{"sku":"m2"}`: {1, 1},
	}
	ents := ListEntities(evs)
	if len(ents) != len(want) {
		panic(fmt.Sprintf("entities: %+v", ents))
	}
	for _, e := range ents {
		if w := want[e.PK]; e.MutationCount != w[0] || e.Uncertain != w[1] {
			t.Errorf("%s: %d mutations, %d uncertain; want %v", e.PK, e.MutationCount, e.Uncertain, w)
		}
	}
	b, _ = json.Marshal(ents)
	if got := strings.Count(string(b), `"uncertain"`); got != 4 {
		panic(fmt.Sprintf("%d entities carry uncertain, want 4 (omitted when zero): %s", got, b))
	}
}

// The bare value of a one-column key matches a number as it is written, also
// from a million on, where a float64 prints with an exponent.
func TestHistoryBareNumericKey(t *testing.T) {
	var e core.Event
	if err := json.Unmarshal([]byte(`{"seq":1,"mutations":[{"store":"postgres","entity":"orders","op":"insert","pk":{"id":1000000}}]}`), &e); err != nil {
		panic(err)
	}
	for _, pk := range []string{"1000000", `{"id":1000000}`, "1e+06"} {
		if h := History([]core.Event{e}, "postgres", "orders", pk); len(h.Mutations) != 1 || h.PK != `{"id":1000000}` {
			t.Errorf("pk %q: %+v", pk, h)
		}
	}
	if h := History([]core.Event{e}, "postgres", "orders", "100000"); len(h.Mutations) != 0 {
		t.Errorf("a different number matched: %+v", h)
	}
}
