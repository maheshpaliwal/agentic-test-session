package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// An update whose row could not be read back is shown as such, with its
// before image, by show, history and entities. An SDK from before the fix
// also sent a diff of every column against nothing; it is not printed.
func TestAfterImageUnavailableIsSaidPlainly(t *testing.T) {
	before := core.Payload(map[string]any{"sku": "widget", "qty": 10})
	oldDiff := []core.FieldChange{{Field: "qty", Old: 10}, {Field: "sku", Old: "widget"}}
	for name, diff := range map[string][]core.FieldChange{"current SDK": nil, "older SDK": oldDiff} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			printEvent(&out, &core.Event{Seq: 7, Kind: core.KindDBWrite, Flags: []string{"pk_unknown", "after_unavailable"},
				Mutations: []core.Mutation{{Store: "postgres", Entity: "stock", Op: "update", Before: before, Diff: diff}}})
			printHistory(&out, client.EntityHistory{Store: "postgres", Entity: "stock", PK: "{}",
				Mutations: []client.EntityMutation{{Seq: 7, Op: "update", Kind: core.KindDBWrite, Before: before, Diff: diff}}})
			got := out.String()
			for _, want := range []string{
				"mutation  update postgres/stock (key unknown)\n            after image unavailable: the new values were not captured\n",
				"  before:", `"qty": 10`,
				"db.write\n       after image unavailable: the new values were not captured\n",
			} {
				if !strings.Contains(got, want) {
					panic(fmt.Sprintf("missing %q in:\n%s", want, got))
				}
			}
			if strings.Contains(got, "∅") || strings.Contains(got, "null") {
				panic(fmt.Sprintf("output still shows values going to nothing:\n%s", got))
			}
		})
	}

	var out bytes.Buffer
	printEntities(&out, []client.Entity{{Store: "postgres", Entity: "stock", PK: "{}", MutationCount: 2},
		{Store: "postgres", Entity: "stock", PK: `{"sku":"widget"}`, MutationCount: 1}})
	if got := out.String(); !strings.Contains(got, "{} (key unknown)") || !strings.Contains(got, `{"sku":"widget"}`) {
		panic(fmt.Sprintf("entities:\n%s", got))
	}
}

// A delete and an update with both images keep their diff.
func TestDiffStillPrintedWhereItMeansSomething(t *testing.T) {
	var out bytes.Buffer
	row := core.Payload(map[string]any{"sku": "widget"})
	printEvent(&out, &core.Event{Seq: 1, Kind: core.KindDBWrite, Mutations: []core.Mutation{
		{Store: "postgres", Entity: "stock", Op: "delete", PK: map[string]any{"sku": "widget"}, Before: row,
			Diff: []core.FieldChange{{Field: "sku", Old: "widget"}}},
		{Store: "postgres", Entity: "stock", Op: "update", PK: map[string]any{"sku": "widget"}, Before: row, After: row,
			Diff: []core.FieldChange{{Field: "qty", Old: 10, New: 9}}},
	}})
	got := out.String()
	if !strings.Contains(got, `sku: "widget" -> ∅`) || !strings.Contains(got, "qty: 10 -> 9") || strings.Contains(got, "unavailable") {
		panic(fmt.Sprintf("output:\n%s", got))
	}
}
