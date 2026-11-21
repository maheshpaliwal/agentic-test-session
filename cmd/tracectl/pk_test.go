package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func TestResolveKey(t *testing.T) {
	ents := []client.Entity{
		{Store: "postgres", Entity: "orders", PK: `{"id":42}`},
		{Store: "postgres", Entity: "orders", PK: `{"id":1000000}`},
		{Store: "postgres", Entity: "orders", PK: `{}`},
		{Store: "postgres", Entity: "items", PK: `{"order_id":7,"sku":"a"}`},
		{Store: "postgres", Entity: "items", PK: `{"order_id":7,"sku":"b,c=d"}`},
		{Store: "postgres", Entity: "items", PK: `{"order_id":"7","sku":"x"}`},
		{Store: "postgres", Entity: "mixed", PK: `{"id":1}`},
		{Store: "postgres", Entity: "mixed", PK: `{"id":"1"}`},
		{Store: "postgres", Entity: "big", PK: `{"id":9007199254740993}`},
		{Store: "postgres", Entity: "big", PK: `{"id":9007199254740992}`},
		{Store: "postgres", Entity: "html", PK: `{"k":"a\u003cb"}`},
		{Store: "redis", Entity: "cart", PK: `{"key":"user:1/cart"}`},
		{Store: "redis", Entity: "cart", PK: `{"key":"a=b"}`},
		{Store: "old", Entity: "t", PK: ""}, // an unknown key listed as an empty string
		{Store: "evil", Entity: "t", PK: "{\"id\":\"\u009b31m\u202e\"}"},
	}
	for _, tc := range []struct {
		store, entity, typed string
		want                 string // the key resolved to
		err                  string // or a part of the error
	}{
		// the forms that worked before
		{"postgres", "orders", `{"id":42}`, `{"id":42}`, ""},
		{"postgres", "orders", `42`, `{"id":42}`, ""},
		{"postgres", "orders", `{}`, `{}`, ""},
		// the unknown key without knowing its spelling
		{"postgres", "orders", ``, `{}`, ""},
		{"postgres", "orders", `  `, `{}`, ""},
		{"old", "t", ``, `{}`, ""},
		{"postgres", "items", ``, "", "an unknown key"},
		// JSON, loosely typed
		{"postgres", "items", `{ "sku": "a", "order_id": 7 }`, `{"order_id":7,"sku":"a"}`, ""},
		{"postgres", "html", `{"k":"a<b"}`, `{"k":"a\u003cb"}`, ""},
		{"postgres", "items", `{"order_id":7}`, "", "changed nothing"},             // a part of the key is not the key
		{"postgres", "items", `{"order_id":7,"sku":"a"} x`, "", "changed nothing"}, // nothing may follow
		{"postgres", "items", `{"order_id":7,"sku":"a","x":1}`, "", "changed nothing"},
		// pairs
		{"postgres", "items", `order_id=7,sku=a`, `{"order_id":7,"sku":"a"}`, ""},
		{"postgres", "items", `sku=a, order_id=7`, `{"order_id":7,"sku":"a"}`, ""},
		{"postgres", "items", `order_id=7,sku=x`, `{"order_id":"7","sku":"x"}`, ""},
		{"postgres", "orders", `id=42`, `{"id":42}`, ""},
		{"postgres", "items", `order_id=7`, "", "changed nothing"},
		{"postgres", "items", `order_id=7,sku=a,sku=a`, "", "changed nothing"},
		{"postgres", "items", `order_id=7,sku=b,c=d`, "", "changed nothing"}, // a value with a comma needs JSON
		{"postgres", "items", `{"order_id":7,"sku":"b,c=d"}`, `{"order_id":7,"sku":"b,c=d"}`, ""},
		// the bare value of a one-column key
		{"postgres", "orders", `1000000`, `{"id":1000000}`, ""},
		{"redis", "cart", `user:1/cart`, `{"key":"user:1/cart"}`, ""},
		{"redis", "cart", `a=b`, `{"key":"a=b"}`, ""}, // reads as a pair first, which names nothing
		{"postgres", "items", `7`, "", "changed nothing"},
		// never a guess
		{"postgres", "mixed", `1`, "", "names 2 keys"},
		{"postgres", "mixed", `id=1`, "", "names 2 keys"},
		{"postgres", "mixed", `{"id":"1"}`, `{"id":"1"}`, ""},
		{"postgres", "big", `9007199254740993`, `{"id":9007199254740993}`, ""},
		{"postgres", "big", `{"id": 9007199254740992}`, `{"id":9007199254740992}`, ""},
		{"postgres", "orders", `42.0`, "", "changed nothing"},
		{"postgres", "orders", `43`, "", `with key 43; it recorded: {"id":42}, {"id":1000000}, {} (key unknown)`},
		{"postgres", "nope", `1`, "", "changed nothing in postgres/nope"},
		{"mysql", "orders", `42`, "", "changed nothing in mysql/orders"},
		// hostile
		{"postgres", "orders", `{"id":` + strings.Repeat("[", 100000), "", "changed nothing"},
		{"postgres", "orders", strings.Repeat(`{"a":`, 20000), "", "changed nothing"},
		{"postgres", "orders", "\x1b[2J\x1b]0;owned\x07", "", "changed nothing"},
		{"postgres", "orders", "42\n{\"id\":42}", "", "changed nothing"},
		{"postgres", "orders", "\xff\xfe=\x00", "", "changed nothing"},
		{"postgres", "orders", strings.Repeat("=,", 50000), "", "changed nothing"},
		{"postgres\x1b[31m", "orders\r\n", `1`, "", "changed nothing"},
		{"evil", "t", `nope`, "", "it recorded"},
	} {
		got, err := resolveKey(ents, tc.store, tc.entity, tc.typed)
		name := tc.store + "/" + tc.entity + " " + clip(tc.typed, 40)
		if tc.err == "" {
			if err != nil || got != tc.want {
				t.Errorf("%s: got %q, %v; want %q", name, got, err, tc.want)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: got %q, %v; want an error with %q", name, got, err, tc.err)
			continue
		}
		// What was typed, and what was recorded, must not reach the terminal raw or at length.
		if msg := err.Error(); len(msg) > 1000 || strings.ContainsFunc(msg, func(r rune) bool { return !unicode.IsPrint(r) }) {
			t.Errorf("%s: unsafe error text (%d bytes): %q", name, len(msg), clip(msg, 200))
		}
	}
}

func TestCLIHistoryKeyForms(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")
	env.ents = []client.Entity{
		{Store: "postgres", Entity: "orders", PK: `{"id":42}`, MutationCount: 2},
		{Store: "postgres", Entity: "orders", PK: `{}`, MutationCount: 1},
		{Store: "postgres", Entity: "items", PK: `{"order_id":7,"sku":"a"}`, MutationCount: 1},
		{Store: "postgres", Entity: "ledger", PK: `{"n":-5}`, MutationCount: 1},
	}
	for _, tc := range []struct {
		args []string
		want string // the key the collector is asked for
	}{
		{[]string{"postgres", "orders", "42"}, `{"id":42}`},
		{[]string{"postgres", "orders", `{"id":42}`}, `{"id":42}`},
		{[]string{"postgres", "orders", `id=42`}, `{"id":42}`},
		{[]string{"postgres", "orders", ``}, `{}`},
		{[]string{"postgres", "orders", `{}`}, `{}`},
		{[]string{"postgres", "orders", "--unknown-key"}, `{}`},
		{[]string{"--unknown-key", "postgres", "orders", "--json"}, `{}`},
		{[]string{"postgres", "orders", "", "--unknown-key"}, `{}`},
		{[]string{"postgres", "items", "sku=a,order_id=7"}, `{"order_id":7,"sku":"a"}`},
		{[]string{"postgres", "items", `{"sku":"a", "order_id":7}`}, `{"order_id":7,"sku":"a"}`},
		{[]string{"postgres", "ledger", "--", "-5"}, `{"n":-5}`},
		{[]string{"postgres", "ledger", "n=-5"}, `{"n":-5}`},
	} {
		env.histPKs = nil
		code, out, e := env.tc(append([]string{"history", "run-1"}, tc.args...)...)
		if code != 0 || len(env.histPKs) != 1 || env.histPKs[0] != tc.want || !strings.Contains(out, tc.want) {
			t.Errorf("history %q: exit %d, asked the collector for %q, want %q\n%s%s", tc.args, code, env.histPKs, tc.want, out, e)
		}
	}

	// An entity that is not there is an error that says what was looked up and where to look, not
	// an empty history (or the router's "404 page not found").
	env.histPKs = nil
	code, out, e := env.tc("history", "run-1", "postgres", "orders", "43")
	if code != 1 || out != "" || len(env.histPKs) != 0 || !strings.Contains(e, `postgres/orders with key 43`) ||
		!strings.Contains(e, `{"id":42}`) || !strings.Contains(e, "tracectl entities run-1") {
		panic(fmt.Sprintf("missing entity: %d %q %v\n%s", code, out, env.histPKs, e))
	}
	if code, _, e := env.tc("history", "run-1", "postgres", "items", "--unknown-key"); code != 1 || !strings.Contains(e, "an unknown key") {
		panic(fmt.Sprintf("no unknown key recorded: %d %s", code, e))
	}
	if code, _, e := env.tc("history", "run-1", "postgres", "payments", "1"); code != 1 || !strings.Contains(e, "changed nothing in postgres/payments") {
		panic(fmt.Sprintf("missing table: %d %s", code, e))
	}
	for _, args := range [][]string{
		{"history", "run-1", "postgres", "orders"},                        // no key, and no --unknown-key
		{"history", "run-1", "postgres", "orders", "42", "--unknown-key"}, // both
		{"history", "run-1", "postgres", "orders", "42", "43"},
		{"history", "run-1", "--unknown-key"},
	} {
		if code, _, e := env.tc(args...); code != 1 || !strings.Contains(e, "<store> <entity> <pk>") || !strings.Contains(e, "--unknown-key") || len(env.histPKs) != 0 {
			t.Errorf("%q: %d %s", args, code, e)
		}
	}
}

// The flags of the event that made a change say how far its recorded key and
// images can be trusted: history and entities must not hide them.
func TestCLIHistoryAndEntitiesShowUncertainty(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")
	yes := true
	img := core.PayloadRef{Inline: json.RawMessage(`{"qty":1}`)}
	env.ents = []client.Entity{
		{Store: "postgres", Entity: "stock", PK: `{"sku":"w"}`, MutationCount: 3, Uncertain: 2},
		{Store: "postgres", Entity: "orders", PK: `{"id":42}`, MutationCount: 2},
	}
	env.hist = []client.EntityMutation{
		{Seq: 2, Op: "update", Service: "api", Kind: core.KindDBWrite, Effective: &yes, Before: img, Flags: []string{"pk_not_unique", "after_unavailable"}},
		{Seq: 5, Op: "update", Service: "api", Kind: core.KindDBWrite, Effective: &yes, Before: img, After: img,
			Diff: []core.FieldChange{{Field: "qty", Old: 1, New: 2}}, Flags: []string{"image_skipped:deadline"}},
		{Seq: 8, Op: "update", Service: "api", Kind: core.KindDBWrite, Effective: &yes, Before: img, After: img, Flags: []string{"redacted"}},
		{Seq: 9, Op: "delete", Service: "api", Kind: core.KindDBWrite, Effective: &yes, Before: img}, // as an older collector sends it
	}

	code, out, e := env.tc("entities", "run-1")
	if code != 0 || !strings.Contains(out, "3 mutation(s), 2 uncertain\n") || !strings.Contains(out, "2 mutation(s)\n") || strings.Count(out, "uncertain") != 1 {
		panic(fmt.Sprintf("entities: %d\n%s%s", code, out, e))
	}
	var ents []map[string]any
	if _, out, _ := env.tc("entities", "run-1", "--json"); json.Unmarshal([]byte(out), &ents) != nil || len(ents) != 2 || ents[0]["uncertain"] != 2.0 {
		panic(fmt.Sprintf("entities --json: %s", out))
	} else if _, has := ents[1]["uncertain"]; has {
		panic(fmt.Sprintf("an entity without uncertain mutations must not carry the field: %s", out))
	}

	code, out, e = env.tc("history", "run-1", "postgres", "stock", "w")
	want := `postgres/stock {"sku":"w"}: 4 mutation(s), 2 uncertain (see the flags)  by api
#2     update   effective    api          db.write
       flags: pk_not_unique, after_unavailable
       after image unavailable: the new values were not captured
#5     update   effective    api          db.write
       flags: image_skipped:deadline
       qty: 1 -> 2
#8     update   effective    api          db.write
       flags: redacted
#9     delete   effective    api          db.write
`
	if code != 0 || out != want {
		panic(fmt.Sprintf("history: %d %s\ngot:\n%s\nwant:\n%s", code, e, out, want))
	}
	var h struct {
		Mutations []map[string]any `json:"mutations"`
	}
	if _, out, _ := env.tc("history", "run-1", "postgres", "stock", "w", "--json"); json.Unmarshal([]byte(out), &h) != nil || len(h.Mutations) != 4 {
		panic(fmt.Sprintf("history --json: %s", out))
	}
	if f, _ := h.Mutations[0]["flags"].([]any); len(f) != 2 || f[0] != "pk_not_unique" {
		panic(fmt.Sprintf("history --json flags: %v", h.Mutations[0]))
	}
	if _, has := h.Mutations[3]["flags"]; has {
		panic(fmt.Sprintf("a change without flags must not carry the field: %v", h.Mutations[3]))
	}

	// A history without flags (an older collector) prints none, and no count of uncertain changes.
	env.hist = nil
	if _, out, _ := env.tc("history", "run-1", "postgres", "orders", "42"); strings.Contains(out, "flags") || strings.Contains(out, "uncertain") ||
		!strings.HasPrefix(out, `postgres/orders {"id":42}: 2 mutation(s)  by api, worker`+"\n") {
		panic(fmt.Sprintf("history without flags:\n%s", out))
	}
}

// --short prints one line a script can read; without it the output is the
// session as JSON, as before.
func TestCLIShortOutput(t *testing.T) {
	env := newResultsEnv(t)
	_, id, _ := env.tc("start", "--name", "run-1", "--id-only")
	id = strings.TrimSpace(id)

	if code, out, e := env.tc("status", "run-1", "--short"); code != 0 || out != id+" ACTIVE\n" {
		panic(fmt.Sprintf("status --short: %d %q %s", code, out, e))
	}
	if code, out, e := env.tc("rename", "--short", "run-1", "run-2"); code != 0 || out != id+" ACTIVE\n" {
		panic(fmt.Sprintf("rename --short: %d %q %s", code, out, e))
	}
	if code, out, e := env.tc("stop", "run-2", "--wait", "--short"); code != 0 || out != id+" SEALED\n" {
		panic(fmt.Sprintf("stop --wait --short: %d %q %s", code, out, e))
	}
	if code, out, e := env.tc("wait", "--short", "run-2"); code != 0 || out != id+" SEALED\n" {
		panic(fmt.Sprintf("wait --short: %d %q %s", code, out, e))
	}
	_, id2, _ := env.tc("start", "--name", "doomed", "--id-only")
	if code, out, e := env.tc("abort", "doomed", "--short"); code != 0 || out != strings.TrimSpace(id2)+" ABORTED\n" {
		panic(fmt.Sprintf("abort --short: %d %q %s", code, out, e))
	}

	// Unchanged without the flag.
	for _, cmd := range []string{"status", "wait"} {
		var info client.Info
		_, out, _ := env.tc(cmd, "run-2")
		if err := json.NewDecoder(strings.NewReader(out)).Decode(&info); err != nil || info.ID != id || info.State != "SEALED" {
			panic(fmt.Sprintf("%s: %v\n%s", cmd, err, out))
		}
	}
	if _, out, _ := env.tc("status", "run-2"); !strings.Contains(out, "\nname: run-2\n") {
		panic(fmt.Sprintf("status lost its name line:\n%s", out))
	}
}

func TestPrintInfoShortMarksIncomplete(t *testing.T) {
	var b strings.Builder
	printInfo(&b, client.Info{ID: "ts_1", State: "SEALED", Incomplete: true}, true)
	if b.String() != "ts_1 SEALED incomplete\n" {
		panic(fmt.Sprintf("got %q", b.String()))
	}
}
