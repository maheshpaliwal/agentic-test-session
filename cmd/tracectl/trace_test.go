package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func TestCLITrace(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")

	code, out, e := env.tc("trace", "run-1")
	want := []string{
		"#1 api api.in POST /orders  50ms",
		"  #2 api db.write INSERT INTO orders  2ms  [1 mutation(s)]",
		"    ~> #4 worker worker.end ship  ERR carrier timeout",
		"#3 worker db.read SELECT 1",
		"4 span(s)",
	}
	if code != 0 || strings.TrimRight(out, "\n") != strings.Join(want, "\n") {
		panic(fmt.Sprintf("trace: %d %s\n%s", code, e, out))
	}
	// --errors keeps only branches leading to a failure.
	if _, out, _ := env.tc("trace", "run-1", "--errors"); strings.Contains(out, "SELECT 1") || !strings.Contains(out, "#4 worker") ||
		!strings.Contains(out, "3 span(s), 1 hidden") {
		panic(fmt.Sprintf("--errors:\n%s", out))
	}
	if _, out, _ := env.tc("trace", "run-1", "--depth", "1"); strings.Contains(out, "#2") || !strings.Contains(out, "2 span(s), 2 hidden") {
		panic(fmt.Sprintf("--depth:\n%s", out))
	}
	if code, out, _ := env.tc("trace", "run-1", "--json"); code != 0 || !strings.Contains(out, `"roots"`) {
		panic(fmt.Sprintf("--json: %d\n%s", code, out))
	}
}

func TestCLIShow(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")

	code, out, e := env.tc("show", "run-1", "2")
	for _, want := range []string{
		"#2  api  db.write  INSERT INTO orders",
		"duration  2ms",
		"tx        tx1",
		"attr      rows=1",
		"request (redacted):",
		`"[REDACTED]"`,
		`mutation  insert postgres/orders {"id":42}  effective`,
		`status: ∅ -> "new"`,
	} {
		if code != 0 || !strings.Contains(out, want) {
			panic(fmt.Sprintf("show: %d %s\nmissing %q in:\n%s", code, e, want, out))
		}
	}
	if code, out, _ := env.tc("show", "run-1", "#4"); code != 0 || !strings.Contains(out, "error     carrier") {
		panic(fmt.Sprintf("show #4: %d\n%s", code, out))
	}
	if code, out, _ := env.tc("show", "--json", "run-1", "2"); code != 0 || !strings.Contains(out, `"seq": 2`) {
		panic(fmt.Sprintf("show --json: %d\n%s", code, out))
	}
	if code, _, e := env.tc("show", "run-1", "9"); code != 1 || !strings.Contains(e, "no event #9") {
		panic(fmt.Sprintf("missing event: %d %s", code, e))
	}
	if code, _, e := env.tc("show", "run-1", "abc"); code == 0 || !strings.Contains(e, "not an event sequence number") {
		panic(fmt.Sprintf("bad seq: %d %s", code, e))
	}
}

// A value that changed under redaction is recorded with the marker on both
// sides. Printed as two values it reads as no change; show, history and the
// digest say that it changed. The members a Redis set gained and lost print
// as the recorded diff has them.
func TestCLIRedactedChange(t *testing.T) {
	yes := true
	diff := []core.FieldChange{
		{Field: "password", Old: core.RedactedValue, New: core.RedactedValue},
		{Field: "token", New: core.RedactedValue}, // a new field: one side only, not a hidden change
		{Field: "added", New: []any{"d"}},
		{Field: "removed", Old: []any{"b"}},
	}
	want := []string{"password: changed (value hidden)", `token: ∅ -> "[REDACTED]"`, `added: ∅ -> ["d"]`, `removed: ["b"] -> ∅`}

	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")
	env.events = map[string][]core.Event{"run-1": {{Seq: 1, Service: "api", Kind: core.KindDBWrite, Name: "UPDATE users", Mutations: []core.Mutation{
		{Store: "postgres", Entity: "users", Op: "update", PK: map[string]any{"id": 42}, Effective: &yes, Diff: diff}}}}}
	env.hist = []client.EntityMutation{{Seq: 1, Op: "update", Service: "api", Kind: core.KindDBWrite, Effective: &yes, Diff: diff}}
	for _, args := range [][]string{{"show", "run-1", "1"}, {"history", "run-1", "postgres", "orders", "42"}} {
		code, out, e := env.tc(args...)
		if code != 0 {
			panic(fmt.Sprintf("%v: %d %s", args, code, e))
		}
		for _, w := range want {
			if !strings.Contains(out, w) {
				t.Errorf("%v: missing %q in:\n%s", args, w, out)
			}
		}
		if strings.Contains(out, `"[REDACTED]" -> "[REDACTED]"`) {
			t.Errorf("%v: a hidden change is printed as two equal values:\n%s", args, out)
		}
	}

	renv := newReportEnv(t, core.Event{Seq: 1, Service: "api", Kind: core.KindDBWrite, Name: "UPDATE users", Mutations: []core.Mutation{
		{Store: "postgres", Entity: "users", Op: "update", PK: map[string]any{"id": 42}, Effective: &yes, Diff: diff}}})
	_, out, _ := renv.tc("digest", reportID)
	wantAll(t, checkDigestMarkdown(t, out)["### Mutations"],
		`  #1 update effective: "password" changed (value hidden); "token" ∅ -> "[REDACTED]"; "added" ∅ -> ["d"]; "removed" ["b"] -> ∅`+"\n")
	_, out, _ = renv.tc("digest", reportID, "--json")
	f := checkDigestJSON(t, out).Mutations[0].Changes[0].Fields
	if len(f) != 4 || !f[0].Hidden || f[1].Hidden || f[2].Hidden {
		t.Errorf("fields: %+v", f)
	}
}
