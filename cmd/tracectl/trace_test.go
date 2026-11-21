package main

import (
	"fmt"
	"strings"
	"testing"
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
