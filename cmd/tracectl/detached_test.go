package main

import (
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The control plane forgets finished sessions (retention, restart) while the
// collector still holds their events: reading by id must still work.
func TestResultsOfASessionTheControlPlaneForgot(t *testing.T) {
	env := newResultsEnv(t)
	code, out, e := env.tc("summary", "ts_forgotten0001")
	if code != 0 || !strings.Contains(out, "events     4 over 50ms") {
		t.Fatalf("summary by id of a forgotten session: %d %s\n%s", code, e, out)
	}
	if code, out, _ := env.tc("events", "ts_forgotten0001", "--limit", "1"); code != 0 || !strings.Contains(out, "1 of 4 events") {
		t.Fatalf("events: %d\n%s", code, out)
	}
	// Whether its seal failed is not known any more, so that gate cannot pass.
	if code, _, e := env.tc("summary", "ts_forgotten0001", "--fail-on-incomplete"); code != exitCheckFailed || !strings.Contains(e, "no longer knows") {
		t.Fatalf("--fail-on-incomplete on a forgotten session: %d %s", code, e)
	}
	if code, _, e := env.tc("report", "ts_forgotten0001", "--fail-on-incomplete"); code != exitCheckFailed || !strings.Contains(e, "no longer knows") {
		t.Fatalf("report --fail-on-incomplete on a forgotten session: %d %s", code, e)
	}
	// The collector answers for any id, with nothing: a mistyped id must stay an error, not
	// read as an empty session that passes every gate.
	held := env.sum
	env.sum = client.Summary{}
	for _, args := range [][]string{{"summary", "ts_typo0001", "--fail-on-error", "--fail-on-incomplete"}, {"events", "ts_typo0001"},
		{"report", "ts_typo0001", "--fail-on-error"}, {"export", "ts_typo0001"}} {
		if code, out, e := env.tc(args...); code != 1 || !strings.Contains(e, "404") {
			t.Fatalf("%v with an unknown id: exit %d\n%s\n%s", args, code, out, e)
		}
	}
	env.sum = held
	// A name cannot be resolved without the control plane, and lifecycle commands still need it.
	if code, _, e := env.tc("summary", "no-such-name"); code != 1 || !strings.Contains(e, "no session named") {
		t.Fatalf("unknown name: %d %s", code, e)
	}
	if code, _, _ := env.tc("stop", "ts_forgotten0001"); code == 0 {
		t.Fatal("stop of a session the control plane does not know must fail")
	}
}
