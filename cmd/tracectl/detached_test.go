package main

import (
	"fmt"
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
		panic(fmt.Sprintf("summary by id of a forgotten session: %d %s\n%s", code, e, out))
	}
	if code, out, _ := env.tc("events", "ts_forgotten0001", "--limit", "1"); code != 0 || !strings.Contains(out, "1 of 4 events") {
		panic(fmt.Sprintf("events: %d\n%s", code, out))
	}
	// Whether its seal failed is not known any more, so that gate cannot pass.
	if code, _, e := env.tc("summary", "ts_forgotten0001", "--fail-on-incomplete"); code != exitCheckFailed || !strings.Contains(e, "no longer knows") {
		panic(fmt.Sprintf("--fail-on-incomplete on a forgotten session: %d %s", code, e))
	}
	if code, _, e := env.tc("report", "ts_forgotten0001", "--fail-on-incomplete"); code != exitCheckFailed || !strings.Contains(e, "no longer knows") {
		panic(fmt.Sprintf("report --fail-on-incomplete on a forgotten session: %d %s", code, e))
	}
	// The collector answers for any id, with nothing: a mistyped id must stay an error, not
	// read as an empty session that passes every gate.
	held := env.sum
	env.sum = client.Summary{}
	for _, args := range [][]string{{"summary", "ts_typo0001", "--fail-on-error", "--fail-on-incomplete"}, {"events", "ts_typo0001"},
		{"report", "ts_typo0001", "--fail-on-error"}, {"export", "ts_typo0001"}} {
		if code, out, e := env.tc(args...); code != 1 || !strings.Contains(e, "404") || !strings.Contains(e, `no session with id "ts_typo0001"`) {
			panic(fmt.Sprintf("%v with an unknown id: exit %d\n%s\n%s", args, code, out, e))
		}
	}
	env.sum = held
	// A name cannot be resolved without the control plane, and lifecycle commands still need it.
	if code, _, e := env.tc("summary", "no-such-name"); code != 1 || !strings.Contains(e, "no session named") {
		panic(fmt.Sprintf("unknown name: %d %s", code, e))
	}
	if code, _, _ := env.tc("stop", "ts_forgotten0001"); code == 0 {
		panic("stop of a session the control plane does not know must fail")
	}
}

// An empty reference is not sent as a name filter, so it would match any
// session: it is refused instead.
func TestEmptySessionReferenceIsRefused(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")
	for _, cmd := range []string{"summary", "stop", "delete", "status"} {
		if code, _, e := env.tc(cmd, ""); code != 1 || !strings.Contains(e, "a session id or name is required") {
			panic(fmt.Sprintf("%s with an empty reference: %d %s", cmd, code, e))
		}
	}
}

// start --id-only prints the id and nothing else, so a script can capture it.
func TestStartIDOnly(t *testing.T) {
	env := newResultsEnv(t)
	code, out, e := env.tc("start", "--name", "run-1", "--id-only")
	if code != 0 || e != "" || !strings.HasPrefix(out, "ts_") || strings.Count(out, "\n") != 1 || strings.ContainsAny(out, "{ \"") {
		panic(fmt.Sprintf("start --id-only: %d %q %q", code, out, e))
	}
	if code, _, e := env.tc("status", strings.TrimSpace(out)); code != 0 {
		panic(fmt.Sprintf("the printed id does not resolve: %d %s", code, e))
	}
	// A second session under the same name: started, with a note that names the earlier one, and still only
	// the id on standard output.
	first := strings.TrimSpace(out)
	code, out, e = env.tc("start", "--name", "run-1", "--id-only")
	if code != 0 || !strings.HasPrefix(out, "ts_") || strings.Count(out, "\n") != 1 || strings.TrimSpace(out) == first ||
		!strings.Contains(e, first) || !strings.Contains(e, `also named "run-1"`) {
		panic(fmt.Sprintf("second start with the name: %d %q %q", code, out, e))
	}
}
