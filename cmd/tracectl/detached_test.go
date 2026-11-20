package main

import (
	"strings"
	"testing"
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
	// A name cannot be resolved without the control plane, and lifecycle commands still need it.
	if code, _, e := env.tc("summary", "no-such-name"); code != 1 || !strings.Contains(e, "no session named") {
		t.Fatalf("unknown name: %d %s", code, e)
	}
	if code, _, _ := env.tc("stop", "ts_forgotten0001"); code == 0 {
		t.Fatal("stop of a session the control plane does not know must fail")
	}
}
