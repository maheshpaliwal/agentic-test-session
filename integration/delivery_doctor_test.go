package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The walkthrough again, seen while the session runs: orders-api has a
// collector URL where nothing listens. It is connected and its reports
// arrive, so doctor used to show it like inventory, and the first sign was
// the incomplete session at the end. Now doctor names it, with what it holds
// and the remedy, and --require-delivery fails on it.
func TestDelivery_DoctorNamesTheServiceThatCannotDeliver(t *testing.T) {
	e := newDeliveryEnv(t, "http://ciuser:hunter2@"+freeAddr(t)) // a closed port on 127.0.0.1
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// No session yet: both services are connected and nothing is held.
	if code, out := e.runTracectl("doctor", "--require-services", "2", "--require-delivery"); code != 0 || strings.Contains(out, "CANNOT DELIVER") {
		panic(fmt.Sprintf("doctor before the session: exit %d\n%s", code, out))
	}

	s, err := e.session(ctx, "doctor-wrong-collector-url")
	if err != nil {
		panic(err)
	}
	if err := e.request(ctx, s); err != nil {
		panic(err)
	}
	// orders-api says so with its next periodic report.
	var code int
	var out string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if code, out = e.runTracectl("doctor", "--require-delivery"); code == 3 {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("doctor --require-delivery never failed: exit %d\n%s", code, out))
		}
	}
	t.Logf("doctor during the run:\n%s", out)
	flagged := 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "CANNOT DELIVER") {
			continue
		}
		flagged++
		if !strings.Contains(line, "orders-api") || !strings.Contains(line, "2 event(s) undelivered in 1 session(s)") || !strings.Contains(line, "connection refused") {
			t.Errorf("flagged line: %q", line)
		}
	}
	if flagged != 1 || !strings.Contains(out, "note: orders-api cannot deliver its events to the collector: check TSESSION_COLLECTOR_URL of orders-api") ||
		!strings.Contains(out, "check failed: --require-delivery: orders-api (") {
		t.Errorf("doctor --require-delivery: %d service(s) flagged, want orders-api alone, with the remedy\n%s", flagged, out)
	}
	if strings.Contains(out, "ciuser") || strings.Contains(out, "hunter2") {
		t.Errorf("doctor shows the credentials of the collector URL:\n%s", out)
	}
	if code, js := e.runTracectl("doctor", "--json"); code != 0 || !strings.Contains(js, `"delivery_pending"`) || !strings.Contains(js, `"undelivered": 2`) {
		t.Errorf("doctor --json: exit %d\n%s", code, js)
	}

	// The session over, doctor has nothing running to look at: the check is
	// one for during a run.
	if _, err := s.StopAndWait(ctx); err != nil {
		panic(err)
	}
	if code, out := e.runTracectl("doctor", "--require-delivery"); code != 0 || strings.Contains(out, "CANNOT DELIVER") {
		t.Errorf("doctor after the session: exit %d\n%s", code, out)
	}
}
