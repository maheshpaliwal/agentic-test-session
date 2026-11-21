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
//
// And before the session: the preflight `doctor --require-services 2
// --require-delivery` used to pass, exit 0 and "connected", until a session
// ran and an event was lost. The SDK now looks whether its collector is there
// while it has nothing to send, and doctor says so with no session running.
func TestDelivery_DoctorNamesTheServiceThatCannotDeliver(t *testing.T) {
	addr := freeAddr(t) // a closed port on 127.0.0.1
	e := newDeliveryEnv(t, "http://ciuser:hunter2@"+addr)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// unreachable is the line of the one service doctor says cannot reach
	// its collector.
	unreachable := func(when, out string) string {
		var lines []string
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "CANNOT REACH COLLECTOR") {
				lines = append(lines, line)
			}
		}
		if len(lines) != 1 || !strings.Contains(lines[0], "orders-api") ||
			!strings.Contains(lines[0], "CANNOT REACH COLLECTOR: ") || !strings.Contains(lines[0], " http://"+addr+"/") || !strings.Contains(lines[0], "connection refused") {
			panic(fmt.Sprintf("doctor %s: want orders-api alone, with the address and the reason: %q\n%s", when, lines, out))
		}
		if strings.Contains(out, "ciuser") || strings.Contains(out, "hunter2") {
			panic(fmt.Sprintf("doctor %s shows the credentials of the collector URL:\n%s", when, out))
		}
		return lines[0]
	}

	// No session yet. Both services are connected and nothing is held, and
	// orders-api has found out that its collector is not there.
	var code int
	var out string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if code, out = e.runTracectl("doctor", "--require-services", "2", "--require-delivery"); code == 3 {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("the preflight passes over a service whose collector URL leads nowhere: exit %d\n%s", code, out))
		}
	}
	t.Logf("doctor before the session:\n%s", out)
	unreachable("before the session", out)
	if strings.Contains(out, "CANNOT DELIVER") || !strings.Contains(out, "active sessions  0") ||
		!strings.Contains(out, "note: orders-api cannot reach its collector, so nothing it records would arrive: check TSESSION_COLLECTOR_URL of orders-api") ||
		!strings.Contains(out, "check failed: --require-delivery: orders-api (") || !strings.Contains(out, "cannot reach its collector: Get http://"+addr) {
		panic(fmt.Sprintf("doctor before the session: exit %d\n%s", code, out))
	}
	// It is --require-delivery that fails; the services are connected.
	if code, out := e.runTracectl("doctor", "--require-services", "2"); code != 0 {
		panic(fmt.Sprintf("doctor --require-services 2: exit %d\n%s", code, out))
	}
	if code, js := e.runTracectl("doctor", "--json"); code != 0 || !strings.Contains(js, `"reachable": false`) || !strings.Contains(js, `"reachable": true`) ||
		strings.Contains(js, "hunter2") {
		panic(fmt.Sprintf("doctor --json before the session: exit %d\n%s", code, js))
	}

	s, err := e.session(ctx, "doctor-wrong-collector-url")
	if err != nil {
		panic(err)
	}
	if err := e.request(ctx, s); err != nil {
		panic(err)
	}
	// orders-api says what it holds with its next periodic report.
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if code, out = e.runTracectl("doctor", "--require-delivery"); code == 3 && strings.Contains(out, "CANNOT DELIVER") {
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

	// What it holds, and why, says it all: the line does not say twice that
	// the collector is not there.
	if strings.Contains(out, "CANNOT REACH COLLECTOR") {
		t.Errorf("doctor during the run names the unreachable collector twice:\n%s", out)
	}

	// The session over, doctor has nothing running to look at for what a
	// service holds: that check is one for during a run. That the collector of
	// orders-api is not there still shows.
	if _, err := s.StopAndWait(ctx); err != nil {
		panic(err)
	}
	code, out = e.runTracectl("doctor", "--require-delivery")
	if code != 3 || strings.Contains(out, "CANNOT DELIVER") {
		t.Errorf("doctor after the session: exit %d\n%s", code, out)
	}
	unreachable("after the session", out)
}
