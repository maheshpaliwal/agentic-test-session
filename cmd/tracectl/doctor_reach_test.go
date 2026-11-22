package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// say posts a batch without reports in an instance's name, with what it says
// about its collector (a JSON object, or "" for an SDK that says nothing).
func (env *resultsEnv) say(service, instance, collector string) {
	body := fmt.Sprintf(`{"service":%q,"instance":%q,"reports":[]`, service, instance)
	if collector != "" {
		body += `,"collector":` + collector
	}
	resp, err := http.Post(env.cp+"/v1/delivery", "application/json", strings.NewReader(body+"}"))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("the batch: %s %s", resp.Status, b))
	}
}

// A service whose collector URL leads nowhere was "connected" until a
// session ran and an event of it was lost. It finds that out by itself now
// and says so; doctor shows it with no session running, with the remedy, and
// --require-delivery fails on it. A service that says nothing about its
// collector (an older SDK) is not known to be unreachable, and passes.
func TestCLIDoctorShowsAServiceThatCannotReachItsCollectorBeforeAnySession(t *testing.T) {
	env := newResultsEnv(t)
	env.follow(t, "orders-api", "orders-1")
	env.follow(t, "inventory", "inventory-1")
	env.follow(t, "legacy", "legacy-1")
	env.say("inventory", "inventory-1", `{"reachable":true}`)
	env.say("legacy", "legacy-1", "")

	// Nothing is known of orders-api yet: not a finding.
	code, out, e := env.tc("doctor", "--require-services", "3", "--require-delivery")
	if code != 0 || strings.Contains(out, "CANNOT") || strings.Contains(out, "note:") ||
		!strings.Contains(out, "1 of 3 said whether they reach their collector, of the others that is not known") {
		panic(fmt.Sprintf("doctor before orders-api said anything: %d %s\n%s", code, e, out))
	}

	env.say("orders-api", "orders-1", `{"reachable":false,"error":"Get http://ciuser:hunter2@10.0.0.9:9/healthz?token=tok123: dial tcp 10.0.0.9:9: connect: connection refused \u001b[31m"}`)
	code, out, e = env.tc("doctor")
	if code != 0 || !strings.Contains(out, "active sessions  0") ||
		!strings.Contains(out, "note: orders-api cannot reach its collector, so nothing it records would arrive: check TSESSION_COLLECTOR_URL of orders-api") {
		panic(fmt.Sprintf("doctor with a service that cannot reach its collector: %d %s\n%s", code, e, out))
	}
	flagged := 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "CANNOT REACH COLLECTOR: ") {
			continue
		}
		flagged++
		if !strings.Contains(line, "orders-api") || !strings.Contains(line, "CANNOT REACH COLLECTOR: Get http://") || !strings.Contains(line, "connection refused") {
			t.Errorf("flagged line: %q", line)
		}
	}
	if flagged != 1 || strings.Count(out, "note:") != 1 {
		t.Errorf("%d service(s) flagged, want orders-api alone:\n%s", flagged, out)
	}
	code, _, e = env.tc("doctor", "--require-services", "3", "--require-delivery")
	if code != 3 || !strings.Contains(e, "check failed: --require-delivery: orders-api (orders-1) cannot reach its collector: Get http://") ||
		strings.Contains(e, "inventory") || strings.Contains(e, "legacy") {
		panic(fmt.Sprintf("doctor --require-delivery: %d %s", code, e))
	}
	_, js, _ := env.tc("doctor", "--json")
	if !strings.Contains(js, `"reachable": false`) || !strings.Contains(js, `"reachable": true`) {
		panic("doctor --json:\n" + js)
	}
	for _, secret := range []string{"ciuser", "hunter2", "tok123", "\x1b"} {
		if strings.Contains(out+js+e, secret) {
			t.Errorf("doctor shows %q:\n%s\n%s\n%s", secret, out, js, e)
		}
	}

	// The URL put right, the service says so, and the finding is gone.
	env.say("orders-api", "orders-1", `{"reachable":true}`)
	if code, out, e := env.tc("doctor", "--require-delivery"); code != 0 || strings.Contains(out, "CANNOT") || strings.Contains(out, "note:") {
		panic(fmt.Sprintf("doctor after the service reached its collector: %d %s\n%s", code, e, out))
	}
}

// What doctor makes of a service's word about its collector, case by case.
func TestDoctorRequireDeliveryNamesServicesThatCannotReachTheirCollector(t *testing.T) {
	long := time.Now().Add(-time.Minute)
	no := func(why string) *client.CollectorReach { return &client.CollectorReach{Error: why, At: long} }
	rep := doctorReport{ControlPlane: probeResult{OK: true}, Collector: probeResult{OK: true}, Subscribers: []client.Subscriber{
		{Service: "orders-api", Instance: "pod-1", ConnectedAt: long, Collector: no("Get http://u:hunter2@c:1/healthz: no such host " + strings.Repeat("y", 400))},
		{Service: "inventory", Instance: "pod-2", ConnectedAt: long, Collector: &client.CollectorReach{Reachable: true, At: long}},
		{Service: "legacy", Instance: "pod-3", ConnectedAt: long},
		// Its reports do not arrive: what it said about its collector is old.
		{Service: "cut-off", Instance: "pod-4", ConnectedAt: long, DeliveryUnheard: true, Collector: no("refused")},
		// It holds events: that line says why, once.
		{Service: "stuck", Instance: "pod-5", ConnectedAt: long, Collector: no("refused"),
			Pending: &client.PendingDelivery{Sessions: 1, Undelivered: 2, LastError: "Post http://c:1/v1/ingest: refused"}},
	}}
	rep.noteDelivery()
	got := rep.undeliverable()
	if len(got) != 3 || !strings.HasPrefix(got[0], "orders-api (pod-1) cannot reach its collector: Get http://c:1/healthz: no such host yyy") ||
		got[1] != "cut-off (pod-4) sends no delivery reports" || got[2] != "stuck (pod-5) cannot deliver: Post http://c:1/v1/ingest: refused" {
		panic(fmt.Sprintf("undeliverable = %q", got))
	}
	if strings.Contains(got[0], "hunter2") || len(got[0]) > 300 {
		panic(fmt.Sprintf("the service's words as they came: %q", got[0]))
	}
	var out strings.Builder
	rep.print(&out)
	if n := strings.Count(out.String(), "CANNOT REACH COLLECTOR: "); n != 1 || strings.Count(out.String(), "CANNOT DELIVER: ") != 1 ||
		strings.Count(out.String(), "NO DELIVERY REPORTS") != 1 {
		panic(fmt.Sprintf("%d lines say that a collector is not reached:\n%s", n, out.String()))
	}
	// Three notes: two services that cannot reach the collector and are not
	// stuck with events, and the one that is.
	if len(rep.Notes) != 3 {
		panic(fmt.Sprintf("notes: %q", rep.Notes))
	}
}
