package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

func TestCLIDoctor(t *testing.T) {
	env := newResultsEnv(t)

	// Nothing connected yet: both services answer, and the report says why a session would stay empty.
	code, out, e := env.tc("doctor")
	if code != 0 || !strings.Contains(out, "control plane  "+env.cp+"  ok") || !strings.Contains(out, "collector      "+env.col+"  ok") ||
		!strings.Contains(out, "connected services  0") || !strings.Contains(out, "TSESSION_ENABLED=true") {
		panic(fmt.Sprintf("doctor with no services: %d %s\n%s", code, e, out))
	}

	if code, _, e := env.tc("doctor", "--require-services", "1"); code != 3 || !strings.Contains(e, "0 service(s) connected") {
		panic(fmt.Sprintf("doctor --require-services with none connected: %d %s", code, e))
	}

	named := runtime.NewSubscriberFor(env.cp, "orders-api", "orders-api-1")
	defer named.Close()
	unnamed := runtime.NewSubscriber(env.cp)
	defer unnamed.Close()
	env.tc("start", "--name", "run-1")
	deadline := time.Now().Add(5 * time.Second)
	for {
		code, out, e = env.tc("doctor")
		if strings.Contains(out, "connected services  2") || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 0 || !strings.Contains(out, "active sessions  1") || !strings.Contains(out, "orders-api") ||
		!strings.Contains(out, "orders-api-1") || !strings.Contains(out, "1 unnamed") || strings.Contains(out, "note:") {
		panic(fmt.Sprintf("doctor with services: %d %s\n%s", code, e, out))
	}
	if code, _, e := env.tc("doctor", "--require-services", "2"); code != 0 {
		panic(fmt.Sprintf("doctor --require-services with two connected: %d %s", code, e))
	}
	if code, out, _ := env.tc("doctor", "--json"); code != 0 || !strings.Contains(out, `"service": "orders-api"`) ||
		!strings.Contains(out, `"active_sessions": 1`) {
		panic(fmt.Sprintf("doctor --json: %d\n%s", code, out))
	}

	// An unreachable collector fails the command but still reports the rest.
	env.col = "http://127.0.0.1:1"
	code, out, e = env.tc("doctor")
	if code != 1 || !strings.Contains(out, "UNREACHABLE") || !strings.Contains(out, "connected services  2") ||
		!strings.Contains(e, "unreachable") {
		panic(fmt.Sprintf("doctor with the collector down: %d %s\n%s", code, e, out))
	}
}

// A service whose /healthz answers 503 with a reason (a background goroutine
// of it panicked and waits for its restart) is shown as degraded, with the
// reason, cleaned; the rest of the report is still made, and the command
// fails.
func TestCLIDoctorShowsADegradedService(t *testing.T) {
	env := newResultsEnv(t)
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "degraded: a background goroutine panicked and waits to be restarted: janitor; the log has the panic \x1b[31m http://u:hunter2@h/\n")
	}))
	defer sick.Close()
	env.col = sick.URL
	code, out, e := env.tc("doctor")
	if code != 1 || !strings.Contains(out, "collector      "+env.col+"  DEGRADED  a background goroutine panicked and waits to be restarted: janitor") ||
		!strings.Contains(out, "control plane  "+env.cp+"  ok") || !strings.Contains(out, "connected services  0") ||
		!strings.Contains(e, "a service is degraded") || strings.Contains(e, "unreachable") {
		panic(fmt.Sprintf("doctor with a degraded collector: %d %s\n%s", code, e, out))
	}
	if strings.Contains(out, "hunter2") || strings.Contains(out, "\x1b") {
		panic("the reason is printed as it came:\n" + out)
	}
	if code, out, _ := env.tc("doctor", "--json"); code != 1 || !strings.Contains(out, `"degraded": "a background goroutine panicked`) {
		panic(fmt.Sprintf("doctor --json: %d\n%s", code, out))
	}

	// A degraded control plane still answers: its part of the report is made.
	env = newResultsEnv(t)
	real := env.cp
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "degraded: a background goroutine panicked and waits to be restarted: sweep; the log has the panic\n")
			return
		}
		resp, err := http.Get(real + r.URL.RequestURI())
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer front.Close()
	env.cp = front.URL
	code, out, e = env.tc("doctor")
	if code != 1 || !strings.Contains(out, "DEGRADED  a background goroutine panicked and waits to be restarted: sweep") ||
		!strings.Contains(out, "active sessions  0") || !strings.Contains(e, "a service is degraded") {
		panic(fmt.Sprintf("doctor with a degraded control plane: %d %s\n%s", code, e, out))
	}

	// A 503 without the reason is not a degraded service: it is not ours.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "upstream down", 503) }))
	defer other.Close()
	env.col = other.URL
	if code, out, e := env.tc("doctor"); code != 1 || !strings.Contains(out, "UNREACHABLE  GET /healthz: 503") || !strings.Contains(e, "unreachable") {
		panic(fmt.Sprintf("doctor behind a failing proxy: %d %s\n%s", code, e, out))
	}
}

func TestCLIKill(t *testing.T) {
	env := newResultsEnv(t)
	sub := runtime.NewSubscriber(env.cp)
	defer sub.Close()
	env.tc("start", "--name", "run-1")
	// recording waits until the follower's view of "some session is recording" is want.
	recording := func(want bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); sub.AnyActive() != want; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				panic(fmt.Sprintf("the follower never saw recording=%v", want))
			}
		}
	}
	recording(true)

	if code, out, e := env.tc("kill", "status"); code != 0 || !strings.HasPrefix(out, "kill switch: off") {
		panic(fmt.Sprintf("kill status: %d %s\n%s", code, e, out))
	}
	if code, out, _ := env.tc("doctor"); code != 0 || strings.Contains(out, "KILL SWITCH") {
		panic(fmt.Sprintf("doctor with the switch off: %d\n%s", code, out))
	}
	if code, out, _ := env.tc("doctor", "--json"); code != 0 || !strings.Contains(out, `"kill_switch": false`) {
		panic(fmt.Sprintf("doctor --json with the switch off: %d\n%s", code, out))
	}

	code, out, e := env.tc("kill", "on")
	if code != 0 || !strings.HasPrefix(out, "kill switch: ON") || !strings.Contains(out, "recording is stopped") {
		panic(fmt.Sprintf("kill on: %d %s\n%s", code, e, out))
	}
	recording(false) // the SDK side really stops, with the session still ACTIVE
	if code, out, _ := env.tc("kill", "status"); code != 0 || !strings.HasPrefix(out, "kill switch: ON") {
		panic(fmt.Sprintf("kill status when on: %d\n%s", code, out))
	}
	if code, out, _ := env.tc("doctor"); code != 0 || !strings.Contains(out, "KILL SWITCH ON  nothing is recorded") ||
		!strings.Contains(out, "tracectl kill off") || !strings.Contains(out, "active sessions  1") {
		panic(fmt.Sprintf("doctor with the switch on: %d\n%s", code, out))
	}
	if code, out, _ := env.tc("doctor", "--json"); code != 0 || !strings.Contains(out, `"kill_switch": true`) {
		panic(fmt.Sprintf("doctor --json with the switch on: %d\n%s", code, out))
	}

	if code, out, e := env.tc("kill", "off"); code != 0 || !strings.HasPrefix(out, "kill switch: off") {
		panic(fmt.Sprintf("kill off: %d %s\n%s", code, e, out))
	}
	recording(true)

	// Bad arguments fail without changing anything.
	for _, args := range [][]string{{"kill"}, {"kill", "maybe"}, {"kill", "on", "off"}} {
		if code, _, e := env.tc(args...); code != 1 || !strings.Contains(e, "usage: tracectl kill on|off|status") {
			panic(fmt.Sprintf("%v: %d %s", args, code, e))
		}
	}
	if _, out, _ := env.tc("kill", "status"); !strings.HasPrefix(out, "kill switch: off") {
		panic(fmt.Sprintf("bad arguments changed the switch: %s", out))
	}
}

// doctor prints the URLs it was given, often into a CI log: what may be a
// credential in them stays out.
func TestDoctorHidesCredentialsInURLs(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"--cp", "http://ciuser:hunter2@127.0.0.1:1/?token=tok123", "--collector", "http://ciuser:hunter2@127.0.0.1:1", "doctor"}, &out, &errb)
	all := out.String() + errb.String()
	if code != 1 || !strings.Contains(all, "http://127.0.0.1:1") {
		panic(fmt.Sprintf("exit %d\n%s", code, all))
	}
	for _, secret := range []string{"ciuser", "hunter2", "tok123"} {
		if strings.Contains(all, secret) {
			t.Errorf("doctor shows %q:\n%s", secret, all)
		}
	}
}

// follow opens the active-set stream as an SDK that sends delivery reports
// under instance. The stream is closed with the test.
func (env *resultsEnv) follow(t *testing.T, service, instance string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.cp+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, service)
	req.Header.Set(core.HeaderInstance, instance)
	req.Header.Set(core.HeaderDeliveryInstance, instance)
	tr := &http.Transport{DisableKeepAlives: true} // one connection, gone with the test
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	t.Cleanup(func() { cancel(); <-done; tr.CloseIdleConnections() })
}

// reportDelivery posts a delivery report in an instance's name.
func (env *resultsEnv) reportDelivery(id string, d controlplane.Delivery) {
	b, _ := json.Marshal(d)
	resp, err := http.Post(env.cp+"/v1/sessions/"+id+"/delivery", "application/json", bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); resp.StatusCode/100 != 2 {
		panic(fmt.Sprintf("delivery report: %s %s", resp.Status, body))
	}
}

// A service that follows and reports, and whose events do not reach the
// collector, used to look like every other connected one. doctor shows what
// it holds and why, with the remedy, and --require-delivery fails on it; a
// service that delivers adds nothing to the output.
func TestCLIDoctorShowsAServiceThatCannotDeliver(t *testing.T) {
	env := newResultsEnv(t)
	env.follow(t, "orders-api", "orders-1")
	env.follow(t, "inventory", "inventory-1")
	env.tc("start", "--name", "run-1")
	active, err := client.List(context.Background(), client.Options{BaseURL: env.cp}, "ACTIVE")
	if err != nil || len(active) != 1 {
		panic(fmt.Sprintf("active sessions: %v %v", active, err))
	}
	id := active[0].ID

	// Both deliver: nothing new in the output, and the check passes.
	env.reportDelivery(id, controlplane.Delivery{Service: "orders-api", Instance: "orders-1", Recorded: 2, Delivered: 2})
	env.reportDelivery(id, controlplane.Delivery{Service: "inventory", Instance: "inventory-1", Recorded: 1, Delivered: 1})
	code, out, e := env.tc("doctor", "--require-delivery")
	if code != 0 || !strings.Contains(out, "connected services  2") || strings.Contains(out, "CANNOT DELIVER") || strings.Contains(out, "note:") {
		panic(fmt.Sprintf("doctor with healthy services: %d %s\n%s", code, e, out))
	}
	if _, out, _ := env.tc("doctor", "--json"); strings.Contains(out, "delivery_pending") {
		panic(fmt.Sprintf("doctor --json with healthy services names pending delivery:\n%s", out))
	}

	// Events on their way (held, no failed request) are not a finding.
	env.reportDelivery(id, controlplane.Delivery{Service: "inventory", Instance: "inventory-1", Recorded: 3, Delivered: 1, Buffered: 2})
	if code, out, e := env.tc("doctor", "--require-delivery"); code != 0 || strings.Contains(out, "CANNOT DELIVER") || strings.Contains(out, "note:") {
		panic(fmt.Sprintf("doctor with events on their way: %d %s\n%s", code, e, out))
	}

	// orders-api cannot reach its collector. Its error carries a URL with a credential.
	env.reportDelivery(id, controlplane.Delivery{Service: "orders-api", Instance: "orders-1", Recorded: 5, Delivered: 2, Buffered: 3,
		LastError: `Post "http://ciuser:hunter2@10.0.0.9:9/v1/ingest?token=tok123": dial tcp 10.0.0.9:9: connect: connection refused`})
	code, out, e = env.tc("doctor")
	if code != 0 || !strings.Contains(out, "CANNOT DELIVER: 3 event(s) undelivered in 1 session(s); last error: ") ||
		!strings.Contains(out, "connection refused") ||
		!strings.Contains(out, "note: orders-api cannot deliver its events to the collector: check TSESSION_COLLECTOR_URL of orders-api") {
		panic(fmt.Sprintf("doctor with a service that cannot deliver: %d %s\n%s", code, e, out))
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "inventory") && (strings.Contains(line, "CANNOT DELIVER") || strings.HasPrefix(line, "note:")) {
			t.Errorf("inventory delivers and is flagged: %q", line)
		}
	}
	_, js, _ := env.tc("doctor", "--json")
	var rep doctorReport
	if err := json.Unmarshal([]byte(js), &rep); err != nil {
		panic(fmt.Sprintf("doctor --json: %v\n%s", err, js))
	}
	var orders *client.PendingDelivery
	for _, s := range rep.Subscribers {
		if s.Service == "orders-api" {
			orders = s.Pending
		}
	}
	if orders == nil || orders.Undelivered != 3 || orders.Sessions != 1 || !strings.Contains(orders.LastError, "connection refused") || len(rep.Notes) != 1 {
		panic(fmt.Sprintf("doctor --json: orders-api pending = %+v, notes %q\n%s", orders, rep.Notes, js))
	}
	code, _, e = env.tc("doctor", "--require-delivery")
	if code != 3 || !strings.Contains(e, "check failed: --require-delivery: orders-api (orders-1) cannot deliver: ") || strings.Contains(e, "inventory") {
		panic(fmt.Sprintf("doctor --require-delivery: %d %s", code, e))
	}
	for _, secret := range []string{"ciuser", "hunter2", "tok123"} {
		if strings.Contains(out+js+e, secret) {
			t.Errorf("doctor shows %q:\n%s\n%s\n%s", secret, out, js, e)
		}
	}

	// The collector reachable again, its next report takes the finding back.
	env.reportDelivery(id, controlplane.Delivery{Service: "orders-api", Instance: "orders-1", Recorded: 5, Delivered: 5})
	if code, out, e := env.tc("doctor", "--require-delivery"); code != 0 || strings.Contains(out, "CANNOT DELIVER") || strings.Contains(out, "note:") {
		panic(fmt.Sprintf("doctor after the service recovered: %d %s\n%s", code, e, out))
	}
}

// --require-delivery fails on a service whose reports do not arrive; one
// that lost events to a full ring while the collector answers is not named,
// and neither is anything from a control plane without delivery_pending.
func TestDoctorRequireDeliveryNamesUnheardServices(t *testing.T) {
	long := time.Now().Add(-time.Minute)
	rep := doctorReport{Subscribers: []client.Subscriber{
		{Service: "orders-api", Instance: "pod-1", DeliveryInstance: "pod-1-ab", ConnectedAt: long, DeliveryUnheard: true},
		{Service: "inventory", Instance: "pod-2", DeliveryInstance: "pod-2-cd", ConnectedAt: long, LastDeliveryReport: long},
		{Service: "legacy", Instance: "pod-3", ConnectedAt: long},
		{Service: "lossy", Instance: "pod-4", DeliveryInstance: "pod-4-ef", ConnectedAt: long, Pending: &client.PendingDelivery{Sessions: 1, Dropped: 4}},
	}}
	rep.noteDelivery()
	if got := rep.undeliverable(); len(got) != 1 || got[0] != "orders-api (pod-1) sends no delivery reports" || len(rep.Notes) != 0 {
		panic(fmt.Sprintf("undeliverable = %q, notes %q", got, rep.Notes))
	}
}
