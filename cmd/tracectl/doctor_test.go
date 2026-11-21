package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

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
