package main

import (
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
		t.Fatalf("doctor with no services: %d %s\n%s", code, e, out)
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
		t.Fatalf("doctor with services: %d %s\n%s", code, e, out)
	}
	if code, out, _ := env.tc("doctor", "--json"); code != 0 || !strings.Contains(out, `"service": "orders-api"`) ||
		!strings.Contains(out, `"active_sessions": 1`) {
		t.Fatalf("doctor --json: %d\n%s", code, out)
	}

	// An unreachable collector fails the command but still reports the rest.
	env.col = "http://127.0.0.1:1"
	code, out, e = env.tc("doctor")
	if code != 1 || !strings.Contains(out, "UNREACHABLE") || !strings.Contains(out, "connected services  2") ||
		!strings.Contains(e, "unreachable") {
		t.Fatalf("doctor with the collector down: %d %s\n%s", code, e, out)
	}
}
