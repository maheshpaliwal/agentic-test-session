package controlplane

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// An SDK whose subscriber panicked stops recording and says so. Nothing else
// would show it: the sessions that ran sealed complete, with nothing
// unconfirmed, and without the events the service would have recorded: a
// green gate over a service that had stopped recording. Now every session
// that runs when the control plane is told is incomplete, with a reason that
// names the service and the instance. A session started later is not: the
// instance took no part in it.
func TestAnInstanceThatStoppedRecordingMakesTheRunningSessionsIncomplete(t *testing.T) {
	var logs logBuffer
	col := newFlaky()
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: logs.logger()})
	defer m.Close()
	start := func(name string) Session {
		s, err := m.Start(StartRequest{Name: name, QuiesceSeconds: 1})
		if err != nil {
			panic(err)
		}
		return s
	}
	stop := func(s Session) Session {
		if _, err := m.Stop(s.ID); err != nil {
			panic(err)
		}
		return waitState(t, m, s.ID, StateSealed)
	}
	done := stop(start("ended before"))
	running, draining := start("running"), start("draining")
	col.busy.Store(true) // the drain of "draining" waits
	if _, err := m.Stop(draining.ID); err != nil {
		panic(err)
	}

	// As the SDK sends it: a batch without reports, over HTTP.
	body := `{"service":"orders-api","instance":"orders-1-abc","reports":[],
		"stopped":"its subscriber panicked repeatedly and stays stopped; it records nothing more \u001b[31m http://u:hunter2@h/ ` + strings.Repeat("x ", 400) + `"}`
	if w := do(t, m.Handler(), "POST", "/v1/delivery", "127.0.0.1:7070", body, map[string]string{"Content-Type": "application/json"}); w.Code != http.StatusOK {
		panic(fmt.Sprintf("the batch: %d %s", w.Code, w.Body.String()))
	}
	later := start("started later")
	col.busy.Store(false)

	for _, s := range []Session{stop(running), waitState(t, m, draining.ID, StateSealed)} {
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || len(s.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("%s: %+v", s.Name, s))
		}
		r := s.IncompleteReasons[0]
		if !strings.HasPrefix(r, "service orders-api (instance orders-1-abc) stopped recording after an internal error (its subscriber panicked repeatedly and stays stopped") ||
			!strings.Contains(r, "events of this session it would have recorded since are missing") {
			panic(s.Name + ": " + r)
		}
		if strings.Contains(r, "hunter2") || strings.Contains(r, "\x1b") || len(r) > 700 {
			panic(fmt.Sprintf("%s: the instance's words are stored as they came (%d bytes): %q", s.Name, len(r), r))
		}
	}
	for _, s := range []Session{stop(later), done} {
		if got, _ := m.Get(s.ID); got.Incomplete || len(got.IncompleteReasons) != 0 || len(got.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("%s did not run when the instance stopped: %+v", s.Name, got))
		}
	}
	if out := logs.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "a service stopped recording after an internal error") ||
		!strings.Contains(out, "service=orders-api") || strings.Contains(out, "hunter2") {
		t.Errorf("log:\n%s", out)
	}
}

// With the delivery check turned off nothing makes a session incomplete; the
// stop is still said, as a warning.
func TestAnInstanceThatStoppedRecordingIsAWarningWithTheCheckOff(t *testing.T) {
	col := newFlaky()
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: -1,
		Logger: (&logBuffer{}).logger()})
	defer m.Close()
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1})
	if err != nil {
		panic(err)
	}
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "svc", Instance: "i", Stopped: "its subscriber panicked"}); err != nil {
		panic(err)
	}
	// Told twice, it is said once.
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "svc", Instance: "i", Stopped: "its subscriber panicked"}); err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	got := waitState(t, m, s.ID, StateSealed)
	if got.Incomplete || len(got.IncompleteReasons) != 0 || len(got.DeliveryUnconfirmed) != 1 ||
		!strings.Contains(got.DeliveryUnconfirmed[0], "service svc (instance i) stopped recording after an internal error") {
		panic(fmt.Sprintf("%+v", got))
	}
}
