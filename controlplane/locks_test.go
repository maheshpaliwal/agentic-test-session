package controlplane

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
)

// returnsWithin reports whether f returns within d: a call that waits for a
// lock nobody will release does not.
func returnsWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// truncCollector answers a list of instances that was cut short, so that a
// drain asks about the others by name (countsFor), under the manager's lock.
type truncCollector struct{ *flakyCollector }

func (c *truncCollector) DrainStatus(context.Context, string) (DrainStatus, error) {
	return DrainStatus{Instances: []InstanceCount{{Service: "a", Instance: "i", Events: 1}}, InstancesTruncated: true}, nil
}

func (c *truncCollector) InstanceCounts(_ context.Context, _ string, q []InstanceCount) ([]InstanceCount, error) {
	return q, nil
}

// A drain takes the manager's lock to list the instances it asks the
// collector about. A panic under it used to end the control plane; now that
// the drain is started again it must leave the lock released, or everything
// stops (the active set, starts, the sweep, /metrics, Close) while /healthz
// says ok.
func TestDrainPanicUnderTheManagerLockLeavesItReleased(t *testing.T) {
	restore, hits := faultpoint.PanicAt("countsFor.locked", 1)
	defer restore()
	col := &truncCollector{newFlaky()}
	col.failing.Store(false)
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: (&logBuffer{}).logger()})
	s, err := m.Start(StartRequest{Name: "p", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	for deadline := time.Now().Add(10 * time.Second); hits() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the drain never asked for the instances")
		}
	}
	for what, call := range map[string]func(){
		"Active":      func() { m.Active() },
		"Start":       func() { _, _ = m.Start(StartRequest{Name: "q"}) },
		"Sweep":       func() { m.Sweep(time.Now()) },
		"Subscribers": func() { m.Subscribers() },
		"/metrics": func() {
			req := httptest.NewRequest("GET", "/metrics", nil)
			req.Host = "127.0.0.1:7070"
			m.Handler().ServeHTTP(httptest.NewRecorder(), req)
		},
	} {
		if !returnsWithin(5*time.Second, call) {
			panic(what + " waits for the lock the drain held when it panicked")
		}
	}
	// The drain was started again and seals the session.
	if got := waitState(t, m, s.ID, StateSealed); got.SealError != "" {
		panic(fmt.Sprintf("after the restart: %+v", got))
	}
	if !panicMetric(t, m, "drain", 1) {
		t.Errorf("metrics:\n%s", scrapeMetrics(t, m))
	}
	if !returnsWithin(5*time.Second, m.Close) {
		panic("Close waits for the lock")
	}
}
