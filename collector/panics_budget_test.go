package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// healthz asks GET /healthz, from a Host the guard would refuse.
func healthz(c *Collector) (int, string) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Host = "collector:7071"
	c.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// A janitor that panics every time uses up the panic budget: it is not
// started again, the owner is told once (the command then shuts down and
// exits 1), and /healthz says why. While it waited for a restart /healthz
// said that too. Ingest goes on meanwhile.
func TestAJanitorThatKeepsPanickingSpendsThePanicBudget(t *testing.T) {
	var logs syncBuf
	spent := make(chan string, 4)
	c := New(store.NewMem(), Config{IdleEvict: 4 * time.Millisecond, Window: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		OnPanicBudgetSpent: func(reason string) { spent <- reason }})
	if code, body := healthz(c); code != 200 || body != "ok" {
		panic(fmt.Sprintf("/healthz before anything happened: %d %q", code, body))
	}
	c.seq.mu.Lock()
	c.seq.actors["poison"] = nil // the janitor panics on it, at every sweep
	c.seq.mu.Unlock()
	degraded, reason := "", ""
	for deadline := time.Now().Add(30 * time.Second); reason == ""; time.Sleep(time.Millisecond) {
		if code, body := healthz(c); code == http.StatusServiceUnavailable && degraded == "" {
			degraded = body
		}
		select {
		case reason = <-spent:
		default:
			if time.Now().After(deadline) {
				panic("the budget was never spent:\n" + logs.String())
			}
		}
	}
	if degraded != "degraded: a background goroutine panicked and waits to be restarted: janitor; the log has the panic\n" {
		t.Errorf("/healthz while the janitor waited for its restart: %q", degraded)
	}
	if !strings.Contains(reason, "the panic budget is spent: 5 panics of background goroutines within 10m0s") || c.Health() != reason {
		panic(fmt.Sprintf("reason %q, health %q", reason, c.Health()))
	}
	if code, body := healthz(c); code != http.StatusServiceUnavailable || body != "degraded: "+reason+"\n" {
		panic(fmt.Sprintf("/healthz: %d %q", code, body))
	}
	time.Sleep(50 * time.Millisecond)
	if n := c.tel.panics.With("janitor").Value(); n != 5 || len(spent) != 0 {
		panic(fmt.Sprintf("%d panics, told %d more times", n, len(spent)))
	}
	if res, err := c.Ingest(context.Background(), "", []core.Event{ev("a", core.KindAPIIn, 1, 1)}); err != nil || res.Accepted != 1 {
		panic(fmt.Sprintf("ingest with the janitor gone: %+v %v", res, err))
	}
	c.seq.mu.Lock()
	delete(c.seq.actors, "poison")
	c.seq.mu.Unlock()
	if !returnsWithin(10*time.Second, c.Close) {
		panic("Close waits for a janitor that is gone")
	}
	if out := logs.String(); !strings.Contains(out, "is not restarted: the panic budget of the service is spent") {
		t.Errorf("log:\n%s", out)
	}
}

// Without an owner to tell, the collector says in its log that it stays
// degraded.
func TestACollectorWithoutAnOwnerStaysDegraded(t *testing.T) {
	var logs syncBuf
	spy := &sweepSpy{Store: store.NewMem()}
	spy.override = func(time.Time, string, int) ([]store.SessionInfo, error) { panic("always") }
	c := New(spy, Config{Retention: time.Hour, RetentionInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	defer c.Close()
	for deadline := time.Now().Add(30 * time.Second); !strings.Contains(logs.String(), "stays degraded"); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(logs.String())
		}
	}
	if code, body := healthz(c); code != http.StatusServiceUnavailable || !strings.Contains(body, "the panic budget is spent") {
		panic(fmt.Sprintf("/healthz: %d %q", code, body))
	}
	if n := spy.pages.Load(); n != 5 {
		panic(fmt.Sprintf("%d sweeps, want the 5 of the budget", n))
	}
}

// The goroutine that ends a retention sweep when the collector closes ends
// with the sweep, also when the sweep panics: a store that panics now and
// then must not leave one more goroutine behind each time.
func TestARetentionSweepThatPanicsLeavesNoWatcherBehind(t *testing.T) {
	spy := &sweepSpy{Store: store.NewMem()}
	spy.override = func(time.Time, string, int) ([]store.SessionInfo, error) { panic("always") }
	c := New(spy, Config{Retention: time.Hour, RetentionInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&syncBuf{}, nil))})
	defer c.Close()
	for deadline := time.Now().Add(10 * time.Second); c.tel.panics.With("retention").Value() < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the loop was not restarted")
		}
	}
	// The loop now waits 200ms for its restart: no sweep runs, so no watcher
	// of one may be left.
	watchers := func() int {
		buf := make([]byte, 1<<20)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "retentionSweep.func")
	}
	for deadline := time.Now().Add(150 * time.Millisecond); watchers() > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("%d watcher(s) of sweeps that panicked are still waiting", watchers()))
		}
	}
	if n := c.tel.panics.With("retention_stop").Value(); n != 0 {
		panic(fmt.Sprintf("%d panics of the watcher", n))
	}
}
