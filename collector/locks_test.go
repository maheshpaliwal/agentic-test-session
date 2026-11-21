package collector

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
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

// The janitor holds the sequencer's lock while it looks for idle actors, and
// every ingest takes that lock. A panic under it used to end the collector;
// now that the janitor is started again it must leave the lock released (or
// the collector stops taking events while /healthz says ok), and the janitor
// must go on evicting.
func TestJanitorPanicUnderTheLockLeavesItReleasedAndTheJanitorIsRestarted(t *testing.T) {
	var logs syncBuf
	c := New(store.NewMem(), Config{IdleEvict: 5 * time.Millisecond, Window: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	ctx := context.Background()
	if _, err := c.Ingest(ctx, "", []core.Event{ev("a", core.KindAPIIn, 1, 1)}); err != nil {
		panic(err)
	}
	// An actor that is not there makes the janitor panic in the middle of its
	// sweep, with the lock held.
	c.seq.mu.Lock()
	c.seq.actors["poison"] = nil
	c.seq.mu.Unlock()
	panics := func() uint64 { return c.tel.panics.With("janitor").Value() }
	for deadline := time.Now().Add(10 * time.Second); panics() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the janitor did not reach the poisoned actor")
		}
	}
	if !returnsWithin(5*time.Second, func() {
		c.seq.mu.Lock()
		delete(c.seq.actors, "poison")
		c.seq.mu.Unlock()
	}) {
		panic("the sequencer's lock is still held after the janitor panicked under it")
	}
	var ierr error
	e := ev("b", core.KindAPIIn, 2, 2)
	e.SessionID = "ts_test2"
	if !returnsWithin(5*time.Second, func() { _, ierr = c.Ingest(ctx, "", []core.Event{e}) }) || ierr != nil {
		panic(fmt.Sprintf("ingest after the panic: %v", ierr))
	}
	var metrics bytes.Buffer
	if !returnsWithin(5*time.Second, func() { _, _ = c.tel.reg.WriteTo(&metrics) }) ||
		!strings.Contains(metrics.String(), `tsession_collector_goroutine_panics_total{goroutine="janitor"}`) {
		panic("metrics after the panic:\n" + metrics.String())
	}
	// Started again, the janitor evicts both sessions once they are idle.
	for deadline := time.Now().Add(20 * time.Second); c.seq.actorCount() > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("the janitor was not started again: %d actors left after %d panics\n%s", c.seq.actorCount(), panics(), logs.String()))
		}
	}
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("/healthz after the janitor ran again: %d %s", rec.Code, rec.Body.String())
	}
	if !returnsWithin(10*time.Second, c.Close) {
		panic("Close waits for a lock the janitor left held")
	}
	if out := logs.String(); !strings.Contains(out, "goroutine=janitor") || !strings.Contains(out, "level=ERROR") {
		t.Errorf("log:\n%s", out)
	}
}

// drop is what a delete (the API's, and the retention loop's) calls: a panic
// in it leaves the lock released too.
func TestDropPanicUnderTheLockLeavesItReleased(t *testing.T) {
	c := New(store.NewMem(), Config{IdleEvict: -1, Logger: slog.New(slog.NewTextHandler(&syncBuf{}, nil))})
	defer c.Close()
	if _, err := c.Ingest(context.Background(), "", []core.Event{ev("a", core.KindAPIIn, 1, 1)}); err != nil {
		panic(err)
	}
	c.seq.mu.Lock()
	a := c.seq.actors[sid]
	c.seq.mu.Unlock()
	if !c.seq.with(sid, false, false, func(a *sessionActor) { _ = a.flushNow() }) {
		panic("no actor")
	}
	a.stop() // stopped behind the sequencer's back: drop stops it again, which panics
	<-a.done
	panicked := true
	func() {
		defer func() { _ = recover() }()
		c.seq.drop(sid)
		panicked = false
	}()
	if !panicked {
		panic("drop did not panic: the test proves nothing")
	}
	if !returnsWithin(5*time.Second, func() { c.seq.actorCount() }) {
		panic("the sequencer's lock is still held after drop panicked under it")
	}
}
