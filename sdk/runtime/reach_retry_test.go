package runtime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// idleExporter is an Exporter whose export goroutine does not run: the test
// calls maybeCheck itself, as the loop does on its ticker, and owns the
// fields that belong to the loop (checked).
func idleExporter(t *testing.T, url string, reachCheck time.Duration) *Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exporter{ctx: ctx, cancel: cancel, rec: NewRecorder(16),
		cfg:    ExporterConfig{CollectorURL: url, ReachCheck: reachCheck, HTTPTimeout: 10 * time.Second},
		client: &http.Client{Timeout: 10 * time.Second, Transport: newPlainTransport()}}
	t.Cleanup(func() { cancel(); e.checks.Wait() })
	return e
}

// A check that failed is made again after reachRetry at the latest, however
// long ReachCheck is: a collector that comes up after the service is found
// within seconds, not at the next regular check. One that succeeded waits
// for ReachCheck. (Nothing held the retry: without it every test passed, and
// a service started before its collector said "not reachable" for the whole
// ReachCheck.)
func TestAFailedCheckOfTheCollectorIsMadeAgainSoon(t *testing.T) {
	col := newHealthCollector(t, http.StatusBadGateway, "no upstream")
	e := idleExporter(t, col.srv.URL, time.Hour)
	// tick is one turn of the loop with the last check started ago: it
	// reports whether a check was made.
	tick := func(ago time.Duration) bool {
		before := col.checks.Load()
		if ago > 0 {
			e.checked = time.Now().Add(-ago)
		}
		e.maybeCheck()
		e.checks.Wait()
		return col.checks.Load() == before+1
	}
	if !tick(0) {
		panic("the first check was not made")
	}
	if known, ok, _ := reachOf(e); !known || ok {
		panic("a 502 did not count as not there")
	}
	// Not reached: not again at once, and again once reachRetry has passed.
	if tick(reachRetry / 2) {
		t.Errorf("a failed check was made again after %v: the retry is every %v", reachRetry/2, reachRetry)
	}
	if !tick(reachRetry + time.Second) {
		t.Errorf("a failed check was not made again after %v, with ReachCheck at an hour", reachRetry+time.Second)
	}
	// Reached: the next check is ReachCheck away.
	col.answer(http.StatusOK, "ok\n")
	if !tick(reachRetry + time.Second) {
		panic("the check that finds the collector was not made")
	}
	if known, ok, _ := reachOf(e); !known || !ok {
		panic("a 200 did not count as there")
	}
	if tick(reachRetry + time.Second) {
		t.Errorf("a check that succeeded was made again after %v: ReachCheck is an hour", reachRetry+time.Second)
	}
	if tick(59 * time.Minute) {
		t.Error("a check that succeeded was made again before ReachCheck had passed")
	}
	if !tick(61 * time.Minute) {
		t.Error("no check after ReachCheck had passed")
	}

	// The retry never makes the checks rarer: a ReachCheck shorter than
	// reachRetry stays the period while the collector is not reached.
	down := newHealthCollector(t, http.StatusBadGateway, "no upstream")
	e = idleExporter(t, down.srv.URL, 100*time.Millisecond)
	col = down
	if !tick(0) || tick(50*time.Millisecond) || !tick(time.Second) {
		t.Errorf("ReachCheck 100ms, collector not reached: %d checks; want one at once, none after 50ms, one after 1s", down.checks.Load())
	}
}

// A request with events that does not get through says that the collector is
// not reached, as a failed check does: while events wait no check is made,
// so nothing else would say it. A request that is answered says that it is.
func TestAFailedIngestSaysThatTheCollectorIsNotReached(t *testing.T) {
	addr := deadAddr()
	rec := NewRecorder(16)
	// Recorded before the exporter starts: an event waits from its first turn, so no check is ever made.
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase})
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://ci:hunter2@" + addr, FlushInterval: 2 * time.Millisecond, ReachCheck: time.Hour,
		RetryBackoff: time.Millisecond, MaxCooldown: 10 * time.Millisecond})
	defer exp.Close(100 * time.Millisecond)
	eventually(t, "the failed request to say that the collector is not reached", func() bool { known, _, _ := reachOf(exp); return known })
	known, ok, why := reachOf(exp)
	// "Post": it was the ingest that said so, not a check ("Get .../healthz").
	if !known || ok || !strings.Contains(why, "Post http://"+addr+"/v1/ingest") || strings.Contains(why, "hunter") || strings.Contains(why, "healthz") {
		panic(fmt.Sprintf("reachable=%v: %q", ok, why))
	}
	if rec.Len() != 1 {
		panic(fmt.Sprintf("%d events buffered: the event must wait", rec.Len()))
	}

	// The other way round: reached by a check, then the ingest fails to connect.
	col := newHealthCollector(t, http.StatusOK, "ok\n")
	rec2 := NewRecorder(16)
	exp2 := NewExporter(rec2, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: time.Hour,
		RetryBackoff: time.Millisecond, MaxCooldown: 10 * time.Millisecond})
	defer exp2.Close(100 * time.Millisecond)
	eventually(t, "the first check to find the collector", func() bool { known, ok, _ := reachOf(exp2); return known && ok })
	col.srv.Close() // gone: nothing listens there any more
	rec2.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase})
	eventually(t, "the failed request to take the word of the check back", func() bool { _, ok, _ := reachOf(exp2); return !ok })
	if _, _, why := reachOf(exp2); !strings.Contains(why, "/v1/ingest") {
		panic(fmt.Sprintf("why: %q", why))
	}
	if n := col.checks.Load(); n != 1 {
		panic(fmt.Sprintf("%d checks: the second word came from a check, not from the ingest", n))
	}
}
