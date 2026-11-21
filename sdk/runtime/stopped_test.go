package runtime

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
)

// slowRestarts makes the SDK's loops wait d for a restart, so that a test can
// look at what holds meanwhile. Call it after captureLog, which puts the
// backoff back.
func slowRestarts(d time.Duration) { loopBackoff, loopMaxBackoff = d, d }

// A subscriber that panicked no longer follows the control plane: the set
// it has would stand for as long as its restarts take, with sessions in it
// that have ended and a kill switch it does not see. So recording stops at
// the first panic, at once, and resumes when the loop, started again, has
// fetched the set anew.
func TestSubscriberStopsRecordingAtItsFirstPanicAndResumesWithAFreshSet(t *testing.T) {
	out := captureLog(t)
	slowRestarts(400 * time.Millisecond)
	cp := newReportCP(t, `{"active":["s1"],"delivery_reports":true}`)
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	defer sub.Close()
	eventually(t, "the subscriber to follow s1", func() bool { return sub.IsActive("s1") })

	restore, hits := faultpoint.PanicAt("subscriber.apply", 1)
	defer restore()
	cp.set(`{"active":["s1","s2"],"delivery_reports":true}`) // applying it panics
	eventually(t, "the subscriber to panic", func() bool { return hits() == 1 })
	began := time.Now()
	for deadline := began.Add(150 * time.Millisecond); sub.IsActive("s1") || sub.AnyActive(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the subscriber panicked and the set it had still stands: recording goes on for sessions it no longer follows")
		}
	}
	if note := sub.stopNote.Load(); note == nil || !strings.Contains(*note, "its subscriber panicked; it records again") {
		panic(fmt.Sprintf("what the control plane is to be told: %v", note))
	}
	time.Sleep(100 * time.Millisecond) // the restart is 400ms after the panic
	if sub.IsActive("s1") || sub.IsActive("s2") {
		panic("recording resumed before the subscriber was started again")
	}
	eventually(t, "recording to resume with the fresh set", func() bool { return sub.IsActive("s1") && sub.IsActive("s2") })
	if took := time.Since(began); took < 300*time.Millisecond {
		panic(fmt.Sprintf("recording resumed %v after the panic, before the restart", took))
	}
	if n := panicLines(out.String()); n != 1 || !strings.Contains(out.String(), "the subscriber goroutine panicked (string) and is restarted") {
		t.Errorf("%d panics logged:\n%s", n, out.String())
	}

	// The kill switch thrown while the subscriber is down is seen by the
	// fresh set: the stale one does not come back.
	restore2, hits2 := faultpoint.PanicAt("subscriber.apply", 1)
	defer restore2()
	cp.set(`{"active":["s1","s2"],"kill":true,"delivery_reports":true}`)
	eventually(t, "the subscriber to panic again", func() bool { return hits2() == 1 })
	eventually(t, "the fresh set", func() bool { return !sub.snap.Load().blind })
	if sub.IsActive("s1") || !sub.snap.Load().kill {
		panic("the kill switch is ignored after the restart")
	}
}

// A subscriber that panics every time stays stopped: nothing is active, for
// good, and the log says what no longer works.
func TestSubscriberThatAlwaysPanicsStaysStoppedAndRecordsNothing(t *testing.T) {
	out := captureLog(t)
	restore, _ := faultpoint.PanicAt("subscriber.apply", -1)
	defer restore()
	cp := newReportCP(t, `{"active":["s1"],"delivery_reports":true}`)
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	select {
	case <-sub.done:
	case <-time.After(20 * time.Second):
		panic("the subscriber never gave up:\n" + out.String())
	}
	if sub.IsActive("s1") || sub.AnyActive() || !sub.snap.Load().blind {
		panic("a subscriber that stays stopped still names sessions")
	}
	if note := sub.stopNote.Load(); note == nil || !strings.Contains(*note, "stays stopped; it records nothing more until the service is restarted") {
		panic(fmt.Sprintf("what the control plane is to be told: %v", note))
	}
	if logs := out.String(); panicLines(logs) != loopRestarts+1 || !strings.Contains(logs, "stays stopped: nothing more is recorded until the process is restarted") {
		t.Errorf("log:\n%s", logs)
	}
	if !returnsWithin(5*time.Second, sub.Close) {
		panic("Close waits for a subscriber that is gone")
	}
}

// stoppedNotes is what the control plane was told about this process having
// stopped recording.
func (cp *reportCP) stoppedNotes() []string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]string(nil), cp.stopped...)
}

// A service whose subscriber panicked has stopped recording, and nothing in
// its delivery counts shows it: what it would have recorded is not counted
// anywhere. The reporter, which goes on, tells the control plane, once per
// stop, also when it has nothing else to report. What was recorded before is
// still accounted for: no account is forgotten while the set is unknown.
func TestReporterTellsTheControlPlaneThatRecordingStopped(t *testing.T) {
	out := captureLog(t)
	slowRestarts(300 * time.Millisecond)
	cp := newReportCP(t, `{"active":["s1"],"delivery_reports":true}`)
	col := newTestCollector(t)
	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond, FirstDelay: time.Millisecond,
		Interval: 10 * time.Millisecond, KickEvery: 5 * time.Millisecond})
	defer func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }()
	eventually(t, "the subscriber to follow s1", func() bool { return sub.IsActive("s1") })
	for i := range 3 {
		rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	last := func() deliveryReport {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		if n := len(cp.got["s1"]); n > 0 {
			return cp.got["s1"][n-1]
		}
		return deliveryReport{}
	}
	eventually(t, "the report of the three events", func() bool { return last().Delivered == 3 })
	if len(cp.stoppedNotes()) != 0 {
		panic(fmt.Sprintf("told of a stop that did not happen: %v", cp.stoppedNotes()))
	}

	restore, hits := faultpoint.PanicAt("subscriber.apply", 1)
	defer restore()
	cp.set(`{"active":["s1","s2"],"delivery_reports":true}`)
	eventually(t, "the subscriber to panic", func() bool { return hits() == 1 })
	eventually(t, "the control plane to be told", func() bool { return len(cp.stoppedNotes()) > 0 })
	if notes := cp.stoppedNotes(); len(notes) != 1 || !strings.Contains(notes[0], "its subscriber panicked; it records again once it has fetched the active sessions anew") {
		panic(fmt.Sprintf("the control plane was told: %q", notes))
	}
	if a := rec.accounts()["s1"]; a.recorded != 3 || a.delivered != 3 {
		panic(fmt.Sprintf("the account of s1 while the set is unknown: %+v", a))
	}
	// Recording resumes, the reports go on, and the stop is not told again.
	eventually(t, "recording to resume", func() bool { return sub.IsActive("s1") })
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase + 10})
	eventually(t, "the report of the fourth event", func() bool { return last().Delivered == 4 })
	if notes := cp.stoppedNotes(); len(notes) != 1 || sub.stopNote.Load() != nil {
		panic(fmt.Sprintf("the stop was told %d times", len(notes)))
	}

	// A subscriber that gives up: the control plane is told that too, and the
	// reporter still accounts for what was recorded.
	slowRestarts(time.Millisecond)
	restore2, _ := faultpoint.PanicAt("subscriber.apply", -1)
	defer restore2()
	cp.set(`{"active":["s1"],"delivery_reports":true}`)
	select {
	case <-sub.done:
	case <-time.After(20 * time.Second):
		panic("the subscriber never gave up:\n" + out.String())
	}
	eventually(t, "the control plane to be told that recording stays stopped", func() bool {
		notes := cp.stoppedNotes()
		return strings.Contains(notes[len(notes)-1], "stays stopped; it records nothing more until the service is restarted")
	})
	if a := rec.accounts()["s1"]; a.recorded != 4 {
		panic(fmt.Sprintf("the account of s1 after the subscriber gave up: %+v", a))
	}
}

// The reporter runs inside the host application like the exporter: a panic
// in a pass is logged, the loop is started again, and the reports go on.
func TestReporterSurvivesAPanicAndGoesOnReporting(t *testing.T) {
	out := captureLog(t)
	cp := newReportCP(t, `{"active":["s1"],"delivery_reports":true}`)
	col := newTestCollector(t)
	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	restore, hits := faultpoint.PanicAt("reporter.pass", 2)
	defer restore()
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond, FirstDelay: time.Millisecond, Interval: 10 * time.Millisecond})
	defer func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }()
	eventually(t, "the subscriber to follow s1", func() bool { return sub.IsActive("s1") })
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase})
	eventually(t, "a report after the panics", func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		return hits() == 2 && len(cp.got["s1"]) > 0
	})
	if logs := out.String(); panicLines(logs) != 2 || !strings.Contains(logs, "the delivery reporter goroutine panicked (string) and is restarted") ||
		strings.Contains(logs, "stays stopped") {
		t.Errorf("log:\n%s", logs)
	}
	if len(cp.stoppedNotes()) != 0 {
		panic("a reporter that panicked made the control plane mark sessions: recording never stopped")
	}
}
