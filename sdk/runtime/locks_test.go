package runtime

import (
	"fmt"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
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

// recovered runs f as a wrapper runs recording code: a panic is swallowed.
func recovered(f func()) {
	defer core.Recover()
	f()
}

// The recorder's lock is taken by Record, on the host's request path. A panic
// under it used to end the process; recovered (the reporter is started again,
// a wrapper swallows it) it must leave the lock released, or every request of
// the host that records an event waits for good.
func TestAPanicUnderTheRecorderLockLeavesItReleased(t *testing.T) {
	record := func(rec *Recorder, sid, after string) {
		t.Helper()
		if !returnsWithin(5*time.Second, func() { rec.Record(core.Event{SessionID: sid, LocalSeq: 1}) }) {
			panic("Record waits for the recorder's lock after " + after)
		}
	}

	// forget, as the reporter calls it.
	rec := NewRecorder(8)
	rec.Record(core.Event{SessionID: "s", LocalSeq: 1})
	restore, hits := faultpoint.PanicAt("recorder.forget", 1)
	defer restore()
	recovered(func() { rec.forget("s") })
	if hits() != 1 {
		panic("forget did not reach the fault point")
	}
	record(rec, "s", "a panic in forget")

	// Record itself, and what the exporter calls: a recorder whose maps are
	// gone panics in the middle of each.
	for name, call := range map[string]func(r *Recorder){
		"Record":       func(r *Recorder) { r.acct = nil; r.Record(core.Event{SessionID: "new"}) },
		"AddDropCount": func(r *Recorder) { r.drops = nil; r.AddDropCount("s", 1) },
		"AddDrops":     func(r *Recorder) { r.drops = nil; r.AddDrops([]core.Event{{SessionID: "s"}}) },
		"take":         func(r *Recorder) { r.take(0, 8, func(string) bool { panic("want") }) },
		"takeDrops":    func(r *Recorder) { r.AddDropCount("s", 1); r.takeDrops(func(string) bool { panic("want") }) },
	} {
		rec := NewRecorder(8)
		rec.Record(core.Event{SessionID: "s", LocalSeq: 1})
		panicked := true
		recovered(func() { call(rec); panicked = false })
		if !panicked {
			panic(name + " did not panic: the test proves nothing")
		}
		rec.mu.Lock() // the test's own repair, which also needs the lock
		rec.acct, rec.drops = map[string]*account{}, map[string]uint64{}
		rec.last, rec.lastSID = nil, ""
		rec.mu.Unlock()
		record(rec, "s", "a panic in "+name)
	}
}

// The same from end to end: the reporter panics inside forget, is started
// again, and a goroutine of the host records on.
func TestTheHostRecordsOnAfterTheReporterPanickedUnderTheRecorderLock(t *testing.T) {
	out := captureLog(t)
	cp := newReportCP(t, `{"active":[],"delivery_reports":true}`)
	rec := NewRecorder(64)
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://127.0.0.1:1", FlushInterval: time.Hour})
	restore, hits := faultpoint.PanicAt("recorder.forget", 1)
	defer restore()
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond})
	rec.Record(core.Event{SessionID: "gone", LocalSeq: 1}) // not in the active set: the reporter forgets it
	for deadline := time.Now().Add(10 * time.Second); hits() == 0 || panicLines(out.String()) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the reporter never reached forget")
		}
	}
	if !returnsWithin(5*time.Second, func() { rec.Record(core.Event{SessionID: "s", LocalSeq: 2}) }) {
		panic("the host's Record waits for the lock the reporter held when it panicked")
	}
	// The reporter came back and forgot the session on a later pass.
	for deadline := time.Now().Add(10 * time.Second); len(rec.accounts()) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("the reporter was not started again: accounts %+v\n%s", rec.accounts(), out.String()))
		}
	}
	if !returnsWithin(10*time.Second, func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }) {
		panic("the SDK does not stop")
	}
}
