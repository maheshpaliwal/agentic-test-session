package runtime

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// panicEnc is an encoder that panics, with something of the host's in the
// panic value, for its first n calls (n < 0: always).
type panicEnc struct {
	n     int64
	calls atomic.Int64
}

func (*panicEnc) ContentType() string { return "application/x-test" }
func (p *panicEnc) Encode(b []core.Event) ([]byte, error) {
	if c := p.calls.Add(1); p.n < 0 || c <= p.n {
		panic(fmt.Sprintf("cannot encode order of customer hunter2-secret (%d events)", len(b)))
	}
	return []byte{byte(len(b))}, nil
}

// captureLog collects the process log for the test and makes the SDK's
// restart backoff short.
func captureLog(t *testing.T) *lockedBuf {
	t.Helper()
	var out lockedBuf
	prev, flags := log.Writer(), log.Flags()
	log.SetOutput(&out)
	log.SetFlags(0)
	base, limit := loopBackoff, loopMaxBackoff
	loopBackoff, loopMaxBackoff = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() {
		log.SetOutput(prev)
		log.SetFlags(flags)
		loopBackoff, loopMaxBackoff = base, limit
	})
	return &out
}

// panicLines counts the log lines that report a panic.
func panicLines(out string) int { return strings.Count(out, "goroutine panicked") }

// goroutinesSettle waits until no more goroutines run than before.
func goroutinesSettle(before int) {
	for deadline := time.Now().Add(5 * time.Second); goruntime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			panic(fmt.Sprintf("%d goroutines, %d before:\n%s", goruntime.NumGoroutine(), before, buf[:goruntime.Stack(buf, true)]))
		}
	}
}

// The export goroutine runs inside the host application. A panic in it (here
// in the host's own encoder) used to end the host; now it is logged, the loop
// is started again, and the events that pass held are sent by the next.
func TestExporterSurvivesAPanicAndLosesNothing(t *testing.T) {
	out := captureLog(t)
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Add(1)
		w.Write([]byte(`{"accepted":3}`))
	}))
	defer srv.Close()
	before := goruntime.NumGoroutine()
	rec := NewRecorder(16)
	enc := &panicEnc{n: 2}
	exp := NewExporter(rec, ExporterConfig{CollectorURL: srv.URL, FlushInterval: time.Millisecond, Encoder: enc})
	for i := range 3 {
		rec.Record(core.Event{SessionID: "s", LocalSeq: uint64(i + 1)})
	}
	for deadline := time.Now().Add(10 * time.Second); rec.Len() > 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("the events were never delivered after the panics; log:\n%s", out.String()))
		}
	}
	if !exp.Flush(2 * time.Second) {
		panic("flush after the restarts")
	}
	exp.Close(time.Second)
	srv.CloseClientConnections()
	goroutinesSettle(before)
	logs := out.String()
	if n := panicLines(logs); n != 2 || got.Load() == 0 {
		panic(fmt.Sprintf("%d panics logged, want 2; %d requests reached the collector\n%s", n, got.Load(), logs))
	}
	if !strings.Contains(logs, "tsession: error: the exporter goroutine panicked (string) and is restarted in 1ms (restart 1 of 5)") ||
		!strings.Contains(logs, "the application is not affected") || !strings.Contains(logs, "(*panicEnc).Encode (") {
		t.Errorf("log:\n%s", logs)
	}
	if strings.Contains(logs, "hunter2") || strings.Contains(logs, "0x") || strings.Contains(logs, "stays stopped") {
		t.Errorf("the log carries the panic value or arguments, or gives up early:\n%s", logs)
	}
}

// A loop that panics every time is restarted loopRestarts times, with a
// growing backoff, and then stays stopped: the log has one line per panic and
// no more, ever; Flush and Close return; nothing leaks.
func TestExporterThatAlwaysPanicsStaysStoppedAfterItsRestarts(t *testing.T) {
	out := captureLog(t)
	before := goroutinesBefore()
	rec := NewRecorder(16)
	enc := &panicEnc{n: -1}
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://127.0.0.1:1", FlushInterval: time.Millisecond, Encoder: enc})
	rec.Record(core.Event{SessionID: "s", LocalSeq: 1})
	select {
	case <-exp.done:
	case <-time.After(10 * time.Second):
		panic(fmt.Sprintf("the exporter never gave up; log:\n%s", out.String()))
	}
	rec.Record(core.Event{SessionID: "s", LocalSeq: 2}) // recording goes on, into the ring
	if exp.Flush(time.Second) {
		panic("Flush reports success with the exporter stopped")
	}
	began := time.Now()
	exp.Close(time.Second)
	if took := time.Since(began); took > 500*time.Millisecond {
		t.Errorf("Close took %v with the exporter stopped", took)
	}
	goroutinesSettle(before)
	logs := out.String()
	if n, calls := panicLines(logs), enc.calls.Load(); n != loopRestarts+1 || calls != loopRestarts+1 {
		panic(fmt.Sprintf("%d panics logged for %d runs, want %d and %d\n%s", n, calls, loopRestarts+1, loopRestarts+1, logs))
	}
	for _, want := range []string{"restarted in 1ms (restart 1 of 5)", "restarted in 2ms (restart 2 of 5)", "restarted in 4ms (restart 3 of 5)",
		"restarted in 4ms (restart 5 of 5)", "panicked 6 times (string) and stays stopped: recorded events are no longer sent to the collector"} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, "hunter2") {
		t.Errorf("the log carries the panic value:\n%s", logs)
	}
}

func goroutinesBefore() int { return goruntime.NumGoroutine() }

// A runtime panic's text names no data and is logged; when a loop stays
// stopped its owner is told, once (the subscriber then says that nothing is
// active, so recording ends instead of going on for sessions long over).
func TestStartLoopTellsTheOwnerWhenItStaysStopped(t *testing.T) {
	out := captureLog(t)
	before := goroutinesBefore()
	var stopped, dones atomic.Int32
	done := make(chan struct{})
	var none []int
	startLoop("subscriber", "nothing more is recorded", nil, func() { _ = none[len(out.String())+1] },
		func() { dones.Add(1); close(done) }, func() { stopped.Add(1) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("the loop never gave up")
	}
	goroutinesSettle(before)
	logs := out.String()
	if stopped.Load() != 1 || dones.Load() != 1 || panicLines(logs) != loopRestarts+1 ||
		!strings.Contains(logs, "index out of range") || !strings.Contains(logs, "stays stopped: nothing more is recorded until the process is restarted") {
		panic(fmt.Sprintf("stopped %d, done %d\n%s", stopped.Load(), dones.Load(), logs))
	}

	// Stopped by its owner while it waits for a restart, it ends at once and
	// is not told that it stays stopped.
	stop, done2 := make(chan struct{}), make(chan struct{})
	loopBackoff, loopMaxBackoff = time.Hour, time.Hour
	startLoop("exporter", "x", stop, func() { panic("once") }, func() { close(done2) }, func() { stopped.Add(1) })
	for deadline := time.Now().Add(10 * time.Second); panicLines(out.String()) != loopRestarts+2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("the panic was not logged")
		}
	}
	close(stop)
	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		panic("Close would hang on a loop that waits for its restart")
	}
	if stopped.Load() != 1 {
		panic("a loop its owner stopped was reported as stopped for good")
	}
}
