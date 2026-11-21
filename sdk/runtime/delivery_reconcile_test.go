package runtime

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// F8: an account that is forgotten while the ring still holds events of its
// session, and an event recorded for the session afterwards. The new event
// must not start an account at zero that the older ones are then delivered
// against: that reported recorded:1 delivered:6 and a buffered count that had
// wrapped around.
func TestAccountForgottenWithEventsInTheRing(t *testing.T) {
	r := NewRecorder(16)
	for i := range 5 {
		r.Record(core.Event{SessionID: "s", LocalSeq: uint64(i + 1)})
	}
	r.forget("s") // the session left the active set; its events are still in the ring
	if got := r.accounts(); got != nil {
		t.Fatalf("a forgotten account is listed: %+v", got)
	}
	r.Record(core.Event{SessionID: "s", LocalSeq: 6}) // a span that ended after the session did
	if a := r.accounts()["s"]; a != (account{recorded: 6}) || a.buffered() != 6 {
		t.Fatalf("after the late event: %+v", a)
	}
	r.Commit(6)
	a := r.accounts()["s"]
	if a != (account{recorded: 6, delivered: 6}) || a.buffered() != 0 || len(accountErrors(r)) > 0 {
		t.Fatalf("after delivery: %+v buffered=%d %v", a, a.buffered(), accountErrors(r))
	}
	if r.deliveredTotal() != 6 {
		t.Fatalf("total %d", r.deliveredTotal())
	}
	// Forgotten again with nothing in the ring: the account goes at once, and
	// one that was put aside goes when its events have left.
	r.forget("s")
	r.Record(core.Event{SessionID: "t", LocalSeq: 7})
	r.forget("t")
	r.Commit(1)
	if got := r.accounts(); got != nil || len(r.acct) != 0 {
		t.Fatalf("accounts left: %+v (%d kept)", got, len(r.acct))
	}
	// A count can never wrap, whatever happens to an account.
	if b := (account{recorded: 1, delivered: 6}).buffered(); b != 0 {
		t.Fatalf("buffered %d", b)
	}
}

// The total counts what was delivered for all sessions, also for those whose
// account is gone, and never an event the collector rejected.
func TestDeliveredTotal(t *testing.T) {
	col := newTestCollector(t)
	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: time.Hour})
	defer exp.Close(time.Second)
	for i := range 4 {
		rec.Record(core.Event{SessionID: []string{"a", "b"}[i%2], Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	rec.forget("b")
	if !exp.Flush(5*time.Second) || rec.deliveredTotal() != 4 {
		t.Fatalf("total %d after 4 events were delivered", rec.deliveredTotal())
	}
	col.answer = func(n int) string { return fmt.Sprintf(`{"accepted":%d,"rejected":1}`, n-1) }
	for i := range 3 {
		rec.Record(core.Event{SessionID: "a", Kind: core.KindAPIIn, LocalSeq: seqBase + 10 + uint64(i)})
	}
	exp.Flush(5 * time.Second)
	if got := rec.deliveredTotal(); got != 6 {
		t.Fatalf("total %d, want 6: a rejected event is not delivered", got)
	}
}

// The events of a draining session are sent before those of the others: the
// control plane waits for them, and they must not queue behind the backlog
// of sessions that are still running. Every session's own events keep their
// order.
func TestExporterSendsDrainingSessionsFirst(t *testing.T) {
	var mu sync.Mutex
	var requests [][]core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		evs, _ := decodeFrames(body)
		mu.Lock()
		requests = append(requests, evs)
		mu.Unlock()
		fmt.Fprintf(w, `{"accepted":%d}`, len(evs))
	}))
	defer srv.Close()
	rec := NewRecorder(256)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: srv.URL, FlushInterval: time.Hour, BatchSize: 4})
	defer exp.Close(time.Second)
	seq := uint64(0)
	record := func(sid string, n int) {
		for range n {
			seq++
			rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, LocalSeq: seqBase + seq})
		}
	}
	record("running", 10)
	record("draining", 3)
	record("running", 2)
	record("draining", 3)
	exp.setUrgent(map[string]struct{}{"draining": {}})
	if !exp.Flush(5 * time.Second) {
		t.Fatal("flush")
	}
	mu.Lock()
	defer mu.Unlock()
	var order []string
	last := map[string]uint64{}
	for _, req := range requests {
		for _, e := range req {
			order = append(order, e.SessionID)
			if e.LocalSeq <= last[e.SessionID] {
				t.Fatalf("session %s: event %d sent after %d", e.SessionID, e.LocalSeq, last[e.SessionID])
			}
			last[e.SessionID] = e.LocalSeq
		}
	}
	if len(order) != 18 || strings.Join(order[:6], ",") != strings.Repeat("draining,", 5)+"draining" {
		t.Fatalf("sent in the order %v: want the 6 events of the draining session first", order)
	}
	if len(accountErrors(rec)) > 0 || rec.Len() != 0 {
		t.Fatalf("ring %d, %v", rec.Len(), accountErrors(rec))
	}
}

// The text of a failed request names no credential, wherever a URL stands
// in it.
func TestScrubErrorNestedURL(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://user:p@ss@collector.internal/v1/ingest?token=tok_abc",
		Err: errors.New(`proxyconnect tcp: proxy https://proxyuser:pr0xypw@proxy.internal:3128/?k=tok_def refused`)}
	got := scrubError(err)
	if got != "Post https://collector.internal/v1/ingest: proxyconnect tcp: proxy https://proxy.internal:3128/ refused" {
		t.Fatalf("scrubbed: %q", got)
	}
}

// logTo captures the process log for a test.
func logTo(t *testing.T) *lockedBuf {
	t.Helper()
	var out lockedBuf
	prev := log.Writer()
	log.SetOutput(&out)
	t.Cleanup(func() { log.SetOutput(prev) })
	return &out
}

// F4: something between the service and the control plane refuses the report
// (a proxy answering 403). That is a failure: it is logged once, without the
// URL's credentials, and the report is retried with a growing pause until it
// arrives. It used to count as sent, without a word.
func TestReporterRefusedReportsAreLoggedAndRetried(t *testing.T) {
	out := logTo(t)
	e := newReportEnv(t, `{"active":["s1"]}`, nil)
	e.sub.base = strings.Replace(e.sub.base, "http://", "http://cpuser:cpsecret@", 1)
	e.cp.setStatus(http.StatusForbidden)
	e.record("s1", 2)
	eventually(t, "several attempts", func() bool { return e.cp.postCount() >= 4 })
	if n := e.cp.postCount(); n > 8 {
		t.Fatalf("%d attempts already: the pause between them does not grow", n)
	}
	logged := out.String()
	if n := strings.Count(logged, "cannot send delivery reports"); n != 1 {
		t.Fatalf("%d log lines for one outage:\n%s", n, logged)
	}
	if !strings.Contains(logged, "HTTP 403") || strings.Contains(logged, "cpsecret") || strings.Contains(logged, "cpuser") || strings.Contains(logged, "s1") {
		t.Fatalf("log line: %s", logged)
	}
	if got := e.cp.reports("s1"); len(got) != 0 {
		t.Fatalf("reports %+v", got)
	}
	// Recording and export are not affected.
	if a := e.rec.accounts()["s1"]; a != (account{recorded: 2, delivered: 2}) {
		t.Fatalf("account %+v", a)
	}
	e.cp.setStatus(0)
	e.settled("s1", deliveryReport{Recorded: 2, Delivered: 2})
	if logged := out.String(); strings.Count(logged, "delivery reports reach the control plane") != 1 || strings.Contains(logged, "cpsecret") {
		t.Fatalf("log after the recovery:\n%s", logged)
	}
	// A second outage within the minute is not logged again.
	e.cp.setStatus(http.StatusForbidden)
	e.record("s1", 1)
	n := e.cp.postCount()
	eventually(t, "attempts", func() bool { return e.cp.postCount() >= n+2 })
	if logged := out.String(); strings.Count(logged, "cannot send delivery reports") != 1 {
		t.Fatalf("a flapping control plane floods the log:\n%s", logged)
	}
}

// A control plane that fails once is retried without a word; one that keeps
// failing is logged once.
func TestReporterLogsAnOutageOnce(t *testing.T) {
	out := logTo(t)
	e := newReportEnv(t, `{"active":["s1"]}`, nil)
	e.cp.setStatus(http.StatusBadGateway)
	e.record("s1", 1)
	eventually(t, "an attempt", func() bool { return e.cp.postCount() == 1 })
	if logged := out.String(); strings.Contains(logged, "delivery reports") {
		t.Fatalf("one failed report was logged:\n%s", logged)
	}
	eventually(t, "more attempts", func() bool { return e.cp.postCount() >= 3 })
	e.cp.setStatus(0)
	e.settled("s1", deliveryReport{Recorded: 1, Delivered: 1})
	logged := out.String()
	if strings.Count(logged, "cannot send delivery reports") != 1 || !strings.Contains(logged, "HTTP 502") || strings.Count(logged, "again") != 1 {
		t.Fatalf("log:\n%s", logged)
	}
}

// A report the control plane accepted is sent again, unchanged, when the
// stream is opened anew: the control plane behind it may be another one, or
// have restarted and lost what it was told.
func TestReporterResendsAfterReconnect(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1","s2"]}`, nil)
	e.record("s1", 2)
	e.record("s2", 1)
	e.settled("s1", deliveryReport{Recorded: 2, Delivered: 2})
	e.settled("s2", deliveryReport{Recorded: 1, Delivered: 1})
	time.Sleep(testInterval + 50*time.Millisecond) // nothing changed: nothing more is sent
	if n := e.cp.reportCount(); n != 2 {
		t.Fatalf("%d reports, want 2", n)
	}
	e.cp.drop <- struct{}{}
	eventually(t, "both reports again", func() bool { return len(e.cp.reports("s1")) == 2 && len(e.cp.reports("s2")) == 2 })
	// In one request, the one that tells the new control plane that the
	// reports of this process arrive.
	if _, hellos, sizes := e.cp.seen(); hellos != 1 || sizes[len(sizes)-1] != 2 {
		t.Fatalf("after the reconnect: %d batches without reports, batches of %v", hellos, sizes)
	}
	if got := e.cp.reports("s1"); got[1].counts() != got[0].counts() {
		t.Fatalf("resent report %+v, first %+v", got[1], got[0])
	}
}

// A session this process recorded nothing for is not reported, draining or
// not: the control plane looks for the report of an instance only when the
// collector has events of it for the session. (Reporting "nothing" for every
// draining session made an environment of many services send a request per
// service and session, and a late one cost a healthy session its verdict.)
func TestReporterDoesNotReportASessionItRecordedNothingFor(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1","idle"]}`, nil)
	e.record("s1", 1)
	e.settled("s1", deliveryReport{Recorded: 1, Delivered: 1})
	e.cp.set(`{"active":["s1","idle"],"draining":["idle"]}`)
	time.Sleep(100 * time.Millisecond)
	e.cp.drop <- struct{}{}
	eventually(t, "the report of s1 again", func() bool { return len(e.cp.reports("s1")) == 2 })
	if got := e.cp.reports("idle"); len(got) != 0 {
		t.Fatalf("a session without events was reported: %+v", got)
	}
	if posts, _, _ := e.cp.seen(); posts != 2 {
		t.Fatalf("%d requests with reports, want 2", posts)
	}
	// An event after all (a span that ended late): the session is reported
	// like any other from then on.
	e.record("idle", 2)
	e.settled("idle", deliveryReport{Recorded: 2, Delivered: 2, Final: true})
}

// When the stream is opened the reporter tells the control plane, in one
// request without reports, that its reports arrive: a control plane that
// never hears from an instance that follows it knows that something between
// them refuses its reports. Not again while the stream stays up.
func TestReporterSaysThatItsReportsArrive(t *testing.T) {
	e := newReportEnv(t, `{"active":[]}`, nil)
	time.Sleep(60 * time.Millisecond)
	if posts, hellos, _ := e.cp.seen(); posts != 0 || hellos != 1 {
		t.Fatalf("%d requests with reports, %d without, want 0 and 1", posts, hellos)
	}
	e.cp.drop <- struct{}{}
	eventually(t, "again after the reconnect", func() bool { _, hellos, _ := e.cp.seen(); return hellos == 2 })
	// Refused: it is retried, and logged like any report that does not arrive.
	out := logTo(t)
	e.cp.setStatus(http.StatusForbidden)
	e.cp.drop <- struct{}{}
	eventually(t, "attempts", func() bool { return e.cp.postCount() >= 2 })
	if logged := out.String(); strings.Count(logged, "cannot send delivery reports") != 1 || !strings.Contains(logged, "HTTP 403") {
		t.Fatalf("log:\n%s", logged)
	}
	e.cp.setStatus(0)
	eventually(t, "it arrives", func() bool { _, hellos, _ := e.cp.seen(); return hellos == 3 })
}

// The names a report carries are the ones the control plane knows the
// process by from the stream: without the spaces HTTP trims from a header.
// (A service named "orders-api " used to be two instances to the control
// plane, one that followed it and one that reported, and every session of it
// was marked incomplete.)
func TestReporterNamesItselfAsTheStreamDoes(t *testing.T) {
	cp := newReportCP(t, `{"active":["s1"]}`)
	col := newTestCollector(t)
	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "orders-api ", "raw", " pod-7-ab12 ")
	rep := newReporter(rec, exp, sub, "orders-api ", " pod-7-ab12 ", reporterConfig{Tick: 5 * time.Millisecond})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the active set", sub.Ready) // a session it does not know to be active is not accounted for
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase + 1})
	eventually(t, "a report", func() bool { return len(cp.reports("s1")) >= 1 })
	if got := cp.last("s1"); got.Service != "orders-api" || got.Instance != "pod-7-ab12" {
		t.Fatalf("report names %q / %q", got.Service, got.Instance)
	}
	if s := cp.streams(); len(s) != 1 || s[0] != "pod-7-ab12" {
		t.Fatalf("the stream announced %q", s)
	}
}

// A report that fails is retried after a pause that grows, so that a control
// plane in trouble is not hammered. Not while the report of a draining
// session waits: the control plane seals that session DeliveryGrace after it
// last heard of this process, and an outage of its report endpoint that ends
// before then must not cost the session its report.
func TestReporterRetriesQuicklyWhileASessionDrains(t *testing.T) {
	e := newReportEnvOn(t, newReportCP(t, `{"active":["s1"]}`), nil, func(c *reporterConfig) { c.RetryBase = 300 * time.Millisecond })
	eventually(t, "the reporter has said hello", func() bool { _, hellos, _ := e.cp.seen(); return hellos == 1 })
	e.cp.setStatus(http.StatusServiceUnavailable)
	e.record("s1", 1)
	eventually(t, "a failed attempt", func() bool { return e.cp.postCount() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := e.cp.postCount(); n > 2 {
		t.Fatalf("%d attempts in 200ms with a pause of 300ms and more: no backoff", n)
	}
	// The session drains: the report is tried every DrainRetry (5ms here).
	e.cp.set(`{"active":["s1"],"draining":["s1"]}`)
	before := e.cp.postCount()
	time.Sleep(250 * time.Millisecond)
	if n := e.cp.postCount() - before; n < 8 {
		t.Fatalf("%d attempts in 250ms while a session drains, want one every 5ms or so", n)
	}
	e.cp.setStatus(0)
	began := time.Now()
	e.settled("s1", deliveryReport{Recorded: 1, Delivered: 1, Final: true})
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Fatalf("the report arrived %v after the control plane was back", took)
	}
}

// While a draining session's events wait behind those of other sessions, its
// report is sent again when the total moved: the control plane sees that the
// instance is working through a backlog, and waits.
func TestReporterReportsProgressOnOtherSessions(t *testing.T) {
	e := newReportEnv(t, `{"active":["stuck","other"]}`, nil)
	// The collector fails the session "stuck" by name and takes the others.
	e.col.failSession.Store("stuck")
	e.record("stuck", 3)
	e.cp.set(`{"active":["stuck","other"],"draining":["stuck"]}`)
	eventually(t, "a final report with the events still buffered", func() bool {
		r := e.cp.last("stuck")
		return r.Final && r.Buffered == 3
	})
	n := len(e.cp.reports("stuck"))
	time.Sleep(100 * time.Millisecond)
	if got := len(e.cp.reports("stuck")); got != n {
		t.Fatalf("%d more reports with nothing new to say", got-n)
	}
	e.record("other", 5)
	eventually(t, "a report with the new total", func() bool {
		r := e.cp.last("stuck")
		return r.DeliveredTotal == 5 && r.Buffered == 3 && r.Delivered == 0
	})
}

// While the stream is down this process learns of a drain late or never, so
// what it holds is reported at once: the first report does not wait for
// FirstDelay, a change does not wait for Interval, and a session it recorded
// nothing for is not reported (the control plane expects that of followers
// of the stream only).
func TestReporterReportsEagerlyWithoutTheStream(t *testing.T) {
	cp := &reportCP{fakeCP: newFakeCP(`{"active":["s1","idle"],"draining":["idle"]}`), got: map[string][]deliveryReport{}, answer: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/active", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, cp.get()) })
	mux.HandleFunc("/v1/active/stream", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no stream here", http.StatusBadGateway) })
	mux.HandleFunc("POST /v1/delivery", cp.batch)
	cp.srv = httptest.NewServer(mux)
	t.Cleanup(cp.srv.Close)

	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://127.0.0.1:1", FlushInterval: 2 * time.Millisecond, RetryBackoff: time.Millisecond,
		MaxCooldown: 20 * time.Millisecond, HTTPTimeout: time.Second})
	sub := NewSubscriberFor(cp.srv.URL, "svc", "raw")
	rep := newReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: 5 * time.Millisecond, Interval: time.Hour, FirstDelay: time.Hour,
		KickEvery: 30 * time.Millisecond})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the active set, from a poll", sub.Ready)

	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase + 1})
	eventually(t, "a first report of undelivered events", func() bool { return cp.last("s1").Buffered == 1 })
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase + 2})
	eventually(t, "the change", func() bool { return cp.last("s1").Buffered == 2 })
	if got := cp.last("s1"); got.Final || got.LastError == "" {
		t.Fatalf("report %+v", got)
	}
	if got := cp.reports("idle"); len(got) != 0 {
		t.Fatalf("reports of a session nothing was recorded for, without the stream: %+v", got)
	}
}

// Many sessions that change at once are a few requests, not one each: all
// that is due goes into one batch, Batch reports at most, the draining
// sessions first. (One request per session made 500 sessions that stopped
// together wait behind each other for longer than the control plane waits.)
func TestReporterBatchesReports(t *testing.T) {
	const sessions = 600
	var active, draining []string
	for i := range sessions {
		active = append(active, fmt.Sprintf(`"s%03d"`, i))
		if i%4 == 0 {
			draining = append(draining, fmt.Sprintf(`"s%03d"`, i))
		}
	}
	cp := newReportCP(t, `{"active":[`+strings.Join(active, ",")+`],"draining":[`+strings.Join(draining, ",")+`]}`)
	col := newTestCollector(t)
	rec := NewRecorder(1024)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: time.Hour})
	sub := newSubscriber(cp.srv.URL, "svc", "raw", "svc-1")
	rep := manualReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: time.Hour})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the active set", func() bool { return sub.Ready() && sub.streaming.Load() })
	for i := range sessions {
		rec.Record(core.Event{SessionID: fmt.Sprintf("s%03d", i), Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	if !exp.Flush(5 * time.Second) {
		t.Fatal("flush")
	}
	rep.pass() // the reporter's own loop never runs one (Tick)
	posts, hellos, sizes := cp.seen()
	if posts != 3 || hellos != 0 || fmt.Sprint(sizes) != "[256 256 88]" {
		t.Fatalf("%d requests with reports (%d without), batches of %v; want 3 of 256, 256 and 88", posts, hellos, sizes)
	}
	if got := cp.all(); len(got) != sessions {
		t.Fatalf("%d sessions reported, want all %d", len(got), sessions)
	}
	cp.mu.Lock()
	order := append([]string(nil), cp.order...)
	cp.mu.Unlock()
	for i, sid := range order[:len(draining)] {
		if !strings.Contains(strings.Join(draining, ","), `"`+sid+`"`) {
			t.Fatalf("report %d is of %s: the draining sessions go first", i, sid)
		}
	}
	// Nothing changed: the next pass sends nothing.
	rep.pass()
	if posts, hellos, _ := cp.seen(); posts != 3 || hellos != 0 {
		t.Fatalf("%d requests after a pass with nothing to say", posts+hellos)
	}

	// At shutdown the last word on every session is batched as well.
	rep.close(5 * time.Second)
	if posts, _, sizes := cp.seen(); posts != 6 || fmt.Sprint(sizes[3:]) != "[256 256 88]" {
		t.Fatalf("at close: %d requests, batches of %v", posts-3, sizes[3:])
	}
	if got := cp.last("s007"); !got.Final || got.Recorded != 1 || got.Delivered != 1 {
		t.Fatalf("last report %+v", got)
	}
}

// A request that takes long ends the pass: the rest is sent by the next one,
// from what the accounts say then, so a slow control plane is not sent
// reports that are stale by the time it reads them.
func TestReporterSlowBatchEndsThePass(t *testing.T) {
	cp := newReportCP(t, `{"active":["a","b","c"]}`)
	col := newTestCollector(t)
	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: time.Hour})
	sub := newSubscriber(cp.srv.URL, "svc", "raw", "svc-1")
	rep := manualReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: time.Hour, Batch: 1, PassBudget: time.Nanosecond}) // every request outlasts the budget
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the active set", func() bool { return sub.Ready() && sub.streaming.Load() })
	for i, sid := range []string{"a", "b", "c"} {
		rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	if !exp.Flush(5 * time.Second) {
		t.Fatal("flush")
	}
	rep.pass()
	if n := cp.reportCount(); n != 1 {
		t.Fatalf("%d reports in a pass whose first request outlasted its budget, want 1", n)
	}
	rep.pass()
	rep.pass()
	if n := cp.reportCount(); n != 3 {
		t.Fatalf("%d reports after three passes, want 3", n)
	}
}
