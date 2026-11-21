package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// accountErrors holds the delivery accounts against the ring: for every
// session with an account, what the account says is still buffered is what
// the ring holds of it. That is the invariant the reports rest on:
// recorded = delivered + dropped + buffered.
func accountErrors(r *Recorder) (bad []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return accountErrorsLocked(r)
}

func accountErrorsLocked(r *Recorder) (bad []string) {
	inRing := map[string]uint64{}
	for i := range r.n {
		inRing[r.at(i).ev.SessionID]++
	}
	for sid, a := range r.acct {
		if a.delivered+a.dropped > a.recorded {
			bad = append(bad, fmt.Sprintf("session %s: delivered %d + dropped %d > recorded %d", sid, a.delivered, a.dropped, a.recorded))
		} else if a.buffered() != inRing[sid] {
			bad = append(bad, fmt.Sprintf("session %s: account %+v says %d buffered, the ring holds %d", sid, *a, a.buffered(), inRing[sid]))
		}
	}
	return bad
}

// The consumers that read the ring without an exporter, and forget.
func TestAccountsPeekCommitDrainForget(t *testing.T) {
	r := NewRecorder(4)
	for i := range 6 { // the last two overflow
		r.Record(core.Event{SessionID: []string{"a", "b"}[i%2], LocalSeq: uint64(i + 1)})
	}
	want := func(when string, a, b account) {
		t.Helper()
		got := r.accounts()
		if got["a"] != a || got["b"] != b || len(accountErrors(r)) > 0 {
			panic(fmt.Sprintf("%s: accounts %+v, want a=%+v b=%+v; %v", when, got, a, b, accountErrors(r)))
		}
	}
	want("after an overflow", account{recorded: 3, dropped: 1}, account{recorded: 3, dropped: 1})
	r.Peek(4) // takes nothing
	want("after Peek", account{recorded: 3, dropped: 1}, account{recorded: 3, dropped: 1})
	r.Commit(1)
	want("after Commit", account{recorded: 3, delivered: 1, dropped: 1}, account{recorded: 3, dropped: 1})
	r.Drain(2)
	want("after Drain", account{recorded: 3, delivered: 2, dropped: 1}, account{recorded: 3, delivered: 1, dropped: 1})
	// The sdk.dropped events that carried the counts were given up on: the
	// events they stand for are not dropped a second time.
	r.AddDrops([]core.Event{dropEvent("a", 1), dropEvent("b", 1)})
	want("after drop counts came back", account{recorded: 3, delivered: 2, dropped: 1}, account{recorded: 3, delivered: 1, dropped: 1})

	r.forget("b")
	if got := r.accounts(); len(got) != 1 || got["a"].recorded != 3 {
		panic(fmt.Sprintf("after forget: %+v", got))
	}
	r.Drain(4) // b's last event leaves the ring after its account was forgotten: nothing breaks
	if got := r.accounts(); len(got) != 1 || len(r.acct) != 1 {
		panic(fmt.Sprintf("an account came back without an event being recorded: %+v (%d kept)", got, len(r.acct)))
	}
	r.Record(core.Event{SessionID: "b", LocalSeq: 9}) // a span that ended after the session did
	if got := r.accounts(); got["b"] != (account{recorded: 1}) {
		panic(fmt.Sprintf("a late event: %+v", got))
	}
	r.forget("a")
	r.forget("b")
	if r.accounts() != nil {
		panic(fmt.Sprintf("accounts left: %+v", r.accounts()))
	}
}

// testCollector answers ingest requests: 503 while down, otherwise 200 with
// the counts it is told to report.
type testCollector struct {
	srv      *httptest.Server
	down     atomic.Bool
	events   atomic.Int64 // events of the requests it accepted
	requests atomic.Int64
	answer   func(n int) string // body of a 200 for n events; nil: all accepted
	// failSession names a session whose events are failed by name (HTTP 500
	// with failed_sessions) while the events of the others are taken.
	failSession atomic.Value
}

func newTestCollector(t *testing.T) *testCollector {
	c := &testCollector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if c.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		evs, _ := decodeFrames(body)
		if sid, _ := c.failSession.Load().(string); sid != "" {
			others := 0
			for _, e := range evs {
				if e.SessionID != sid {
					others++
				}
			}
			if others < len(evs) {
				c.events.Add(int64(others))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, `{"error":"storage error","accepted":%d,"failed_sessions":{%q:"storage"}}`, others, sid)
				return
			}
		}
		c.events.Add(int64(len(evs)))
		if c.answer != nil {
			_, _ = io.WriteString(w, c.answer(len(evs)))
			return
		}
		fmt.Fprintf(w, `{"accepted":%d}`, len(evs))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// Collector duplicates count as delivered (the collector has the event);
// events it rejects count as dropped, and never as both.
func TestAccountsDuplicatesAndRejections(t *testing.T) {
	col := newTestCollector(t)
	col.answer = func(n int) string { return `{"accepted":0,"duplicates":3,"rejected":1}` }
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: time.Hour})
	defer exp.Close(time.Second)
	for i := range 4 {
		rec.Record(core.Event{SessionID: "s", Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	if !exp.Flush(5 * time.Second) {
		panic("flush")
	}
	// The rejection also goes out as an sdk.dropped event, which this
	// collector rejects again: that feeds the drop count, not the account.
	if a := rec.accounts()["s"]; a != (account{recorded: 4, delivered: 3, dropped: 1}) || len(accountErrors(rec)) > 0 {
		panic(fmt.Sprintf("account %+v %v", a, accountErrors(rec)))
	}
}

// reportCP is a control plane for the reporter: the active set of fakeCP and
// the delivery endpoint, which records what it is sent.
type reportCP struct {
	*fakeCP
	srv *httptest.Server

	mu     sync.Mutex
	status int // answer of the delivery endpoints; 0: 200; 404: as if the routes did not exist
	// noBatch makes it a control plane from before POST /v1/delivery: it takes
	// one report per request (or none, with status 404).
	noBatch bool
	// answer is what a batch says of the reports of a session ("closed", ...)
	// instead of "accepted"; the per-session endpoint answers 409 for it.
	answer map[string]string
	got    map[string][]deliveryReport
	order  []string // the sessions in the order their reports arrived
	posts  int      // requests that carried reports, or were refused
	hellos int      // batches without reports that were accepted
	sizes  []int    // the reports of every accepted batch
	// announced is the X-Tsession-Delivery-Instance of every stream opened.
	announced []string
	// stopped is what the accepted batches said about recording having
	// stopped (deliveryBatch.Stopped), in order.
	stopped []string
}

func newReportCP(t *testing.T, state string) *reportCP {
	cp := &reportCP{fakeCP: newFakeCP(state), got: map[string][]deliveryReport{}, answer: map[string]string{}}
	mux := http.NewServeMux()
	inner := cp.fakeCP.mux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/active/stream" {
			cp.mu.Lock()
			cp.announced = append(cp.announced, r.Header.Get(core.HeaderDeliveryInstance))
			cp.mu.Unlock()
		}
		inner.ServeHTTP(w, r)
	})
	mux.HandleFunc("POST /v1/sessions/{id}/delivery", cp.delivery)
	mux.HandleFunc("POST /v1/delivery", cp.batch)
	cp.srv = httptest.NewServer(mux)
	t.Cleanup(func() { close(cp.drop); cp.srv.CloseClientConnections(); cp.srv.Close() })
	return cp
}

// delivery is the endpoint for one report.
func (cp *reportCP) delivery(w http.ResponseWriter, r *http.Request) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.posts++
	if cp.status != 0 {
		http.Error(w, "no", cp.status)
		return
	}
	sid := r.PathValue("id")
	if cp.answer[sid] != "" {
		http.Error(w, cp.answer[sid], http.StatusConflict)
		return
	}
	var d deliveryReport
	if r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&d) != nil {
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	cp.got[sid] = append(cp.got[sid], d)
	cp.order = append(cp.order, sid)
	_, _ = io.WriteString(w, `{"accepted":true}`)
}

// batch is the endpoint for the reports of many sessions.
func (cp *reportCP) batch(w http.ResponseWriter, r *http.Request) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.noBatch || cp.status == http.StatusNotFound {
		cp.posts++
		http.NotFound(w, r)
		return
	}
	if cp.status != 0 {
		cp.posts++
		http.Error(w, "no", cp.status)
		return
	}
	var b deliveryBatch
	if r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&b) != nil {
		cp.posts++
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	if len(b.Reports) == 0 {
		cp.hellos++
	} else {
		cp.posts++
	}
	if b.Stopped != "" {
		cp.stopped = append(cp.stopped, b.Stopped)
	}
	cp.sizes = append(cp.sizes, len(b.Reports))
	results := make([]map[string]string, len(b.Reports))
	for i, x := range b.Reports {
		status := cp.answer[x.Session]
		if status == "" {
			status = "accepted"
			cp.got[x.Session] = append(cp.got[x.Session], deliveryReport{Service: b.Service, Instance: b.Instance, Recorded: x.Recorded, Delivered: x.Delivered,
				Dropped: x.Dropped, Buffered: x.Buffered, DeliveredTotal: b.DeliveredTotal, LastError: x.LastError, Final: x.Final})
			cp.order = append(cp.order, x.Session)
		}
		results[i] = map[string]string{"session": x.Session, "status": status}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
}

// seen returns the counters of the requests: those that carried reports or
// were refused, the accepted batches without reports, and the reports of
// every accepted batch.
func (cp *reportCP) seen() (posts, hellos int, sizes []int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.posts, cp.hellos, append([]int(nil), cp.sizes...)
}

// streams returns what every stream opened so far announced.
func (cp *reportCP) streams() []string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]string(nil), cp.announced...)
}

// reportCount is the number of reports received, of all sessions.
func (cp *reportCP) reportCount() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.order)
}

func (cp *reportCP) set404(noBatch bool, status int) {
	cp.mu.Lock()
	cp.noBatch, cp.status = noBatch, status
	cp.mu.Unlock()
}

func (cp *reportCP) setAnswer(sid, status string) {
	cp.mu.Lock()
	cp.answer[sid] = status
	cp.mu.Unlock()
}

// all returns the reports received, by session.
func (cp *reportCP) all() map[string][]deliveryReport {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	out := make(map[string][]deliveryReport, len(cp.got))
	for sid, r := range cp.got {
		out[sid] = append([]deliveryReport(nil), r...)
	}
	return out
}

func (cp *reportCP) setStatus(code int) {
	cp.mu.Lock()
	cp.status = code
	cp.mu.Unlock()
}

func (cp *reportCP) reports(sid string) []deliveryReport {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]deliveryReport(nil), cp.got[sid]...)
}

func (cp *reportCP) last(sid string) deliveryReport {
	if r := cp.reports(sid); len(r) > 0 {
		return r[len(r)-1]
	}
	return deliveryReport{}
}

// postCount is the number of requests that carried reports or were refused.
func (cp *reportCP) postCount() int {
	posts, _, _ := cp.seen()
	return posts
}

// reportEnv is a recorder, an exporter, a subscriber and a reporter with
// short timers, against a testCollector and a reportCP.
type reportEnv struct {
	t   *testing.T
	col *testCollector
	cp  *reportCP
	rec *Recorder
	exp *Exporter
	sub *Subscriber
	rep *reporter
	seq uint64
}

const (
	testInterval   = 300 * time.Millisecond
	testFirstDelay = 150 * time.Millisecond
)

func newReportEnv(t *testing.T, state string, mut func(*ExporterConfig)) *reportEnv {
	t.Helper()
	e := newReportEnvOn(t, newReportCP(t, state), mut)
	// The reporter has said that its reports arrive: from here on a test
	// counts the requests it causes itself.
	eventually(t, "the reporter has said hello", func() bool { _, hellos, _ := e.cp.seen(); return hellos == 1 })
	return e
}

// newReportEnvOn is newReportEnv against a control plane the test prepared.
func newReportEnvOn(t *testing.T, cp *reportCP, mut func(*ExporterConfig), tune ...func(*reporterConfig)) *reportEnv {
	t.Helper()
	e := &reportEnv{t: t, col: newTestCollector(t), cp: cp}
	cfg := ExporterConfig{CollectorURL: e.col.srv.URL, FlushInterval: 2 * time.Millisecond, RetryBackoff: time.Millisecond,
		MaxCooldown: 20 * time.Millisecond, HTTPTimeout: time.Second}
	if mut != nil {
		mut(&cfg)
	}
	e.rec = NewRecorder(64)
	e.exp = NewExporter(e.rec, cfg)
	e.sub = newSubscriber(e.cp.srv.URL, "svc", "raw-instance", "svc-1-abcdef")
	rcfg := reporterConfig{Tick: 5 * time.Millisecond,
		Interval: testInterval, FirstDelay: testFirstDelay, DrainFlush: 50 * time.Millisecond, KickEvery: 20 * time.Millisecond,
		RetryBase: 20 * time.Millisecond, DrainRetry: 5 * time.Millisecond, Heartbeat: time.Hour}
	for _, f := range tune {
		f(&rcfg)
	}
	e.rep = newReporter(e.rec, e.exp, e.sub, "svc", "svc-1-abcdef", rcfg)
	e.t.Cleanup(func() {
		e.exp.Close(10 * time.Millisecond)
		if !e.rep.stopped() {
			e.rep.close(10 * time.Millisecond)
		}
		e.sub.Close()
	})
	eventually(t, "the subscriber has the active set", e.sub.Ready)
	return e
}

// stopped reports whether close was called.
func (r *reporter) stopped() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (e *reportEnv) record(sid string, n int) {
	for range n {
		e.seq++
		e.rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, LocalSeq: seqBase + e.seq})
	}
}

// settled waits until the last report of sid is want, and fails if another
// one follows within a few ticks.
func (e *reportEnv) settled(sid string, want deliveryReport) {
	e.t.Helper()
	want.Service, want.Instance = "svc", "svc-1-abcdef"
	deadline := time.Now().Add(5 * time.Second)
	for e.cp.last(sid).counts() != want {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("session %s: reports %+v, want the last to be %+v", sid, e.cp.reports(sid), want))
		}
		time.Sleep(2 * time.Millisecond)
	}
	n := len(e.cp.reports(sid))
	time.Sleep(40 * time.Millisecond)
	if got := e.cp.reports(sid); len(got) != n {
		panic(fmt.Sprintf("session %s: reports kept coming without a change: %+v", sid, got))
	}
}

// The schedule of a healthy session: one report once the events are
// delivered, none while nothing changes, the next change not before
// Interval, and a final report when the session turns draining.
func TestReporterSchedule(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1","other"]}`, nil)
	if e.cp.postCount() != 0 {
		panic("a report without an event: a session in which nothing was recorded is never reported")
	}
	start := time.Now()
	e.record("s1", 3)
	e.settled("s1", deliveryReport{Recorded: 3, Delivered: 3})
	if d := time.Since(start); d >= testFirstDelay {
		t.Errorf("the first report took %v: delivered events are reported at once, not after FirstDelay (%v)", d, testFirstDelay)
	}
	if got := e.cp.reports("s1"); len(got) != 1 {
		panic(fmt.Sprintf("reports %+v, want one", got))
	}

	// A change is reported, but not sooner than Interval after the last report.
	sent := time.Now()
	e.record("s1", 2)
	e.settled("s1", deliveryReport{Recorded: 5, Delivered: 5})
	if d := time.Since(sent); d < testInterval-60*time.Millisecond {
		t.Errorf("the second report came %v after the first, want about Interval (%v)", d, testInterval)
	}

	// Draining: sent now, and reported as final once it is delivered.
	e.record("s1", 1)
	e.cp.set(`{"active":["s1","other"],"draining":["s1"]}`)
	sent = time.Now()
	e.settled("s1", deliveryReport{Recorded: 6, Delivered: 6, Final: true})
	if d := time.Since(sent); d >= testInterval {
		t.Errorf("the final report took %v", d)
	}

	// Events recorded during the drain, after the final report (a worker that
	// is still running): a new final report replaces it. The counts are
	// cumulative, so the control plane needs nothing else.
	e.record("s1", 2)
	sent = time.Now()
	e.settled("s1", deliveryReport{Recorded: 8, Delivered: 8, Final: true})
	if d := time.Since(sent); d >= testInterval {
		t.Errorf("the report of events recorded during the drain took %v: it does not wait for Interval", d)
	}

	// The session is over: its account goes, and nothing more is sent.
	e.cp.set(`{"active":["other"]}`)
	eventually(t, "the account is forgotten", func() bool { return e.rec.accounts() == nil })
	if len(e.cp.reports("other")) != 0 {
		panic(fmt.Sprintf("a session without events was reported: %+v", e.cp.reports("other")))
	}
}

// Events that cannot be delivered: the first report waits FirstDelay for
// them, then says so, with the error and without the URL's credentials.
func TestReporterUndeliveredEvents(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1"]}`, func(c *ExporterConfig) {
		c.CollectorURL = "http://user:hunter2@127.0.0.1:1/?token=hunter2" // nothing listens there
	})
	start := time.Now()
	e.record("s1", 2)
	eventually(t, "a report", func() bool { return len(e.cp.reports("s1")) > 0 })
	if d := time.Since(start); d < testFirstDelay-20*time.Millisecond {
		t.Errorf("undelivered events were reported after %v, before the exporter had FirstDelay (%v) to deliver them", d, testFirstDelay)
	}
	got := e.cp.last("s1")
	if got.Recorded != 2 || got.Delivered != 0 || got.Buffered != 2 || got.Dropped != 0 || got.Final {
		panic(fmt.Sprintf("report %+v", got))
	}
	if !strings.Contains(got.LastError, "127.0.0.1:1") || strings.Contains(got.LastError, "hunter2") || len(got.LastError) > maxReportError {
		panic(fmt.Sprintf("last_error %q: want the address, without credentials, bounded", got.LastError))
	}

	// Draining: it cannot be delivered now either, and after DrainFlush the
	// report is final all the same.
	e.cp.set(`{"active":["s1"],"draining":["s1"]}`)
	eventually(t, "a final report", func() bool { return e.cp.last("s1").Final })
	if got := e.cp.last("s1"); got.Buffered != 2 || got.Delivered != 0 || got.LastError == "" {
		panic(fmt.Sprintf("final report %+v", got))
	}

	// Shutdown: what the exporter could not deliver is discarded with it, so
	// the last word calls it dropped; the control plane has nothing to wait for.
	e.exp.Close(10 * time.Millisecond)
	e.rep.close(time.Second)
	if got := e.cp.last("s1"); got.Recorded != 2 || got.Dropped != 2 || got.Buffered != 0 || !got.Final || got.LastError == "" {
		panic(fmt.Sprintf("report at shutdown %+v", got))
	}
}

// A collector that comes back during the drain is noticed at once, not
// after the exporter's cooldown: the control plane waits only so long.
func TestReporterRetriesDeliveryWhileDraining(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1"]}`, func(c *ExporterConfig) {
		c.MaxCooldown, c.FlushInterval = time.Hour, 100*time.Millisecond // a pause of up to 6.4s after a few failures
	})
	e.col.down.Store(true)
	e.record("s1", 3)
	e.cp.set(`{"active":["s1"],"draining":["s1"]}`)
	eventually(t, "a final report with the events still buffered", func() bool {
		r := e.cp.last("s1")
		return r.Final && r.Buffered == 3
	})
	if got := e.cp.last("s1"); got.LastError != "the collector answered HTTP 503" {
		panic(fmt.Sprintf("last_error %q", got.LastError))
	}
	// By now the exporter has failed often enough to pause for a long time.
	time.Sleep(100 * time.Millisecond)
	e.col.down.Store(false)
	back := time.Now()
	e.settled("s1", deliveryReport{Recorded: 3, Delivered: 3, Final: true})
	if d := time.Since(back); d > 2*time.Second {
		t.Errorf("delivery resumed %v after the collector came back", d)
	}
}

// A control plane from before delivery reports has neither endpoint and
// answers 404: the reporter goes quiet, without a log line, and the stream
// no longer announces that this process reports. It asks again when the
// stream is opened anew (the control plane may have been upgraded meanwhile).
func TestReporterFallsSilentOn404(t *testing.T) {
	out := logTo(t)
	cp := newReportCP(t, `{"active":["s1","s2"]}`)
	cp.set404(false, http.StatusNotFound)
	e := newReportEnvOn(t, cp, nil)
	// The stream is opened anew without the announcement, once.
	eventually(t, "a stream that does not announce reports", func() bool { s := cp.streams(); return len(s) == 2 && s[0] == "svc-1-abcdef" && s[1] == "" })
	eventually(t, "the batch endpoint was asked for at both connects", func() bool { posts, _, _ := cp.seen(); return posts == 2 })
	e.record("s1", 1)
	e.record("s2", 1)
	// One attempt at the per-session endpoint, then nothing.
	eventually(t, "an attempt at the older endpoint", func() bool { posts, _, _ := cp.seen(); return posts == 3 })
	e.record("s1", 5)
	time.Sleep(testInterval + 100*time.Millisecond)
	if posts, _, _ := cp.seen(); posts != 3 || len(cp.streams()) != 2 {
		panic(fmt.Sprintf("%d requests to a control plane that answered 404, want 3 (%d streams)", posts, len(cp.streams())))
	}
	// Recording and export are not affected, and nothing is logged.
	if a := e.rec.accounts(); a["s1"] != (account{recorded: 6, delivered: 6}) || a["s2"] != (account{recorded: 1, delivered: 1}) {
		panic(fmt.Sprintf("accounts %+v", a))
	}
	if logged := out.String(); strings.Contains(logged, "delivery reports") {
		panic(fmt.Sprintf("logged about a control plane without the endpoint:\n%s", logged))
	}

	// The same after a reconnect, with one more round of attempts...
	cp.drop <- struct{}{}
	eventually(t, "another round after the reconnect", func() bool { posts, _, _ := cp.seen(); return posts == 5 })
	time.Sleep(testInterval + 100*time.Millisecond)
	if posts, _, _ := cp.seen(); posts != 5 {
		panic(fmt.Sprintf("%d requests, want 5", posts))
	}
	// ...and reports resume when the endpoint is there: everything, also
	// what did not change since, and the stream announces them again.
	cp.set404(false, 0)
	cp.drop <- struct{}{}
	e.settled("s1", deliveryReport{Recorded: 6, Delivered: 6})
	e.settled("s2", deliveryReport{Recorded: 1, Delivered: 1})
	eventually(t, "a stream that announces reports again", func() bool { s := cp.streams(); return s[len(s)-1] == "svc-1-abcdef" })
}

// A control plane that knows the per-session endpoint only (one from before
// the batch): every report goes there, a request each and Burst of them per
// pass, the draining sessions first, and the stream does not announce this
// process: such a control plane would take the announcement for a promise to
// report every draining session, which this SDK does not keep.
func TestReporterFallsBackToOneReportPerRequest(t *testing.T) {
	var active, draining []string
	for i := range 40 {
		active = append(active, fmt.Sprintf(`"s%02d"`, i))
		if i%4 == 0 {
			draining = append(draining, fmt.Sprintf(`"s%02d"`, i))
		}
	}
	cp := newReportCP(t, `{"active":[`+strings.Join(active, ",")+`],"draining":[`+strings.Join(draining, ",")+`]}`)
	cp.set404(true, 0)
	col := newTestCollector(t)
	rec := NewRecorder(256)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: time.Hour})
	sub := newSubscriber(cp.srv.URL, "svc", "raw", "svc-1")
	rep := manualReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: time.Hour, Burst: 6})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the active set", func() bool { return sub.Ready() && sub.streaming.Load() })
	pass := rep.pass // the reporter's own loop never runs one (Tick)
	pass()           // asks for the batch endpoint: 404
	eventually(t, "a stream that does not announce reports", func() bool {
		s := cp.streams()
		return len(s) == 2 && s[1] == "" && sub.streaming.Load()
	})
	pass() // asks again on the new stream
	if posts, hellos, _ := cp.seen(); posts != 2 || hellos != 0 {
		panic(fmt.Sprintf("%d requests, %d batches accepted", posts, hellos))
	}
	for i := range 40 {
		rec.Record(core.Event{SessionID: fmt.Sprintf("s%02d", i), Kind: core.KindAPIIn, LocalSeq: seqBase + uint64(i)})
	}
	if !exp.Flush(5 * time.Second) {
		panic("flush")
	}
	pass()
	if n := cp.reportCount(); n != 6 {
		panic(fmt.Sprintf("%d reports in one pass, want 6", n))
	}
	for sid := range cp.all() {
		if !strings.Contains(strings.Join(draining, ","), `"`+sid+`"`) {
			panic(fmt.Sprintf("session %s was reported before the draining ones", sid))
		}
	}
	for range 10 {
		pass()
	}
	if got := cp.all(); len(got) != 40 {
		panic(fmt.Sprintf("%d sessions reported, want all 40", len(got)))
	}
	if s := cp.streams(); len(s) != 2 {
		panic(fmt.Sprintf("streams %q", s))
	}
	// A session that is finished: its endpoint answers 409, and nothing more
	// is sent for it.
	cp.setAnswer("s04", "closed")
	rec.Record(core.Event{SessionID: "s04", Kind: core.KindAPIIn, LocalSeq: seqBase + 100})
	rec.Record(core.Event{SessionID: "s08", Kind: core.KindAPIIn, LocalSeq: seqBase + 101})
	if !exp.Flush(5 * time.Second) {
		panic("flush")
	}
	before, _, _ := cp.seen()
	for range 3 { // both drain: a change is reported at once
		pass()
	}
	if posts, _, _ := cp.seen(); posts != before+2 || len(cp.reports("s04")) != 1 || len(cp.reports("s08")) != 2 {
		panic(fmt.Sprintf("%d requests after a 409, want 2 (s04 %d reports, s08 %d)", posts-before, len(cp.reports("s04")), len(cp.reports("s08"))))
	}
}

// The control plane says of every report of a batch what became of it. For a
// session that is finished or being sealed nothing more is sent; the other
// sessions of the batch are not affected.
func TestReporterStopsOnAClosedSession(t *testing.T) {
	for _, status := range []string{"closed", "unknown", "invalid"} {
		t.Run(status, func(t *testing.T) {
			e := newReportEnv(t, `{"active":["s1","s2"]}`, nil)
			e.cp.setAnswer("s1", status)
			e.record("s1", 1)
			e.record("s2", 1)
			e.settled("s2", deliveryReport{Recorded: 1, Delivered: 1})
			before, _, _ := e.cp.seen()
			e.record("s1", 1)
			e.cp.setAnswer("s1", "")
			time.Sleep(testInterval + 100*time.Millisecond)
			if posts, _, _ := e.cp.seen(); len(e.cp.reports("s1")) != 0 || posts != before {
				panic(fmt.Sprintf("reports of a session the control plane closed: %+v (%d more requests)", e.cp.reports("s1"), posts-before))
			}
		})
	}
	// A session that already keeps the reports of as many instances as it
	// can ("full"): the control plane judges the refused report by what it
	// says, so the reporter goes on telling it of changes, at its usual pace.
	f := newReportEnv(t, `{"active":["s1"]}`, nil)
	f.cp.setAnswer("s1", "full")
	f.record("s1", 1)
	eventually(t, "a first report", func() bool { return f.cp.postCount() == 1 })
	time.Sleep(60 * time.Millisecond)
	if n := f.cp.postCount(); n != 1 {
		panic(fmt.Sprintf("%d requests for a report that did not change, want 1", n))
	}
	f.record("s1", 1)
	eventually(t, "the change is reported", func() bool { return f.cp.postCount() == 2 })

	// A report the control plane could not store is sent again.
	e := newReportEnv(t, `{"active":["s1"]}`, nil)
	e.cp.setAnswer("s1", "failed")
	e.record("s1", 1)
	eventually(t, "attempts", func() bool { posts, _, _ := e.cp.seen(); return posts >= 3 })
	e.cp.setAnswer("s1", "")
	e.settled("s1", deliveryReport{Recorded: 1, Delivered: 1})
}

// A control plane that fails is retried, and the report that gets through
// carries the current counts.
func TestReporterRetriesFailedReports(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1"]}`, nil)
	e.cp.setStatus(http.StatusInternalServerError)
	e.record("s1", 2)
	eventually(t, "an attempt", func() bool { return e.cp.postCount() >= 1 })
	e.record("s1", 1)
	e.cp.setStatus(0)
	e.settled("s1", deliveryReport{Recorded: 3, Delivered: 3})
}

// Under the kill switch no session is active for recording, but what was
// recorded before must still be accounted for.
func TestReporterReportsUnderKillSwitch(t *testing.T) {
	e := newReportEnv(t, `{"active":["s1"]}`, nil)
	e.record("s1", 2)
	e.settled("s1", deliveryReport{Recorded: 2, Delivered: 2})
	e.cp.set(`{"active":["s1"],"kill":true}`)
	eventually(t, "the kill switch is on", func() bool { return !e.sub.IsActive("s1") })
	e.record("s1", 1) // a span that began before the switch
	e.cp.set(`{"active":["s1"],"kill":true,"draining":["s1"]}`)
	e.settled("s1", deliveryReport{Recorded: 3, Delivered: 3, Final: true})
	if e.rec.accounts() == nil {
		panic("the account of a live session was forgotten under the kill switch")
	}
}

// Start and Stop: the reports name the instance as the events do (unique for
// the process), and Stop sends the last word.
func TestRuntimeStopSendsFinalReport(t *testing.T) {
	cp := newReportCP(t, `{"active":["s1"]}`)
	if err := Start(Config{Service: "orders", Instance: "pod-7", ControlPlaneURL: cp.srv.URL, CollectorURL: "http://127.0.0.1:1"}); err != nil {
		panic(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			Stop()
		}
	}()
	inst := core.GetConfig().Instance
	if !strings.HasPrefix(inst, "pod-7-") {
		panic(fmt.Sprintf("instance %q", inst))
	}
	eventually(t, "the session is active", func() bool { return core.AnyActive() })
	ActiveRecorder().Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: core.NextLocalSeq()})
	Stop()
	stopped = true
	got := cp.last("s1")
	if got.Service != "orders" || got.Instance != inst || got.Recorded != 1 || got.Dropped != 1 || got.Buffered != 0 || !got.Final {
		panic(fmt.Sprintf("report at Stop %+v, want one dropped event of instance %s", got, inst))
	}
}

// manualReporter is a reporter whose passes the test runs itself: its own
// loop, which runs one on every tick and whenever the stream is opened, is
// stopped. close works as on any other.
func manualReporter(rec *Recorder, exp *Exporter, sub *Subscriber, service, instance string, cfg reporterConfig) *reporter {
	r := newReporter(rec, exp, sub, service, instance, cfg)
	close(r.stop)
	<-r.done
	r.stop = make(chan struct{})
	return r
}
