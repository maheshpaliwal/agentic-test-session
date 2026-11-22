package runtime

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// deadAddr is an address on which nothing listens.
func deadAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// healthCollector is a collector whose /healthz answers what a test says,
// and whose ingest takes everything unless it is down.
type healthCollector struct {
	srv    *httptest.Server
	mu     sync.Mutex
	status int
	body   string
	checks atomic.Int64 // GET /healthz
	ingest atomic.Int64 // anything else
	down   atomic.Bool  // the ingest answers 503
}

func newHealthCollector(t *testing.T, status int, body string) *healthCollector {
	c := &healthCollector{status: status, body: body}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
			c.ingest.Add(1)
			if c.down.Load() {
				http.Error(w, "down", http.StatusServiceUnavailable)
			}
			return
		}
		c.checks.Add(1)
		c.mu.Lock()
		status, body := c.status, c.body
		c.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *healthCollector) answer(status int, body string) {
	c.mu.Lock()
	c.status, c.body = status, body
	c.mu.Unlock()
}

func reachOf(e *Exporter) (known, ok bool, why string) {
	r := e.reach.Load()
	if r == nil {
		return false, false, ""
	}
	return true, r.ok, r.err
}

// A collector URL that leads nowhere used to show only once a session ran
// and an event was recorded for it: until then the service was "connected".
// An exporter with nothing to send now asks the collector whether it is
// there, and says why not, without the credentials of the URL.
func TestAnIdleExporterFindsOutThatItsCollectorIsNotThere(t *testing.T) {
	addr := deadAddr()
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://ci:hunter2@" + addr + "/?token=hunter3", FlushInterval: 2 * time.Millisecond, ReachCheck: 10 * time.Millisecond})
	defer exp.Close(time.Second)
	eventually(t, "the exporter to find its collector missing", func() bool { known, _, _ := reachOf(exp); return known })
	_, ok, why := reachOf(exp)
	if ok || !strings.Contains(why, "Get http://"+addr) || strings.Contains(why, "hunter") || len(why) > maxReportError {
		panic(fmt.Sprintf("reachable=%v: %q", ok, why))
	}
	// Nothing was recorded, so nothing was said about delivery: the exporter
	// is as it was otherwise.
	if exp.lastError() != "" || rec.Len() != 0 {
		panic(fmt.Sprintf("a check left its mark on the exporter: %q, %d buffered", exp.lastError(), rec.Len()))
	}
}

// What counts as "there": anything that answers and is not the failure of a
// server. A collector that says it is degraded is there; a gateway whose
// upstream is gone is not; a proxy that passes only the ingest (404) is.
func TestWhatAnExporterTakesForACollectorThatIsThere(t *testing.T) {
	col := newHealthCollector(t, http.StatusBadGateway, "no upstream")
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: 10 * time.Millisecond})
	defer exp.Close(time.Second)
	is := func(want bool) func() bool {
		return func() bool { known, ok, _ := reachOf(exp); return known && ok == want }
	}
	eventually(t, "502 to count as not there", is(false))
	if _, _, why := reachOf(exp); why != "GET "+col.srv.URL+"/healthz answered HTTP 502" {
		panic(why)
	}
	col.answer(http.StatusOK, "ok\n")
	eventually(t, "200 to count as there", is(true))
	col.answer(http.StatusServiceUnavailable, "unavailable")
	eventually(t, "a bare 503 to count as not there", is(false))
	col.answer(http.StatusServiceUnavailable, "degraded: a background goroutine panicked\n")
	eventually(t, "a degraded collector to count as there", is(true))
	col.answer(http.StatusInternalServerError, "boom")
	eventually(t, "500 to count as not there", is(false))
	col.answer(http.StatusNotFound, "no such route")
	eventually(t, "404 to count as there", is(true))
	if _, _, why := reachOf(exp); why != "" {
		panic("a reason is left: " + why)
	}
	if col.ingest.Load() != 0 {
		panic("a check went to the ingest")
	}
}

// The check is for an exporter with nothing to send. While events wait, the
// requests that carry them say whether the collector is there, and no
// request is added to them; an Exporter made without ReachCheck never asks.
func TestTheCollectorIsNotCheckedWhileEventsWaitNorWithoutReachCheck(t *testing.T) {
	col := newHealthCollector(t, http.StatusOK, "ok\n")
	col.down.Store(true)
	rec := NewRecorder(16)
	rec.Record(core.Event{SessionID: "s1", Kind: core.KindAPIIn, LocalSeq: seqBase})
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: 5 * time.Millisecond,
		MaxCooldown: 10 * time.Millisecond})
	defer exp.Close(100 * time.Millisecond)
	eventually(t, "the event to be tried a few times", func() bool { return col.ingest.Load() >= 5 })
	if n := col.checks.Load(); n != 0 || rec.Len() != 1 {
		panic(fmt.Sprintf("%d checks while an event waited (%d buffered)", n, rec.Len()))
	}
	// Delivered: the request said that the collector is there, and the idle
	// exporter goes on asking.
	col.down.Store(false)
	eventually(t, "the event to be delivered", func() bool { return rec.Len() == 0 })
	if known, ok, _ := reachOf(exp); !known || !ok {
		panic("a delivered batch did not say that the collector is there")
	}
	eventually(t, "the idle exporter to ask", func() bool { return col.checks.Load() > 0 })

	other := newHealthCollector(t, http.StatusOK, "ok\n")
	plain := NewExporter(NewRecorder(16), ExporterConfig{CollectorURL: other.srv.URL, FlushInterval: time.Millisecond})
	time.Sleep(100 * time.Millisecond)
	plain.Close(time.Second)
	if n := other.checks.Load() + other.ingest.Load(); n != 0 || plain.reach.Load() != nil {
		panic(fmt.Sprintf("an exporter without ReachCheck made %d requests", n))
	}
}

// A collector that accepts the connection and never answers holds a check
// for the whole timeout. The export loop does not wait for it: a Flush with
// nothing to send returns at once, as it did, and so does Close, which ends
// the check.
func TestACheckThatHangsHoldsUpNeitherFlushNorClose(t *testing.T) {
	release := make(chan struct{})
	asked := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case asked <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	exp := NewExporter(NewRecorder(16), ExporterConfig{CollectorURL: srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: time.Hour, HTTPTimeout: time.Hour})
	select {
	case <-asked:
	case <-time.After(10 * time.Second):
		panic("no check was made")
	}
	done := make(chan bool, 1)
	go func() {
		ok := exp.Flush(time.Hour)
		exp.Close(time.Hour)
		done <- ok
	}()
	select {
	case ok := <-done:
		if !ok {
			panic("Flush with nothing to send reported events left")
		}
	case <-time.After(10 * time.Second):
		panic("Flush or Close waits for a check that hangs")
	}
	if exp.reach.Load() != nil {
		panic("a check cut short by Close said something about the collector")
	}
}

// The reporter tells the control plane whether this process reaches its
// collector, also while no session runs: once when it knows, again when that
// changes, and again on a new stream; not with every heartbeat's worth of
// requests while nothing changes.
func TestReporterTellsTheControlPlaneWhetherTheCollectorIsReached(t *testing.T) {
	cp := newReportCP(t, `{"active":[],"delivery_reports":true}`)
	col := newHealthCollector(t, http.StatusBadGateway, "no upstream")
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: 5 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	sub.maxBackoff = 20 * time.Millisecond
	// Saying the same again after a time is another test's: here it would blur the counts.
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond, RetellDown: time.Hour, RetellUp: time.Hour})
	defer func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }()
	told := func() []collectorNote {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		return append([]collectorNote(nil), cp.reach...)
	}
	last := func() collectorNote {
		if t := told(); len(t) > 0 {
			return t[len(t)-1]
		}
		return collectorNote{Reachable: true}
	}
	eventually(t, "the control plane to be told that the collector is not reached", func() bool { return !last().Reachable })
	if got := last(); got.Error != "GET "+col.srv.URL+"/healthz answered HTTP 502" {
		panic(fmt.Sprintf("%+v", got))
	}
	// The checks go on and say the same: nothing more is sent.
	n, checks := len(told()), col.checks.Load()
	eventually(t, "more checks", func() bool { return col.checks.Load() >= checks+10 })
	if got := told(); len(got) != n {
		panic(fmt.Sprintf("told %d more times although nothing changed: %+v", len(got)-n, got))
	}
	if _, hellos, _ := cp.seen(); hellos > 2 {
		panic(fmt.Sprintf("%d requests of an idle process", hellos))
	}
	col.answer(http.StatusOK, "ok\n")
	eventually(t, "the control plane to be told that the collector is reached", func() bool { return last().Reachable })
	if got := last(); got.Error != "" {
		panic(fmt.Sprintf("%+v", got))
	}
	// A new stream may lead to another control plane: it is told as well.
	n = len(told())
	cp.drop <- struct{}{}
	eventually(t, "the new stream to be told", func() bool { got := told(); return len(got) == n+1 && got[n].Reachable })
}

// An exporter that never checks (one made with NewExporter, as before) has
// nothing to tell: the requests carry no word about the collector, and an
// idle process sends its one hello and no more.
func TestReporterSaysNothingOfTheCollectorWithoutACheck(t *testing.T) {
	cp := newReportCP(t, `{"active":[],"delivery_reports":true}`)
	col := newHealthCollector(t, http.StatusOK, "ok\n")
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond})
	defer func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }()
	eventually(t, "the hello", func() bool { _, hellos, _ := cp.seen(); return hellos == 1 })
	time.Sleep(100 * time.Millisecond)
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.hellos != 1 || len(cp.reach) != 0 || cp.bare != 1 {
		panic(fmt.Sprintf("%d hellos, %d of them about the collector: %+v", cp.hellos, len(cp.reach), cp.reach))
	}
}

// What the control plane was told is told again after a time: a report from
// another caller that named this instance and said the opposite does not
// stand for good. Often while the collector is not reached, seldom while it
// is.
func TestReporterTellsTheControlPlaneAgainAfterATime(t *testing.T) {
	cp := newReportCP(t, `{"active":[],"delivery_reports":true}`)
	col := newHealthCollector(t, http.StatusBadGateway, "no upstream")
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: col.srv.URL, FlushInterval: 2 * time.Millisecond, ReachCheck: 5 * time.Millisecond})
	sub := newSubscriber(cp.srv.URL, "svc", "i", "i")
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{Tick: 2 * time.Millisecond, RetellDown: 40 * time.Millisecond, RetellUp: time.Hour})
	defer func() { exp.Close(time.Second); rep.close(time.Second); sub.Close() }()
	told := func() []collectorNote {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		return append([]collectorNote(nil), cp.reach...)
	}
	start := time.Now()
	eventually(t, "the control plane to be told three times that the collector is not reached", func() bool { return len(told()) >= 3 })
	for _, n := range told() {
		if n.Reachable {
			panic(fmt.Sprintf("%+v", told()))
		}
	}
	// Not more often than the period: at most one more than the periods that went by.
	if got, most := len(told()), int(time.Since(start)/(40*time.Millisecond))+1; got > most {
		panic(fmt.Sprintf("told %d times in %v, the period is 40ms", got, time.Since(start)))
	}
	// Reached again: said once, and with RetellUp an hour away not again.
	col.answer(http.StatusOK, "ok\n")
	eventually(t, "the control plane to be told that the collector is reached", func() bool { got := told(); return got[len(got)-1].Reachable })
	n := len(told())
	checks := col.checks.Load()
	eventually(t, "more checks", func() bool { return col.checks.Load() >= checks+10 })
	if got := told(); len(got) != n {
		panic(fmt.Sprintf("told %d more times while the collector is reached", len(got)-n))
	}
}

func TestReporterRetellDefaults(t *testing.T) {
	rec := NewRecorder(1)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: "http://127.0.0.1:1", ReachCheck: 30 * time.Second})
	defer exp.Close(time.Second)
	sub := newSubscriber("http://127.0.0.1:1", "svc", "i", "i")
	defer sub.Close()
	rep := newReporter(rec, exp, sub, "svc", "i", reporterConfig{})
	defer rep.close(time.Second)
	if rep.cfg.RetellDown != 30*time.Second || rep.cfg.RetellUp != 5*time.Minute {
		panic(fmt.Sprintf("down %v, up %v", rep.cfg.RetellDown, rep.cfg.RetellUp))
	}
}
