package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptCollector is an in-process Collector for the drain rule: its
// drain-status is quiet unless a test changes it, and its Seal can be held.
type scriptCollector struct {
	mu       sync.Mutex
	st       DrainStatus
	sealHold chan struct{} // non-nil: Seal waits until it is closed
	sealing  chan struct{} // closed when the first Seal call arrives
	once     sync.Once
}

func newScriptCollector() *scriptCollector { return &scriptCollector{sealing: make(chan struct{})} }

func (c *scriptCollector) DrainStatus(context.Context, string) (DrainStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st, nil
}

// arrived makes drain-status change, as it does when events reach the
// collector: the quiet window starts over.
func (c *scriptCollector) arrived() {
	c.mu.Lock()
	c.st.EventCount++
	c.mu.Unlock()
}

func (c *scriptCollector) setPending(n int) {
	c.mu.Lock()
	c.st.Pending = n
	c.mu.Unlock()
}

func (c *scriptCollector) Seal(ctx context.Context, _ string) (SealResult, error) {
	c.once.Do(func() { close(c.sealing) })
	c.mu.Lock()
	hold := c.sealHold
	c.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return SealResult{}, ctx.Err()
		}
	}
	return SealResult{BundleURI: "file:///bundle/x"}, nil
}

func (c *scriptCollector) Delete(context.Context, string) error { return nil }

// The drain tests run with a "second" of 10ms: quiesce is 2 units (20ms),
// and the grace is set per test, far above the timer noise of a busy machine.
const testGrace = 400 * time.Millisecond

type deliveryEnv struct {
	t   *testing.T
	m   *Manager
	col *scriptCollector
	id  string
}

func newDeliveryEnv(t *testing.T, grace time.Duration, mut func(*StartRequest)) *deliveryEnv {
	t.Helper()
	col := newScriptCollector()
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: grace})
	t.Cleanup(m.Close)
	req := StartRequest{Name: "d", QuiesceSeconds: 2, MaxDrainSeconds: 1000}
	if mut != nil {
		mut(&req)
	}
	s, err := m.Start(req)
	if err != nil {
		t.Fatal(err)
	}
	return &deliveryEnv{t: t, m: m, col: col, id: s.ID}
}

// rep builds a report: buffered is what is neither delivered nor dropped.
func rep(service, instance string, recorded, delivered, dropped int64) Delivery {
	return Delivery{Service: service, Instance: instance, Recorded: recorded, Delivered: delivered, Dropped: dropped,
		Buffered: recorded - delivered - dropped}
}

func final(d Delivery) Delivery { d.Final = true; return d }

func failing(d Delivery, err string) Delivery { d.LastError = err; return d }

func (e *deliveryEnv) report(d Delivery) {
	e.t.Helper()
	if ok, err := e.m.ReportDelivery(e.id, d); err != nil || !ok {
		e.t.Fatalf("report %+v: accepted=%v err=%v", d, ok, err)
	}
}

// stop stops the session and returns it sealed, with how long the drain took.
func (e *deliveryEnv) stop() (Session, time.Duration) {
	e.t.Helper()
	began := time.Now()
	if _, err := e.m.Stop(e.id); err != nil {
		e.t.Fatal(err)
	}
	return e.sealed(), time.Since(began)
}

func (e *deliveryEnv) sealed() Session {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := e.m.Get(e.id); s.State.Terminal() {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	s, _ := e.m.Get(e.id)
	e.t.Fatalf("session never sealed: %+v", s)
	return s
}

func wantComplete(t *testing.T, s Session) {
	t.Helper()
	if s.State != StateSealed || s.Incomplete || len(s.IncompleteReasons) > 0 || len(s.DeliveryUnconfirmed) > 0 || s.DrainTimedOut {
		t.Fatalf("want a complete session: incomplete=%v reasons=%q unconfirmed=%q timed_out=%v delivery=%+v",
			s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed, s.DrainTimedOut, s.Delivery)
	}
}

func wantShortfall(t *testing.T, s Session, parts ...string) {
	t.Helper()
	if s.State != StateSealed || !s.Incomplete || len(s.IncompleteReasons) != 1 || s.DrainTimedOut || s.SealError != "" {
		t.Fatalf("want a session incomplete for one delivery shortfall: incomplete=%v reasons=%q unconfirmed=%q timed_out=%v",
			s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed, s.DrainTimedOut)
	}
	for _, p := range parts {
		if !strings.Contains(s.IncompleteReasons[0], p) {
			t.Fatalf("reason %q does not say %q", s.IncompleteReasons[0], p)
		}
	}
}

// What the verdict says for every kind of last report. The rule is
// conservative on purpose: "incomplete" needs evidence from the instance.
func TestDeliveryVerdict(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    Delivery
		want string // "", "incomplete" or "unconfirmed"
	}{
		{"everything delivered, final", final(rep("a", "i", 5, 5, 0)), ""},
		{"everything delivered, periodic report (instance gone since)", rep("a", "i", 5, 5, 0), ""},
		{"nothing recorded", final(rep("a", "i", 0, 0, 0)), ""},
		{"final with undelivered events", final(rep("a", "i", 5, 3, 0)), "incomplete"},
		{"final, nothing delivered", failing(final(rep("a", "i", 5, 0, 0)), "connection refused"), "incomplete"},
		{"dropped events", final(rep("a", "i", 5, 4, 1)), "incomplete"},
		{"dropped events, periodic report", rep("a", "i", 5, 4, 1), "incomplete"},
		{"went silent failing, never delivered", failing(rep("a", "i", 5, 0, 0), "connection refused"), "incomplete"},
		{"went silent holding events after delivering some", failing(rep("a", "i", 5, 3, 0), "timeout"), "unconfirmed"},
		{"went silent holding events, no error seen", rep("a", "i", 5, 0, 0), "unconfirmed"},
	} {
		short, unc := deliveryVerdict([]Delivery{tc.d})
		got := ""
		switch {
		case len(short) == 1 && len(unc) == 0:
			got = "incomplete"
		case len(short) == 0 && len(unc) == 1:
			got = "unconfirmed"
		case len(short)+len(unc) > 0:
			got = fmt.Sprintf("%q %q", short, unc)
		}
		if got != tc.want {
			t.Errorf("%s: verdict %q, want %q", tc.name, got, tc.want)
		}
	}

	short, unc := deliveryVerdict([]Delivery{
		failing(final(rep("orders-api", "host-1-ab12cd", 7, 0, 0)), "Post http://127.0.0.1:1/v1/ingest: connection refused"),
		final(rep("inventory", "host-2-ef34ab", 3, 3, 0)),
		final(rep("", "bare", 4, 2, 1)),
	})
	want := []string{
		"service orders-api (instance host-1-ab12cd): 0 of 7 recorded events reached the collector, 7 still undelivered (last error: Post http://127.0.0.1:1/v1/ingest: connection refused)",
		"instance bare: 2 of 4 recorded events reached the collector, 1 still undelivered, 1 dropped",
	}
	if !reflect.DeepEqual(short, want) || len(unc) != 0 {
		t.Fatalf("reasons\n got %q\nwant %q\nunconfirmed %q", short, want, unc)
	}

	// Many instances: the list is bounded.
	var many []Delivery
	for i := range 40 {
		many = append(many, final(rep("svc", fmt.Sprintf("i%02d", i), 1, 0, 0)))
	}
	if short, _ := deliveryVerdict(many); len(short) != maxDeliveryReasons+1 || short[maxDeliveryReasons] != "and 24 more instance(s)" {
		t.Fatalf("%d reasons, last %q", len(short), short[len(short)-1])
	}
}

// A healthy session: reports with nothing left to deliver do not hold the
// drain and do not flag anything. Neither does a session without reports (an
// SDK older than the reports, or header mode with nothing recorded).
func TestDrainHealthyDeliveryIsNotFlagged(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.report(rep("orders-api", "o-1", 4, 4, 0))
	e.report(final(rep("inventory", "i-1", 2, 2, 0)))
	s, took := e.stop()
	wantComplete(t, s)
	if took >= testGrace {
		t.Fatalf("a healthy drain took %v: it must not wait the grace (%v)", took, testGrace)
	}
	if len(s.Delivery) != 2 || s.Delivery[0].Service != "inventory" || s.Delivery[1].Service != "orders-api" || s.Delivery[1].At.IsZero() {
		t.Fatalf("delivery %+v", s.Delivery)
	}

	old := newDeliveryEnv(t, testGrace, nil)
	s, took = old.stop()
	wantComplete(t, s)
	if len(s.Delivery) != 0 || took >= testGrace {
		t.Fatalf("no reports: delivery %+v, drain %v", s.Delivery, took)
	}
}

// The walkthrough: one service cannot reach the collector. The drain waits
// the grace for it, then seals the session incomplete with a reason that
// names it. Nothing else marks this session: the collector saw no problem.
func TestDrainMarksUndeliveredEventsIncomplete(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.report(rep("inventory", "i-1", 1, 1, 0))
	e.report(failing(rep("orders-api", "o-1", 2, 0, 0), "dial tcp 127.0.0.1:1: connect: connection refused"))
	if _, err := e.m.Stop(e.id); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	e.report(failing(final(rep("orders-api", "o-1", 2, 0, 0)), "dial tcp 127.0.0.1:1: connect: connection refused"))
	s := e.sealed()
	wantShortfall(t, s, "service orders-api (instance o-1)", "0 of 2 recorded events", "connection refused")
	if took := time.Since(began); took < testGrace-20*time.Millisecond || took > testGrace+2*time.Second {
		t.Fatalf("sealed after %v: the drain waits the grace (%v) for the instance, and no longer", took, testGrace)
	}
	if strings.Contains(s.IncompleteReasons[0], "inventory") {
		t.Fatalf("inventory delivered everything: %q", s.IncompleteReasons)
	}
}

// Risk 1: a collector that is slow, or down for a while, and delivers within
// the drain must never produce "incomplete".
func TestDrainWaitsForSlowAndRecoveringDelivery(t *testing.T) {
	t.Run("slow but progressing for longer than the grace", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.report(rep("svc", "i", 10, 0, 0))
		go func() {
			for n := int64(1); n <= 10; n++ {
				time.Sleep(testGrace / 4)
				e.col.arrived()
				_, _ = e.m.ReportDelivery(e.id, final(rep("svc", "i", 10, n, 0)))
			}
		}()
		s, took := e.stop()
		wantComplete(t, s)
		if took < 2*testGrace {
			t.Fatalf("sealed after %v, before the last events arrived", took)
		}
		if d := s.Delivery[0]; d.Delivered != 10 || d.Buffered != 0 {
			t.Fatalf("delivery %+v", d)
		}
	})
	t.Run("down, back within the grace", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.report(failing(rep("svc", "i", 3, 0, 0), "503"))
		go func() {
			time.Sleep(testGrace / 4)
			_, _ = e.m.ReportDelivery(e.id, failing(final(rep("svc", "i", 3, 0, 0)), "503"))
			time.Sleep(testGrace / 4)
			e.col.arrived()
			_, _ = e.m.ReportDelivery(e.id, final(rep("svc", "i", 3, 3, 0)))
		}()
		s, took := e.stop()
		wantComplete(t, s)
		if took < testGrace/2 {
			t.Fatalf("sealed after %v, before the events arrived", took)
		}
	})
	// The grace counts from the stop at the earliest: an instance that was
	// stuck long before gets all of it once it has been told to flush.
	t.Run("stuck long before the stop", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.report(failing(rep("svc", "i", 3, 0, 0), "503"))
		time.Sleep(testGrace + 50*time.Millisecond)
		go func() {
			time.Sleep(testGrace / 2)
			e.col.arrived()
			_, _ = e.m.ReportDelivery(e.id, final(rep("svc", "i", 3, 3, 0)))
		}()
		s, took := e.stop()
		wantComplete(t, s)
		if took < testGrace/2 {
			t.Fatalf("sealed after %v", took)
		}
	})
}

// Risk 2: events recorded during the drain, after the instance sent its
// final report. The counts are cumulative, so the later report replaces the
// final one; the drain waits for the new events from the moment it hears of
// them, however long it has been running.
func TestDrainEventsRecordedAfterTheFinalReport(t *testing.T) {
	t.Run("delivered", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.setPending(1) // async work keeps the drain open
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		e.report(final(rep("worker", "w", 5, 5, 0)))
		time.Sleep(testGrace + 100*time.Millisecond) // longer than the grace: the drain is old now
		e.report(final(rep("worker", "w", 8, 5, 0))) // the worker recorded three more
		e.col.setPending(0)
		heard := time.Now()
		time.Sleep(testGrace / 2) // the drain must be waiting for them, not sealing
		if s, _ := e.m.Get(e.id); s.State != StateDraining {
			t.Fatalf("sealed %v after hearing of new events, without waiting for them: %+v", time.Since(heard), s)
		}
		e.col.arrived()
		e.report(final(rep("worker", "w", 8, 8, 0)))
		s := e.sealed()
		wantComplete(t, s)
		if d := s.Delivery[0]; d.Recorded != 8 || d.Delivered != 8 || !d.Final {
			t.Fatalf("delivery %+v", d)
		}
	})
	t.Run("never delivered", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		e.report(final(rep("worker", "w", 5, 5, 0)))
		e.report(final(rep("worker", "w", 8, 5, 0)))
		heard := time.Now()
		s := e.sealed()
		wantShortfall(t, s, "service worker", "5 of 8 recorded events", "3 still undelivered")
		if took := time.Since(heard); took < testGrace-20*time.Millisecond {
			t.Fatalf("sealed %v after the report, before the grace (%v)", took, testGrace)
		}
	})
	// A report sent before the instance knew of the drain cannot take back a
	// final one, and one with lower counters is an old one: ignored.
	t.Run("old reports", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.report(final(rep("svc", "i", 8, 8, 0)))
		e.report(rep("svc", "i", 9, 9, 0))
		for _, old := range []Delivery{rep("svc", "i", 8, 8, 0), rep("svc", "i", 10, 7, 0)} {
			if ok, err := e.m.ReportDelivery(e.id, old); ok || err != nil {
				t.Fatalf("report %+v behind the stored one: accepted=%v err=%v", old, ok, err)
			}
		}
		s, _ := e.m.Get(e.id)
		if d := s.Delivery[0]; len(s.Delivery) != 1 || d.Recorded != 9 || d.Delivered != 9 || !d.Final {
			t.Fatalf("delivery %+v", s.Delivery)
		}
	})
}

// An instance that is gone at the stop is judged on its last report, after
// the grace: incomplete only with evidence that it was failing.
func TestDrainJudgesVanishedInstanceOnItsLastReport(t *testing.T) {
	e := newDeliveryEnv(t, 100*time.Millisecond, nil)
	e.report(rep("gone", "g", 5, 3, 0))
	e.report(rep("fine", "f", 5, 5, 0))
	s, took := e.stop()
	if s.Incomplete || len(s.IncompleteReasons) > 0 || len(s.DeliveryUnconfirmed) != 1 ||
		!strings.Contains(s.DeliveryUnconfirmed[0], "service gone (instance g): 3 of 5 recorded events") ||
		!strings.Contains(s.DeliveryUnconfirmed[0], "unknown") {
		t.Fatalf("a periodic report that left events open: incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
	}
	if took < 80*time.Millisecond {
		t.Fatalf("sealed after %v: the instance might still have delivered", took)
	}

	e = newDeliveryEnv(t, 100*time.Millisecond, nil)
	e.report(failing(rep("gone", "g", 5, 0, 0), "no route to host"))
	s, _ = e.stop()
	wantShortfall(t, s, "service gone (instance g): 0 of 5", "no route to host")
}

// Dropped events are lost for good: nothing to wait for.
func TestDrainDroppedEventsAreIncompleteWithoutWaiting(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.report(final(rep("svc", "i", 5, 3, 2)))
	s, took := e.stop()
	wantShortfall(t, s, "3 of 5 recorded events", "2 dropped")
	if took >= testGrace {
		t.Fatalf("the drain waited %v for events that are gone", took)
	}
}

// Everything stays bounded by max_drain.
func TestDrainDeliveryIsBoundedByMaxDrain(t *testing.T) {
	e := newDeliveryEnv(t, time.Hour, func(r *StartRequest) { r.MaxDrainSeconds = 10 }) // 100ms
	e.report(final(rep("svc", "i", 5, 0, 0)))
	s, took := e.stop()
	if !s.DrainTimedOut || !s.Incomplete || len(s.IncompleteReasons) != 1 || took > 5*time.Second {
		t.Fatalf("drain %v: %+v", took, s)
	}
}

// With the check turned off reports are kept and shown, nothing is waited
// for and nothing is marked incomplete.
func TestDeliveryCheckOff(t *testing.T) {
	e := newDeliveryEnv(t, -1, nil)
	e.report(final(rep("svc", "i", 5, 0, 0)))
	s, took := e.stop()
	if s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 || len(s.Delivery) != 1 || took > 2*time.Second {
		t.Fatalf("check off: drain %v %+v", took, s)
	}
}

func postDelivery(t *testing.T, url, id, contentType, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/sessions/"+id+"/delivery", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The endpoint: what it accepts, what it refuses, and what it stores of
// self-reported text.
func TestDeliveryEndpointBounds(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	srv := httptest.NewServer(e.m.Handler())
	defer srv.Close()
	const ok = `{"service":"orders-api","instance":"o-1","recorded":3,"delivered":2,"dropped":0,"buffered":1}`
	post := func(body string) (int, string) { return postDelivery(t, srv.URL, e.id, "application/json", body) }

	if code, body := post(ok); code != http.StatusOK || !strings.Contains(body, `"accepted":true`) {
		t.Fatalf("a valid report: %d %s", code, body)
	}
	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"not JSON":                    {`{"instance":`, http.StatusBadRequest},
		"no instance":                 {`{"service":"a","recorded":1,"delivered":1}`, http.StatusBadRequest},
		"instance of control bytes":   {`{"instance":"\u0001\u0002","recorded":1,"delivered":1}`, http.StatusBadRequest},
		"negative counter":            {`{"instance":"i","recorded":1,"delivered":-1,"buffered":2}`, http.StatusBadRequest},
		"counters do not add up":      {`{"instance":"i","recorded":5,"delivered":1,"buffered":1}`, http.StatusBadRequest},
		"more delivered than there":   {`{"instance":"i","recorded":1,"delivered":2}`, http.StatusBadRequest},
		"counter beyond 2^53":         {`{"instance":"i","recorded":9007199254740993,"delivered":9007199254740993}`, http.StatusBadRequest},
		"counter that is not a count": {`{"instance":"i","recorded":"many"}`, http.StatusBadRequest},
		"body over 4 KB":              {`{"instance":"i","last_error":"` + strings.Repeat("x", 5000) + `"}`, http.StatusRequestEntityTooLarge},
	} {
		if code, body := post(tc.body); code != tc.want {
			t.Errorf("%s: %d %s, want %d", name, code, body, tc.want)
		}
	}
	// The guard in front of every endpoint applies: a body needs the JSON
	// content type (which a cross-site form cannot send), and a foreign Host
	// is refused.
	if code, _ := postDelivery(t, srv.URL, e.id, "text/plain", ok); code != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain body: %d, want 415", code)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sessions/"+e.id+"/delivery", strings.NewReader(ok))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "evil.example"
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("foreign Host: %v %v, want 403", resp, err)
	}
	if code, _ := postDelivery(t, srv.URL, "ts_unknown", "application/json", ok); code != http.StatusNotFound {
		t.Errorf("unknown session: %d, want 404", code)
	}

	// Self-reported text is cleaned before it is stored: names like those of
	// the stream, the error printable, bounded and without URL credentials.
	long := strings.Repeat("n", 300)
	body, _ := json.Marshal(map[string]any{"service": "pay\x1b[31m\nments", "instance": long, "recorded": 2, "buffered": 2,
		"last_error": "Post \"https://user:s3cret@collector.internal/v1/ingest\": dial\ttcp: \x00refused " + strings.Repeat("e", 400)})
	if code, resp := post(string(body)); code != http.StatusOK {
		t.Fatalf("report with unclean text: %d %s", code, resp)
	}
	s, _ := e.m.Get(e.id)
	var got Delivery
	for _, d := range s.Delivery {
		if d.Service == "pay[31mments" {
			got = d
		}
	}
	if got.Instance != long[:128] || len(got.LastError) != maxDeliveryError || strings.Contains(got.LastError, "s3cret") ||
		!strings.HasPrefix(got.LastError, `Post "https://collector.internal/v1/ingest": dialtcp: refused eee`) {
		t.Fatalf("stored %+v (last_error %d bytes)", got, len(got.LastError))
	}

	// The session shows the reports, and nothing else changed.
	resp, err := http.Get(srv.URL + "/v1/sessions/" + e.id)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		State      State
		Incomplete bool
		Delivery   []Delivery
	}
	err = json.NewDecoder(resp.Body).Decode(&doc)
	resp.Body.Close()
	if err != nil || doc.State != StateActive || doc.Incomplete || len(doc.Delivery) != 2 || doc.Delivery[0].Buffered != 1 || doc.Delivery[0].At.IsZero() {
		t.Fatalf("session document: %+v %v", doc, err)
	}
}

// At most MaxDeliveryInstances instances per session: further ones get 409
// (the SDK stops reporting that session), known ones can still update, and
// the session says that it does not account for everyone.
func TestDeliveryInstanceLimit(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	for i := range MaxDeliveryInstances {
		e.report(rep("svc", fmt.Sprintf("i%03d", i), 1, 1, 0))
	}
	if ok, err := e.m.ReportDelivery(e.id, rep("svc", "one-too-many", 1, 1, 0)); ok || !errors.Is(err, ErrBadState) {
		t.Fatalf("instance %d: accepted=%v err=%v, want ErrBadState (409)", MaxDeliveryInstances+1, ok, err)
	}
	e.report(rep("svc", "i007", 2, 2, 0))
	s, _ := e.stop()
	if len(s.Delivery) != MaxDeliveryInstances || s.Incomplete || len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "more than 256 instances") {
		t.Fatalf("delivery %d incomplete=%v unconfirmed=%q", len(s.Delivery), s.Incomplete, s.DeliveryUnconfirmed)
	}
}

// Reports are for running sessions: 409 once the drain has stopped looking
// (the verdict is being made), after the seal, and after an abort.
func TestDeliveryReportsAfterTheDrainGet409(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, func(r *StartRequest) { r.QuiesceSeconds = 20 })
	hold := make(chan struct{})
	e.col.sealHold = hold
	srv := httptest.NewServer(e.m.Handler())
	defer srv.Close()
	const body = `{"instance":"late","recorded":3,"delivered":0,"buffered":3,"final":true}`

	e.report(final(rep("svc", "i", 2, 2, 0)))
	if _, err := e.m.Stop(e.id); err != nil {
		t.Fatal(err)
	}
	// DRAINING is still a running session.
	e.report(final(rep("svc", "i", 3, 3, 0)))
	<-e.col.sealing
	if code, resp := postDelivery(t, srv.URL, e.id, "application/json", body); code != http.StatusConflict || !strings.Contains(resp, "being sealed") {
		t.Fatalf("report while the session is being sealed: %d %s, want 409", code, resp)
	}
	close(hold)
	s := e.sealed()
	wantComplete(t, s) // the late report was not let in, so it cannot flag the session either
	if code, resp := postDelivery(t, srv.URL, e.id, "application/json", body); code != http.StatusConflict || !strings.Contains(resp, "SEALED") {
		t.Fatalf("report after the seal: %d %s, want 409", code, resp)
	}
	if got, _ := e.m.Get(e.id); len(got.Delivery) != 1 || got.Delivery[0].Recorded != 3 {
		t.Fatalf("delivery changed after the seal: %+v", got.Delivery)
	}

	a := newDeliveryEnv(t, testGrace, nil)
	a.report(rep("svc", "i", 2, 0, 0))
	if _, err := a.m.Abort(a.id); err != nil {
		t.Fatal(err)
	}
	if ok, err := a.m.ReportDelivery(a.id, rep("svc", "i", 3, 0, 0)); ok || !errors.Is(err, ErrBadState) {
		t.Fatalf("report after abort: accepted=%v err=%v", ok, err)
	}
	// An aborted session is not judged: it was never drained.
	if got, _ := a.m.Get(a.id); got.Incomplete || len(got.IncompleteReasons) > 0 || len(got.Delivery) != 1 {
		t.Fatalf("aborted: %+v", got)
	}
}

// The active set names the draining sessions, so an SDK knows when to flush
// and send its final report. The field is absent while none is draining: an
// SDK that does not know it sees what it always saw.
func TestActiveSetNamesDrainingSessions(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.setPending(1)
	b, _ := json.Marshal(e.m.Active())
	if strings.Contains(string(b), "draining") {
		t.Fatalf("active set without a draining session: %s", b)
	}
	if _, err := e.m.Stop(e.id); err != nil {
		t.Fatal(err)
	}
	a := e.m.Active()
	if len(a.Active) != 1 || len(a.Draining) != 1 || a.Draining[0] != e.id {
		t.Fatalf("active set %+v", a)
	}
	e.col.setPending(0)
	e.sealed()
	if a := e.m.Active(); len(a.Active) != 0 || len(a.Draining) != 0 {
		t.Fatalf("active set after the seal %+v", a)
	}
}

// A seal that succeeds on a background retry knows nothing of events that
// never reached the collector: it must not clear the flag they set.
func TestSealRetryKeepsDeliveryShortfall(t *testing.T) {
	for _, shortfall := range []bool{true, false} {
		c := newFlaky()
		m := newRetryMgr(t, c, nil)
		s, err := m.Start(StartRequest{Name: "r", QuiesceSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		if shortfall {
			if _, err := m.ReportDelivery(s.ID, final(rep("orders-api", "o", 4, 1, 3))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := m.Stop(s.ID); err != nil {
			t.Fatal(err)
		}
		got := waitState(t, m, s.ID, StateSealed)
		if !got.SealPending || !got.Incomplete || (len(got.IncompleteReasons) == 1) != shortfall {
			t.Fatalf("shortfall=%v, seal failing: %+v", shortfall, got)
		}
		c.failing.Store(false) // the collector's seal now says "complete"
		got = waitSession(t, m, s.ID, "sealed by a retry", func(s Session) bool { return !s.SealPending })
		if got.SealError != "" || got.BundleURI == "" || got.Incomplete != shortfall || (len(got.IncompleteReasons) == 1) != shortfall {
			t.Fatalf("shortfall=%v, after the seal retry: incomplete=%v reasons=%q seal_error=%q", shortfall, got.Incomplete, got.IncompleteReasons, got.SealError)
		}
	}
}

// The new fields survive a restart on every path of the file store: the
// journal, the snapshot and the archive. And a store file written by a
// control plane that knows more fields than this one still loads.
func TestFileStoreKeepsDeliveryFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	live := storeSession(1, "live", StateActive)
	live.Delivery = []Delivery{{Service: "orders-api", Instance: "o-1", Recorded: 7, Delivered: 5, Buffered: 2, LastError: "boom", At: at}}
	done := storeSession(2, "done", StateSealed)
	done.Incomplete = true
	done.Delivery = []Delivery{
		{Service: "inventory", Instance: "i-1", Recorded: 3, Delivered: 3, Final: true, At: at},
		{Service: "orders-api", Instance: "o-1", Recorded: 7, Delivered: 0, Dropped: 1, Buffered: 6, LastError: "connection refused", Final: true, At: at},
	}
	done.IncompleteReasons = []string{"service orders-api (instance o-1): 0 of 7 recorded events reached the collector"}
	done.DeliveryUnconfirmed = []string{"service x (instance y): unknown"}
	mustPut(t, fs, live, done)

	check := func(what string, s *FileStore) {
		t.Helper()
		for _, want := range []Session{live, done} {
			got, ok, err := s.GetErr(want.ID)
			if err != nil || !ok || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %s\n got %+v (%v %v)\nwant %+v", what, want.ID, got, ok, err, want)
			}
		}
	}
	check("as written", fs)
	check("replayed from the journal", wantSameAfterReopen(t, fs))
	if err := fs.Compact(); err != nil {
		t.Fatal(err)
	}
	check("read from the snapshot", wantSameAfterReopen(t, fs))
	fs.Evict(done.ID)
	if _, hot := fs.MemStore.Get(done.ID); hot {
		t.Fatal("the session was not archived")
	}
	check("read from the archive", fs)
	check("read from the archive after a restart", wantSameAfterReopen(t, fs))

	// A Manager started on that store keeps the verdict of the finished
	// session and the reports of the running one.
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	re := openStore(t, path)
	m := NewManager(Config{Store: re, Collector: newScriptCollector(), PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond})
	t.Cleanup(m.Close)
	if got, _ := m.Get(done.ID); !got.Incomplete || len(got.IncompleteReasons) != 1 || len(got.Delivery) != 2 {
		t.Fatalf("finished session after a restart: %+v", got)
	}
	if ok, err := m.ReportDelivery(live.ID, rep("orders-api", "o-1", 9, 9, 0)); !ok || err != nil {
		t.Fatalf("report after a restart: %v %v", ok, err)
	}
	if got, _ := m.Get(live.ID); len(got.Delivery) != 1 || got.Delivery[0].Delivered != 9 {
		t.Fatalf("running session after a restart: %+v", got.Delivery)
	}

	// Unknown fields are ignored, in a snapshot and in a journal record.
	path2 := filepath.Join(t.TempDir(), "newer.json")
	newer := `[{"id":"ts_newer0001","name":"n","mode":"header","state":"SEALED","created_at":"2026-05-06T07:08:09Z",
	  "delivery":[{"instance":"i","recorded":1,"delivered":1,"buffered":0,"at":"2026-05-06T07:08:09Z","a_later_field":{"x":1}}],
	  "a_later_field":[1,2,3]}]`
	appendRaw(t, path2, newer)
	appendRaw(t, path2+".journal", `{"op":"put","a_later_field":true,"session":{"id":"ts_newer0002","state":"ACTIVE","created_at":"2026-05-06T07:09:09Z","a_later_field":1}}`+"\n")
	fs2 := openStore(t, path2)
	if got := fs2.List(); len(got) != 2 || got[0].ID != "ts_newer0001" || len(got[0].Delivery) != 1 || got[1].ID != "ts_newer0002" {
		t.Fatalf("store file with fields this version does not know: %+v", got)
	}
}

// A control plane restarted during a drain has the reports (they are stored
// with the session) and gives every instance the grace again.
func TestDrainResumedAfterRestartStillJudgesDelivery(t *testing.T) {
	store := NewMemStore()
	col := newScriptCollector()
	cfg := Config{Store: store, Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: 150 * time.Millisecond}
	col.setPending(1)
	m := NewManager(cfg)
	s, err := m.Start(StartRequest{Name: "r", QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ReportDelivery(s.ID, failing(final(rep("orders-api", "o", 2, 0, 0)), "connection refused")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		t.Fatal(err)
	}
	m.Close() // the process ends mid-drain

	col.setPending(0)
	began := time.Now()
	m2 := NewManager(cfg)
	t.Cleanup(m2.Close)
	got := waitState(t, m2, s.ID, StateSealed)
	wantShortfall(t, got, "service orders-api", "0 of 2")
	if took := time.Since(began); took < 130*time.Millisecond {
		t.Fatalf("sealed %v after the restart, without the grace", took)
	}
}
