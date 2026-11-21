package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// askCollector is a scriptCollector that can also be asked about instances
// by name (InstanceCounter), as a collector of this version can.
type askCollector struct {
	*scriptCollector
	mu    sync.Mutex
	has   map[string]int64 // by deliveryKey: what it has, in its list or not
	fail  bool             // the question is not answered
	asked [][]InstanceCount
}

func (c *askCollector) InstanceCounts(_ context.Context, _ string, ask []InstanceCount) ([]InstanceCount, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asked = append(c.asked, append([]InstanceCount(nil), ask...))
	if c.fail {
		return nil, errors.New("collector POST instance-counts: 404 Not Found")
	}
	out := make([]InstanceCount, len(ask))
	for i, q := range ask {
		out[i] = InstanceCount{Service: q.Service, Instance: q.Instance, Events: c.has[deliveryKey(q.Service, q.Instance)]}
	}
	return out, nil
}

func (c *askCollector) askedFor(service, instance string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ask := range c.asked {
		for _, q := range ask {
			if q.Service == service && q.Instance == instance {
				return true
			}
		}
	}
	return false
}

// newAskEnv is newDeliveryEnv with a collector whose list of instances is
// cut short: it names crowd instances only (made-up ones that sort first),
// and answers for the others when asked by name, unless fail.
func newAskEnv(t *testing.T, fail bool, has map[string]int64) (*deliveryEnv, *askCollector) {
	t.Helper()
	col := &askCollector{scriptCollector: newScriptCollector(), has: has, fail: fail}
	col.st.InstancesTruncated = true
	for i := range 512 {
		col.st.Instances = append(col.st.Instances, InstanceCount{Service: fmt.Sprintf("a%04d", i), Instance: fmt.Sprintf("x%04d", i), Events: 1})
	}
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: testGrace})
	t.Cleanup(m.Close)
	s, err := m.Start(StartRequest{Name: "d", QuiesceSeconds: 2, MaxDrainSeconds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return &deliveryEnv{t: t, m: m, col: col.scriptCollector, id: s.ID}, col
}

// The review's F3 (c): a caller ingests events of 513 made-up instances, so
// that the collector's list is cut before the instance with the shortfall,
// and forges its report. The control plane asks the collector about the
// instances whose reports it holds by name; a list can be crowded out, an
// answer to a question cannot.
func TestDrainAsksTheCollectorByName(t *testing.T) {
	real := deliveryKey("real", "real-1")
	t.Run("a forged report of everything delivered", func(t *testing.T) {
		e, col := newAskEnv(t, false, map[string]int64{real: 0})
		e.report(final(rep("real", "real-1", 10, 0, 0)))
		e.report(final(rep("real", "real-1", 10, 10, 0))) // forged
		s, _ := e.stop()
		wantShortfall(t, s, "service real (instance real-1): 0 of 10 recorded events reached the collector")
		if !col.askedFor("real", "real-1") || col.askedFor("a0000", "x0000") {
			t.Fatalf("asked %v: want the reporting instance, and none that the list names", col.asked)
		}
	})
	t.Run("an honest report", func(t *testing.T) {
		e, _ := newAskEnv(t, false, map[string]int64{real: 10})
		e.report(final(rep("real", "real-1", 10, 10, 0)))
		wantComplete(t, func() Session { s, _ := e.stop(); return s }())
	})
	// The collector does not answer (one from before the question, or one
	// that fails). The instance's word is all there is, and that never makes
	// a session complete.
	t.Run("no answer: an honest report is unconfirmed", func(t *testing.T) {
		e, _ := newAskEnv(t, true, nil)
		e.report(final(rep("real", "real-1", 10, 10, 0)))
		s, _ := e.stop()
		if s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 ||
			!strings.Contains(s.DeliveryUnconfirmed[0], "service real (instance real-1): 10 of 10 recorded events were delivered by the instance's own count (the collector's count for the instance was not available)") {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
	})
	t.Run("no answer: a final shortfall is not taken back by a later report", func(t *testing.T) {
		e, _ := newAskEnv(t, true, nil)
		e.report(final(rep("real", "real-1", 10, 0, 0)))
		e.report(final(rep("real", "real-1", 10, 10, 0))) // forged, or true: nothing but the word says so
		s, _ := e.stop()
		wantShortfall(t, s, "service real (instance real-1): 10 of 10 recorded events were delivered by the instance's own count", "an earlier final report of it held undelivered events")
	})
	t.Run("no answer: dropped events are incomplete", func(t *testing.T) {
		e, _ := newAskEnv(t, true, nil)
		e.report(final(rep("real", "real-1", 10, 0, 10)))
		s, _ := e.stop()
		wantShortfall(t, s, "service real (instance real-1)", "10 dropped by the SDK")
	})
	// The connected instances are asked about as well: one that took part and
	// sent no report is found although the list does not name it.
	t.Run("a connected instance the list leaves out", func(t *testing.T) {
		e, col := newAskEnv(t, false, map[string]int64{deliveryKey("orders-api", "o-1"): 3})
		defer follow(e.m, "orders-api", "o-1")()
		s, took := e.stop()
		wantShortfall(t, s, "service orders-api (instance o-1): the collector has 3 events from it, but none of its delivery reports for the session arrived")
		if took < testGrace-20*time.Millisecond || !col.askedFor("orders-api", "o-1") {
			t.Fatalf("sealed after %v, asked %d times", took, len(col.asked))
		}
	})
	t.Run("no answer for a connected instance without a report", func(t *testing.T) {
		e, _ := newAskEnv(t, true, nil)
		defer follow(e.m, "orders-api", "o-1")()
		defer follow(e.m, "inventory", "i-1")()
		hello(t, e.m, "orders-api", "o-1")
		hello(t, e.m, "inventory", "i-1")
		s, took := e.stop()
		if s.Incomplete || len(s.DeliveryUnconfirmed) != 1 ||
			s.DeliveryUnconfirmed[0] != "the collector's list of instances was cut short and it did not answer for 2 connected instance(s) that sent no report, so whether they took part is unknown" {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
		if took >= testGrace/2 {
			t.Fatalf("the drain waited %v", took)
		}
	})
}

// A list that is not cut short is not followed by a question.
func TestDrainDoesNotAskWithoutNeed(t *testing.T) {
	col := &askCollector{scriptCollector: newScriptCollector().counting()}
	m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: testGrace})
	t.Cleanup(m.Close)
	s, err := m.Start(StartRequest{Name: "d", QuiesceSeconds: 2, MaxDrainSeconds: 1000})
	if err != nil {
		t.Fatal(err)
	}
	e := &deliveryEnv{t: t, m: m, col: col.scriptCollector, id: s.ID}
	e.report(final(rep("svc", "i", 2, 2, 0)))
	col.scriptCollector.has("svc", "i", 2)
	wantComplete(t, func() Session { s, _ := e.stop(); return s }())
	if len(col.asked) != 0 {
		t.Fatalf("asked %v", col.asked)
	}
}

// The review's F3 (e): a caller fills the 256 places a session has for
// reporting instances, so that the report of the instance with the shortfall
// is refused. A refused report is judged by what it said: one that said that
// events were lost makes the session incomplete, naming the instance. The
// others are counted in delivery_unconfirmed.
func TestDeliveryRefusedReportsAreJudgedByTheirWord(t *testing.T) {
	fill := func(e *deliveryEnv) {
		e.col.counting()
		for i := range MaxDeliveryInstances {
			e.report(final(rep("f", fmt.Sprintf("f-%03d", i), 1, 1, 0)))
			e.col.has("f", fmt.Sprintf("f-%03d", i), 1)
		}
	}
	refused := func(e *deliveryEnv, d Delivery) {
		e.t.Helper()
		if ok, err := e.m.ReportDelivery(e.id, d); ok || !errors.Is(err, ErrBadState) || !errors.Is(err, errDeliveryFull) {
			e.t.Fatalf("report %+v: accepted=%v err=%v, want it refused for the cap", d, ok, err)
		}
	}
	t.Run("dropped events", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		refused(e, final(rep("real", "real-1", 10, 0, 10)))
		refused(e, rep("fine", "fine-1", 4, 4, 0))
		s, took := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 ||
			s.IncompleteReasons[0] != "service real (instance real-1): its delivery report said that events were dropped or left undelivered; the report was refused because the session already keeps reports of 256 instances" ||
			len(s.DeliveryUnconfirmed) != 1 || s.DeliveryUnconfirmed[0] != "more than 256 instances reported; the reports of 1 others were refused and are not accounted for" {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
		if took >= testGrace {
			t.Fatalf("drain %v", took)
		}
	})
	t.Run("a final report that still held events, and a later one that takes it back", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		refused(e, rep("real", "real-1", 10, 0, 0)) // periodic: it may still deliver
		refused(e, final(rep("real", "real-1", 10, 4, 0)))
		refused(e, final(rep("real", "real-1", 10, 10, 0)))
		s, _ := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || !strings.Contains(s.IncompleteReasons[0], "service real (instance real-1)") || len(s.DeliveryUnconfirmed) != 0 {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
	})
	// A connected instance whose report was refused is not taken for one whose
	// reports do not arrive.
	t.Run("refused and connected, with its events at the collector", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		defer follow(e.m, "fine", "fine-1")()
		e.col.has("fine", "fine-1", 4)
		refused(e, final(rep("fine", "fine-1", 4, 4, 0)))
		s, took := e.stop()
		if s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "the reports of 1 others were refused") {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
		if took >= testGrace/2 {
			t.Fatalf("the drain waited %v for an instance that reported", took)
		}
	})
	// What is remembered of the refused is bounded too. Beyond the bound a
	// connected instance with events and no stored report reads as one whose
	// reports do not arrive: filling the bound cannot make a session complete.
	t.Run("more refused instances than are remembered", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		for i := range maxDeliveryRefused {
			refused(e, final(rep("g", fmt.Sprintf("g-%04d", i), 1, 1, 0)))
		}
		defer follow(e.m, "real", "real-1")()
		e.col.has("real", "real-1", 4)
		refused(e, final(rep("real", "real-1", 10, 4, 6)))
		s, _ := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || !strings.Contains(s.IncompleteReasons[0], "service real (instance real-1): the collector has 4 events from it, but none of its delivery reports") ||
			len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "the reports of more than 1024 others were refused") {
			t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
		}
	})
}

// The review's F6: forty instances with a shortfall are sixteen reasons and
// "and 24 more", once. (The list used to be cut twice, and said "and 1 more".)
func TestDeliveryReasonsAreBoundedOnce(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.counting()
	for i := range 30 {
		e.report(final(rep("svc", fmt.Sprintf("i%02d", i), 1, 0, 1)))
	}
	for i := range 10 { // connected, events at the collector, no report
		defer follow(e.m, "quiet", fmt.Sprintf("q%02d", i))()
		e.col.has("quiet", fmt.Sprintf("q%02d", i), 1)
	}
	for i := range 20 { // connected, never heard of, nothing at the collector
		defer follow(e.m, "blocked", fmt.Sprintf("b%02d", i))()
	}
	s, _ := e.stop()
	if !s.Incomplete || len(s.IncompleteReasons) != maxDeliveryReasons+1 || s.IncompleteReasons[maxDeliveryReasons] != "and 24 more" ||
		!strings.HasPrefix(s.IncompleteReasons[0], "service svc (instance i00)") {
		t.Fatalf("%d reasons, first %q, last %q", len(s.IncompleteReasons), s.IncompleteReasons[0], s.IncompleteReasons[len(s.IncompleteReasons)-1])
	}
	if len(s.DeliveryUnconfirmed) != maxDeliveryReasons+1 || s.DeliveryUnconfirmed[maxDeliveryReasons] != "and 4 more" {
		t.Fatalf("%d unconfirmed, last %q", len(s.DeliveryUnconfirmed), s.DeliveryUnconfirmed[len(s.DeliveryUnconfirmed)-1])
	}
	if got := capReasons([]string{"a", "b"}); len(got) != 2 {
		t.Fatalf("a short list was changed: %q", got)
	}
}

// Mutation that survived the review: once the reports are closed the
// collector is asked again, and the verdict is made from that answer. An
// event that was delivered and reported between the drain's last look and
// the close must not read as missing.
func TestDrainJudgesAgainstCountsReadAfterTheReportsClosed(t *testing.T) {
	e := newDeliveryEnv(t, time.Hour, func(r *StartRequest) { r.MaxDrainSeconds = 10 }) // 100ms
	e.col.counting().has("svc", "i", 3)
	e.col.after = func(st DrainStatus) DrainStatus {
		// What the collector answers once the reports are closed: the last two
		// events have arrived meanwhile.
		e.m.mu.Lock()
		closed := e.m.delivery[e.id] != nil && e.m.delivery[e.id].closed
		e.m.mu.Unlock()
		if closed {
			st.Instances = []InstanceCount{{Service: "svc", Instance: "i", Events: 5}}
		}
		return st
	}
	e.report(final(rep("svc", "i", 5, 5, 0)))
	s, _ := e.stop()
	if !s.DrainTimedOut || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 0 {
		t.Fatalf("timed_out=%v reasons=%q unconfirmed=%q: the verdict was made from counts older than the reports", s.DrainTimedOut, s.IncompleteReasons, s.DeliveryUnconfirmed)
	}
}

func postBatch(t *testing.T, url, body string) (int, []DeliveryResult, string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/delivery", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out struct {
		Results []DeliveryResult `json:"results"`
	}
	_ = json.Unmarshal(b, &out)
	return resp.StatusCode, out.Results, string(b)
}

// POST /v1/delivery: the reports of one instance for many sessions in one
// request, with what became of each in the answer.
func TestDeliveryBatchEndpoint(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	srv := httptest.NewServer(e.m.Handler())
	defer srv.Close()
	other, err := e.m.Start(StartRequest{Name: "other"})
	if err != nil {
		t.Fatal(err)
	}
	gone, err := e.m.Start(StartRequest{Name: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.m.Abort(gone.ID); err != nil {
		t.Fatal(err)
	}
	e.report(rep("orders-api", "o-1", 9, 9, 0))

	body, _ := json.Marshal(map[string]any{"service": "orders-api ", "instance": "o-1", "delivered_total": 40, "reports": []map[string]any{
		{"session": other.ID, "recorded": 3, "delivered": 2, "buffered": 1, "last_error": "Post \"https://user:s3cret@collector/v1/ingest\": refused"},
		{"session": e.id, "recorded": 5, "delivered": 5},                    // behind the stored 9
		{"session": gone.ID, "recorded": 1, "delivered": 1},                 // aborted
		{"session": "ts_unknown", "recorded": 1, "delivered": 1},            // no such session
		{"session": other.ID, "recorded": 3, "delivered": 5},                // does not add up
		{"session": other.ID, "recorded": "many"},                           // not a report
		{"session": other.ID, "last_error": strings.Repeat("x", 5000)},      // over 4 KiB
		{"session": other.ID, "recorded": 4, "delivered": 4, "final": true}, // a later report of the same session
	}})
	code, res, raw := postBatch(t, srv.URL, string(body))
	want := []string{DeliveryAccepted, DeliveryStale, DeliveryClosed, DeliveryUnknown, DeliveryInvalid, DeliveryInvalid, DeliveryInvalid, DeliveryAccepted}
	if code != http.StatusOK || len(res) != len(want) {
		t.Fatalf("HTTP %d %s", code, raw)
	}
	for i, w := range want {
		if res[i].Status != w {
			t.Errorf("report %d: %q (%s), want %q", i, res[i].Status, res[i].Error, w)
		}
	}
	if res[0].Session != other.ID || res[4].Error == "" || res[6].Error != "report exceeds 4096 bytes" {
		t.Fatalf("results %+v", res)
	}
	got, _ := e.m.Get(other.ID)
	if len(got.Delivery) != 1 || got.Delivery[0].Service != "orders-api" || got.Delivery[0].Recorded != 4 || !got.Delivery[0].Final ||
		got.Delivery[0].DeliveredTotal != 40 || got.Delivery[0].At.IsZero() {
		t.Fatalf("stored %+v", got.Delivery)
	}
	if got, _ := e.m.Get(e.id); len(got.Delivery) != 1 || got.Delivery[0].Recorded != 9 {
		t.Fatalf("the stale report changed the session: %+v", got.Delivery)
	}

	// A batch without reports says that the instance's reports arrive.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, "orders-api ")
	req.Header.Set(core.HeaderDeliveryInstance, "o-2")
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(e.m.Subscribers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || !subs[0].LastDeliveryReport.IsZero() {
		t.Fatalf("subscribers before a report: %+v", subs)
	}
	if code, res, raw := postBatch(t, srv.URL, `{"service":"orders-api","instance":"o-2","reports":[]}`); code != http.StatusOK || len(res) != 0 || !strings.Contains(raw, `"results":[]`) {
		t.Fatalf("a batch without reports: HTTP %d %s", code, raw)
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || subs[0].LastDeliveryReport.IsZero() {
		t.Fatalf("subscribers after a report: %+v", subs)
	}
	resp, err := http.Get(srv.URL + "/v1/subscribers")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"last_delivery_report":"20`) || !strings.Contains(string(b), `"service":"orders-api"`) {
		t.Fatalf("GET /v1/subscribers: %s", b)
	}

	// What it refuses as a whole.
	many := make([]string, MaxDeliveryBatch+1)
	for i := range many {
		many[i] = fmt.Sprintf(`{"session":%q,"recorded":1,"delivered":1}`, other.ID)
	}
	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"not JSON":             {`{"instance":`, http.StatusBadRequest},
		"no instance":          {`{"service":"a","reports":[]}`, http.StatusBadRequest},
		"instance of spaces":   {`{"instance":"  ","reports":[]}`, http.StatusBadRequest},
		"more than 256":        {`{"instance":"i","reports":[` + strings.Join(many, ",") + `]}`, http.StatusBadRequest},
		"body over the bound":  {`{"instance":"i","pad":"` + strings.Repeat("x", maxDeliveryBatchBody) + `"}`, http.StatusRequestEntityTooLarge},
		"256 reports are fine": {`{"instance":"i","reports":[` + strings.Join(many[:MaxDeliveryBatch], ",") + `]}`, http.StatusOK},
	} {
		if code, _, raw := postBatch(t, srv.URL, tc.body); code != tc.want {
			t.Errorf("%s: HTTP %d %.200s, want %d", name, code, raw, tc.want)
		}
	}
	resp, err = http.Post(srv.URL+"/v1/delivery", "text/plain", strings.NewReader(`{"instance":"i","reports":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain body: %d, want 415", resp.StatusCode)
	}
}

// The review's F5: a service named with a trailing space. HTTP trims the
// name on the stream's header, JSON does not trim it in a report, and the
// events carry it as it is. All three are one instance to the control plane.
func TestDeliveryNamesCompareCleaned(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.counting().has("orders-api ", " o-1", 3)
	srv := httptest.NewServer(e.m.Handler())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, "orders-api ")
	req.Header.Set(core.HeaderDeliveryInstance, " o-1")
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(e.m.Subscribers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if code, body := postDelivery(t, srv.URL, e.id, "application/json", `{"service":"orders-api ","instance":" o-1","recorded":3,"delivered":3,"final":true}`); code != http.StatusOK {
		t.Fatalf("report: %d %s", code, body)
	}
	s, took := e.stop()
	wantComplete(t, s)
	if len(s.Delivery) != 1 || s.Delivery[0].Service != "orders-api" || s.Delivery[0].Instance != "o-1" || took >= testGrace/2 {
		t.Fatalf("drain %v, delivery %+v", took, s.Delivery)
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || subs[0].LastDeliveryReport.IsZero() {
		t.Fatalf("the report was not taken for one of the connected instance: %+v", subs)
	}
}
