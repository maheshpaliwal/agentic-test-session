package controlplane

import (
	"context"
	"encoding/json"
	"errors"
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
		panic(err)
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
			panic(fmt.Sprintf("asked %v: want the reporting instance, and none that the list names", col.asked))
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
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
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
		wantShortfall(t, s, "the collector has 3 event(s) from orders-api/o-1, which did not report how many it recorded")
		if took < testGrace-20*time.Millisecond || !col.askedFor("orders-api", "o-1") {
			panic(fmt.Sprintf("sealed after %v, asked %d times", took, len(col.asked)))
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
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
		}
		if took >= testGrace/2 {
			panic(fmt.Sprintf("the drain waited %v", took))
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
		panic(err)
	}
	e := &deliveryEnv{t: t, m: m, col: col.scriptCollector, id: s.ID}
	e.report(final(rep("svc", "i", 2, 2, 0)))
	col.scriptCollector.has("svc", "i", 2)
	wantComplete(t, func() Session { s, _ := e.stop(); return s }())
	if len(col.asked) != 0 {
		panic(fmt.Sprintf("asked %v", col.asked))
	}
}

// The review's F3 (e), and the third review's F1: a caller fills the 256
// places a session has for reporting instances, so that the report of the
// instance with the shortfall is not stored. Such a report is answered "full"
// and kept for the verdict all the same: it is replaced, waited for and held
// against the collector's count like a stored one.
func TestDeliveryReportsBeyondTheCapAreJudgedLikeTheOthers(t *testing.T) {
	fill := func(e *deliveryEnv) {
		e.col.counting()
		for i := range MaxDeliveryInstances {
			e.report(final(rep("f", fmt.Sprintf("f-%03d", i), 1, 1, 0)))
			e.col.has("f", fmt.Sprintf("f-%03d", i), 1)
		}
	}
	full := func(e *deliveryEnv, d Delivery) {
		e.t.Helper()
		if ok, err := e.m.ReportDelivery(e.id, d); ok || !errors.Is(err, ErrBadState) || !errors.Is(err, errDeliveryFull) {
			panic(fmt.Sprintf("report %+v: accepted=%v err=%v, want it answered full", d, ok, err))
		}
	}
	verdict := func(s Session) string {
		return fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
	}
	t.Run("dropped events", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		full(e, final(rep("real", "real-1", 10, 0, 10)))
		full(e, rep("fine", "fine-1", 4, 4, 0))
		e.col.has("fine", "fine-1", 4)
		s, took := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 ||
			s.IncompleteReasons[0] != "service real (instance real-1): 0 of 10 recorded events reached the collector, 10 dropped by the SDK" ||
			len(s.DeliveryUnconfirmed) != 0 || len(s.Delivery) != MaxDeliveryInstances {
			panic(verdict(s))
		}
		if took >= testGrace {
			panic(fmt.Sprintf("drain %v", took))
		}
	})
	// The third review's c3: the instance is killed after a report that was
	// not final and still held everything. Its word used to be dropped, and
	// the session ended unconfirmed where it is incomplete without the crowd.
	t.Run("a report that is not final and holds events the collector does not have", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		full(e, rep("real", "real-1", 10, 0, 0))
		s, took := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || len(s.DeliveryUnconfirmed) != 0 ||
			s.IncompleteReasons[0] != "service real (instance real-1): 0 of 10 recorded events reached the collector; the instance stopped reporting before the session ended" {
			panic(verdict(s))
		}
		if took < testGrace-20*time.Millisecond {
			panic(fmt.Sprintf("sealed after %v: an instance that owes events is waited for, stored or not", took))
		}
	})
	t.Run("a final report that still held events, and a later one that takes it back", func(t *testing.T) {
		for _, c := range []struct {
			have int64
			want string
		}{{4, "service real (instance real-1): 4 of 10 recorded events reached the collector"}, {10, ""}} {
			e := newDeliveryEnv(t, testGrace, nil)
			fill(e)
			full(e, rep("real", "real-1", 10, 0, 0)) // periodic: it may still deliver
			full(e, final(rep("real", "real-1", 10, 4, 0)))
			full(e, final(rep("real", "real-1", 10, 10, 0)))
			e.col.has("real", "real-1", c.have)
			s, _ := e.stop()
			if c.want == "" {
				wantComplete(t, s)
			} else if !s.Incomplete || len(s.IncompleteReasons) != 1 || s.IncompleteReasons[0] != c.want || len(s.DeliveryUnconfirmed) != 0 {
				panic(fmt.Sprintf("collector has %d: %s", c.have, verdict(s)))
			}
		}
	})
	// An old report of such an instance is stale, like one of a stored instance.
	t.Run("an old report", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		full(e, final(rep("real", "real-1", 10, 10, 0)))
		if ok, err := e.m.ReportDelivery(e.id, rep("real", "real-1", 9, 9, 0)); ok || err != nil {
			panic(fmt.Sprintf("an older report: accepted=%v err=%v, want it stale", ok, err))
		}
		e.col.has("real", "real-1", 10)
		s, _ := e.stop()
		wantComplete(t, s)
	})
	// A connected instance whose report is kept that way is not taken for one
	// whose reports do not arrive.
	t.Run("connected, with its events at the collector", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		defer follow(e.m, "fine", "fine-1")()
		e.col.has("fine", "fine-1", 4)
		full(e, final(rep("fine", "fine-1", 4, 4, 0)))
		s, took := e.stop()
		wantComplete(t, s)
		if took >= testGrace/2 {
			panic(fmt.Sprintf("the drain waited %v for an instance that reported", took))
		}
	})
	// What is kept beside the session is bounded too. Beyond the bound a
	// connected instance with events and no kept report reads as one whose
	// reports do not arrive: filling the bound cannot make a session complete.
	t.Run("more instances than are kept", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		fill(e)
		for i := range maxDeliveryRefused {
			full(e, final(rep("g", fmt.Sprintf("g-%04d", i), 1, 1, 0)))
			e.col.has("g", fmt.Sprintf("g-%04d", i), 1)
		}
		defer follow(e.m, "real", "real-1")()
		e.col.has("real", "real-1", 4)
		full(e, rep("late", "late-1", 10, 4, 0)) // not final, holds events: nothing is kept of it
		s, _ := e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || s.IncompleteReasons[0] != "the collector has 4 event(s) from real/real-1, which did not report how many it recorded; its log says why its reports do not arrive" ||
			len(s.DeliveryUnconfirmed) != 1 || s.DeliveryUnconfirmed[0] != "more than 1280 instances reported; the reports of the ones beyond that are not accounted for" {
			panic(verdict(s))
		}
		// One of them that says that events were lost: the session is incomplete,
		// without the instance's name.
		e = newDeliveryEnv(t, testGrace, nil)
		fill(e)
		for i := range maxDeliveryRefused {
			full(e, final(rep("g", fmt.Sprintf("g-%04d", i), 1, 1, 0)))
			e.col.has("g", fmt.Sprintf("g-%04d", i), 1)
		}
		full(e, final(rep("real", "real-1", 10, 4, 6)))
		s, _ = e.stop()
		if !s.Incomplete || len(s.IncompleteReasons) != 1 || !strings.HasPrefix(s.IncompleteReasons[0], "more than 1280 instances reported, and a report of one beyond that said that events were dropped or left undelivered") ||
			len(s.DeliveryUnconfirmed) != 1 {
			panic(verdict(s))
		}
	})
}

// The third review's F1 (c1, c1b): a report in a real instance's name that
// arrives before its own, with a low recorded and a high dropped, made every
// report of the real instance stale (its dropped was lower), and the session
// complete with the collector holding as much as the forged report named. A
// report that names more events is a later one.
func TestDeliveryPreloadedReportDoesNotHideTheRealOnes(t *testing.T) {
	t.Run("recorded 1, dropped 1, final; then the real instance loses 10 of 100", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("real", "real-1", 90)
		e.report(final(rep("real", "real-1", 1, 0, 1)))
		e.report(final(rep("real", "real-1", 100, 90, 0)))
		s, _ := e.stop()
		wantShortfall(t, s, "service real (instance real-1): 90 of 100 recorded events reached the collector")
	})
	t.Run("recorded 11, dropped 11, final; then the real instance drops 10 of 100", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("real", "real-1", 90)
		e.report(final(rep("real", "real-1", 11, 0, 11)))
		e.report(final(rep("real", "real-1", 100, 90, 10)))
		s, _ := e.stop()
		wantShortfall(t, s, "service real (instance real-1): 90 of 100 recorded events reached the collector, 10 dropped by the SDK")
	})
	// A report that names as many events and less delivered or dropped is an
	// old one, as before; so is one that names fewer events.
	t.Run("what is stale", func(t *testing.T) {
		for _, c := range []struct {
			prev, next Delivery
			stale      bool
		}{
			{rep("a", "i", 10, 5, 0), rep("a", "i", 9, 9, 0), true},
			{rep("a", "i", 10, 5, 0), rep("a", "i", 10, 4, 0), true},
			{rep("a", "i", 10, 5, 2), rep("a", "i", 10, 6, 1), true},
			{rep("a", "i", 10, 5, 0), rep("a", "i", 10, 5, 0), false},
			{rep("a", "i", 10, 5, 0), rep("a", "i", 10, 6, 0), false},
			{rep("a", "i", 10, 5, 5), rep("a", "i", 11, 0, 0), false},
			{rep("a", "i", 1, 0, 1), rep("a", "i", 100, 90, 0), false},
		} {
			if got := staleReport(c.next, c.prev); got != c.stale {
				panic(fmt.Sprintf("%+v after %+v: stale=%v, want %v", c.next, c.prev, got, c.stale))
			}
		}
	})
	// Final is kept across a report that names the same events (an older one
	// that arrived late), and not across one that names more: a final report
	// in the instance's name by somebody else does not make its own final.
	t.Run("final", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("real", "real-1", 12)
		e.report(final(rep("real", "real-1", 10, 10, 0)))
		e.report(rep("real", "real-1", 10, 10, 0))
		if s, _ := e.m.Get(e.id); len(s.Delivery) != 1 || !s.Delivery[0].Final {
			panic(fmt.Sprintf("final was taken back by a report of the same events: %+v", s.Delivery))
		}
		e.report(rep("real", "real-1", 11, 11, 0))
		if s, _ := e.m.Get(e.id); len(s.Delivery) != 1 || s.Delivery[0].Final {
			panic(fmt.Sprintf("a later report that is not final was stored as final: %+v", s.Delivery))
		}
		// The collector has more than its last report names: the instance
		// recorded on and stopped reporting.
		s, _ := e.stop()
		if s.Incomplete || len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "the collector has 12 events, more than the 11 of the instance's last report") {
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
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
		panic(fmt.Sprintf("%d reasons, first %q, last %q", len(s.IncompleteReasons), s.IncompleteReasons[0], s.IncompleteReasons[len(s.IncompleteReasons)-1]))
	}
	if len(s.DeliveryUnconfirmed) != maxDeliveryReasons+1 || s.DeliveryUnconfirmed[maxDeliveryReasons] != "and 4 more" {
		panic(fmt.Sprintf("%d unconfirmed, last %q", len(s.DeliveryUnconfirmed), s.DeliveryUnconfirmed[len(s.DeliveryUnconfirmed)-1]))
	}
	if got := capReasons([]string{"a", "b"}); len(got) != 2 {
		panic(fmt.Sprintf("a short list was changed: %q", got))
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
		panic(fmt.Sprintf("timed_out=%v reasons=%q unconfirmed=%q: the verdict was made from counts older than the reports", s.DrainTimedOut, s.IncompleteReasons, s.DeliveryUnconfirmed))
	}
}

func postBatch(t *testing.T, url, body string) (int, []DeliveryResult, string) {
	t.Helper()
	resp, err := http.Post(url+"/v1/delivery", "application/json", strings.NewReader(body))
	if err != nil {
		panic(err)
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
		panic(err)
	}
	gone, err := e.m.Start(StartRequest{Name: "gone"})
	if err != nil {
		panic(err)
	}
	if _, err := e.m.Abort(gone.ID); err != nil {
		panic(err)
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
		panic(fmt.Sprintf("HTTP %d %s", code, raw))
	}
	for i, w := range want {
		if res[i].Status != w {
			t.Errorf("report %d: %q (%s), want %q", i, res[i].Status, res[i].Error, w)
		}
	}
	if res[0].Session != other.ID || res[4].Error == "" || res[6].Error != "report exceeds 4096 bytes" {
		panic(fmt.Sprintf("results %+v", res))
	}
	got, _ := e.m.Get(other.ID)
	if len(got.Delivery) != 1 || got.Delivery[0].Service != "orders-api" || got.Delivery[0].Recorded != 4 || !got.Delivery[0].Final ||
		got.Delivery[0].DeliveredTotal != 40 || got.Delivery[0].At.IsZero() {
		panic(fmt.Sprintf("stored %+v", got.Delivery))
	}
	if got, _ := e.m.Get(e.id); len(got.Delivery) != 1 || got.Delivery[0].Recorded != 9 {
		panic(fmt.Sprintf("the stale report changed the session: %+v", got.Delivery))
	}

	// A batch without reports says that the instance's reports arrive.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, "orders-api ")
	req.Header.Set(core.HeaderDeliveryInstance, "o-2")
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer stream.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(e.m.Subscribers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || !subs[0].LastDeliveryReport.IsZero() {
		panic(fmt.Sprintf("subscribers before a report: %+v", subs))
	}
	if code, res, raw := postBatch(t, srv.URL, `{"service":"orders-api","instance":"o-2","reports":[]}`); code != http.StatusOK || len(res) != 0 || !strings.Contains(raw, `"results":[]`) {
		panic(fmt.Sprintf("a batch without reports: HTTP %d %s", code, raw))
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || subs[0].LastDeliveryReport.IsZero() {
		panic(fmt.Sprintf("subscribers after a report: %+v", subs))
	}
	resp, err := http.Get(srv.URL + "/v1/subscribers")
	if err != nil {
		panic(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"last_delivery_report":"20`) || !strings.Contains(string(b), `"service":"orders-api"`) {
		panic(fmt.Sprintf("GET /v1/subscribers: %s", b))
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
		panic(err)
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
		panic(err)
	}
	defer stream.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(e.m.Subscribers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if code, body := postDelivery(t, srv.URL, e.id, "application/json", `{"service":"orders-api ","instance":" o-1","recorded":3,"delivered":3,"final":true}`); code != http.StatusOK {
		panic(fmt.Sprintf("report: %d %s", code, body))
	}
	s, took := e.stop()
	wantComplete(t, s)
	if len(s.Delivery) != 1 || s.Delivery[0].Service != "orders-api" || s.Delivery[0].Instance != "o-1" || took >= testGrace/2 {
		panic(fmt.Sprintf("drain %v, delivery %+v", took, s.Delivery))
	}
	if subs := e.m.Subscribers(); len(subs) != 1 || subs[0].LastDeliveryReport.IsZero() {
		panic(fmt.Sprintf("the report was not taken for one of the connected instance: %+v", subs))
	}
}

// Hundreds of sessions drain at once when a test run ends, and each drain
// asks the collector several times a second. The requests share connections:
// with the two idle connections per host of http.DefaultTransport most of
// them opened one of their own, and a machine ran out of local ports (found
// with 500 sessions stopping together, four times in a row).
func TestCollectorClientKeepsItsConnections(t *testing.T) {
	// What the collector answers: its drain-status with few instances and with
	// many, and an error with a body. The third review's F2: from about 120
	// instances on the answer is sent in chunks, and the end of the body can
	// arrive after the JSON value was decoded. A body that is closed before it
	// was read to its end costs the connection: 2560 connections for 2560
	// requests. Here the end always arrives late.
	many := DrainStatus{Instances: make([]InstanceCount, 512)}
	for i := range many.Instances {
		many.Instances[i] = InstanceCount{Service: fmt.Sprintf("service-%04d", i), Instance: fmt.Sprintf("host-%04d-a1b2c3", i), Events: int64(i)}
	}
	var opened atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "ts_few"):
			_, _ = io.WriteString(w, `{"pending":0,"instances":[]}`)
		case strings.Contains(r.URL.Path, "ts_many"):
			// As the collector writes it: an encoder, so a newline follows the
			// value; in chunks, the last of which is late.
			b, _ := json.Marshal(many)
			_, _ = w.Write(b)
			w.(http.Flusher).Flush()
			time.Sleep(2 * time.Millisecond)
			_, _ = io.WriteString(w, "\n")
		default:
			http.Error(w, strings.Repeat("the store is down ", 400), http.StatusInternalServerError)
		}
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	ctx := context.Background()
	calls := map[string]func(c *HTTPCollector, id string) error{
		"drain-status": func(c *HTTPCollector, id string) error { _, err := c.DrainStatus(ctx, id); return err },
		"instance-counts": func(c *HTTPCollector, id string) error {
			_, err := c.InstanceCounts(ctx, id, many.Instances[:3])
			return err
		},
		"seal":   func(c *HTTPCollector, id string) error { _, err := c.Seal(ctx, id); return err },
		"delete": func(c *HTTPCollector, id string) error { return c.Delete(ctx, id) },
	}
	for name, c := range map[string]*HTTPCollector{
		"the default client":     {BaseURL: srv.URL},
		"the client of the main": {BaseURL: srv.URL, Client: NewCollectorClient(30 * time.Second)},
	} {
		for call, do := range calls {
			for _, id := range []string{"ts_few", "ts_many", "ts_failing"} {
				// The connections the last round left idle would hide a round
				// that keeps none: each starts with none.
				if c.Client != nil {
					c.Client.CloseIdleConnections()
				} else {
					defaultCollectorClient.CloseIdleConnections()
				}
				opened.Store(0)
				const drains, polls = 32, 20
				var wg sync.WaitGroup
				for range drains {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range polls {
							if err := do(c, id); (err != nil) != (id == "ts_failing") {
								t.Errorf("%s %s: %v", call, id, err)
								return
							}
						}
					}()
				}
				wg.Wait()
				if n := opened.Load(); n > 2*drains {
					t.Errorf("%s, %s of %s: %d connections for %d requests of %d concurrent drains, want about one per drain", name, call, id, n, drains*polls, drains)
				}
			}
		}
	}
}

// The third review's F4: the active set says that this control plane takes
// delivery reports, in the snapshot and on the stream, with sessions or
// without. An SDK that reads it does not take a 404 to its reports for "a
// control plane without the endpoint".
func TestActiveSetSaysThatReportsAreTaken(t *testing.T) {
	m := NewManager(Config{Collector: newScriptCollector(), TimeUnit: 10 * time.Millisecond})
	t.Cleanup(m.Close)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/active")
	if err != nil {
		panic(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"delivery_reports":true`) {
		panic(fmt.Sprintf("GET /v1/active: %s", b))
	}
	ch, cancel := m.Subscribe()
	defer cancel()
	if a := <-ch; !a.DeliveryReports {
		panic(fmt.Sprintf("the set a follower is sent: %+v", a))
	}
}

// The third review's F5: whether the reports of a connected instance arrive
// is judged by how long nothing has arrived of it, not by whether anything
// ever did. An instance whose reports got through when it connected and are
// refused since (and whose collector is unreachable) recorded for a session
// and left no trace: the session was complete. It heartbeats twice per grace
// while a session runs, so a grace without a word names it.
func TestDeliveryUnheardIsJudgedByTheTimeSinceTheLastWord(t *testing.T) {
	silence := testGrace + testGrace/4
	t.Run("heard once, then nothing for the grace: unconfirmed, not waited for", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "orders-api", "o-1")()
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(silence)
		if subs := e.m.Subscribers(); len(subs) != 1 || !subs[0].DeliveryUnheard {
			panic(fmt.Sprintf("the list of followers does not mark it: %+v", subs))
		}
		s, took := e.stop()
		if s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 ||
			!strings.HasPrefix(s.DeliveryUnconfirmed[0], "service orders-api (instance o-1): it follows the control plane, but neither a delivery report nor a heartbeat of it has arrived for ") ||
			!strings.HasSuffix(s.DeliveryUnconfirmed[0], "(its log says why); the collector has no events from it for this session, so whether it recorded any is unknown") {
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
		}
		if took >= testGrace/2 {
			panic(fmt.Sprintf("the drain waited %v for it", took))
		}
	})
	t.Run("the collector has its events: incomplete", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("orders-api", "o-1", 3)
		defer follow(e.m, "orders-api", "o-1")()
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(silence)
		s, _ := e.stop()
		wantShortfall(t, s, "the collector has 3 event(s) from orders-api/o-1, which did not report how many it recorded")
		if len(s.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("named twice: unconfirmed=%q", s.DeliveryUnconfirmed))
		}
	})
	t.Run("heartbeats: heard, however long the session", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "orders-api", "o-1")()
		for range 6 { // three graces
			hello(t, e.m, "orders-api", "o-1")
			time.Sleep(e.m.heartbeatPeriod())
		}
		hello(t, e.m, "orders-api", "o-1")
		if subs := e.m.Subscribers(); len(subs) != 1 || subs[0].DeliveryUnheard {
			panic(fmt.Sprintf("marked although it heartbeats: %+v", subs))
		}
		s, _ := e.stop()
		wantComplete(t, s)
	})
	// A report for the session that was in order and not final, and then
	// nothing: the instance may have recorded more, and neither its report nor
	// the collector would show it.
	t.Run("a report that was in order, then nothing for the grace", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("orders-api", "o-1", 3)
		defer follow(e.m, "orders-api", "o-1")()
		e.report(rep("orders-api", "o-1", 3, 3, 0))
		time.Sleep(silence)
		s, _ := e.stop()
		if s.Incomplete || len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "neither a delivery report nor a heartbeat of it has arrived for ") ||
			!strings.HasSuffix(s.DeliveryUnconfirmed[0], "its last report for this session, of 3 events, was not final, so whether it recorded more since is unknown") {
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
		}
		// With a final report it has said all there is to say.
		e = newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("orders-api", "o-1", 3)
		defer follow(e.m, "orders-api", "o-1")()
		e.report(final(rep("orders-api", "o-1", 3, 3, 0)))
		time.Sleep(silence)
		s, _ = e.stop()
		wantComplete(t, s)
	})
	// Without a session an instance sends no heartbeat: its silence says
	// nothing, however long, and it is given the time to send its first when a
	// session begins.
	t.Run("no session: silence says nothing", func(t *testing.T) {
		col := newScriptCollector()
		col.counting()
		m := NewManager(Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, DeliveryGrace: testGrace})
		t.Cleanup(m.Close)
		defer follow(m, "orders-api", "o-1")()
		hello(t, m, "orders-api", "o-1")
		time.Sleep(silence)
		if subs := m.Subscribers(); len(subs) != 1 || subs[0].DeliveryUnheard {
			panic(fmt.Sprintf("marked while the control plane has no session: %+v", subs))
		}
		s, err := m.Start(StartRequest{Name: "d", QuiesceSeconds: 2, MaxDrainSeconds: 1000})
		if err != nil {
			panic(err)
		}
		// Its silence counts from here: a first heartbeat that is slow on its
		// way (a quarter of the grace) costs the session nothing.
		time.Sleep(testGrace / 4)
		if subs := m.Subscribers(); subs[0].DeliveryUnheard {
			panic(fmt.Sprintf("marked a quarter of the grace after a session began: %+v", subs))
		}
		hello(t, m, "orders-api", "o-1") // what the SDK does when it learns of the session
		e := &deliveryEnv{t: t, m: m, col: col, id: s.ID}
		sealed, _ := e.stop()
		wantComplete(t, sealed)
		// The session is over: the mark is not left standing by its end.
		time.Sleep(silence)
		if subs := m.Subscribers(); subs[0].DeliveryUnheard {
			panic(fmt.Sprintf("marked after the last session ended: %+v", subs))
		}
	})
	if a := (&Manager{cfg: Config{DeliveryGrace: 10 * time.Second}}).activeLocked(); !a.DeliveryReports || a.DeliveryHeartbeatMs != 5000 {
		panic(fmt.Sprintf("the active set asks for a heartbeat every %dms, want half the grace", a.DeliveryHeartbeatMs))
	}
	if a := (&Manager{cfg: Config{DeliveryGrace: -1}}).activeLocked(); a.DeliveryHeartbeatMs != 0 {
		panic(fmt.Sprintf("the active set asks for a heartbeat with the check off: %+v", a))
	}
}
