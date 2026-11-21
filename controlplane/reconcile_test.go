package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// counts builds what a collector that counts per instance answered.
func counts(truncated bool, kv ...any) collectorCounts {
	st := DrainStatus{Instances: []InstanceCount{}, InstancesTruncated: truncated}
	for i := 0; i+3 <= len(kv); i += 3 {
		st.Instances = append(st.Instances, InstanceCount{Service: kv[i].(string), Instance: kv[i+1].(string), Events: int64(kv[i+2].(int))})
	}
	return countsOf(st, true)
}

func total(d Delivery, n int64) Delivery { d.DeliveredTotal = n; return d }

// The verdict holds an instance's last report against what the collector has
// from it. The instance's own word does not condemn it (it may not have seen
// the answer to a request the collector stored), and does not acquit it
// either (a report can be wrong, or forged).
func TestDeliveryVerdictAgainstTheCollector(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    Delivery
		have int // what the collector has from the instance
		want string
	}{
		{"all delivered, final", final(rep("a", "i", 5, 5, 0)), 5, ""},
		{"all delivered, periodic report", rep("a", "i", 5, 5, 0), 5, ""},
		// F1: failing when it last reported, then delivered everything, then killed.
		{"last report failing, the collector has everything", failing(rep("a", "i", 21, 0, 0), "the collector answered HTTP 503"), 21, ""},
		// F2: the collector stored the batch, the SDK timed out waiting for the answer.
		{"final report says nothing arrived, the collector has everything", failing(final(rep("a", "i", 21, 0, 0)), "context deadline exceeded"), 21, ""},
		{"final report counts as dropped at shutdown what the collector has", final(rep("a", "i", 21, 0, 21)), 21, ""},
		// A report that claims more than the collector has: wrong or forged.
		{"report says all delivered, the collector has none", final(rep("a", "i", 5, 5, 0)), 0, "incomplete"},
		{"report says all delivered, the collector has fewer", rep("a", "i", 5, 5, 0), 4, "incomplete"},
		{"final with undelivered events the collector does not have", final(rep("a", "i", 5, 3, 0)), 3, "incomplete"},
		{"went silent holding events the collector does not have", rep("a", "i", 5, 3, 0), 3, "incomplete"},
		{"went silent failing, the collector has nothing", failing(rep("a", "i", 5, 0, 0), "connection refused"), 0, "incomplete"},
		{"dropped by a full ring", final(rep("a", "i", 5, 4, 1)), 4, "incomplete"},
		{"dropped, periodic report, the collector has later events too", rep("a", "i", 5, 4, 1), 9, "incomplete"},
		// F5, the part that can be seen: it recorded after its last report.
		{"the collector has more than the last, periodic, report", rep("a", "i", 5, 5, 0), 9, "unconfirmed"},
		{"the collector has more than a final report (a report in flight)", final(rep("a", "i", 5, 5, 0)), 9, ""},
		{"nothing recorded", final(rep("a", "i", 0, 0, 0)), 0, ""},
	} {
		short, unc := deliveryVerdict([]Delivery{tc.d}, counts(false, "a", "i", tc.have, "other", "x", 100))
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
			t.Errorf("%s: verdict %q, want %q (%q %q)", tc.name, got, tc.want, short, unc)
		}
	}

	// The reason names the service, the instance and both numbers.
	short, unc := deliveryVerdict([]Delivery{
		failing(final(rep("orders-api", "host-1-ab12cd", 7, 0, 0)), "dial tcp 127.0.0.1:1: connect: connection refused"),
		rep("inventory", "host-2-ef34ab", 5, 5, 0),
		final(rep("", "bare", 4, 2, 1)),
		rep("billing", "host-3", 2, 2, 0),
	}, counts(false, "inventory", "host-2-ef34ab", 3, "", "bare", 2, "billing", "host-3", 6))
	want := []string{
		"service orders-api (instance host-1-ab12cd): 0 of 7 recorded events reached the collector (last error: dial tcp 127.0.0.1:1: connect: connection refused)",
		"service inventory (instance host-2-ef34ab): 3 of 5 recorded events reached the collector; the instance stopped reporting before the session ended",
		"instance bare: 2 of 4 recorded events reached the collector, 1 dropped by the SDK",
	}
	if fmt.Sprint(short) != fmt.Sprint(want) {
		t.Fatalf("reasons\n got %q\nwant %q", short, want)
	}
	if len(unc) != 1 || !strings.HasPrefix(unc[0], "service billing (instance host-3): the collector has 6 events, more than the 2 of the instance's last report") {
		t.Fatalf("unconfirmed %q", unc)
	}

	// An instance the collector's list leaves out: none of its events arrived,
	// unless the list was cut, and then nothing is known of it.
	d := []Delivery{final(rep("a", "i", 5, 5, 0))}
	if short, _ := deliveryVerdict(d, counts(false)); len(short) != 1 || !strings.Contains(short[0], "0 of 5 recorded events reached the collector") {
		t.Fatalf("not in a complete list: %q", short)
	}
	if short, unc := deliveryVerdict(d, counts(true)); len(short)+len(unc) != 0 {
		t.Fatalf("not in a truncated list, and the instance says all is delivered: %q %q", short, unc)
	}
	if short, unc := deliveryVerdict([]Delivery{rep("a", "i", 5, 3, 0)}, counts(true)); len(short) != 0 || len(unc) != 1 {
		t.Fatalf("not in a truncated list, holding events: %q %q", short, unc)
	}

	// The names of a report are cleaned; the collector's are compared cleaned.
	c := countsOf(DrainStatus{Instances: []InstanceCount{{Service: "pay\x1bments", Instance: "hé-1", Events: 2}, {Service: "payments", Instance: "h-1", Events: 3}}}, true)
	if n, ok := c.of("payments", "h-1"); !ok || n != 5 {
		t.Fatalf("cleaned names: %d %v", n, ok)
	}
}

// F1: a service whose collector was down when it last reported, which then
// delivered everything and was killed before its next report. Its last word
// is "0 delivered, HTTP 503"; the collector has every event. The session is
// complete, and the drain does not wait for an instance that owes nothing.
func TestDrainDoesNotCondemnOnAStaleReport(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.counting()
	e.report(failing(rep("orders-api", "o-1", 21, 0, 0), "the collector answered HTTP 503"))
	e.col.has("orders-api", "o-1", 21)
	s, took := e.stop()
	wantComplete(t, s)
	if took >= testGrace {
		t.Fatalf("the drain waited %v for an instance whose events the collector has", took)
	}
}

// F2: every request of the SDK timed out after the collector had stored the
// batch. The final report says that nothing arrived, and a shutdown counts
// the events as dropped; the collector has them all.
func TestDrainBelievesTheCollectorOverTheReport(t *testing.T) {
	for name, d := range map[string]Delivery{
		"undelivered":         failing(final(rep("orders-api", "o-1", 21, 0, 0)), "context deadline exceeded"),
		"dropped at shutdown": failing(final(rep("orders-api", "o-1", 21, 0, 21)), "context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			e := newDeliveryEnv(t, testGrace, nil)
			e.col.counting().has("orders-api", "o-1", 21)
			e.report(d)
			s, took := e.stop()
			wantComplete(t, s)
			if took >= testGrace {
				t.Fatalf("the drain waited %v", took)
			}
		})
	}
	// The SDK sees no progress, the collector does: the drain waits while the
	// collector's count for the instance rises, however long, and judges by it.
	t.Run("the collector's count is the progress", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		e.report(failing(final(rep("orders-api", "o-1", 8, 0, 0)), "context deadline exceeded"))
		go func() {
			for n := int64(1); n <= 8; n++ {
				time.Sleep(testGrace / 3)
				e.col.has("orders-api", "o-1", n)
			}
		}()
		s, took := e.stop()
		wantComplete(t, s)
		if took < 2*testGrace {
			t.Fatalf("sealed after %v, before the collector had the events", took)
		}
	})
	t.Run("and it stops short", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		e.report(failing(final(rep("orders-api", "o-1", 8, 0, 0)), "context deadline exceeded"))
		go func() {
			time.Sleep(testGrace / 2)
			e.col.has("orders-api", "o-1", 5)
		}()
		s, took := e.stop()
		wantShortfall(t, s, "service orders-api (instance o-1): 5 of 8 recorded events reached the collector", "context deadline exceeded")
		if took < testGrace+testGrace/2-40*time.Millisecond {
			t.Fatalf("sealed after %v: the grace counts from the last event the collector got", took)
		}
	})
}

// F3: the grace is for an instance that gets nowhere, not for a session whose
// events wait behind those of other sessions in the same instance. While the
// instance's total rises, in a report of this session or of any other, the
// session is not sealed, for much longer than the grace.
func TestDrainWaitsForAnInstanceWorkingThroughABacklog(t *testing.T) {
	for _, via := range []string{"this session's reports", "another session's reports"} {
		t.Run(via, func(t *testing.T) {
			e := newDeliveryEnv(t, testGrace, nil)
			e.col.counting()
			other, err := e.m.Start(StartRequest{Name: "other", QuiesceSeconds: 2, MaxDrainSeconds: 1000})
			if err != nil {
				t.Fatal(err)
			}
			e.report(total(rep("orders-api", "o-1", 601, 0, 0), 0))
			done := make(chan struct{})
			go func() {
				defer close(done)
				// 12 steps of a third of the grace: four graces of backlog.
				for n := int64(1); n <= 12; n++ {
					time.Sleep(testGrace / 3)
					if via == "this session's reports" {
						_, _ = e.m.ReportDelivery(e.id, total(final(rep("orders-api", "o-1", 601, 0, 0)), 256*n))
					} else {
						_, _ = e.m.ReportDelivery(other.ID, total(rep("orders-api", "o-1", 256*n, 256*n, 0), 256*n))
					}
				}
				e.col.has("orders-api", "o-1", 601)
				_, _ = e.m.ReportDelivery(e.id, total(final(rep("orders-api", "o-1", 601, 601, 0)), 256*12+601))
			}()
			s, took := e.stop()
			<-done
			wantComplete(t, s)
			if took < 3*testGrace {
				t.Fatalf("sealed after %v: the session was not waited for", took)
			}
		})
	}
	// An instance whose total stands still gets the grace and no more.
	t.Run("no progress at all", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		e.report(total(rep("orders-api", "o-1", 601, 0, 0), 512))
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		for range 3 {
			time.Sleep(testGrace / 4)
			e.report(total(final(rep("orders-api", "o-1", 601, 0, 0)), 512))
		}
		s := e.sealed()
		wantShortfall(t, s, "0 of 601 recorded events reached the collector")
		if took := time.Since(began); took < testGrace-20*time.Millisecond || took > testGrace+2*time.Second {
			t.Fatalf("sealed after %v, want the grace (%v)", took, testGrace)
		}
	})
}

// F7: a report is whatever its sender says. One that claims more than the
// instance's real one replaces it (the counters only rise), and the real one
// is then ignored; but "all delivered" does not make it so: the collector has
// what it has.
func TestForgedReportCannotClearAShortfall(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.counting()
	e.report(failing(rep("orders-api", "o-1", 21, 0, 0), "connection refused"))
	e.report(total(final(rep("orders-api", "o-1", 500, 500, 0)), 500)) // forged: "everything was delivered"
	if ok, err := e.m.ReportDelivery(e.id, failing(final(rep("orders-api", "o-1", 21, 0, 0)), "connection refused")); ok || err != nil {
		t.Fatalf("the real report, behind the forged one: accepted=%v err=%v", ok, err)
	}
	// Lower counters never replace higher ones.
	for _, lower := range []Delivery{rep("orders-api", "o-1", 499, 499, 0), rep("orders-api", "o-1", 600, 400, 0), rep("orders-api", "o-1", 0, 0, 0)} {
		if ok, err := e.m.ReportDelivery(e.id, lower); ok || err != nil {
			t.Fatalf("report %+v behind the stored one: accepted=%v err=%v", lower, ok, err)
		}
	}
	s, _ := e.stop()
	wantShortfall(t, s, "service orders-api (instance o-1): 0 of 500 recorded events reached the collector")
}

// At the instance cap an instance the session keeps can still report, a new
// one is refused, and the session then says that it does not account for
// everyone. Reports of "nothing recorded" take no place.
func TestDeliveryInstanceLimitKeepsKnownInstances(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.col.counting()
	for i := range 50 {
		e.report(final(rep("idle", fmt.Sprintf("n%03d", i), 0, 0, 0)))
	}
	for i := range MaxDeliveryInstances {
		e.report(rep("svc", fmt.Sprintf("i%03d", i), 1, 1, 0))
		e.col.has("svc", fmt.Sprintf("i%03d", i), 1)
	}
	if s, _ := e.m.Get(e.id); len(s.Delivery) != MaxDeliveryInstances {
		t.Fatalf("%d reports stored", len(s.Delivery))
	}
	// The instance with the real shortfall is one of them: its report counts.
	e.report(failing(final(rep("svc", "i007", 9, 1, 0)), "connection refused"))
	if ok, err := e.m.ReportDelivery(e.id, rep("svc", "one-too-many", 1, 1, 0)); ok || !errors.Is(err, ErrBadState) {
		t.Fatalf("instance %d: accepted=%v err=%v, want ErrBadState (409)", MaxDeliveryInstances+1, ok, err)
	}
	s := func() Session {
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		return e.sealed()
	}()
	if !s.Incomplete || len(s.IncompleteReasons) != 1 || !strings.Contains(s.IncompleteReasons[0], "instance i007): 1 of 9") ||
		len(s.DeliveryUnconfirmed) != 1 || !strings.Contains(s.DeliveryUnconfirmed[0], "more than 256 instances") {
		t.Fatalf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed)
	}
}

// follow connects an SDK instance to the active set the way the stream does.
func follow(m *Manager, service, deliveryInstance string) func() {
	_, leave := m.subscribe(Subscriber{Service: service, Instance: "configured", DeliveryInstance: deliveryInstance, stream: true})
	return leave
}

// F4: an instance that follows the active set and sends reports owes one for
// every draining session, also when it recorded nothing. If none arrives (a
// proxy refuses the POST), nothing vouches for its events: the drain waits
// the grace for the report and the session is incomplete, naming it.
func TestDrainSilentReporterIsIncomplete(t *testing.T) {
	t.Run("connected, reports never arrive", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("orders-api", "o-1", 3)
		defer follow(e.m, "orders-api", "o-1")()
		defer follow(e.m, "inventory", "i-1")()
		e.report(final(rep("inventory", "i-1", 2, 2, 0)))
		e.col.has("inventory", "i-1", 2)
		s, took := e.stop()
		wantShortfall(t, s, "service orders-api (instance o-1): it follows the control plane but none of its delivery reports arrived", "the collector has 3 events from it")
		if took < testGrace-20*time.Millisecond || took > testGrace+2*time.Second {
			t.Fatalf("sealed after %v, want the grace (%v)", took, testGrace)
		}
	})
	t.Run("recorded nothing and says so", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "idle", "n-1")()
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		e.report(final(rep("idle", "n-1", 0, 0, 0)))
		began := time.Now()
		s := e.sealed()
		wantComplete(t, s)
		if took := time.Since(began); took >= testGrace {
			t.Fatalf("sealed %v after the report", took)
		}
		if len(s.Delivery) != 0 {
			t.Fatalf("a report of nothing was stored: %+v", s.Delivery)
		}
	})
	t.Run("gone before the grace is over", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		leave := follow(e.m, "orders-api", "o-1")
		time.AfterFunc(testGrace/3, leave)
		s, took := e.stop()
		wantComplete(t, s)
		if took >= testGrace {
			t.Fatalf("the drain waited %v for an instance that had left", took)
		}
	})
	t.Run("an SDK that sends no reports, and other followers", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().has("old", "old-1", 4)
		defer follow(e.m, "old", "")()
		_, leave := e.m.SubscribeAs("embedded", "e-1")
		defer leave()
		s, took := e.stop()
		wantComplete(t, s)
		if took >= testGrace {
			t.Fatalf("the drain waited %v", took)
		}
	})
	t.Run("connects during the drain", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting().setPending(1)
		if _, err := e.m.Stop(e.id); err != nil {
			t.Fatal(err)
		}
		time.Sleep(testGrace + 50*time.Millisecond) // the drain is older than the grace
		defer follow(e.m, "late", "l-1")()
		e.col.setPending(0)
		time.Sleep(testGrace / 2)
		if s, _ := e.m.Get(e.id); s.State != StateDraining {
			t.Fatalf("sealed before the new instance had the grace to report: %+v", s)
		}
		e.report(final(rep("late", "l-1", 0, 0, 0)))
		wantComplete(t, e.sealed())
	})
	t.Run("check off", func(t *testing.T) {
		e := newDeliveryEnv(t, -1, nil)
		defer follow(e.m, "orders-api", "o-1")()
		s, took := e.stop()
		wantComplete(t, s)
		if took > 2*time.Second {
			t.Fatalf("drain %v", took)
		}
	})
}

// The stream says which instance sends reports, and under which name.
func TestStreamNamesTheReportingInstance(t *testing.T) {
	m := NewManager(Config{})
	t.Cleanup(m.Close)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, "orders-api")
	req.Header.Set(core.HeaderInstance, "pod-7")
	req.Header.Set(core.HeaderDeliveryInstance, "pod-7-4711-ab12cd")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Subscribers()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	subs := m.Subscribers()
	if len(subs) != 1 || subs[0].Instance != "pod-7" || subs[0].DeliveryInstance != "pod-7-4711-ab12cd" {
		t.Fatalf("subscribers %+v", subs)
	}
}

// The drain stops looking at reports when it ends at max_drain as well: one
// that arrives while the session is being sealed is refused, not stored and
// left unjudged.
func TestDrainTimedOutClosesTheReports(t *testing.T) {
	e := newDeliveryEnv(t, time.Hour, func(r *StartRequest) { r.MaxDrainSeconds = 10 }) // 100ms
	hold := make(chan struct{})
	e.col.sealHold = hold
	e.report(final(rep("svc", "i", 5, 0, 0)))
	if _, err := e.m.Stop(e.id); err != nil {
		t.Fatal(err)
	}
	<-e.col.sealing
	if ok, err := e.m.ReportDelivery(e.id, final(rep("svc", "i", 5, 5, 0))); ok || !errors.Is(err, ErrBadState) || !strings.Contains(err.Error(), "being sealed") {
		t.Fatalf("report after the drain timed out: accepted=%v err=%v, want ErrBadState", ok, err)
	}
	close(hold)
	s := e.sealed()
	if !s.DrainTimedOut || !s.Incomplete || len(s.IncompleteReasons) != 1 || s.Delivery[0].Delivered != 0 {
		t.Fatalf("timed out: %+v", s)
	}
}

// A list of sessions carries each session's verdict and leaves the reports
// to the session itself: 256 instances per session do not belong in a page.
func TestSessionListOmitsDeliveryReports(t *testing.T) {
	e := newDeliveryEnv(t, testGrace, nil)
	e.report(final(rep("orders-api", "o-1", 5, 3, 2)))
	e.report(rep("gone", "g-1", 4, 1, 0))
	s, _ := e.stop()
	if len(s.IncompleteReasons) != 1 || len(s.DeliveryUnconfirmed) != 1 {
		t.Fatalf("verdict: %+v", s)
	}
	srv := httptest.NewServer(e.m.Handler())
	defer srv.Close()
	get := func(path string) string {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	for _, path := range []string{"/v1/sessions", "/v1/sessions?limit=10", "/v1/sessions?state=SEALED"} {
		var list []map[string]json.RawMessage
		body := get(path)
		if err := json.Unmarshal([]byte(body), &list); err != nil || len(list) != 1 {
			t.Fatalf("%s: %v %s", path, err, body)
		}
		if _, has := list[0]["delivery"]; has || list[0]["incomplete_reasons"] == nil || list[0]["delivery_unconfirmed"] == nil {
			t.Fatalf("%s: %s", path, body)
		}
	}
	var one Session
	if err := json.Unmarshal([]byte(get("/v1/sessions/"+e.id)), &one); err != nil || len(one.Delivery) != 2 {
		t.Fatalf("the session itself: %v %+v", err, one)
	}
	// The stored session is untouched by a list.
	if got, _ := e.m.Get(e.id); len(got.Delivery) != 2 {
		t.Fatalf("stored session after a list: %+v", got.Delivery)
	}
}

// What an instance reports as its last error is stored and shown: a URL in
// it keeps scheme, host and path, and nothing that can be a credential.
func TestDeliveryErrorIsScrubbed(t *testing.T) {
	for in, want := range map[string]string{
		`Post "https://collector.internal/v1/ingest?token=s3cret&x=1": EOF`:            `Post "https://collector.internal/v1/ingest": EOF`,
		`Post "https://user:p@ss@w0rd@collector.internal/v1/ingest": EOF`:              `Post "https://collector.internal/v1/ingest": EOF`,
		`Post "https://user:pa/ss@collector.internal:8443/v1/ingest#frag": EOF`:        `Post "https://collector.internal:8443/v1/ingest": EOF`,
		`post https://u:12/x@h.example/p?k=v failed, then http://t0ken@h2.example/ ok`: `post https://h.example/p failed, then http://h2.example/ ok`,
		`dial tcp 127.0.0.1:1: connect: connection refused`:                            `dial tcp 127.0.0.1:1: connect: connection refused`,
		`the collector answered HTTP 503`:                                              `the collector answered HTTP 503`,
		`Get "http://[::1": bad`:                                                       `Get "<url>": bad`,
	} {
		if got := cleanDeliveryError(in); got != want {
			t.Errorf("clean(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	for _, secret := range []string{"s3cret", "w0rd", "p@ss", "pa/ss", "t0ken"} {
		got := cleanDeliveryError(`x https://user:p@ss@w0rd@h/a?token=s3cret y http://user:pa/ss@h/b z http://t0ken@h/`)
		if strings.Contains(got, secret) {
			t.Errorf("%q survives in %q", secret, got)
		}
	}
}
