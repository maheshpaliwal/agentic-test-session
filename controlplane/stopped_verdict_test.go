package controlplane

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// A collector that counts per instance and has no events of the instance
// that said it stopped: nothing ties the instance to the session, whether
// the collector leaves it out of its list or names it with a count of zero.
// That is a warning, not an incomplete session. (Zero events used to be told
// from some events by "> 0" alone, with no test: ">= 0" made every running
// session incomplete on the word of any caller.)
func TestAStoppedInstanceWithNoEventsAtACountingCollectorIsAWarningOnly(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(*scriptCollector)
	}{
		{"the collector does not name the instance", func(c *scriptCollector) { c.has("inventory", "inventory-1", 4) }},
		{"the collector names it with zero events", func(c *scriptCollector) { c.has("orders-api", "orders-1-abc", 0) }},
		{"the collector names nobody", func(*scriptCollector) {}},
	} {
		col := newScriptCollector().counting()
		m := stoppedMgr(col, &logBuffer{}, nil)
		s := mustStart(m, "p")
		tt.setup(col)
		sayStopped(t, m, `{"service":"orders-api","instance":"orders-1-abc","reports":[],"stopped":"its subscriber panicked"}`)
		got := mustSeal(t, m, s.ID)
		if got.Incomplete || len(got.IncompleteReasons) != 0 || len(got.DeliveryUnconfirmed) != 1 ||
			!strings.HasPrefix(got.DeliveryUnconfirmed[0], stoppedPrefix+"its subscriber panicked) while this session ran") ||
			!strings.Contains(got.DeliveryUnconfirmed[0], "whether it took part is unknown") {
			t.Errorf("%s: incomplete=%v reasons=%q unconfirmed=%q", tt.name, got.Incomplete, got.IncompleteReasons, got.DeliveryUnconfirmed)
		}
		m.Close()
	}

	// The rule itself, at its edge: one event ties, none does not, and a
	// collector that cannot say (no counts, or a list cut short that does not
	// name the instance) ties nothing either.
	s := Session{StoppedInstances: []StoppedInstance{{Service: "svc", Instance: "i", Why: "w"}}}
	counted := func(n int64) collectorCounts {
		return countsOf(DrainStatus{Instances: []InstanceCount{{Service: "svc", Instance: "i", Events: n}}}, true)
	}
	for _, tt := range []struct {
		name   string
		counts collectorCounts
		kept   map[string]Delivery
		short  bool
	}{
		{"one event", counted(1), nil, true},
		{"zero events", counted(0), nil, false},
		{"not in the list", countsOf(DrainStatus{Instances: []InstanceCount{}}, true), nil, false},
		{"not in a list cut short", countsOf(DrainStatus{Instances: []InstanceCount{}, InstancesTruncated: true}, true), nil, false},
		{"no counts", collectorCounts{}, nil, false},
		{"zero events and a report", counted(0), map[string]Delivery{deliveryKey("svc", "i"): {Service: "svc", Instance: "i"}}, true},
	} {
		shortfalls, unconfirmed := stoppedReasons(s, tt.kept, tt.counts)
		if (len(shortfalls) == 1) != tt.short || (len(unconfirmed) == 1) == tt.short || len(shortfalls)+len(unconfirmed) != 1 {
			t.Errorf("%s: shortfalls %q, unconfirmed %q; want a shortfall: %v", tt.name, shortfalls, unconfirmed, tt.short)
		}
	}
}

// The list of sessions carries each session's verdict and leaves what is
// behind it to GET /v1/sessions/{id}: the notes of stopped instances (up to
// 64 per session) are not in a page of sessions, as the reports are not.
func TestTheListOfSessionsLeavesTheStoppedNotesOut(t *testing.T) {
	col := newScriptCollector().counting()
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	s := mustStart(m, "p")
	sayStopped(t, m, fmt.Sprintf(`{"service":"orders-api","instance":"orders-1-abc","reports":[{"session":%q,"recorded":2,"delivered":2}],"stopped":"its subscriber panicked"}`, s.ID))
	get := func(path string) string {
		w := do(t, m.Handler(), "GET", path, "127.0.0.1:7070", "", nil)
		if w.Code != http.StatusOK {
			panic(fmt.Sprintf("GET %s: %d %s", path, w.Code, w.Body.String()))
		}
		return w.Body.String()
	}
	check := func(when string) {
		// The session itself has the note and the report.
		var one Session
		if err := json.Unmarshal([]byte(get("/v1/sessions/"+s.ID)), &one); err != nil || len(one.StoppedInstances) != 1 ||
			one.StoppedInstances[0].Instance != "orders-1-abc" || len(one.Delivery) != 1 {
			panic(fmt.Sprintf("%s: GET /v1/sessions/{id}: %v %+v", when, err, one))
		}
		for _, path := range []string{"/v1/sessions", "/v1/sessions?limit=10", "/v1/sessions?state=" + string(one.State), "/v1/sessions?name=p"} {
			body := get(path)
			var list []Session
			if err := json.Unmarshal([]byte(body), &list); err != nil || len(list) != 1 || list[0].ID != s.ID {
				panic(fmt.Sprintf("%s: GET %s: %v %s", when, path, err, body))
			}
			if list[0].StoppedInstances != nil || list[0].Delivery != nil || strings.Contains(body, "stopped_instances") || strings.Contains(body, `"delivery":`) {
				t.Errorf("%s: GET %s carries the notes or the reports: %s", when, path, body)
			}
		}
	}
	check("while the session runs")
	got := mustSeal(t, m, s.ID)
	check("once it is sealed")
	// The verdict the notes led to is in the list.
	if body := get("/v1/sessions"); !got.Incomplete || !strings.Contains(body, `"incomplete":true`) || !strings.Contains(body, "stopped recording after an internal error") {
		t.Errorf("the list lacks the verdict: %s", body)
	}
}

// delivery_unconfirmed lists what was found first and what was only said
// last: a note is anybody's word, the list is cut at its end, and notes that
// nothing ties to the session must not push out what the control plane
// worked out. Among the notes, those of instances that followed the control
// plane come before those of strangers, and that notes were left out is said
// last of all.
func TestDeliveryUnconfirmedListsWhatWasFoundBeforeWhatWasSaid(t *testing.T) {
	col := newScriptCollector().counting()
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	s := mustStart(m, "p")
	// Found: the collector has more events of an instance than its last report said.
	col.has("inventory", "inventory-1", 3)
	if _, err := m.ReportDelivery(s.ID, Delivery{Service: "inventory", Instance: "inventory-1", Recorded: 2, Delivered: 2}); err != nil {
		panic(err)
	}
	// Said: two strangers, whose names sort before and after the one found.
	for _, svc := range []string{"aaa", "zzz"} {
		sayStopped(t, m, fmt.Sprintf(`{"service":%q,"instance":"x","reports":[],"stopped":"its subscriber panicked"}`, svc))
	}
	got := mustSeal(t, m, s.ID)
	u := got.DeliveryUnconfirmed
	if len(u) != 3 || !strings.HasPrefix(u[0], "service inventory (instance inventory-1): the collector has 3 events, more than the 2 of the instance's last report") ||
		!strings.HasPrefix(u[1], "service aaa (instance x) stopped recording") || !strings.HasPrefix(u[2], "service zzz (instance x) stopped recording") {
		t.Errorf("delivery_unconfirmed, in this order:\n%s", strings.Join(u, "\n"))
	}
	if got.Incomplete {
		t.Errorf("incomplete: %q", got.IncompleteReasons)
	}

	// With the check off the shortfalls are warnings too, and still come first.
	off := stoppedMgr(newScriptCollector().counting(), &logBuffer{}, func(c *Config) { c.DeliveryGrace = -1 })
	defer off.Close()
	s = mustStart(off, "q")
	sayStopped(t, off, `{"service":"aaa","instance":"x","reports":[],"stopped":"its subscriber panicked"}`)
	if _, err := off.ReportDelivery(s.ID, Delivery{Service: "orders-api", Instance: "orders-1", Recorded: 5, Delivered: 2, Dropped: 3, Final: true}); err != nil {
		panic(err)
	}
	got = mustSeal(t, off, s.ID)
	if u := got.DeliveryUnconfirmed; got.Incomplete || len(u) != 2 || !strings.HasPrefix(u[0], "service orders-api (instance orders-1): ") ||
		!strings.HasPrefix(u[1], "service aaa (instance x) stopped recording") {
		t.Errorf("with the check off, in this order:\n%s", strings.Join(u, "\n"))
	}

	// The notes among themselves: connected, then strangers, then "more".
	_, unconfirmed := stoppedReasons(Session{StoppedInstancesMore: true, StoppedInstances: []StoppedInstance{
		{Service: "a-stranger", Instance: "1", Why: "w"}, {Service: "b-connected", Instance: "1", Why: "w", Connected: true}, {Service: "c-stranger", Instance: "1", Why: "w"}}},
		nil, collectorCounts{})
	if len(unconfirmed) != 4 || !strings.HasPrefix(unconfirmed[0], "service b-connected ") || strings.Contains(unconfirmed[0], "did not follow") ||
		!strings.HasPrefix(unconfirmed[1], "service a-stranger ") || !strings.Contains(unconfirmed[1], "it did not follow the control plane then") ||
		!strings.HasPrefix(unconfirmed[2], "service c-stranger ") || !strings.HasPrefix(unconfirmed[3], "more than 64 instances said that they stopped recording") {
		t.Errorf("the notes, in this order:\n%s", strings.Join(unconfirmed, "\n"))
	}
}
