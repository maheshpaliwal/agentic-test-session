package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The demo collector answers GET /v1/sessions?ids= as the collector does.
func TestDemoCollectorListByID(t *testing.T) {
	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	s := demoServer(t, func() time.Time { return now })
	type entry map[string]any

	// Without ids nothing changes: every session, with its time range.
	var full []entry
	r, b := get(t, s.URL, "/col/v1/sessions")
	decode(t, b, &full)
	if r.StatusCode != 200 || len(full) != 5 || r.Header.Get(headerUnanswered) != "" {
		panic(fmt.Sprintf("full list: %d, %d sessions, header %q", r.StatusCode, len(full), r.Header.Get(headerUnanswered)))
	}
	want := map[string]entry{}
	for _, e := range full {
		if _, ok := e["first_ns"]; !ok {
			panic(fmt.Sprintf("full list entry without first_ns: %v", e))
		}
		want[e["id"].(string)] = e
	}

	// With ids: only those, in the order asked, unknown ones left out.
	var got []entry
	r, b = get(t, s.URL, "/col/v1/sessions?ids=ts_demo0003,ts_nope,ts_demo0001,,ts_demo0003&ids=ts_demo0005")
	decode(t, b, &got)
	if r.StatusCode != 200 || r.Header.Get(headerUnanswered) != "0" || len(got) != 3 {
		panic(fmt.Sprintf("by id: %d, header %q, %s", r.StatusCode, r.Header.Get(headerUnanswered), b))
	}
	for i, id := range []string{"ts_demo0003", "ts_demo0001", "ts_demo0005"} {
		g, w := got[i], want[id]
		if g["id"] != id || g["event_count"] != w["event_count"] || g["sealed"] != w["sealed"] || len(g) != 3 {
			panic(fmt.Sprintf("element %d: %v, full list has %v", i, g, w))
		}
	}
	if r, b = get(t, s.URL, "/col/v1/sessions?ids="); r.StatusCode != 200 || strings.TrimSpace(string(b)) != "[]" {
		panic(fmt.Sprintf("empty ids: %d %s", r.StatusCode, b))
	}

	// A session without events is not one the collector holds events for.
	resp, err := http.Post(s.URL+"/cp/v1/sessions", "application/json", strings.NewReader(`{"name":"empty"}`))
	if err != nil {
		panic(err)
	}
	var started Session
	err = json.NewDecoder(resp.Body).Decode(&started)
	resp.Body.Close()
	if err != nil || started.ID == "" {
		panic(fmt.Sprintf("start: %v %+v", err, started))
	}
	if r, b = get(t, s.URL, "/col/v1/sessions?ids="+started.ID+",ts_demo0001"); r.StatusCode != 200 || strings.Contains(string(b), started.ID) || !strings.Contains(string(b), "ts_demo0001") {
		panic(fmt.Sprintf("session without events: %d %s", r.StatusCode, b))
	}

	// Bad requests.
	many := make([]string, maxListIDs+1)
	for i := range many {
		many[i] = fmt.Sprintf("ts_%04d", i)
	}
	for name, q := range map[string]string{"invalid": "ts_demo0001,bad%20id", "dot": ".x", "too many": strings.Join(many, ",")} {
		if r, b = get(t, s.URL, "/col/v1/sessions?ids="+q); r.StatusCode != 400 || !strings.Contains(string(b), `"error"`) {
			panic(fmt.Sprintf("%s: %d %s", name, r.StatusCode, b))
		}
	}
	if r, b = get(t, s.URL, "/col/v1/sessions?ids="+strings.Join(many[:maxListIDs], ",")); r.StatusCode != 200 || strings.TrimSpace(string(b)) != "[]" {
		panic(fmt.Sprintf("%d ids: %d %s", maxListIDs, r.StatusCode, b))
	}
}

// Through the proxy a page of ids reaches the collector as sent (200 ids of
// 30 characters are about 6 KB of query; the largest request is 1000 ids),
// and the header that says how many ids went unanswered comes back.
func TestProxyForwardsTheCollectorIDsQuery(t *testing.T) {
	var seen []string
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		w.Header().Set(headerUnanswered, "2")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(col.Close)
	h, err := New(Config{ControlPlaneURL: col.URL, CollectorURL: col.URL})
	if err != nil {
		panic(err)
	}
	d := httptest.NewServer(h)
	t.Cleanup(d.Close)
	for _, n := range []int{200, maxListIDs} {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("ts_%027d", i)
		}
		q := "ids=" + strings.Join(ids, ",")
		if len(q) < 30*n {
			panic(fmt.Sprintf("query of %d ids is %d bytes", n, len(q)))
		}
		r, b := get(t, d.URL, "/api/col/v1/sessions?"+q)
		if r.StatusCode != 200 || r.Header.Get(headerUnanswered) != "2" {
			panic(fmt.Sprintf("%d ids: %d %s, header %q", n, r.StatusCode, b, r.Header.Get(headerUnanswered)))
		}
		if got := seen[len(seen)-1]; got != "/v1/sessions?"+q {
			panic(fmt.Sprintf("%d ids: the collector saw a different request (%d bytes, want %d)", n, len(got), len("/v1/sessions?"+q)))
		}
	}
}
