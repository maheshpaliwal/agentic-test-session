package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/dashboard"
	"github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The dashboard answers the control plane's and the collector's read
// endpoints itself in two cases: for the demo's sessions, and for sessions
// opened from files written by `tracectl export`. It cannot import the
// collector (the collector's module depends on the dashboard's), so its views
// are a port (dashboard/views.go). The tests in this file keep the port
// honest: the same events are served by the real collector and control plane,
// by the dashboard from an export of them, and by the demo, and every read
// endpoint must answer the same.
//
// Nothing here needs Postgres or Redis.

// backend is one implementation of the two APIs.
type backend struct{ name, cp, col string }

// reply is one HTTP answer: status, the headers that are part of the
// contract, and the body as decoded JSON (or as text when it is not JSON).
type reply struct {
	Status  int
	Headers map[string]string
	Body    any
}

var contractHeaders = []string{"X-Tsession-Next", "X-Tsession-Total", "X-Tsession-Unreadable", "X-Tsession-Unanswered", "Content-Type"}

func fetch(t *testing.T, base, path string) reply {
	t.Helper()
	resp, err := plain.Get(base + path)
	if err != nil {
		panic(fmt.Sprintf("GET %s%s: %v", base, path, err))
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	r := reply{Status: resp.StatusCode, Headers: map[string]string{}}
	for _, h := range contractHeaders {
		if v := resp.Header.Get(h); v != "" {
			r.Headers[h] = v
		}
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber() // nanosecond times do not fit a float64
	if dec.Decode(&r.Body) != nil {
		r.Body = string(b)
	}
	return r
}

// firstDiff names the first place where a and b differ, "" when they are equal.
func firstDiff(path string, a, b any) string {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok {
			break
		}
		keys := map[string]bool{}
		for k := range x {
			keys[k] = true
		}
		for k := range y {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			xv, xok := x[k]
			yv, yok := y[k]
			if xok != yok {
				return fmt.Sprintf("%s.%s: present %v vs %v", path, k, xok, yok)
			}
			if d := firstDiff(path+"."+k, xv, yv); d != "" {
				return d
			}
		}
		return ""
	case []any:
		y, ok := b.([]any)
		if !ok {
			break
		}
		if len(x) != len(y) {
			return fmt.Sprintf("%s: %d vs %d elements", path, len(x), len(y))
		}
		for i := range x {
			if d := firstDiff(fmt.Sprintf("%s[%d]", path, i), x[i], y[i]); d != "" {
				return d
			}
		}
		return ""
	}
	if !reflect.DeepEqual(a, b) {
		return fmt.Sprintf("%s: %.200v vs %.200v", path, a, b)
	}
	return ""
}

// same fails unless both backends give the same answer to GET path. norm,
// when not nil, is applied to both bodies first; each use of it documents a
// field that legitimately differs.
func same(t *testing.T, a, b backend, pick func(backend) string, path string, norm func(any) any) reply {
	t.Helper()
	ra, rb := fetch(t, pick(a), path), fetch(t, pick(b), path)
	na, nb := ra, rb
	if norm != nil {
		na.Body, nb.Body = norm(ra.Body), norm(rb.Body)
	}
	if d := firstDiff("", map[string]any{"status": na.Status, "headers": fmt.Sprint(na.Headers), "body": na.Body},
		map[string]any{"status": nb.Status, "headers": fmt.Sprint(nb.Headers), "body": nb.Body}); d != "" {
		t.Errorf("GET %s: %s and %s differ at %s", path, a.name, b.name, d)
	}
	return ra
}

func ofCP(b backend) string  { return b.cp }
func ofCol(b backend) string { return b.col }

// sameCollector compares every collector read endpoint for one session,
// byte for byte in meaning: there is nothing to normalise.
func sameCollector(t *testing.T, a, b backend, id string) {
	t.Helper()
	sp := "/v1/sessions/" + id
	for _, p := range []string{
		sp + "/summary", sp + "/trace", sp + "/entities",
		sp + "/events", sp + "/events?from_seq=0", sp + "/events?from_seq=1&limit=10000", sp + "/events?from_seq=5&limit=3",
		sp + "/events?from_seq=4&limit=0", sp + "/events?from_seq=x&limit=y", sp + "/events?from_seq=100000",
		"/v1/sessions?ids=" + id, "/v1/sessions?ids=ts_nope," + id + ",," + id, "/v1/sessions?ids=", "/v1/sessions?ids=bad/id", "/v1/sessions?ids=%zz",
		// A session nobody recorded reads as an empty one.
		"/v1/sessions/ts_nope/summary", "/v1/sessions/ts_nope/trace", "/v1/sessions/ts_nope/entities", "/v1/sessions/ts_nope/events",
		"/v1/sessions/ts_nope/entities/postgres/orders/1",
	} {
		same(t, a, b, ofCol, p, nil)
	}
	// Paging with next_seq, as client.Events and `tracectl export` do it,
	// reads every event once on both.
	for _, be := range []backend{a, b} {
		var seqs []string
		for from, pages := "1", 0; ; pages++ {
			r := fetch(t, be.col, sp+"/events?limit=7&from_seq="+from)
			page := r.Body.(map[string]any)
			evs := page["events"].([]any)
			for _, e := range evs {
				seqs = append(seqs, e.(map[string]any)["seq"].(json.Number).String())
			}
			if next := page["next_seq"].(json.Number).String(); len(evs) < 7 || next == from || pages > 1000 {
				break
			} else {
				from = next
			}
		}
		for i, s := range seqs {
			if s != fmt.Sprint(i+1) {
				t.Errorf("%s: paging /events gave seq %s at position %d (%d events)", be.name, s, i, len(seqs))
				break
			}
		}
	}
	// Every entity's history, by its canonical key and, for a one-column
	// key, by the bare value.
	ents, _ := same(t, a, b, ofCol, sp+"/entities", nil).Body.([]any)
	if len(ents) == 0 {
		t.Errorf("%s: no entities to compare histories of", id)
	}
	for _, e := range ents {
		ent := e.(map[string]any)
		st, en, pk := ent["store"].(string), ent["entity"].(string), ent["pk"].(string)
		hp := sp + "/entities/" + url.PathEscape(st) + "/" + url.PathEscape(en) + "/"
		same(t, a, b, ofCol, hp+url.PathEscape(pk), nil)
		var key map[string]any
		if json.Unmarshal([]byte(pk), &key) == nil && len(key) == 1 {
			for _, v := range key {
				same(t, a, b, ofCol, hp+url.PathEscape(fmt.Sprint(v)), nil)
				if f, ok := v.(float64); ok {
					same(t, a, b, ofCol, hp+url.PathEscape(fmt.Sprintf("%.0f", f)), nil)
				}
			}
		}
		same(t, a, b, ofCol, hp+"no-such-key", nil)
	}
}

// ingest sends events to the collector the way an SDK does: one framed
// protobuf batch to POST /v1/ingest.
func ingest(t *testing.T, colURL string, evs []core.Event) {
	t.Helper()
	body, err := tsessionproto.Encode(evs)
	if err != nil {
		panic(err)
	}
	resp, err := plain.Post(colURL+"/v1/ingest", tsessionproto.ContentType, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var res struct {
		Accepted int `json:"accepted"`
	}
	if resp.StatusCode != 200 || json.Unmarshal(b, &res) != nil || res.Accepted != len(evs) {
		panic(fmt.Sprintf("ingest of %d events: %d %s", len(evs), resp.StatusCode, b))
	}
}

// stored waits until the collector has sequenced n events of the session.
func stored(t *testing.T, colURL, id string, n int) {
	t.Helper()
	waitFor(t, 10*time.Second, fmt.Sprintf("%d events of %s stored", n, id), func() bool {
		sum, ok := fetch(t, colURL, "/v1/sessions/"+id+"/summary").Body.(map[string]any)
		return ok && fmt.Sprint(sum["event_count"]) == fmt.Sprint(n)
	})
}

// relabel returns evs as events of session id.
func relabel(evs []core.Event, id string) []core.Event {
	out := make([]core.Event, len(evs))
	for i, e := range evs {
		e.SessionID, e.Seq = id, 0
		out[i] = e
	}
	return out
}

// edgeEvents is a session made of the cases in which the views have to
// decide something: a transaction that never ended, a commit that failed,
// jobs that no worker finished (one of them still retrying), a job whose
// worker finished before its publish was sequenced, keys that are composite,
// unknown, numeric from a million on or hold a slash, spans without an id
// and with a repeated one, a span whose parent is also its link, events
// dropped by an SDK, and a change whose key matched several rows.
func edgeEvents(id string, base time.Time) []core.Event {
	var evs []core.Event
	add := func(span, parent string, kind core.Kind, name string, startMs, endMs int, f func(*core.Event)) {
		e := core.Event{SessionID: id, TraceID: "tr_edge", SpanID: span, ParentID: parent, Service: "edge-api", Instance: "edge-api-1",
			Kind: kind, Name: name, LocalSeq: uint64(len(evs) + 1),
			StartNs: base.Add(time.Duration(startMs) * time.Millisecond).UnixNano(), EndNs: base.Add(time.Duration(endMs) * time.Millisecond).UnixNano()}
		e.HLC, e.EndHLC = uint64(e.StartNs), uint64(e.EndNs)
		if f != nil {
			f(&e)
		}
		evs = append(evs, e)
	}
	mut := func(tx string, m core.Mutation) func(*core.Event) {
		return func(e *core.Event) { e.TxID, e.Mutations = tx, []core.Mutation{m} }
	}
	links := func(attrs map[string]string, l ...string) func(*core.Event) {
		return func(e *core.Event) { e.LinkIDs, e.Attrs = l, attrs }
	}
	after := func(v any) core.PayloadRef {
		b, _ := json.Marshal(v)
		return core.PayloadRef{Inline: b, Size: len(b)}
	}
	add("sp_root", "", core.KindAPIIn, "POST /edge", 0, 900, func(e *core.Event) { e.Err = "500 Internal Server Error" })
	// A transaction that is still open when the session is sealed.
	add("sp_begin1", "sp_root", core.KindDBTxBegin, "BEGIN", 10, 11, func(e *core.Event) { e.TxID = "tx_open" })
	add("sp_w1", "sp_root", core.KindDBWrite, "UPDATE orders", 12, 20, mut("tx_open", core.Mutation{Store: "postgres", Entity: "orders", Op: "update",
		PK: map[string]any{"id": 1000000}, Before: after(map[string]any{"status": "new"}), After: after(map[string]any{"status": "paid"}),
		Diff: []core.FieldChange{{Field: "status", Old: "new", New: "paid"}}, Source: "sdk"}))
	// A commit that failed: its writes did not take effect.
	add("sp_begin2", "sp_root", core.KindDBTxBegin, "BEGIN", 30, 31, func(e *core.Event) { e.TxID = "tx_bad" })
	add("sp_w2", "sp_root", core.KindDBWrite, "INSERT INTO line/items", 32, 40, mut("tx_bad", core.Mutation{Store: "postgres", Entity: "line/items", Op: "insert",
		PK: map[string]any{"order/id": "o/1", "n": 2}, After: after(map[string]any{"qty": 3})}))
	add("sp_commit2", "sp_root", core.KindDBTxCommit, "COMMIT", 41, 45, func(e *core.Event) {
		e.TxID, e.Attrs = "tx_bad", map[string]string{"commit_failed": "true"}
	})
	// The same row again, outside a transaction; and a change with no key.
	add("sp_w3", "sp_root", core.KindDBWrite, "UPDATE orders", 50, 55, mut("", core.Mutation{Store: "postgres", Entity: "orders", Op: "update",
		PK: map[string]any{"id": 1000000}, After: after(map[string]any{"status": "void"})}))
	add("sp_w4", "sp_root", core.KindDBWrite, "DELETE FROM audit", 56, 58, func(e *core.Event) {
		e.Mutations = []core.Mutation{{Store: "postgres", Entity: "audit", Op: "delete"}, {Store: "postgres", Entity: "audit", Op: "delete"}}
		e.Flags = []string{"pk_not_unique", "after_unavailable"}
	})
	add("sp_w5", "sp_root", core.KindCacheWrite, "SET k", 59, 60, mut("", core.Mutation{Store: "redis", Entity: "k:*", Op: "set", PK: map[string]any{"key": "k:1"}}))
	// A job whose only attempt failed with a retry scheduled: orphaned and
	// retry_pending once sealed. Its worker span has the publish as parent
	// and as link.
	add("sp_pub1", "sp_root", core.KindQueuePub, "jobs.retry", 100, 101, nil)
	add("sp_ws1", "sp_pub1", core.KindWorkerStart, "jobs.retry", 110, 150, links(nil, "sp_pub1"))
	add("sp_we1", "sp_ws1", core.KindWorkerEnd, "jobs.retry", 150, 150, links(map[string]string{"retry_pending": "true"}, "sp_pub1"))
	// A job that a worker finished.
	add("sp_pub2", "sp_root", core.KindQueuePub, "jobs.ok", 200, 201, nil)
	add("sp_ws2", "", core.KindWorkerStart, "jobs.ok", 210, 250, links(nil, "sp_pub2"))
	add("sp_we2", "sp_ws2", core.KindWorkerEnd, "jobs.ok", 250, 250, links(nil, "sp_pub2"))
	// A job sent to the dead-letter queue before its publish span ended.
	add("sp_dlq3", "", core.KindWorkerDLQ, "jobs.dead", 300, 301, links(nil, "sp_pub3"))
	add("sp_pub3", "sp_root", core.KindQueuePub, "jobs.dead", 290, 310, nil)
	// A job nobody picked up.
	add("sp_pub4", "sp_root", core.KindQueuePub, "jobs.lost", 320, 321, nil)
	// Spans the tree has to place without a usable id.
	add("", "sp_root", core.KindDBRead, "SELECT 1", 400, 401, nil)
	add("sp_dup", "sp_root", core.KindDBRead, "SELECT 2", 402, 403, nil)
	add("sp_dup", "sp_root", core.KindDBRead, "SELECT 3", 404, 405, nil)
	add("sp_child_of_dup", "sp_dup", core.KindCacheRead, "GET k", 404, 404, nil)
	add("sp_lost_parent", "sp_never_seen", core.KindAPIOut, "GET https://pay.example.invalid/", 500, 520, func(e *core.Event) { e.Err = "timeout" })
	add("sp_drop1", "", core.KindSDKDropped, "sdk.dropped", 600, 600, func(e *core.Event) { e.Attrs = map[string]string{"count": "7"} })
	add("sp_drop2", "", core.KindSDKDropped, "sdk.dropped", 601, 601, func(e *core.Event) { e.Attrs = map[string]string{"count": "x1"} })
	return evs
}

// liveFixture is three sessions recorded by a real control plane and
// collector and sealed, and the same three exported to files. The third is
// one the collector finds nothing wrong with: the control plane marked it
// incomplete, for events a service recorded and could not deliver.
type liveFixture struct {
	in    *infra
	live  backend
	ids   []string // oldest first
	names []string
	files []string
}

func newLiveFixture(t *testing.T) *liveFixture {
	t.Helper()
	in := newInfra(t, store.NewMem())
	f := &liveFixture{in: in, live: backend{"the services", in.cpSrv.URL, in.colSrv.URL}, names: []string{"checkout", "edge cases", "undelivered"}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	demo := dashboard.NewDemo()
	dir := t.TempDir()
	for i, name := range f.names {
		// Units of 100ms: quiet for 0.3s, and at most 1.5s for the jobs that
		// will never finish.
		s, err := client.Start(ctx, client.Options{BaseURL: in.cpSrv.URL, CollectorURL: in.colSrv.URL, Name: name, InitiatedBy: "api",
			TTLSeconds: 600, QuiesceSeconds: 3, MaxDrainSeconds: 15, PollInterval: 50 * time.Millisecond})
		if err != nil {
			panic(err)
		}
		var evs []core.Event
		switch i {
		case 0:
			// The demo's main session: the recording the dashboard was built around.
			evs = relabel(demo.Recorded("ts_demo0001"), s.ID)
		case 1:
			evs = edgeEvents(s.ID, time.Now())
		default:
			evs = undeliveredEvents(t, in.cpSrv.URL, s.ID, time.Now())
		}
		ingest(t, in.colSrv.URL, evs)
		stored(t, in.colSrv.URL, s.ID, len(evs))
		if info, err := s.StopAndWait(ctx); err != nil || info.State != "SEALED" {
			panic(fmt.Sprintf("stop %s: %+v %v", name, info, err))
		}
		// The opened session is named after its file.
		file := filepath.Join(dir, name+".ndjson")
		out, err := os.Create(file)
		if err != nil {
			panic(err)
		}
		if n, err := s.Export(ctx, out); err != nil || n != len(evs) {
			panic(fmt.Sprintf("export %s: %d events, %v", name, n, err))
		}
		out.Close()
		f.ids, f.files = append(f.ids, s.ID), append(f.files, file)
	}
	return f
}

// undeliveredEvents is a session in which nothing went wrong that an event
// shows: a request of one service and its two writes, all at the collector. What went wrong
// was reported to the control plane (it tells the id's session so, here):
// another service recorded seven events and dropped them all, and the first
// stopped reporting before its last event.
func undeliveredEvents(t *testing.T, cpURL, id string, base time.Time) []core.Event {
	t.Helper()
	for _, body := range []string{
		`{"service":"orders-api","instance":"orders-api-1","recorded":7,"dropped":7,"last_error":"connection refused","final":true}`,
		`{"service":"inventory","instance":"inventory-1","recorded":2,"delivered":2}`,
	} {
		resp, err := plain.Post(cpURL+"/v1/sessions/"+id+"/delivery", "application/json", strings.NewReader(body))
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			panic(fmt.Sprintf("delivery report: %s", resp.Status))
		}
	}
	var evs []core.Event
	for i, kind := range []core.Kind{core.KindAPIIn, core.KindDBWrite, core.KindDBWrite} {
		start := base.Add(time.Duration(i) * 10 * time.Millisecond).UnixNano()
		e := core.Event{SessionID: id, TraceID: "tr_und", SpanID: fmt.Sprintf("sp_und%d", i), Service: "inventory", Instance: "inventory-1",
			Kind: kind, Name: "POST /reserve", LocalSeq: uint64(i + 1), StartNs: start, EndNs: start + 1e6, HLC: uint64(start), EndHLC: uint64(start + 1e6)}
		if i > 0 {
			e.ParentID, e.Name = "sp_und0", "UPDATE stock"
			e.Mutations = []core.Mutation{{Store: "postgres", Entity: "stock", Op: "update", PK: map[string]any{"sku": fmt.Sprintf("sku-%d", i)},
				Diff: []core.FieldChange{{Field: "reserved", Old: 0, New: 1}}, Source: "sdk"}}
		}
		evs = append(evs, e)
	}
	return evs
}

// fileSession removes from a control-plane session (or a list of them) the
// fields an export does not hold. A file is the events and the session's
// verdict, so the dashboard makes these up:
//
//	initiated_by                      "file"
//	created_at, started_at, ended_at  the first and last event time (the control plane has wall-clock times)
//	ttl_seconds, quiesce_seconds, max_drain_seconds   0: nothing drains a file (the limit of a drain that timed out is kept)
//	bundle_uri                        empty: the file is the bundle
//	delivery                          absent: the verdict on the reports is kept, the reports are not
//
// Everything else (id, name when the file is named after the session, mode,
// state, incomplete with its reasons, delivery_unconfirmed, drain_timed_out,
// dropped_events) must agree.
func fileSession(v any) any {
	switch x := v.(type) {
	case []any:
		for i := range x {
			x[i] = fileSession(x[i])
		}
	case map[string]any:
		for _, k := range []string{"initiated_by", "created_at", "started_at", "ended_at", "ttl_seconds", "quiesce_seconds", "max_drain_seconds",
			"bundle_uri", "delivery"} {
			delete(x, k)
		}
	}
	return v
}

func TestOfflineAnswersLikeTheServices(t *testing.T) {
	f := newLiveFixture(t)
	h, err := dashboard.New(dashboard.Config{Open: f.files})
	if err != nil {
		panic(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	file := backend{"the opened files", srv.URL + "/api/cp", srv.URL + "/api/col"}

	// The collector: every answer is the same.
	for _, id := range f.ids {
		sameCollector(t, f.live, file, id)
	}
	same(t, f.live, file, ofCol, "/v1/sessions", nil)
	same(t, f.live, file, ofCol, "/v1/sessions?ids="+strings.Join(f.ids, ","), nil)
	// The edge session is what it was built to be, on both.
	sum := fetch(t, file.col, "/v1/sessions/"+f.ids[1]+"/summary").Body.(map[string]any)
	flags, _ := sum["flags"].(map[string]any)
	if got := fmt.Sprintln(sum["incomplete"], sum["sealed"], sum["dropped_events"], sum["pending_async"], sum["pending_async_ids"],
		flags["orphaned"], flags["retry_pending"], flags["tx_unresolved"]); got != "true true 7 2 [sp_pub1 sp_pub4] 2 1 1\n" {
		t.Errorf("summary of the edge session: %s", got)
	}

	// The control plane: the sessions and the list, with its paging.
	for _, id := range f.ids {
		same(t, f.live, file, ofCP, "/v1/sessions/"+id, fileSession)
	}
	for _, p := range []string{
		"/v1/sessions", "/v1/sessions?state=SEALED", "/v1/sessions?state=ACTIVE", "/v1/sessions?name=" + url.QueryEscape(f.names[1]),
		"/v1/sessions?name=nope", "/v1/sessions?limit=5", "/v1/sessions?limit=3&state=SEALED", "/v1/sessions?limit=1&name=checkout",
		// Errors, with their status and message.
		"/v1/sessions/ts_nope", "/v1/sessions?limit=0", "/v1/sessions?limit=1001", "/v1/sessions?limit=x",
		"/v1/sessions?limit=2&before=%25%25", "/v1/sessions?before=abc",
		"/v1/subscribers",
	} {
		same(t, f.live, file, ofCP, p, fileSession)
	}
	// A page with a next page. The cursor holds the creation time, which
	// differs, so each backend is followed with its own cursor.
	var pages [2][]reply
	for i, be := range []backend{f.live, file} {
		for q := "/v1/sessions?limit=1"; q != ""; {
			r := fetch(t, be.cp, q)
			next := r.Headers["X-Tsession-Next"]
			delete(r.Headers, "X-Tsession-Next")
			r.Body = fileSession(r.Body)
			pages[i] = append(pages[i], r)
			if q = ""; next != "" {
				q = "/v1/sessions?limit=1&before=" + url.QueryEscape(next)
			}
		}
	}
	if d := firstDiff("", fmt.Sprint(pages[0]), fmt.Sprint(pages[1])); d != "" || len(pages[0]) != 3 {
		t.Errorf("paging the list one session at a time: %d and %d pages, %s", len(pages[0]), len(pages[1]), d)
	}
	// The active set: generation counts the changes the control plane made
	// to it, and a file never had any.
	same(t, f.live, file, ofCP, "/v1/active", func(v any) any {
		delete(v.(map[string]any), "generation")
		return v
	})
	for _, p := range []string{"/healthz"} {
		if a, b := fetch(t, f.live.cp, p), fetch(t, file.cp, p); a.Status != 200 || b.Status != 200 {
			t.Errorf("GET %s: %d and %d", p, a.Status, b.Status)
		}
		if a, b := fetch(t, f.live.col, p), fetch(t, file.col, p); a.Status != 200 || b.Status != 200 {
			t.Errorf("GET %s on the collector: %d and %d", p, a.Status, b.Status)
		}
	}

	// The Go client reads the file as it reads the session.
	ctx := context.Background()
	for _, id := range f.ids {
		var got [2]string
		for i, be := range []backend{f.live, file} {
			s, err := client.Attach(ctx, client.Options{BaseURL: be.cp, CollectorURL: be.col}, id)
			if err != nil {
				panic(fmt.Sprintf("attach to %s on %s: %v", id, be.name, err))
			}
			sum, err1 := s.Summary(ctx)
			evs, err2 := s.Events(ctx)
			ents, err3 := s.Entities(ctx)
			tr, err4 := s.Trace(ctx)
			muts, err5 := s.Mutations(ctx, client.MutationFilter{ExcludeRolledBack: true})
			if err := cmpErr(err1, err2, err3, err4, err5); err != nil {
				panic(fmt.Sprintf("client on %s: %v", be.name, err))
			}
			b, _ := json.Marshal([]any{sum, evs, ents, tr, muts, s.Info().State, s.Info().Incomplete, s.Info().DroppedEvents})
			got[i] = string(b)
		}
		if got[0] != got[1] {
			t.Errorf("client results of %s differ between the services and the file", id)
		}
	}
}

func cmpErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// The demo's sessions, recorded by a real collector, read the same from the
// collector as from the demo: sealed ones, an incomplete one and one that is
// still running.
func TestDemoAnswersLikeTheCollector(t *testing.T) {
	in := newInfra(t, store.NewMem())
	d := dashboard.NewDemo()
	mux := http.NewServeMux()
	mux.Handle("/cp/", http.StripPrefix("/cp", d.ControlPlane()))
	mux.Handle("/col/", http.StripPrefix("/col", d.Collector()))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	demo := backend{"the demo", srv.URL + "/cp", srv.URL + "/col"}
	live := backend{"the collector", in.cpSrv.URL, in.colSrv.URL}

	ids := []string{"ts_demo0001", "ts_demo0003", "ts_demo0004", "ts_demo0005"}
	for _, id := range ids {
		var sess controlplane.Session
		b, _ := json.Marshal(fetch(t, demo.cp, "/v1/sessions/"+id).Body)
		if err := json.Unmarshal(b, &sess); err != nil || sess.ID != id {
			panic(fmt.Sprintf("demo session %s: %v %s", id, err, b))
		}
		evs := d.Recorded(id)
		if len(evs) < 10 {
			panic(fmt.Sprintf("%s: %d events", id, len(evs)))
		}
		ingest(t, in.colSrv.URL, evs)
		stored(t, in.colSrv.URL, id, len(evs))
		if sess.State == controlplane.StateSealed {
			if _, err := in.col.Seal(context.Background(), id); err != nil {
				panic(err)
			}
		}
		sameCollector(t, live, demo, id)
	}
	same(t, live, demo, ofCol, "/v1/sessions?ids="+strings.Join(ids, ","), nil)

	// The control plane's side of the demo is written by the control plane's
	// own types; what a running and a finished session look like on the wire
	// is compared with sessions of the real one, key by key.
	ctx := context.Background()
	s, err := client.Start(ctx, client.Options{BaseURL: in.cpSrv.URL, Name: "shape", QuiesceSeconds: 1, MaxDrainSeconds: 5, PollInterval: 50 * time.Millisecond,
		Bindings: []client.Binding{client.BindHeader("x-device-id", "dev_1")}})
	if err != nil {
		panic(err)
	}
	keys := func(be backend, id string) string {
		m, _ := fetch(t, be.cp, "/v1/sessions/"+id).Body.(map[string]any)
		ks := make([]string, 0, len(m))
		for k, v := range m {
			ks = append(ks, fmt.Sprintf("%s:%T", k, v))
		}
		sort.Strings(ks)
		return strings.Join(ks, " ")
	}
	if a, b := keys(live, s.ID), keys(demo, "ts_demo0004"); a != b {
		t.Errorf("a running session\n the control plane: %s\n the demo:          %s", a, b)
	}
	if _, err := s.StopAndWait(ctx); err != nil {
		panic(err)
	}
	// The demo's sealed session has a bundle; the mem store writes none, and
	// the real one lost its bindings' liveness, not the field.
	if a, b := keys(live, s.ID), keys(demo, "ts_demo0001")+" bindings:[]interface {}"; strings.Join(sortedFields(a), " ") != strings.Join(sortedFields(b), " ") {
		t.Errorf("a sealed session\n the control plane: %s\n the demo:          %s", a, b)
	}
	// The active set and the followers decode, strictly, as the control
	// plane's own documents.
	var act controlplane.ActiveSet
	b, _ := json.Marshal(fetch(t, demo.cp, "/v1/active").Body)
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&act); err != nil || act.Generation == 0 || len(act.Active) != 2 || len(act.Bindings) != 2 {
		t.Errorf("the demo's active set: %v %s", err, b)
	}
	var subs []controlplane.Subscriber
	b, _ = json.Marshal(fetch(t, demo.cp, "/v1/subscribers").Body)
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&subs); err != nil || len(subs) != 3 || subs[0].Generation != act.Generation || subs[0].ConnectedAt.IsZero() {
		t.Errorf("the demo's subscribers: %v %s", err, b)
	}
}

func sortedFields(s string) []string {
	f := strings.Fields(s)
	sort.Strings(f)
	return f
}

// tracectlAt runs the tracectl binary against a backend and returns its
// exit code, stdout and stderr.
func tracectlAt(t *testing.T, bin string, be backend, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(bin, append([]string{"--cp", be.cp, "--collector", be.col}, args...)...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	cmd.Env = append(os.Environ(), "TSESSION_CONTROLPLANE_URL=", "TSESSION_COLLECTOR_URL=")
	err := cmd.Run()
	if _, exited := err.(*exec.ExitError); err != nil && !exited {
		panic(fmt.Sprintf("tracectl %v: %v", args, err))
	}
	return cmd.ProcessState.ExitCode(), out.String(), errb.String()
}

// startDashboard runs the dashboard binary on a free loopback port and
// returns the backend named by the tracectl flags it prints.
func startDashboard(t *testing.T, bin string, args ...string) backend {
	t.Helper()
	cmd := exec.Command(bin, append([]string{"-listen", "127.0.0.1:0"}, args...)...)
	cmd.Env = append(os.Environ(), "TSESSION_DASHBOARD_ADDR=", "TSESSION_DASHBOARD_DEMO=")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	found := make(chan backend, 1)
	go func() {
		flags := regexp.MustCompile(`tracectl --cp (\S+) --collector (\S+) `)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if m := flags.FindStringSubmatch(sc.Text()); m != nil {
				found <- backend{"dashboard " + strings.Join(args, " "), m[1], m[2]}
			}
		}
	}()
	select {
	case be := <-found:
		return be
	case <-time.After(20 * time.Second):
		panic(fmt.Sprintf("dashboard %v did not print the tracectl flags", args))

	}
}

// statusDoc is the session document `tracectl status` prints, less what a
// file does not hold (see fileSession), and the lines after it.
func statusDoc(t *testing.T, out string) string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(out))
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		panic(fmt.Sprintf("tracectl status printed no session document: %v\n%s", err, out))
	}
	rest, _ := io.ReadAll(dec.Buffered())
	b, _ := json.Marshal(fileSession(doc))
	return string(b) + "\n" + strings.TrimSpace(string(rest))
}

// The binaries, as a person or an agent uses them: `tracectl export`, then
// `dashboard -open` on the file, then the same tracectl commands with the
// flags the dashboard printed. Every read command prints for the file what
// it prints for the session on the live services.
func TestTracectlReadsAnExportLikeTheLiveSession(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs two binaries")
	}
	f := newLiveFixture(t)
	dir := t.TempDir()
	tracectl := buildBinary(t, dir, "tracectl", ".", "./cmd/tracectl")
	dash := buildBinary(t, dir, "dashboard", ".", "./cmd/dashboard")

	// Export with the CLI this time; it writes what the client wrote.
	var open []string
	for i, id := range f.ids {
		file := filepath.Join(dir, f.names[i]+".ndjson")
		if code, _, errOut := tracectlAt(t, tracectl, f.live, "export", id, "-o", file); code != 0 {
			panic(fmt.Sprintf("tracectl export: %d %s", code, errOut))
		}
		a, _ := os.ReadFile(file)
		b, _ := os.ReadFile(f.files[i])
		if len(a) == 0 || !bytes.Equal(a, b) {
			panic(fmt.Sprintf("tracectl export of %s differs from client.Export", id))
		}
		open = append(open, "-open", file)
	}
	file := startDashboard(t, dash, open...)

	for i, id := range f.ids {
		// One entity of the session, for history.
		_, out, _ := tracectlAt(t, tracectl, f.live, "entities", id, "--json")
		var ents []client.Entity
		if err := json.Unmarshal([]byte(out), &ents); err != nil || len(ents) == 0 {
			panic(fmt.Sprintf("entities of %s: %v %s", id, err, out))
		}
		e := ents[len(ents)-1]
		for _, args := range [][]string{
			{"summary", id}, {"summary", id, "--json"}, {"summary", f.names[i]},
			{"summary", id, "--fail-on-error", "--fail-on-incomplete"}, // exits 3 on both
			// The verdict travels with the export: the gates give the file the
			// exit status they give the session. (They used to pass a file
			// whose session the control plane had marked incomplete.)
			{"summary", id, "--fail-on-incomplete"}, {"report", id, "--fail-on-incomplete"}, {"digest", id, "--fail-on-incomplete"},
			{"summary", id, "--fail-on-unconfirmed"}, {"report", id, "--format", "junit", "--fail-on-unconfirmed"},
			{"status", id},
			{"events", id}, {"events", id, "--json"}, {"events", id, "--errors"},
			{"trace", id}, {"trace", id, "--json"},
			{"entities", id}, {"entities", id, "--json"},
			{"history", id, e.Store, e.Entity, e.PK}, {"history", id, e.Store, e.Entity, e.PK, "--json"},
			{"show", id, "3"}, {"show", id, "3", "--json"},
			{"report", id}, {"report", id, "--format", "junit"},
			{"digest", id}, {"digest", id, "--format", "json"},
			{"diff", f.ids[0], id}, {"diff", f.ids[0], id, "--json"},
			{"status", id, "--short"}, {"list", "--state", "SEALED"},
		} {
			wantCode, want, wantErr := tracectlAt(t, tracectl, f.live, args...)
			code, got, gotErr := tracectlAt(t, tracectl, file, args...)
			if args[0] == "status" && len(args) == 2 {
				// The session document, less what a file does not hold.
				want, got = statusDoc(t, want), statusDoc(t, got)
			}
			if strings.TrimSpace(want) == "" || (wantCode != 0 && wantCode != 3) {
				panic(fmt.Sprintf("tracectl %v on the live session: exit %d\n%s%s", args, wantCode, want, wantErr))
			}
			if code != wantCode || got != want || gotErr != wantErr {
				t.Errorf("tracectl %v: exit %d on the file, %d on the session\n--- file\n%s%s\n--- session\n%s%s", args, code, wantCode, got, gotErr, want, wantErr)
			}
		}
	}
	// The session with the undelivered events: the collector's summary shows
	// nothing, the gates fail on the control plane's verdict, on both.
	und := f.ids[2]
	for be, name := range map[backend]string{f.live: "the session", file: "the file"} {
		if code, out, errOut := tracectlAt(t, tracectl, be, "summary", und, "--fail-on-error"); code != 0 || !strings.Contains(out, "(SEALED, incomplete, delivery unconfirmed)") {
			t.Errorf("summary --fail-on-error of %s: exit %d\n%s%s", name, code, out, errOut)
		}
		for _, cmd := range []string{"summary", "report", "digest"} {
			if code, _, errOut := tracectlAt(t, tracectl, be, cmd, und, "--fail-on-incomplete"); code != 3 ||
				!strings.Contains(errOut, "service orders-api (instance orders-api-1): 0 of 7 recorded events reached the collector, 7 dropped by the SDK") {
				t.Errorf("%s --fail-on-incomplete of %s: exit %d %s", cmd, name, code, errOut)
			}
		}
		if code, _, errOut := tracectlAt(t, tracectl, be, "summary", und, "--fail-on-unconfirmed"); code != 3 ||
			!strings.Contains(errOut, "delivery is unconfirmed (service inventory (instance inventory-1): the collector has 3 events, more than the 2 of the instance's last report") {
			t.Errorf("summary --fail-on-unconfirmed of %s: exit %d %s", name, code, errOut)
		}
	}
	// The second session's drain ran into its limit: the file says so too.
	for be, name := range map[backend]string{f.live: "the session", file: "the file"} {
		if _, out, _ := tracectlAt(t, tracectl, be, "report", f.ids[1]); !strings.Contains(out, "Drain timed out: asynchronous work was still outstanding after the 15s drain limit.") {
			t.Errorf("report of %s does not say that the drain timed out:\n%s", name, out)
		}
	}

	// Changing a file is refused, in words that say why.
	for _, args := range [][]string{{"stop", f.ids[0]}, {"abort", f.ids[0]}, {"rename", f.ids[0], "x"}, {"delete", f.ids[0]}, {"kill", "on"}} {
		if code, out, errOut := tracectlAt(t, tracectl, file, args...); code != 1 || out != "" || !strings.Contains(errOut, "read-only") ||
			!(strings.Contains(errOut, "is a file (checkout.ndjson") || strings.Contains(errOut, "opened from files")) {
			t.Errorf("tracectl %v on a file: exit %d\n%s%s", args, code, out, errOut)
		}
	}

	// The demo, through the same binaries: a sealed and a running session.
	demo := startDashboard(t, dash, "-demo")
	for _, id := range []string{"ts_demo0001", "ts_demo0005", "ts_demo0004"} {
		for _, args := range [][]string{{"summary", id}, {"digest", id}, {"digest", id, "--format", "json"}, {"trace", id}, {"report", id}} {
			if code, out, errOut := tracectlAt(t, tracectl, demo, args...); code != 0 || errOut != "" || strings.TrimSpace(out) == "" {
				t.Errorf("tracectl %v on the demo: exit %d\n%s%s", args, code, out, errOut)
			}
		}
	}
	if _, out, _ := tracectlAt(t, tracectl, demo, "digest", "ts_demo0004"); !strings.Contains(out, "state: ACTIVE") || !strings.Contains(out, "**Live**") {
		t.Errorf("digest of the demo's running session:\n%s", out)
	}
}
