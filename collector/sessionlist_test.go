package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// listSpy counts the store calls a session list makes, fails the per-session
// lookups of the ids in bad and records how many lookups ran at once.
type listSpy struct {
	store.Store
	lists, heads, seals, events atomic.Int64
	running, peak               atomic.Int64
	slow                        time.Duration

	mu      sync.Mutex
	badHead map[string]bool
	badSeal map[string]bool
}

func (s *listSpy) enter() func() {
	n := s.running.Add(1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if s.slow > 0 {
		time.Sleep(s.slow)
	}
	return func() { s.running.Add(-1) }
}

func (s *listSpy) fails(m map[string]bool, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return m[id]
}

func (s *listSpy) ListSessions(ctx context.Context) ([]store.SessionInfo, error) {
	s.lists.Add(1)
	return s.Store.ListSessions(ctx)
}

func (s *listSpy) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	s.events.Add(1)
	return s.Store.Events(ctx, id, from, limit)
}

func (s *listSpy) Head(ctx context.Context, id string) (uint64, error) {
	defer s.enter()()
	s.heads.Add(1)
	if s.fails(s.badHead, id) {
		return 0, errInjected
	}
	return s.Store.Head(ctx, id)
}

func (s *listSpy) GetSeal(ctx context.Context, id string) (*store.SealInfo, error) {
	defer s.enter()()
	s.seals.Add(1)
	if s.fails(s.badSeal, id) {
		return nil, errInjected
	}
	return s.Store.GetSeal(ctx, id)
}

// seedSessions stores n events for each id and seals the ids in sealed,
// straight in the store: no actor, no cached view.
func seedSessions(t *testing.T, st store.Store, counts map[string]int, sealed ...string) {
	t.Helper()
	ctx := context.Background()
	for id, n := range counts {
		evs := make([]core.Event, n)
		for i := range evs {
			evs[i] = core.Event{SessionID: id, SpanID: fmt.Sprintf("s%d", i), Service: "svc", Instance: "i1", Kind: core.KindDBRead,
				Seq: uint64(i + 1), LocalSeq: uint64(i + 1), StartNs: int64(100 + i), EndNs: int64(105 + i)}
		}
		if err := st.AppendEvents(ctx, id, evs); err != nil {
			panic(err)
		}
	}
	for _, id := range sealed {
		if err := st.PutSeal(ctx, id, store.SealInfo{SealedAt: time.Now()}); err != nil {
			panic(err)
		}
	}
}

// listGet returns the status, the X-Tsession-Unanswered header and the body.
func listGet(t *testing.T, url string) (int, string, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return resp.StatusCode, resp.Header.Get(headerUnanswered), b
}

func decodeList(t *testing.T, b []byte) []map[string]any {
	t.Helper()
	var l []map[string]any
	if err := json.Unmarshal(b, &l); err != nil {
		panic(fmt.Sprintf("%v: %s", err, b))
	}
	return l
}

// Without ids the list is what it always was: every session, all five fields,
// by one ListSessions.
func TestSessionListWithoutIDsIsUnchanged(t *testing.T) {
	sp := &listSpy{Store: store.NewMem()}
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	code, hdr, b := listGet(t, srv.URL+"/v1/sessions")
	if code != 200 || strings.TrimSpace(string(b)) != "[]" || hdr != "" {
		panic(fmt.Sprintf("empty list: %d %q %s", code, hdr, b))
	}
	seedSessions(t, sp, map[string]int{"ts_a": 3, "ts_b": 1}, "ts_b")
	code, hdr, b = listGet(t, srv.URL+"/v1/sessions")
	want := `[{"id":"ts_a","event_count":3,"first_ns":100,"last_ns":107,"sealed":false},{"id":"ts_b","event_count":1,"first_ns":100,"last_ns":105,"sealed":true}]`
	if code != 200 || strings.TrimSpace(string(b)) != want || hdr != "" {
		panic(fmt.Sprintf("list: %d %q\n got %s\nwant %s", code, hdr, b, want))
	}
	// Another parameter is not ids.
	if _, _, b2 := listGet(t, srv.URL+"/v1/sessions?limit=1"); string(b2) != string(b) {
		panic(fmt.Sprintf("list with an unrelated parameter: %s", b2))
	}
	if n := sp.lists.Load(); n != 3 {
		panic(fmt.Sprintf("ListSessions calls: %d, want 3", n))
	}
}

// With ids: the requested sessions that have events, in the order asked, with
// id, event_count and sealed; the same values the full list gives.
func TestSessionListByID(t *testing.T) {
	sp := &listSpy{Store: store.NewMem()}
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	seedSessions(t, sp, map[string]int{"ts_a": 3, "ts_b": 1, "ts_c": 7}, "ts_b")
	// A seal without events is not a session the collector holds events for.
	if err := sp.PutSeal(context.Background(), "ts_sealonly", store.SealInfo{SealedAt: time.Now()}); err != nil {
		panic(err)
	}

	code, hdr, b := listGet(t, srv.URL+"/v1/sessions?ids=ts_b,ts_unknown,ts_a,ts_sealonly,,ts_b")
	want := `[{"id":"ts_b","event_count":1,"sealed":true},{"id":"ts_a","event_count":3,"sealed":false}]`
	if code != 200 || strings.TrimSpace(string(b)) != want || hdr != "0" {
		panic(fmt.Sprintf("by id: %d %q\n got %s\nwant %s", code, hdr, b, want))
	}
	// ids given in several parameters are one list.
	_, _, b = listGet(t, srv.URL+"/v1/sessions?ids=ts_c&ids=ts_a")
	if got := decodeList(t, b); len(got) != 2 || got[0]["id"] != "ts_c" || got[1]["id"] != "ts_a" {
		panic(fmt.Sprintf("two ids parameters: %s", b))
	}
	// An empty ids asks for nothing; it is not the full list.
	for _, q := range []string{"?ids=", "?ids=,,"} {
		code, hdr, b = listGet(t, srv.URL+"/v1/sessions"+q)
		if code != 200 || strings.TrimSpace(string(b)) != "[]" || hdr != "0" {
			panic(fmt.Sprintf("%s: %d %q %s", q, code, hdr, b))
		}
	}
	// Each element agrees with the full list.
	var full []store.SessionInfo
	get(t, srv.URL+"/v1/sessions", &full)
	_, _, b = listGet(t, srv.URL+"/v1/sessions?ids=ts_a,ts_b,ts_c")
	byID := decodeList(t, b)
	if len(full) != 3 || len(byID) != 3 {
		panic(fmt.Sprintf("full %d, by id %d", len(full), len(byID)))
	}
	for i, f := range full {
		g := byID[i]
		if g["id"] != f.ID || int(g["event_count"].(float64)) != f.EventCount || g["sealed"] != f.Sealed || len(g) != 3 {
			panic(fmt.Sprintf("element %d: %v, full list has %+v", i, g, f))
		}
	}
}

func TestSessionListByIDRejectsBadRequests(t *testing.T) {
	sp := &listSpy{Store: store.NewMem()}
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	ids := func(n int) string {
		s := make([]string, n)
		for i := range s {
			s[i] = fmt.Sprintf("ts_%04d", i)
		}
		return strings.Join(s, ",")
	}
	for name, q := range map[string]string{
		"invalid id":         "ts_a,bad%20id",
		"leading dot":        ".hidden",
		"path":               "ts_a,..%2Fx",
		"too long":           strings.Repeat("a", 129),
		"too many":           ids(maxListIDs + 1),
		"too many, two keys": ids(maxListIDs) + "&ids=ts_extra",
	} {
		code, _, b := listGet(t, srv.URL+"/v1/sessions?ids="+q)
		var e map[string]string
		if code != 400 || json.Unmarshal(b, &e) != nil || e["error"] == "" {
			panic(fmt.Sprintf("%s: %d %s", name, code, b))
		}
	}
	if n := sp.heads.Load() + sp.seals.Load() + sp.lists.Load(); n != 0 {
		panic(fmt.Sprintf("a rejected request made %d store calls", n))
	}
	// The limit itself is accepted, and repeats do not count towards it.
	if code, hdr, b := listGet(t, srv.URL+"/v1/sessions?ids="+ids(maxListIDs)+","+ids(10)); code != 200 || hdr != "0" || strings.TrimSpace(string(b)) != "[]" {
		panic(fmt.Sprintf("%d ids: %d %q %s", maxListIDs, code, hdr, b))
	}
	if n := sp.heads.Load(); n != maxListIDs {
		panic(fmt.Sprintf("%d ids made %d Head calls", maxListIDs, n))
	}
}

// The ids path costs point lookups for the ids asked, whatever else is
// stored: no ListSessions, no Events, at most listLookups at a time.
func TestSessionListByIDNeverListsOrReadsEvents(t *testing.T) {
	sp := &listSpy{Store: store.NewMem(), slow: time.Millisecond}
	c, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	counts := map[string]int{}
	var all []string
	for i := range 300 {
		id := fmt.Sprintf("ts_%04d", i)
		counts[id] = 1 + i%3
		all = append(all, id)
	}
	seedSessions(t, sp, counts, all[:50]...)
	asked := append(slices.Clone(all[:40]), "ts_none1", "ts_none2")

	code, hdr, b := listGet(t, srv.URL+"/v1/sessions?ids="+strings.Join(asked, ","))
	got := decodeList(t, b)
	if code != 200 || hdr != "0" || len(got) != 40 {
		panic(fmt.Sprintf("%d %q, %d elements", code, hdr, len(got)))
	}
	for i, g := range got {
		if g["id"] != all[i] || int(g["event_count"].(float64)) != counts[all[i]] || g["sealed"] != true {
			panic(fmt.Sprintf("element %d: %v", i, g))
		}
	}
	if l, e := sp.lists.Load(), sp.events.Load(); l != 0 || e != 0 {
		panic(fmt.Sprintf("ListSessions %d, Events %d: want none", l, e))
	}
	// One Head per id asked; one GetSeal per id that has events.
	if h, s := sp.heads.Load(), sp.seals.Load(); h != 42 || s != 40 {
		panic(fmt.Sprintf("Head %d, GetSeal %d: want 42 and 40", h, s))
	}
	if p := sp.peak.Load(); p > listLookups || p < 2 {
		panic(fmt.Sprintf("%d lookups at once, want 2..%d", p, listLookups))
	}
	// Nothing was loaded into the collector either: no actor, no view.
	c.vmu.Lock()
	views := len(c.views)
	c.vmu.Unlock()
	if views != 0 || c.seq.has(all[0]) {
		panic(fmt.Sprintf("views %d, actor %v", views, c.seq.has(all[0])))
	}
}

// A store failure for one id leaves that id out and is counted in the
// header; the other ids are answered.
func TestSessionListByIDSurvivesAFailingLookup(t *testing.T) {
	sp := &listSpy{Store: store.NewMem(), badHead: map[string]bool{"ts_b": true}, badSeal: map[string]bool{"ts_d": true}}
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	seedSessions(t, sp, map[string]int{"ts_a": 2, "ts_b": 2, "ts_c": 2, "ts_d": 2}, "ts_c")

	code, hdr, b := listGet(t, srv.URL+"/v1/sessions?ids=ts_a,ts_b,ts_c,ts_d,ts_unknown")
	want := `[{"id":"ts_a","event_count":2,"sealed":false},{"id":"ts_c","event_count":2,"sealed":true}]`
	if code != 200 || hdr != "2" || strings.TrimSpace(string(b)) != want {
		panic(fmt.Sprintf("%d %q\n got %s\nwant %s", code, hdr, b, want))
	}
	// Every lookup failing is still an answer, not a 500.
	sp.mu.Lock()
	sp.badHead = map[string]bool{"ts_a": true, "ts_b": true}
	sp.mu.Unlock()
	code, hdr, b = listGet(t, srv.URL+"/v1/sessions?ids=ts_a,ts_b")
	if code != 200 || hdr != "2" || strings.TrimSpace(string(b)) != "[]" {
		panic(fmt.Sprintf("all failing: %d %q %s", code, hdr, b))
	}
}

// When the request ends, the lookups not yet started are not made.
func TestSessionListByIDStopsWithTheRequest(t *testing.T) {
	sp := &listSpy{Store: store.NewMem()}
	c, _ := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	seedSessions(t, sp, map[string]int{"ts_a": 1, "ts_b": 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ids := make([]string, 500)
	for i := range ids {
		ids[i] = fmt.Sprintf("ts_%04d", i)
	}
	out, unanswered := c.sessionCounts(ctx, ids)
	if len(out) != 0 || unanswered != len(ids) {
		panic(fmt.Sprintf("%d answered, %d unanswered", len(out), unanswered))
	}
	if n := sp.heads.Load() + sp.seals.Load(); n != 0 {
		panic(fmt.Sprintf("%d store calls after the request ended", n))
	}
}

// A page of the dashboard's ids (200 ids of 30 characters, about 6 KB of
// query) is an ordinary request; so is the largest request allowed.
func TestSessionListByIDAcceptsALongQuery(t *testing.T) {
	sp := &listSpy{Store: store.NewMem()}
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: sp})
	mk := func(n, width int) []string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("ts_%0*d", width-3, i)
		}
		return ids
	}
	page := mk(200, 30)
	seedSessions(t, sp, map[string]int{page[0]: 1, page[199]: 4})
	q := strings.Join(page, ",")
	if len(q) < 6000 {
		panic(fmt.Sprintf("query is %d bytes", len(q)))
	}
	code, hdr, b := listGet(t, srv.URL+"/v1/sessions?ids="+q)
	if got := decodeList(t, b); code != 200 || hdr != "0" || len(got) != 2 || got[1]["id"] != page[199] {
		panic(fmt.Sprintf("%d %q %s", code, hdr, b))
	}
	q = strings.Join(mk(maxListIDs, 128), ",") // 129 KB
	if code, hdr, _ := listGet(t, srv.URL+"/v1/sessions?ids="+q); code != 200 || hdr != "0" {
		panic(fmt.Sprintf("%d ids of 128 bytes: %d %q", maxListIDs, code, hdr))
	}
}
