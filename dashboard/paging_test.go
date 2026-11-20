package dashboard

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
)

// listIDs requests a session list and returns the ids in the order they came.
func listIDs(t *testing.T, base, path string) (*http.Response, []string) {
	t.Helper()
	r, b := get(t, base, path)
	if r.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", path, r.StatusCode, b)
	}
	var ss []Session
	decode(t, b, &ss)
	ids := make([]string, len(ss))
	for i, s := range ss {
		ids[i] = s.ID
	}
	return r, ids
}

// The demo backend pages its session list like the control plane does.
func TestDemoListPaging(t *testing.T) {
	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	s := demoServer(t, func() time.Time { return now })
	const base = "/cp/v1/sessions"

	// Without a limit nothing changes: every session, no paging headers.
	r, ids := listIDs(t, s.URL, base)
	if want := []string{"ts_demo0005", "ts_demo0004", "ts_demo0003", "ts_demo0002", "ts_demo0001"}; !slices.Equal(ids, want) {
		t.Fatalf("unpaged list: %v, want %v", ids, want)
	}
	if r.Header.Get(headerNext) != "" || r.Header.Get(headerTotal) != "" {
		t.Fatalf("unpaged list has paging headers: %v", r.Header)
	}

	// With a limit: newest first by creation time, a cursor while there is more.
	byAge := []string{"ts_demo0002", "ts_demo0004", "ts_demo0005", "ts_demo0001", "ts_demo0003"}
	walk := func(query string, limit int, wantTotal string) []string {
		t.Helper()
		var got []string
		q := fmt.Sprintf("%s?limit=%d%s", base, limit, query)
		for page := 0; page < 20; page++ {
			r, ids := listIDs(t, s.URL, q)
			if r.Header.Get(headerTotal) != wantTotal {
				t.Fatalf("%s: total %q, want %q", q, r.Header.Get(headerTotal), wantTotal)
			}
			got = append(got, ids...)
			next := r.Header.Get(headerNext)
			if next == "" {
				return got
			}
			if len(ids) != limit {
				t.Fatalf("%s: %d sessions and a next page", q, len(ids))
			}
			q = fmt.Sprintf("%s?limit=%d%s&before=%s", base, limit, query, url.QueryEscape(next))
		}
		t.Fatalf("paging %s does not end", query)
		return nil
	}
	for _, limit := range []int{1, 2, 3, 5, 200} {
		if got := walk("", limit, "5"); !slices.Equal(got, byAge) {
			t.Errorf("limit %d: %v, want %v", limit, got, byAge)
		}
	}
	if got := walk("&state=SEALED", 1, "3"); !slices.Equal(got, []string{"ts_demo0005", "ts_demo0001", "ts_demo0003"}) {
		t.Errorf("state=SEALED: %v", got)
	}
	if got := walk("&name="+url.QueryEscape("multi-trigger activity flow"), 2, "1"); !slices.Equal(got, []string{"ts_demo0005"}) {
		t.Errorf("by name: %v", got)
	}
	if got := walk("&name=multi", 2, "0"); len(got) != 0 {
		t.Errorf("a name is matched exactly: %v", got)
	}
	if _, ids := listIDs(t, s.URL, base+"?name="+url.QueryEscape("multi-trigger activity flow")); !slices.Equal(ids, []string{"ts_demo0005"}) {
		t.Errorf("by name without a limit: %v", ids)
	}

	// Sessions started in the same second are told apart by id.
	for i := range 3 {
		resp, err := http.Post(s.URL+base, "application/json", strings.NewReader(fmt.Sprintf(`{"name":"new %d"}`, i)))
		if err != nil || resp.StatusCode != 201 {
			t.Fatalf("start: %v %v", resp, err)
		}
		resp.Body.Close()
	}
	want := append([]string{"ts_demo0008", "ts_demo0007", "ts_demo0006"}, byAge...)
	if got := walk("", 2, "8"); !slices.Equal(got, want) {
		t.Errorf("after three starts: %v, want %v", got, want)
	}

	r, _ = listIDs(t, s.URL, base+"?limit=2")
	next := r.Header.Get(headerNext)
	for _, q := range []string{"limit=0", "limit=-3", "limit=1001", "limit=x", "limit=", "limit=2&before=%25%25", "limit=2&before=bm9wZQ", "before=" + next} {
		if r, b := get(t, s.URL, base+"?"+q); r.StatusCode != 400 || !strings.Contains(string(b), `"error"`) {
			t.Errorf("?%s: %d %s, want 400", q, r.StatusCode, b)
		}
	}
}

// Through the proxy the page reaches the control plane's list with its query
// intact and gets the paging headers back.
func TestProxyPagesTheControlPlaneList(t *testing.T) {
	st := controlplane.NewMemStore()
	at := time.Date(2026, 7, 8, 9, 0, 0, 0, time.UTC)
	for i := range 7 {
		name := "plain"
		if i%2 == 1 {
			name = "odd & even" // needs escaping in a query
		}
		_ = st.Put(controlplane.Session{ID: fmt.Sprintf("ts_proxy%02d", i), Name: name, Mode: controlplane.ModeHeader,
			State: controlplane.StateSealed, CreatedAt: at.Add(time.Duration(i) * time.Second), TTLSeconds: 60})
	}
	m := controlplane.NewManager(controlplane.Config{Store: st})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	h, err := New(Config{ControlPlaneURL: cp.URL, CollectorURL: cp.URL})
	if err != nil {
		t.Fatal(err)
	}
	d := httptest.NewServer(h)
	t.Cleanup(d.Close)

	r, ids := listIDs(t, d.URL, "/api/cp/v1/sessions?limit=3")
	next := r.Header.Get(controlplane.HeaderNext)
	if !slices.Equal(ids, []string{"ts_proxy06", "ts_proxy05", "ts_proxy04"}) || next == "" || r.Header.Get(controlplane.HeaderTotal) != "7" {
		t.Fatalf("first page: %v, next %q, total %q", ids, next, r.Header.Get(controlplane.HeaderTotal))
	}
	r, ids = listIDs(t, d.URL, "/api/cp/v1/sessions?limit=3&before="+url.QueryEscape(next))
	if !slices.Equal(ids, []string{"ts_proxy03", "ts_proxy02", "ts_proxy01"}) || r.Header.Get(controlplane.HeaderNext) == "" {
		t.Fatalf("second page: %v, next %q", ids, r.Header.Get(controlplane.HeaderNext))
	}
	r, ids = listIDs(t, d.URL, "/api/cp/v1/sessions?limit=2&state=SEALED&name="+url.QueryEscape("odd & even"))
	if !slices.Equal(ids, []string{"ts_proxy05", "ts_proxy03"}) || r.Header.Get(controlplane.HeaderTotal) != "3" {
		t.Fatalf("by name: %v, total %q", ids, r.Header.Get(controlplane.HeaderTotal))
	}
	if r, b := get(t, d.URL, "/api/cp/v1/sessions?limit=0"); r.StatusCode != 400 {
		t.Fatalf("limit=0 through the proxy: %d %s", r.StatusCode, b)
	}
}

// The header that says a list is incomplete reaches the page.
func TestProxyPassesTheUnreadableHeader(t *testing.T) {
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(controlplane.HeaderUnreadable, "3")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(cp.Close)
	h, err := New(Config{ControlPlaneURL: cp.URL, CollectorURL: cp.URL})
	if err != nil {
		t.Fatal(err)
	}
	d := httptest.NewServer(h)
	t.Cleanup(d.Close)
	if r, _ := get(t, d.URL, "/api/cp/v1/sessions?limit=200"); r.Header.Get(controlplane.HeaderUnreadable) != "3" {
		t.Fatalf("headers through the proxy: %v", r.Header)
	}
}
