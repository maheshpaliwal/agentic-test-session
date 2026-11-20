package controlplane

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// pageSessions builds n sessions for the paging tests. Three in a row share
// a creation time, and the ids do not ascend with it, so that the order has
// to come from both. Every fifth is ABORTED, a few are ACTIVE, the rest
// SEALED; every tenth is named "nightly".
func pageSessions(n int) []Session {
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	out := make([]Session, n)
	for i := range out {
		at := base.Add(time.Duration(i/3) * time.Second)
		s := Session{
			ID: fmt.Sprintf("ts_page%06d", (i*7919)%n), Name: fmt.Sprintf("flow %d", i), Mode: ModeHeader,
			State: StateSealed, InitiatedBy: "test", CreatedAt: at, StartedAt: at, EndedAt: at.Add(time.Second),
			TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60,
		}
		switch {
		case i%5 == 0:
			s.State = StateAborted
		case i%50 == 1:
			s.State, s.EndedAt = StateActive, time.Time{}
		}
		if i%10 == 3 {
			s.Name = "nightly"
		}
		out[i] = s
	}
	return out
}

// newestFirst returns the ids of the sessions q selects, in page order.
func newestFirst(all []Session, q PageQuery) []string {
	var sel []Session
	for _, s := range all {
		if q.matches(s) {
			sel = append(sel, s)
		}
	}
	sortSessions(sel)
	slices.Reverse(sel)
	ids := make([]string, len(sel))
	for i, s := range sel {
		ids[i] = s.ID
	}
	return ids
}

// plainStore hides the optional capabilities of the store it wraps.
type plainStore struct{ Store }

// pageStores returns the stores the API is tested against, each holding ss.
func pageStores(t *testing.T, ss []Session) map[string]Store {
	t.Helper()
	mem, plain := NewMemStore(), NewMemStore()
	for _, s := range ss {
		_ = mem.Put(s)
		_ = plain.Put(s)
	}
	file := func(cached bool) *FileStore {
		fs := openStore(t, filepath.Join(t.TempDir(), "s.json"))
		if !cached {
			uncached(fs)
		}
		var evict []string
		for i, s := range ss {
			mustPut(t, fs, s)
			if s.State.Terminal() && i%4 != 0 { // the others stay in the hot set
				evict = append(evict, s.ID)
			}
		}
		fs.Evict(evict...)
		if len(fs.arch) != len(evict) {
			t.Fatalf("%d sessions archived, want %d", len(fs.arch), len(evict))
		}
		return fs
	}
	return map[string]Store{"mem": mem, "plain": plainStore{plain}, "file": file(true), "file-uncached": file(false)}
}

func pageServer(t *testing.T, st Store) *httptest.Server {
	t.Helper()
	m := NewManager(Config{Store: st})
	t.Cleanup(m.Close)
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// getList requests GET /v1/sessions with the given query.
func getList(t *testing.T, base string, v url.Values) (*http.Response, []Session) {
	t.Helper()
	resp, err := http.Get(base + "/v1/sessions?" + v.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var list []Session
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(b, &list); err != nil {
			t.Fatalf("GET %v: %v in %.200s", v, err, b)
		}
	}
	return resp, list
}

// walkPages pages through the sessions v selects and returns their ids in
// the order they came, checking the headers of every page on the way.
func walkPages(t *testing.T, base string, v url.Values, limit int, wantTotal string) []string {
	t.Helper()
	v = maps.Clone(v)
	v.Set("limit", strconv.Itoa(limit))
	var ids []string
	for page := 0; ; page++ {
		resp, list := getList(t, base, v)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d of %v: status %d", page, v, resp.StatusCode)
		}
		if got := resp.Header.Get(HeaderTotal); got != wantTotal {
			t.Fatalf("page %d of %v: %s %q, want %q", page, v, HeaderTotal, got, wantTotal)
		}
		if got := resp.Header.Get(HeaderUnreadable); got != "" {
			t.Fatalf("page %d of %v: %s %q", page, v, HeaderUnreadable, got)
		}
		for _, s := range list {
			ids = append(ids, s.ID)
		}
		next := resp.Header.Get(HeaderNext)
		if next == "" {
			if len(list) > limit {
				t.Fatalf("last page of %v has %d sessions, limit %d", v, len(list), limit)
			}
			return ids
		}
		if len(list) != limit {
			t.Fatalf("page %d of %v has %d sessions and a next page, want %d", page, v, len(list), limit)
		}
		if page > 10000 {
			t.Fatalf("paging %v does not end", v)
		}
		v.Set("before", next)
	}
}

// The list API with the paging parameters: paging returns every session
// exactly once, newest first, also among sessions created at the same
// instant, with and without filters, from every kind of store.
func TestListPageAPI(t *testing.T) {
	const n = 2500
	ss := pageSessions(n)
	for name, st := range pageStores(t, ss) {
		t.Run(name, func(t *testing.T) {
			srv := pageServer(t, st)
			isFile := strings.HasPrefix(name, "file")
			for _, tc := range []struct {
				q     PageQuery
				limit int
			}{
				{PageQuery{}, 200},
				{PageQuery{}, 1},
				{PageQuery{}, 1000},
				{PageQuery{State: StateAborted}, 200},
				{PageQuery{State: StateActive}, 7},
				{PageQuery{State: "NO_SUCH_STATE"}, 50},
				{PageQuery{Name: "nightly"}, 100},
				{PageQuery{Name: "nightly", State: StateSealed}, 33},
				{PageQuery{Name: "flow 77"}, 10},
				{PageQuery{Name: "no such name"}, 10},
			} {
				if tc.limit == 1 && testing.Short() {
					continue
				}
				want := newestFirst(ss, tc.q)
				v := url.Values{}
				if tc.q.State != "" {
					v.Set("state", string(tc.q.State))
				}
				if tc.q.Name != "" {
					v.Set("name", tc.q.Name)
				}
				// The file store knows the total from its index, except for a
				// name (a hash) and a state it has no code for.
				total := strconv.Itoa(len(want))
				if isFile && (tc.q.Name != "" || tc.q.State == "NO_SUCH_STATE") {
					total = ""
				}
				if got := walkPages(t, srv.URL, v, tc.limit, total); !slices.Equal(got, want) {
					t.Errorf("%+v limit %d: %d sessions, want %d; first difference at %d", tc.q, tc.limit, len(got), len(want), firstDiff(got, want))
				}
				// The same filters without a limit: everything, oldest first,
				// no paging headers.
				resp, list := getList(t, srv.URL, v)
				slices.Reverse(want)
				got := make([]string, len(list))
				for i, s := range list {
					got[i] = s.ID
				}
				if !slices.Equal(got, want) {
					t.Errorf("%+v without limit: %d sessions, want %d; first difference at %d", tc.q, len(got), len(want), firstDiff(got, want))
				}
				for _, h := range []string{HeaderNext, HeaderTotal, HeaderUnreadable} {
					if resp.Header.Get(h) != "" {
						t.Errorf("%+v without limit: header %s: %q", tc.q, h, resp.Header.Get(h))
					}
				}
			}
		})
	}
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// A request without the new parameters is answered byte for byte as before:
// the JSON array of Store.List, or of the sessions in a state.
func TestListWithoutParametersIsUnchanged(t *testing.T) {
	ss := pageSessions(300)
	for name, st := range pageStores(t, ss) {
		t.Run(name, func(t *testing.T) {
			srv := pageServer(t, st)
			for _, state := range []State{"", StateAborted, "NO_SUCH_STATE"} {
				want := st.List()
				path := "/v1/sessions"
				if state != "" {
					want = slices.DeleteFunc(want, func(s Session) bool { return s.State != state })
					path += "?state=" + string(state)
				}
				wantBody, _ := json.Marshal(want)
				resp, err := http.Get(srv.URL + path)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("%s: status %d, content type %q", path, resp.StatusCode, resp.Header.Get("Content-Type"))
				}
				if string(body) != string(wantBody)+"\n" {
					t.Fatalf("%s: body differs from the list of the store (%d bytes, want %d)", path, len(body), len(wantBody)+1)
				}
				for h := range resp.Header {
					if strings.HasPrefix(h, "X-Tsession-") {
						t.Fatalf("%s: unexpected header %s", path, h)
					}
				}
			}
		})
	}
	// No sessions: an empty array, not null.
	srv := pageServer(t, NewMemStore())
	for _, path := range []string{"/v1/sessions", "/v1/sessions?state=ACTIVE", "/v1/sessions?limit=5", "/v1/sessions?name=x"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "[]\n" {
			t.Errorf("%s with no sessions: %q", path, body)
		}
	}
}

func TestListPageRejectsBadParameters(t *testing.T) {
	for name, st := range pageStores(t, pageSessions(20)) {
		t.Run(name, func(t *testing.T) {
			srv := pageServer(t, st)
			resp, _ := getList(t, srv.URL, url.Values{"limit": {"5"}})
			next := resp.Header.Get(HeaderNext)
			if next == "" {
				t.Fatal("no cursor on the first page")
			}
			for _, q := range []string{
				"limit=0", "limit=-1", "limit=1001", "limit=abc", "limit=", "limit=5.0", "limit=99999999999999999999",
				"limit=5&before=%21%21", "limit=5&before=bm90IGEgY3Vyc29y", "limit=5&before=" + next + "x%2B",
				"before=" + next, // a cursor needs a limit
			} {
				resp, err := http.Get(srv.URL + "/v1/sessions?" + q)
				if err != nil {
					t.Fatal(err)
				}
				var e map[string]string
				_ = json.NewDecoder(resp.Body).Decode(&e)
				resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest || e["error"] == "" {
					t.Errorf("?%s: status %d, error %q, want 400", q, resp.StatusCode, e["error"])
				}
			}
			if resp, list := getList(t, srv.URL, url.Values{"limit": {"1000"}, "before": {next}}); resp.StatusCode != 200 || len(list) != 15 {
				t.Errorf("limit=1000 after the first five: status %d, %d sessions", resp.StatusCode, len(list))
			}
		})
	}
}

// The cursor names a place in the order, not a session: it works after the
// session it was taken from is gone.
func TestListPageCursorSurvivesDelete(t *testing.T) {
	ss := pageSessions(30)
	for name, st := range pageStores(t, ss) {
		t.Run(name, func(t *testing.T) {
			m := NewManager(Config{Store: st})
			t.Cleanup(m.Close)
			want := newestFirst(ss, PageQuery{})
			p, err := m.ListPage(PageQuery{Limit: 10})
			if err != nil || len(p.Sessions) != 10 || p.Next == "" {
				t.Fatalf("first page: %d sessions, next %q, err %v", len(p.Sessions), p.Next, err)
			}
			if err := st.Delete(p.Sessions[9].ID); err != nil {
				t.Fatal(err)
			}
			p, err = m.ListPage(PageQuery{Limit: 10, Before: p.Next})
			if err != nil || len(p.Sessions) != 10 || p.Sessions[0].ID != want[10] || p.Total != 29 {
				t.Fatalf("second page: %d sessions from %s (want %s), total %d, err %v", len(p.Sessions), p.Sessions[0].ID, want[10], p.Total, err)
			}
		})
	}
}

// An index entry is what every archived session costs in memory.
func TestArchRefSize(t *testing.T) {
	if got := unsafe.Sizeof(archRef{}); got != 24 {
		t.Fatalf("archRef is %d bytes, want 24", got)
	}
}

// A page is selected from the index: with 50,000 archived sessions, a page
// of 200 reads no more than 200 lines of the archive, whatever the page and
// the state filter, and a lookup by name reads the lines of the sessions
// with that name and of the few whose name shares its hash.
func TestFileStorePageReadsOnlyItsLines(t *testing.T) {
	const n = 50000
	path := filepath.Join(t.TempDir(), "s.json")
	ids := writeBenchArchive(t, path, n)
	fs := uncached(openStore(t, path)) // every session served is read from the file
	reads := func(what string, q PageQuery, wantSessions int, maxReads int64) Page {
		t.Helper()
		before := fs.lineReads.Load()
		p, err := fs.ListPage(q)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		got := fs.lineReads.Load() - before
		if len(p.Sessions) != wantSessions || got > maxReads || p.Unreadable != 0 {
			t.Fatalf("%s: %d sessions (want %d) for %d archive lines read (want at most %d), %d unreadable",
				what, len(p.Sessions), wantSessions, got, maxReads, p.Unreadable)
		}
		t.Logf("%s: %d sessions, %d archive lines read", what, len(p.Sessions), got)
		return p
	}
	p := reads("first page", PageQuery{Limit: 200}, 200, 200)
	if p.Total != n || p.Sessions[0].ID != ids[n-1] || p.Sessions[199].ID != ids[n-200] {
		t.Fatalf("first page: total %d, from %s to %s", p.Total, p.Sessions[0].ID, p.Sessions[199].ID)
	}
	p = reads("second page", PageQuery{Limit: 200, Before: p.Next}, 200, 200)
	if p.Sessions[0].ID != ids[n-201] {
		t.Fatalf("second page starts at %s, want %s", p.Sessions[0].ID, ids[n-201])
	}
	p = reads("by state", PageQuery{Limit: 200, State: StateSealed}, 200, 200)
	if p.Total != n {
		t.Fatalf("by state: total %d", p.Total)
	}
	reads("by a state no session is in", PageQuery{Limit: 200, State: StateAborted}, 0, 0)
	// 50,000 distinct names in 65,536 hash values: about one other session
	// shares the hash of a name, a handful at worst.
	for _, i := range []int{0, 1234, n - 1} {
		p = reads("by name", PageQuery{Limit: 200, Name: fmt.Sprintf("checkout flow %d", i)}, 1, 8)
		if p.Sessions[0].ID != ids[i] || p.Total != -1 {
			t.Fatalf("by name: got %s, total %d", p.Sessions[0].ID, p.Total)
		}
	}
	reads("by a name no session has", PageQuery{Limit: 200, Name: "no such session"}, 0, 8)
}

// Sessions that share a name: a lookup with a limit reads the newest ones,
// not all of them.
func TestFileStorePageByNameStopsAtTheLimit(t *testing.T) {
	fs := uncached(openStore(t, filepath.Join(t.TempDir(), "s.json")))
	const n = 3000
	ss := make([]Session, n)
	evict := make([]string, n)
	for i := range ss {
		ss[i] = storeSession(i, "nightly", StateSealed)
		evict[i] = ss[i].ID
	}
	mustPut(t, fs, ss...)
	fs.Evict(evict...)
	before := fs.lineReads.Load()
	p, err := fs.ListPage(PageQuery{Name: "nightly", Limit: 1})
	if err != nil || len(p.Sessions) != 1 || p.Sessions[0].ID != ss[n-1].ID || p.Next == "" {
		t.Fatalf("newest by name: %+v, err %v", p, err)
	}
	if got := fs.lineReads.Load() - before; got > 64 {
		t.Fatalf("the newest of %d sessions with one name took %d archive line reads", n, got)
	}
}

// Sessions the store cannot read are missing from a list and from a page,
// and the response says how many.
func TestListSaysWhenSessionsAreUnreadable(t *testing.T) {
	var logs logBuffer
	fs, err := NewFileStore(filepath.Join(t.TempDir(), "s.json"), WithStoreLogger(logs.logger()))
	closeLater(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	uncached(fs)
	const n = 40
	var ids []string
	for i := range n {
		s := archiveSession(i, "a", StateSealed)
		mustPut(t, fs, s)
		ids = append(ids, s.ID)
	}
	mustPut(t, fs, storeSession(n, "hot", StateActive))
	fs.Evict(ids...)
	// Cut the archive after its first ten lines: thirty sessions are in the
	// index and not in the file.
	repair := damageArchive(t, fs, fs.arch[ids[10]].off)
	srv := pageServer(t, fs)

	for _, tc := range []struct {
		query          string
		sessions       int
		unreadable     string
		total, hasNext string
	}{
		{"", 11, "30", "", ""},
		{"state=SEALED", 10, "30", "", ""},
		{"state=ACTIVE", 1, "", "", ""},
		{"limit=5", 1, "4", "41", "yes"}, // the newest five: the hot one and four that are cut off
		{"limit=100", 11, "30", "41", ""},
		{"limit=5&state=ACTIVE", 1, "", "1", ""},
	} {
		resp, list := getList(t, srv.URL, must(url.ParseQuery(tc.query)))
		next := ""
		if resp.Header.Get(HeaderNext) != "" {
			next = "yes"
		}
		if resp.StatusCode != 200 || len(list) != tc.sessions || resp.Header.Get(HeaderUnreadable) != tc.unreadable ||
			resp.Header.Get(HeaderTotal) != tc.total || next != tc.hasNext {
			t.Errorf("?%s: status %d, %d sessions (want %d), unreadable %q (want %q), total %q (want %q), next %q (want %q)", tc.query, resp.StatusCode,
				len(list), tc.sessions, resp.Header.Get(HeaderUnreadable), tc.unreadable, resp.Header.Get(HeaderTotal), tc.total, next, tc.hasNext)
		}
	}
	// Paging goes on past the sessions that cannot be read.
	seen := 0
	v := url.Values{"limit": {"7"}}
	for {
		resp, list := getList(t, srv.URL, v)
		seen += len(list)
		if v.Set("before", resp.Header.Get(HeaderNext)); v.Get("before") == "" {
			break
		}
	}
	if seen != 11 {
		t.Errorf("paging over a damaged archive returned %d sessions, want 11", seen)
	}
	repair()
	if resp, list := getList(t, srv.URL, url.Values{"limit": {"100"}}); len(list) != n+1 || resp.Header.Get(HeaderUnreadable) != "" {
		t.Errorf("after the repair: %d sessions, unreadable %q", len(list), resp.Header.Get(HeaderUnreadable))
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Paging while sessions are put, evicted and deleted: no page run returns a
// session twice or out of order, and the sessions that are in the store for
// the whole run are all returned.
func TestListPageWhileSessionsChange(t *testing.T) {
	const stable, volatile = 600, 300
	ss := pageSessions(stable + volatile)
	isStable := func(i int) bool { return i%3 != 2 }
	for _, name := range []string{"mem", "file", "file-uncached"} {
		t.Run(name, func(t *testing.T) {
			var st Store = NewMemStore()
			fs, _ := st.(*FileStore)
			if name != "mem" {
				fs = openStore(t, filepath.Join(t.TempDir(), "s.json"))
				if name == "file-uncached" {
					uncached(fs)
				}
				st = fs
			}
			wantStable := map[string]bool{}
			for i, s := range ss {
				if isStable(i) {
					wantStable[s.ID] = true
					if err := st.Put(s); err != nil {
						t.Fatal(err)
					}
				}
			}
			pl := st.(PageLister)
			var (
				wg   sync.WaitGroup
				stop atomic.Bool
			)
			churn := func(f func(round int)) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for round := 0; !stop.Load(); round++ {
						f(round)
					}
				}()
			}
			// The volatile sessions come and go; the stable ones are changed
			// (which keeps their place) and, in a file store, archived and
			// brought back into the hot set by the next change.
			churn(func(round int) {
				for i, s := range ss {
					if !isStable(i) {
						_ = st.Put(s)
					}
				}
				for i, s := range ss {
					if !isStable(i) {
						_ = st.Delete(s.ID)
					}
				}
			})
			churn(func(round int) {
				for i, s := range ss {
					if isStable(i) && i%7 == round%7 {
						s.SealError = fmt.Sprintf("changed in round %d", round)
						_ = st.Put(s)
					}
				}
			})
			if fs != nil {
				churn(func(round int) {
					var ids []string
					for i, s := range ss {
						if s.State.Terminal() && (i+round)%2 == 0 {
							ids = append(ids, s.ID)
						}
					}
					fs.Evict(ids...)
				})
			}
			deadline := time.Now().Add(400 * time.Millisecond)
			for run := 0; run < 3 || time.Now().Before(deadline); run++ {
				q := PageQuery{Limit: 1 + run%97}
				seen := map[string]bool{}
				var last pageKey
				for first := true; ; {
					p, err := pl.ListPage(q)
					if err != nil {
						t.Fatal(err)
					}
					if p.Unreadable != 0 {
						t.Fatalf("run %d: %d sessions unreadable", run, p.Unreadable)
					}
					for _, s := range p.Sessions {
						if k := keyOf(s); !first && !k.older(last) {
							t.Fatalf("run %d: %s came after %s: twice or out of order", run, s.ID, last.id)
						} else {
							last, first = k, false
						}
						seen[s.ID] = true
					}
					if q.Before = p.Next; p.Next == "" {
						break
					}
				}
				for id := range wantStable {
					if !seen[id] {
						t.Fatalf("run %d (limit %d): %s was in the store all along and is on no page", run, q.Limit, id)
					}
				}
			}
			stop.Store(true)
			wg.Wait()
		})
	}
}

// BenchmarkListSessions measures GET /v1/sessions through the handler, with
// a file store holding the given number of archived sessions: a page of 200
// (what the dashboard polls), a lookup by name (what tracectl does), and the
// unfiltered list those replace.
//
//	go test -run '^$' -bench ListSessions -benchtime 20x ./controlplane/
func BenchmarkListSessions(b *testing.B) {
	for _, n := range []int{20000, 50000, 100000} {
		path := filepath.Join(b.TempDir(), "s.json")
		writeBenchArchive(b, path, n)
		fs, err := NewFileStore(path)
		closeLater(b, fs)
		if err != nil {
			b.Fatal(err)
		}
		m := NewManager(Config{Store: fs})
		b.Cleanup(m.Close)
		h := m.Handler()
		for _, bc := range []struct{ name, query string }{
			{"limit=200", "?limit=200"},
			{"limit=200,page=50", ""}, // the cursor is filled in below
			{"limit=200,state", "?limit=200&state=SEALED"},
			{"name", "?limit=100&name=" + url.QueryEscape(fmt.Sprintf("checkout flow %d", n/2))},
			{"all", ""},
		} {
			if bc.name == "limit=200,page=50" {
				p, err := fs.ListPage(PageQuery{Limit: 1000})
				for i := 0; err == nil && i < 9; i++ {
					p, err = fs.ListPage(PageQuery{Limit: 1000, Before: p.Next})
				}
				if err != nil || p.Next == "" {
					b.Fatalf("cursor: %v", err)
				}
				bc.query = "?limit=200&before=" + p.Next
			}
			b.Run(fmt.Sprintf("archived=%d/%s", n, bc.name), func(b *testing.B) {
				bytes := 0
				for b.Loop() {
					r := httptest.NewRequest("GET", "/v1/sessions"+bc.query, nil)
					r.Host = "127.0.0.1:7070"
					w := httptest.NewRecorder()
					h.ServeHTTP(w, r)
					if w.Code != 200 {
						b.Fatalf("status %d", w.Code)
					}
					bytes = w.Body.Len()
				}
				b.ReportMetric(float64(bytes), "resp-bytes")
			})
		}
	}
}
