package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// pagingEnv is a control plane holding prepared sessions, which records the
// list requests it gets. With old set it answers them like a control plane
// from before the paging parameters: it ignores name, limit and before, and
// sends no paging headers.
type pagingEnv struct {
	url string
	old bool

	mu    sync.Mutex
	lists []string // the raw queries of GET /v1/sessions, in order
}

func pagingSession(i int, name string, st controlplane.State) controlplane.Session {
	at := time.Date(2026, 7, 8, 9, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
	s := controlplane.Session{ID: fmt.Sprintf("ts_paging%05d", i), Name: name, Mode: controlplane.ModeHeader, State: st,
		InitiatedBy: "test", CreatedAt: at, StartedAt: at, TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60}
	if st.Terminal() {
		s.EndedAt = at.Add(time.Second)
	}
	return s
}

func newPagingEnv(t *testing.T, old bool, sessions ...controlplane.Session) *pagingEnv {
	t.Helper()
	st := controlplane.NewMemStore()
	for _, s := range sessions {
		if err := st.Put(s); err != nil {
			panic(err)
		}
	}
	m := controlplane.NewManager(controlplane.Config{Store: st})
	t.Cleanup(m.Close)
	env := &pagingEnv{old: old}
	h := m.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/sessions" {
			h.ServeHTTP(w, r)
			return
		}
		env.mu.Lock()
		env.lists = append(env.lists, r.URL.RawQuery)
		env.mu.Unlock()
		if env.old {
			q := r.URL.Query()
			q.Del("name")
			q.Del("limit")
			q.Del("before")
			r.URL.RawQuery = q.Encode()
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	env.url = srv.URL
	return env
}

func (env *pagingEnv) tc(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"--cp", env.url}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

// requests returns the list requests since the last call.
func (env *pagingEnv) requests() []string {
	env.mu.Lock()
	defer env.mu.Unlock()
	got := env.lists
	env.lists = nil
	return got
}

// A name is resolved by asking for the sessions with that name, not by
// listing every session; against a control plane that ignores the name
// filter the answer is the same.
func TestResolveByName(t *testing.T) {
	const sealed, active = controlplane.StateSealed, controlplane.StateActive
	var ss []controlplane.Session
	add := func(name string, st controlplane.State) string {
		s := pagingSession(len(ss), name, st)
		ss = append(ss, s)
		return s.ID
	}
	add("other", sealed)
	add("solo", sealed)
	wantSolo := add("solo", sealed) // the latest of two finished ones
	wantMixed := add("mixed", active)
	add("mixed", sealed) // newer, but finished: the running one wins
	add("other", active)
	add("two-running", active)
	wantTwoRunning := add("two-running", controlplane.StateCreated) // the latest running one
	// A running session older than more finished sessions of its name than
	// one request asks for.
	wantOldRunning := add("nightly", active)
	wantManyFinished := ""
	for range namePage + 50 {
		add("nightly", sealed)
		wantManyFinished = add("finished-only", sealed)
	}

	for _, old := range []bool{false, true} {
		t.Run(fmt.Sprintf("old control plane=%v", old), func(t *testing.T) {
			env := newPagingEnv(t, old, ss...)
			for _, tc := range []struct {
				name, want string
				requests   int // list requests to a control plane that knows the parameters
			}{
				{"solo", wantSolo, 1},
				{"mixed", wantMixed, 1},
				{"two-running", wantTwoRunning, 1},
				{"nightly", wantOldRunning, 4},
				{"finished-only", wantManyFinished, 4},
			} {
				code, out, e := env.tc("status", tc.name)
				if code != 0 || !strings.Contains(out, `"id": "`+tc.want+`"`) {
					t.Errorf("status %s: exit %d %s, want session %s, got:\n%s", tc.name, code, e, tc.want, out)
				}
				reqs := env.requests()
				for _, q := range reqs {
					if !strings.Contains(q, "name="+tc.name) || !strings.Contains(q, "limit=") {
						t.Errorf("status %s listed sessions with %q: not by name, or without a limit", tc.name, q)
					}
				}
				if want := map[bool]int{false: tc.requests, true: 1}[old]; len(reqs) != want {
					t.Errorf("status %s: %d list requests %q, want %d", tc.name, len(reqs), reqs, want)
				}
			}
			if code, _, e := env.tc("status", "no-such-name"); code != 1 || !strings.Contains(e, `no session named "no-such-name"`) {
				t.Errorf("unknown name: exit %d %s", code, e)
			}
			// An id is not looked up by name.
			env.requests()
			if code, out, _ := env.tc("status", wantSolo); code != 0 || !strings.Contains(out, wantSolo) || len(env.requests()) != 0 {
				t.Errorf("status by id: exit %d\n%s", code, out)
			}
		})
	}
}

func TestListLimit(t *testing.T) {
	var ss []controlplane.Session
	for i := range 1200 {
		st := controlplane.StateSealed
		if i%100 == 7 {
			st = controlplane.StateAborted
		}
		ss = append(ss, pagingSession(i, fmt.Sprintf("s%d", i), st))
	}
	ids := func(out string) []string {
		var got []string
		for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
			if f := strings.Fields(l); len(f) > 0 {
				got = append(got, f[0])
			}
		}
		return got
	}
	for _, old := range []bool{false, true} {
		t.Run(fmt.Sprintf("old control plane=%v", old), func(t *testing.T) {
			env := newPagingEnv(t, old, ss...)
			// Without --limit: everything, oldest first, no hint, one unparameterised request.
			code, out, e := env.tc("list")
			if got := ids(out); code != 0 || len(got) != 1200 || got[0] != ss[0].ID || got[1199] != ss[1199].ID || e != "" {
				panic(fmt.Sprintf("list: exit %d, %d sessions, stderr %q", code, len(got), e))
			}
			if reqs := env.requests(); len(reqs) != 1 || reqs[0] != "" {
				panic(fmt.Sprintf("list without --limit asked %q", reqs))
			}
			// --limit: the newest N, still oldest first, and a hint that there are more.
			code, out, e = env.tc("list", "--limit", "3")
			if got := ids(out); code != 0 || len(got) != 3 || got[0] != ss[1197].ID || got[2] != ss[1199].ID {
				panic(fmt.Sprintf("list --limit 3: exit %d, got %v", code, got))
			}
			if !strings.Contains(e, "newest 3 sessions") || !strings.Contains(e, "older ones") {
				panic(fmt.Sprintf("list --limit 3: no hint that the list was cut: %q", e))
			}
			if reqs := env.requests(); len(reqs) != 1 || reqs[0] != "limit=3" {
				panic(fmt.Sprintf("list --limit 3 asked %q", reqs))
			}
			// More than one request's worth.
			code, out, e = env.tc("list", "--limit", "1100")
			if got := ids(out); code != 0 || len(got) != 1100 || got[0] != ss[100].ID || got[1099] != ss[1199].ID || !strings.Contains(e, "newest 1100") {
				panic(fmt.Sprintf("list --limit 1100: exit %d, %d sessions, stderr %q", code, len(got), e))
			}
			// A limit that covers everything: no hint.
			code, out, e = env.tc("list", "--limit", "1200")
			if got := ids(out); code != 0 || len(got) != 1200 || e != "" {
				panic(fmt.Sprintf("list --limit 1200: exit %d, %d sessions, stderr %q", code, len(got), e))
			}
			code, out, e = env.tc("list", "--state", "ABORTED", "--limit", "5")
			if got := ids(out); code != 0 || len(got) != 5 || got[4] != ss[1107].ID || got[0] != ss[707].ID || !strings.Contains(e, "newest 5") {
				panic(fmt.Sprintf("list --state ABORTED --limit 5: exit %d, got %v, stderr %q", code, got, e))
			}
			code, out, e = env.tc("list", "--state", "ABORTED", "--limit", "50")
			if got := ids(out); code != 0 || len(got) != 12 || e != "" {
				panic(fmt.Sprintf("list --state ABORTED --limit 50: exit %d, %d sessions, stderr %q", code, len(got), e))
			}
			if code, _, e := env.tc("list", "--limit", "-1"); code != 1 || !strings.Contains(e, "--limit") {
				panic(fmt.Sprintf("list --limit -1: exit %d %q", code, e))
			}
		})
	}
}

func TestListMarks(t *testing.T) {
	for _, tc := range []struct {
		name string
		info client.Info
		want string
	}{
		{"complete", client.Info{}, ""},
		{"incomplete", client.Info{Incomplete: true}, "  INCOMPLETE"},
		{"unconfirmed", client.Info{DeliveryUnconfirmed: []string{"orders/a"}}, "  DELIVERY-UNCONFIRMED"},
		{"both", client.Info{Incomplete: true, DeliveryUnconfirmed: []string{"orders/a"}}, "  INCOMPLETE  DELIVERY-UNCONFIRMED"},
	} {
		if got := listMarks(tc.info); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
