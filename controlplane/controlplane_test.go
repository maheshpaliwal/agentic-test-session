package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCollector is an httptest collector with scriptable drain-status.
type fakeCollector struct {
	srv      *httptest.Server
	mu       sync.Mutex
	pending  int
	lastNs   int64
	dropped  int64
	sealed   atomic.Int32
	sealResp SealResult
}

func newFakeCollector(t *testing.T) *fakeCollector {
	f := &fakeCollector{sealResp: SealResult{BundleURI: "file:///bundle/x"}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(DrainStatus{Pending: f.pending, LastEventUnixNs: f.lastNs, DroppedEvents: f.dropped})
	})
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", func(w http.ResponseWriter, r *http.Request) {
		f.sealed.Add(1)
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(f.sealResp)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCollector) set(pending int, last time.Time, dropped int64) {
	f.mu.Lock()
	f.pending, f.dropped = pending, dropped
	f.lastNs = 0
	if !last.IsZero() {
		f.lastNs = last.UnixNano()
	}
	f.mu.Unlock()
}

func newMgr(t *testing.T, f *fakeCollector, mut func(*Config)) *Manager {
	cfg := Config{
		Collector:    &HTTPCollector{BaseURL: f.srv.URL},
		PollInterval: 5 * time.Millisecond,
		TimeUnit:     10 * time.Millisecond,
	}
	if mut != nil {
		mut(&cfg)
	}
	m := NewManager(cfg)
	t.Cleanup(m.Close)
	return m
}

func waitState(t *testing.T, m *Manager, id string, want State) Session {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := m.Get(id); s.State == want {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, _ := m.Get(id)
	t.Fatalf("session %s state=%s, want %s", id, s.State, want)
	return s
}

func TestFSMHappyPath(t *testing.T) {
	f := newFakeCollector(t)
	f.set(0, time.Time{}, 7)
	m := newMgr(t, f, nil)
	s, err := m.Start(StartRequest{Name: "a", QuiesceSeconds: 2, MaxDrainSeconds: 100})
	if err != nil || s.State != StateActive || s.Mode != ModeHeader || s.InitiatedBy != "api" {
		t.Fatalf("start: %+v %v", s, err)
	}
	if a := m.Active(); len(a.Active) != 1 || a.Active[0] != s.ID || a.Exclusive != "" {
		t.Fatalf("active %+v", a)
	}
	st, err := m.Stop(s.ID)
	if err != nil || st.State != StateDraining {
		t.Fatalf("stop: %+v %v", st, err)
	}
	sealed := waitState(t, m, s.ID, StateSealed)
	if sealed.Incomplete || sealed.DroppedEvents != 7 || sealed.BundleURI != "file:///bundle/x" || sealed.EndedAt.IsZero() {
		t.Fatalf("sealed: %+v", sealed)
	}
	if got := m.Active(); len(got.Active) != 0 {
		t.Fatalf("active after seal: %+v", got)
	}
	// invalid transitions
	if _, err := m.Abort(s.ID); err != ErrBadState {
		t.Fatalf("abort sealed: %v", err)
	}
	if again, err := m.Stop(s.ID); err != nil || again.State != StateSealed {
		t.Fatalf("idempotent stop: %+v %v", again, err)
	}
	if _, err := m.Stop("nope"); err != ErrNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestDrainWaitsForPendingAndQuiet(t *testing.T) {
	f := newFakeCollector(t)
	f.set(2, time.Now(), 0)
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{QuiesceSeconds: 5, MaxDrainSeconds: 1000}) // quiet = 50ms
	m.Stop(s.ID)
	time.Sleep(100 * time.Millisecond)
	if cur, _ := m.Get(s.ID); cur.State != StateDraining || f.sealed.Load() != 0 {
		t.Fatalf("sealed while pending: %+v", cur)
	}
	f.set(0, time.Now(), 0) // pending cleared but just saw an event
	time.Sleep(15 * time.Millisecond)
	if f.sealed.Load() != 0 {
		t.Fatal("sealed before quiescence")
	}
	got := waitState(t, m, s.ID, StateSealed)
	if got.Incomplete {
		t.Fatalf("should be complete: %+v", got)
	}
}

func TestDrainTimeoutMarksIncomplete(t *testing.T) {
	f := newFakeCollector(t)
	f.set(3, time.Now(), 2)
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1, MaxDrainSeconds: 5}) // 50ms
	m.Stop(s.ID)
	got := waitState(t, m, s.ID, StateSealed)
	if !got.Incomplete || got.DroppedEvents != 2 || f.sealed.Load() != 1 {
		t.Fatalf("got %+v sealed=%d", got, f.sealed.Load())
	}
}

func TestSealIncompleteFromCollector(t *testing.T) {
	f := newFakeCollector(t)
	f.sealResp = SealResult{Incomplete: true, Orphaned: []string{"x"}}
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1, MaxDrainSeconds: 100})
	m.Stop(s.ID)
	if got := waitState(t, m, s.ID, StateSealed); !got.Incomplete {
		t.Fatalf("%+v", got)
	}
}

func TestAbortDuringDrain(t *testing.T) {
	f := newFakeCollector(t)
	f.set(1, time.Now(), 0)
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{MaxDrainSeconds: 1000})
	m.Stop(s.ID)
	if a, err := m.Abort(s.ID); err != nil || a.State != StateAborted {
		t.Fatalf("%+v %v", a, err)
	}
	f.set(0, time.Time{}, 0)
	time.Sleep(60 * time.Millisecond)
	if cur, _ := m.Get(s.ID); cur.State != StateAborted || f.sealed.Load() != 0 {
		t.Fatalf("aborted session changed: %+v", cur)
	}
}

func TestExclusiveConflict(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	h := m.Handler()
	post := func(path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Host = "127.0.0.1:7070"
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		h.ServeHTTP(w, r)
		return w
	}
	w := post("/v1/sessions", `{"mode":"exclusive","name":"one"}`)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var s1 Session
	json.Unmarshal(w.Body.Bytes(), &s1)
	if w := post("/v1/sessions", `{"mode":"exclusive"}`); w.Code != 409 {
		t.Fatalf("want 409 got %d", w.Code)
	}
	if w := post("/v1/sessions", `{"mode":"header"}`); w.Code != 201 {
		t.Fatalf("header session should coexist: %d", w.Code)
	}
	if a := m.Active(); a.Exclusive != s1.ID || len(a.Active) != 2 {
		t.Fatalf("%+v", a)
	}
	// still conflicts while DRAINING
	f.set(1, time.Now(), 0)
	post("/v1/sessions/"+s1.ID+"/stop", "")
	if w := post("/v1/sessions", `{"mode":"exclusive"}`); w.Code != 409 {
		t.Fatalf("draining should still conflict: %d", w.Code)
	}
	post("/v1/sessions/"+s1.ID+"/abort", "")
	if w := post("/v1/sessions", `{"mode":"exclusive"}`); w.Code != 201 {
		t.Fatalf("after abort: %d", w.Code)
	}
	if w := post("/v1/sessions", `{"mode":"bogus"}`); w.Code != 400 {
		t.Fatalf("bad mode: %d", w.Code)
	}
}

func TestTTLExpiry(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, func(c *Config) { c.SweepInterval = 5 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)
	s, _ := m.Start(StartRequest{TTLSeconds: 3}) // 30ms
	got := waitState(t, m, s.ID, StateExpired)
	if f.sealed.Load() != 1 || got.Reason != "ttl" || got.SealError != "" || got.Incomplete {
		t.Fatalf("ttl must seal cleanly: %+v sealed=%d", got, f.sealed.Load())
	}
	if got.EndedAt.IsZero() || len(m.Active().Active) != 0 {
		t.Fatalf("%+v", got)
	}
	if _, err := m.Stop(s.ID); err != ErrBadState {
		t.Fatalf("stop expired: %v", err)
	}
}

func TestRESTListGetKill(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	s, _ := m.Start(StartRequest{Name: "x"})
	m.Start(StartRequest{Name: "y"})
	m.Abort(s.ID)

	var list []Session
	getJSON(t, srv.URL+"/v1/sessions", &list)
	if len(list) != 2 {
		t.Fatalf("%d", len(list))
	}
	getJSON(t, srv.URL+"/v1/sessions?state=ABORTED", &list)
	if len(list) != 1 || list[0].ID != s.ID {
		t.Fatalf("%+v", list)
	}
	resp, _ := http.Get(srv.URL + "/v1/sessions/missing")
	if resp.StatusCode != 404 {
		t.Fatalf("%d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err := http.Post(srv.URL+"/v1/kill", "application/json", bytes.NewBufferString(`{"kill":true}`))
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(err, resp)
	}
	resp.Body.Close()
	var a ActiveSet
	getJSON(t, srv.URL+"/v1/active", &a)
	if !a.Kill || len(a.Active) != 1 {
		t.Fatalf("%+v", a)
	}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func TestSSEStream(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, func(c *Config) { c.HeartbeatPeriod = 20 * time.Millisecond })
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	pre, _ := m.Start(StartRequest{Name: "pre"})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/active/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("ct %q", ct)
	}
	lines := make(chan string, 64)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	next := func(prefix string) string {
		t.Helper()
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					t.Fatal("stream closed")
				}
				if strings.HasPrefix(l, prefix) {
					return l
				}
			case <-ctx.Done():
				t.Fatalf("timeout waiting for %q", prefix)
			}
		}
	}
	var a ActiveSet
	json.Unmarshal([]byte(strings.TrimPrefix(next("data: "), "data: ")), &a)
	if len(a.Active) != 1 || a.Active[0] != pre.ID {
		t.Fatalf("snapshot %+v", a)
	}
	next(": heartbeat")
	s2, _ := m.Start(StartRequest{Name: "two", Mode: ModeExclusive})
	for {
		a = ActiveSet{}
		json.Unmarshal([]byte(strings.TrimPrefix(next("data: "), "data: ")), &a)
		if a.Exclusive == s2.ID && len(a.Active) == 2 {
			break
		}
	}
	m.SetKill(true)
	for {
		a = ActiveSet{}
		json.Unmarshal([]byte(strings.TrimPrefix(next("data: "), "data: ")), &a)
		if a.Kill {
			break
		}
	}
}

func TestFileStorePersistenceAndResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeCollector(t)
	f.set(1, time.Now(), 0)
	m := newMgr(t, f, func(c *Config) { c.Store = fs })
	s, _ := m.Start(StartRequest{Name: "persist", MaxDrainSeconds: 1000, QuiesceSeconds: 1})
	m.Stop(s.ID)
	m.Close()

	// "restart": reload and expect the in-flight drain to resume and seal.
	fs2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := fs2.Get(s.ID); !ok || got.State != StateDraining || got.Name != "persist" {
		t.Fatalf("reload: %+v %v", got, ok)
	}
	f.set(0, time.Time{}, 0)
	m2 := newMgr(t, f, func(c *Config) { c.Store = fs2 })
	waitState(t, m2, s.ID, StateSealed)
	fs3, _ := NewFileStore(path)
	if got, _ := fs3.Get(s.ID); got.State != StateSealed {
		t.Fatalf("persisted %+v", got)
	}
}

func TestTTLGoesThroughDraining(t *testing.T) {
	f := newFakeCollector(t)
	f.set(1, time.Now(), 0) // pending forever -> stays DRAINING until max_drain
	m := newMgr(t, f, func(c *Config) { c.SweepInterval = 5 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)
	s, _ := m.Start(StartRequest{TTLSeconds: 2, QuiesceSeconds: 1, MaxDrainSeconds: 20}) // drain up to 200ms
	d := waitState(t, m, s.ID, StateDraining)
	if d.Reason != "ttl" || len(m.Active().Active) != 1 {
		t.Fatalf("draining: %+v", d)
	}
	got := waitState(t, m, s.ID, StateExpired)
	if !got.Incomplete || f.sealed.Load() != 1 {
		t.Fatalf("drain timeout should be incomplete: %+v", got)
	}
}

func TestTTLSealFailureMarksSealedIncomplete(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/seal") {
			calls.Add(1)
			http.Error(w, "boom", 500)
			return
		}
		json.NewEncoder(w).Encode(DrainStatus{})
	}))
	defer srv.Close()
	m := NewManager(Config{
		Collector: &HTTPCollector{BaseURL: srv.URL}, PollInterval: 5 * time.Millisecond,
		SweepInterval: 5 * time.Millisecond, TimeUnit: 10 * time.Millisecond,
		SealRetryBudget: 300 * time.Millisecond,
	})
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)
	s, _ := m.Start(StartRequest{TTLSeconds: 2, QuiesceSeconds: 1})
	got := waitState(t, m, s.ID, StateSealed)
	if !got.Incomplete || got.SealError == "" || got.Reason != "ttl" || calls.Load() < 3 {
		t.Fatalf("got %+v calls=%d", got, calls.Load())
	}
}

func TestSealRetrySucceedsAfterTransientFailure(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/seal") {
			if calls.Add(1) < 3 {
				http.Error(w, "boom", 500)
				return
			}
			json.NewEncoder(w).Encode(SealResult{BundleURI: "b"})
			return
		}
		json.NewEncoder(w).Encode(DrainStatus{})
	}))
	defer srv.Close()
	m := NewManager(Config{Collector: &HTTPCollector{BaseURL: srv.URL}, PollInterval: 5 * time.Millisecond, TimeUnit: 10 * time.Millisecond})
	defer m.Close()
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1})
	m.Stop(s.ID)
	got := waitState(t, m, s.ID, StateSealed)
	if got.Incomplete || got.SealError != "" || got.BundleURI != "b" {
		t.Fatalf("%+v", got)
	}
}
