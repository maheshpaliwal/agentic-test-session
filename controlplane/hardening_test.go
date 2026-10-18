package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func do(t *testing.T, h http.Handler, method, path, host, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Host = host
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestGuardRejectsCSRFAndRebinding(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, func(c *Config) { c.AllowedHosts = []string{"controlplane"} })
	h := m.Handler()
	const lo = "127.0.0.1:7070"
	js := map[string]string{"Content-Type": "application/json"}

	// cross-origin browser POST (no-cors simple request shape)
	w := do(t, h, "POST", "/v1/kill", lo, `{"kill":true}`, map[string]string{"Origin": "http://evil.example", "Content-Type": "text/plain"})
	if w.Code != 403 || m.Active().Kill {
		t.Fatalf("cross-origin kill: %d kill=%v", w.Code, m.Active().Kill)
	}
	w = do(t, h, "POST", "/v1/sessions", lo, `{"name":"x"}`, map[string]string{"Origin": "http://evil.example", "Content-Type": "application/json"})
	if w.Code != 403 || len(m.List("")) != 0 {
		t.Fatalf("cross-origin start: %d", w.Code)
	}
	// Origin: null (sandboxed iframe / no-referrer redirects) with empty body
	s, _ := m.Start(StartRequest{Name: "victim"})
	w = do(t, h, "POST", "/v1/sessions/"+s.ID+"/stop", lo, "", map[string]string{"Origin": "null"})
	if w.Code != 403 {
		t.Fatalf("Origin null stop: %d", w.Code)
	}
	if cur, _ := m.Get(s.ID); cur.State != StateActive {
		t.Fatalf("session was stopped by forbidden request: %s", cur.State)
	}
	// DNS rebinding: attacker hostname resolving to loopback
	if w = do(t, h, "GET", "/v1/active", "evil.example:7070", "", nil); w.Code != 403 {
		t.Fatalf("wrong host: %d", w.Code)
	}
	if w = do(t, h, "POST", "/v1/kill", "evil.example:7070", `{"kill":true}`, js); w.Code != 403 {
		t.Fatalf("wrong host POST: %d", w.Code)
	}
	// text/plain body from a non-browser/no-Origin caller
	if w = do(t, h, "POST", "/v1/kill", lo, `{"kill":true}`, map[string]string{"Content-Type": "text/plain"}); w.Code != 415 {
		t.Fatalf("text/plain: %d", w.Code)
	}
	if w = do(t, h, "POST", "/v1/kill", lo, `{"kill":true}`, nil); w.Code != 415 {
		t.Fatalf("no content type: %d", w.Code)
	}
	if m.Active().Kill {
		t.Fatal("kill switch flipped by rejected request")
	}
	// normal flows
	if w = do(t, h, "POST", "/v1/kill", lo, `{"kill":true}`, js); w.Code != 200 {
		t.Fatalf("json kill: %d", w.Code)
	}
	if w = do(t, h, "GET", "/v1/active", "controlplane:7070", "", nil); w.Code != 200 {
		t.Fatalf("allowed service host: %d", w.Code)
	}
	if w = do(t, h, "POST", "/v1/sessions/"+s.ID+"/stop", lo, "", nil); w.Code != 200 { // sdk/client: empty body, no CT
		t.Fatalf("empty-body stop: %d", w.Code)
	}
	if w = do(t, h, "GET", "/healthz", "10.1.2.3:7070", "", nil); w.Code != 200 {
		t.Fatalf("healthz: %d", w.Code)
	}
}

func TestSSEStreamWorksThroughGuard(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/active/stream")
	if err != nil || resp.StatusCode != 200 {
		t.Fatal(err, resp)
	}
	resp.Body.Close()
}

func TestInputValidation(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	h := m.Handler()
	js := map[string]string{"Content-Type": "application/json"}
	for _, body := range []string{
		`{"ttl_seconds":9223372036854775807}`,
		`{"ttl_seconds":604801}`,
		`{"quiesce_seconds":-1}`,
		`{"max_drain_seconds":99999999999}`,
		`{"ttl_seconds":99999999999999999999}`,
	} {
		if w := do(t, h, "POST", "/v1/sessions", "127.0.0.1:1", body, js); w.Code != 400 {
			t.Errorf("%s: %d, want 400", body, w.Code)
		}
	}
	if w := do(t, h, "POST", "/v1/sessions", "127.0.0.1:1", `{"ttl_seconds":604800}`, js); w.Code != 201 {
		t.Errorf("max ttl: %d", w.Code)
	}
}

func TestDrainIgnoresCollectorClockSkew(t *testing.T) {
	for _, skew := range []time.Duration{5 * time.Hour, -5 * time.Hour} {
		f := newFakeCollector(t)
		f.set(0, time.Now().Add(skew), 0) // steady, never-changing remote timestamp
		m := newMgr(t, f, nil)
		s, _ := m.Start(StartRequest{QuiesceSeconds: 5, MaxDrainSeconds: 1000}) // quiet = 50ms
		start := time.Now()
		m.Stop(s.ID)
		got := waitState(t, m, s.ID, StateSealed)
		if got.Incomplete {
			t.Fatalf("skew %v: %+v", skew, got)
		}
		if el := time.Since(start); el < 45*time.Millisecond {
			t.Fatalf("skew %v: sealed after %v, before local quiesce window", skew, el)
		}
	}
}

func TestDrainResetsOnChangeDespiteSkew(t *testing.T) {
	f := newFakeCollector(t)
	f.set(0, time.Now().Add(-5*time.Hour), 0)
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{QuiesceSeconds: 10, MaxDrainSeconds: 1000}) // 100ms
	m.Stop(s.ID)
	stop := make(chan struct{})
	go func() { // keep "new events" arriving with ancient timestamps
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				f.set(0, time.Now().Add(-5*time.Hour).Add(time.Duration(i)), 0)
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	if f.sealed.Load() != 0 {
		t.Fatal("sealed while remote last-event marker kept changing")
	}
	waitState(t, m, s.ID, StateSealed)
}

func TestSealCollectorDownThenUp(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			http.Error(w, "unavailable", 503)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/seal") {
			json.NewEncoder(w).Encode(SealResult{BundleURI: "b"})
			return
		}
		json.NewEncoder(w).Encode(DrainStatus{})
	}))
	defer srv.Close()
	m := NewManager(Config{Collector: &HTTPCollector{BaseURL: srv.URL}, PollInterval: 5 * time.Millisecond,
		TimeUnit: 10 * time.Millisecond, SealRetryBudget: 2 * time.Second})
	defer m.Close()
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1, MaxDrainSeconds: 30}) // drain budget 300ms
	m.Stop(s.ID)
	time.Sleep(450 * time.Millisecond) // drain times out; seal now retrying
	down.Store(false)
	got := waitState(t, m, s.ID, StateSealed)
	if got.SealError != "" || got.BundleURI != "b" || !got.Incomplete {
		t.Fatalf("recovered seal: %+v (incomplete expected: drain timed out)", got)
	}
}

func TestSealPermanentFailureEndsSealedWithError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/seal") {
			http.Error(w, "bundle dir: permission denied", 500)
			return
		}
		json.NewEncoder(w).Encode(DrainStatus{})
	}))
	defer srv.Close()
	m := NewManager(Config{Collector: &HTTPCollector{BaseURL: srv.URL}, PollInterval: 5 * time.Millisecond,
		TimeUnit: 10 * time.Millisecond, SealRetryBudget: 200 * time.Millisecond})
	defer m.Close()
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1})
	m.Stop(s.ID)
	got := waitState(t, m, s.ID, StateSealed)
	if !got.Incomplete || !strings.Contains(got.SealError, "permission denied") || got.EndedAt.IsZero() {
		t.Fatalf("%+v", got)
	}
	if len(m.Active().Active) != 0 {
		t.Fatal("still active")
	}
}

func TestRetentionPrunesButFileStoreStillReads(t *testing.T) {
	fs, err := NewFileStore(filepath.Join(t.TempDir(), "s.json"))
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeCollector(t)
	m := newMgr(t, f, func(c *Config) { c.Store = fs; c.Retention = time.Hour })
	a, _ := m.Start(StartRequest{Name: "a"})
	b, _ := m.Start(StartRequest{Name: "b", TTLSeconds: MaxDurationSeconds})
	m.Abort(a.ID)
	m.Sweep(time.Now())
	if _, ok := fs.MemStore.Get(a.ID); !ok {
		t.Fatal("pruned too early")
	}
	m.Sweep(time.Now().Add(70 * time.Minute))
	if _, ok := fs.MemStore.Get(a.ID); ok {
		t.Fatal("terminal session not evicted from memory")
	}
	if got, ok := m.Get(a.ID); !ok || got.State != StateAborted {
		t.Fatalf("archived session unreadable: %+v %v", got, ok)
	}
	if l := m.List(""); len(l) != 2 {
		t.Fatalf("list %d", len(l))
	}
	if cur, _ := m.Get(b.ID); cur.State != StateActive {
		t.Fatal("live session touched")
	}
	// restart: archived sessions remain readable
	fs2, err := NewFileStore(fs.path)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := fs2.Get(a.ID); !ok || got.Name != "a" {
		t.Fatal("not readable after reload")
	}
}

func TestMemStoreRetentionBoundsMemory(t *testing.T) {
	f := newFakeCollector(t)
	st := NewMemStore()
	m := newMgr(t, f, func(c *Config) { c.Store = st; c.Retention = time.Minute })
	for i := 0; i < 50; i++ {
		s, _ := m.Start(StartRequest{})
		m.Abort(s.ID)
	}
	m.Sweep(time.Now().Add(time.Hour))
	if n := len(st.List()); n != 0 {
		t.Fatalf("%d sessions retained", n)
	}
	if len(m.terminal) != 0 || len(m.live) != 0 {
		t.Fatalf("indexes not pruned: %d %d", len(m.terminal), len(m.live))
	}
}

func TestSweepScansOnlyLive(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	for i := 0; i < 200; i++ {
		s, _ := m.Start(StartRequest{})
		m.Abort(s.ID)
	}
	m.Start(StartRequest{})
	if len(m.live) != 1 {
		t.Fatalf("live index %d", len(m.live))
	}
	_ = context.Background()
}
