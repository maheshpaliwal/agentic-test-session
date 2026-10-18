package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func hdr(name, v string) Binding { return Binding{Source: "header", Name: name, Value: v} }

func TestBindingValidation(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	long := func(n int) string { return strings.Repeat("a", n) }
	nine := make([]Binding, 9)
	for i := range nine {
		nine[i] = hdr("x-h", fmt.Sprint(i))
	}
	bad := map[string][]Binding{
		"source":    {{Source: "body", Name: "a", Value: "b"}},
		"nosource":  {{Name: "a", Value: "b"}},
		"noname":    {hdr("", "v")},
		"novalue":   {hdr("x", "")},
		"longname":  {hdr(long(65), "v")},
		"longvalue": {hdr("x", long(257))},
		"toomany":   nine,
		"badheader": {hdr("x bad", "v")},
	}
	for name, bs := range bad {
		if _, err := m.Start(StartRequest{Bindings: bs}); !isInvalid(err) {
			t.Errorf("%s: err=%v, want ErrInvalid", name, err)
		}
	}
	// Limits are inclusive.
	ok := make([]Binding, 8)
	for i := range ok {
		ok[i] = Binding{Source: "query", Name: long(64), Value: long(256 - i)}
	}
	if s, err := m.Start(StartRequest{Bindings: ok}); err != nil || len(s.Bindings) != 8 {
		t.Fatalf("8 bindings: %v %+v", err, s.Bindings)
	}
	// Normalization: header lowercased, exact dup removed, other names case kept.
	s, err := m.Start(StartRequest{Bindings: []Binding{hdr("X-User-ID", "U1"), hdr("x-user-id", "U1"),
		{Source: "cookie", Name: "SID", Value: "c"}}})
	if err != nil || len(s.Bindings) != 2 || s.Bindings[0].Name != "x-user-id" || s.Bindings[1].Name != "SID" {
		t.Fatalf("normalize: %v %+v", err, s.Bindings)
	}
}

func isInvalid(err error) bool { return err != nil && strings.Contains(err.Error(), "invalid request") }

func post(t *testing.T, url, body string) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestBindingHTTPShapesAndExclusive400(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, out := post(t, srv.URL+"/v1/sessions", `{"name":"a","mode":"exclusive","bindings":[{"source":"header","name":"x","value":"1"}]}`)
	if resp.StatusCode != 400 {
		t.Fatalf("exclusive+bindings: %d %v", resp.StatusCode, out)
	}
	resp, out = post(t, srv.URL+"/v1/sessions", `{"name":"a","bindings":[{"source":"header","name":"X-User-Id","value":"u_1"}]}`)
	if resp.StatusCode != 201 {
		t.Fatalf("create: %d %v", resp.StatusCode, out)
	}
	id := out["id"].(string)
	b := out["bindings"].([]any)[0].(map[string]any)
	if b["source"] != "header" || b["name"] != "x-user-id" || b["value"] != "u_1" {
		t.Fatalf("session bindings %v", b)
	}
	resp, out = post(t, srv.URL+"/v1/sessions", `{"name":"b","bindings":[{"source":"header","name":"x-USER-id","value":"u_1"}]}`)
	if resp.StatusCode != 409 || out["error"] != "binding_conflict" || out["conflicting_session"] != id {
		t.Fatalf("conflict: %d %v", resp.StatusCode, out)
	}
	cb := out["binding"].(map[string]any)
	if cb["source"] != "header" || cb["name"] != "x-user-id" || cb["value"] != "u_1" {
		t.Fatalf("conflict binding %v", cb)
	}
	// Different value (exact, case-sensitive) does not conflict.
	if resp, _ = post(t, srv.URL+"/v1/sessions", `{"bindings":[{"source":"header","name":"x-user-id","value":"U_1"}]}`); resp.StatusCode != 201 {
		t.Fatalf("case-different value: %d", resp.StatusCode)
	}
	var raw map[string]any
	getJSON(t, srv.URL+"/v1/active", &raw)
	ab := raw["bindings"].([]any)
	found := false
	for _, x := range ab {
		found = found || (x.(map[string]any)["session_id"] == id && x.(map[string]any)["value"] == "u_1")
	}
	if len(ab) != 2 || !found {
		t.Fatalf("active bindings %v", ab)
	}
}

func TestActiveOmitsBindingsWhenNone(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), nil)
	m.Start(StartRequest{Name: "plain"})
	b, _ := json.Marshal(m.Active())
	if bytes.Contains(b, []byte("bindings")) {
		t.Fatalf("unexpected bindings key: %s", b)
	}
}

func TestBindingConflictConcurrent(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var okIDs []string
	conflicts := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := m.Start(StartRequest{Bindings: []Binding{hdr("x-user-id", "same")}})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				okIDs = append(okIDs, s.ID)
			} else if _, isC := err.(*BindingConflictError); isC {
				conflicts++
			} else {
				t.Errorf("unexpected err %v", err)
			}
		}()
	}
	wg.Wait()
	if len(okIDs) != 1 || conflicts != 49 {
		t.Fatalf("winners=%d conflicts=%d", len(okIDs), conflicts)
	}
	if a := m.Active(); len(a.Bindings) != 1 || a.Bindings[0].SessionID != okIDs[0] {
		t.Fatalf("active %+v", a)
	}
}

func TestBindingRemovedOnStopAbortTTL(t *testing.T) {
	f := newFakeCollector(t)
	f.set(1, time.Now().Add(time.Hour), 0) // keep drains open
	m := newMgr(t, f, func(c *Config) { c.SweepInterval = time.Hour })
	a, _ := m.Start(StartRequest{Bindings: []Binding{hdr("x-a", "1")}})
	b, _ := m.Start(StartRequest{Bindings: []Binding{hdr("x-b", "1")}})
	c, _ := m.Start(StartRequest{Bindings: []Binding{hdr("x-c", "1")}, TTLSeconds: 1})
	if n := len(m.Active().Bindings); n != 3 {
		t.Fatalf("want 3 got %d", n)
	}
	// Stop: gone from bindings while still in Active (DRAINING).
	m.Stop(a.ID)
	as := m.Active()
	if len(as.Bindings) != 2 || len(as.Active) != 3 {
		t.Fatalf("after stop %+v", as)
	}
	// Binding of a draining session is free again.
	if _, err := m.Start(StartRequest{Bindings: []Binding{hdr("x-a", "1")}}); err != nil {
		t.Fatalf("rebind after stop: %v", err)
	}
	m.Abort(b.ID)
	if got := m.Active(); len(got.Bindings) != 2 {
		t.Fatalf("after abort %+v", got.Bindings)
	}
	m.Sweep(time.Now().Add(time.Hour))
	for _, ab := range m.Active().Bindings {
		if ab.SessionID == c.ID {
			t.Fatalf("ttl session still bound: %+v", ab)
		}
	}
}

func TestSSEBindingChange(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), func(c *Config) { c.HeartbeatPeriod = time.Hour })
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/active/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	msgs := make(chan ActiveSet, 16)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if l, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				var a ActiveSet
				json.Unmarshal([]byte(l), &a)
				msgs <- a
			}
		}
	}()
	wait := func(want int) ActiveSet {
		t.Helper()
		for {
			select {
			case a := <-msgs:
				if len(a.Bindings) == want {
					return a
				}
			case <-ctx.Done():
				t.Fatalf("timeout waiting for %d bindings", want)
			}
		}
	}
	wait(0)
	s, _ := m.Start(StartRequest{Bindings: []Binding{hdr("x-user-id", "u1"), {Source: "jwt_claim", Name: "sub", Value: "u1"}}})
	if a := wait(2); a.Bindings[0].SessionID != s.ID {
		t.Fatalf("%+v", a)
	}
	m.Stop(s.ID)
	wait(0)
}

func TestFileStoreBindingsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs, _ := NewFileStore(path)
	m := newMgr(t, newFakeCollector(t), func(c *Config) { c.Store = fs; c.Retention = time.Millisecond })
	live, _ := m.Start(StartRequest{TTLSeconds: 600000, Bindings: []Binding{hdr("x-live", "1")}})
	old, _ := m.Start(StartRequest{Bindings: []Binding{{Source: "cookie", Name: "sid", Value: "z"}}})
	m.Abort(old.ID)
	m.Sweep(time.Now().Add(time.Hour)) // evict aborted to archive
	fs2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := fs2.Get(live.ID); !ok || len(got.Bindings) != 1 || got.Bindings[0] != hdr("x-live", "1") {
		t.Fatalf("live reload %+v", got)
	}
	got, ok := fs2.Get(old.ID) // from the archive
	if !ok || len(got.Bindings) != 1 || got.Bindings[0].Source != "cookie" {
		t.Fatalf("archive %+v %v", got, ok)
	}
	// Restart rebuilds the active set with the persisted binding.
	m2 := newMgr(t, newFakeCollector(t), func(c *Config) { c.Store = fs2 })
	if a := m2.Active(); len(a.Bindings) != 1 || a.Bindings[0].SessionID != live.ID {
		t.Fatalf("restart active %+v", a)
	}
}
