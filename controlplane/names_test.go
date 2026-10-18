package controlplane

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

var t0 = time.Date(2026, 10, 11, 14, 32, 7, 0, time.UTC)

func TestAutoNameShapes(t *testing.T) {
	h := Binding{BindHeader, "x-user-id", "act_bind_9"}
	j := Binding{BindJWTClaim, "sub", "u_123"}
	c := Binding{BindCookie, "sid", "abc"}
	q := Binding{BindQuery, "uid", "7"}
	cases := []struct {
		bs   []Binding
		want string
	}{
		{nil, "dashboard 14:32:07"},
		{[]Binding{h}, "x-user-id=act_bind_9"},
		{[]Binding{j}, "jwt:sub=u_123"},
		{[]Binding{c}, "cookie:sid=abc"},
		{[]Binding{q}, "query:uid=7"},
		{[]Binding{h, j}, "x-user-id=act_bind_9 + jwt:sub=u_123"},
		{[]Binding{h, j, c}, "x-user-id=act_bind_9 + jwt:sub=u_123 +1"},
		{[]Binding{h, j, c, q}, "x-user-id=act_bind_9 + jwt:sub=u_123 +2"},
	}
	for _, tc := range cases {
		got, auto := resolveName("", tc.bs, "dashboard", t0)
		if got != tc.want || !auto {
			t.Errorf("%v: got %q auto=%v want %q", tc.bs, got, auto, tc.want)
		}
	}
	// whitespace-only is treated as empty
	if got, auto := resolveName("  \t\n ", nil, "api", t0); !auto || got != "api 14:32:07" {
		t.Fatalf("%q %v", got, auto)
	}
}

func TestStartAutoNameAndJWTAliasFlow(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), nil)
	s, err := m.Start(StartRequest{InitiatedBy: "dashboard", Bindings: []Binding{
		{BindHeader, "X-USER-ID", "act_bind_9"}, {BindJWTClaim, "sub", "u_1"}, {BindCookie, "sid", "z"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !s.NameAuto || s.Name != "x-user-id=act_bind_9 + jwt:sub=u_1 +1" {
		t.Fatalf("%+v", s)
	}
	s2, _ := m.Start(StartRequest{Name: "mine"})
	if s2.NameAuto || s2.Name != "mine" {
		t.Fatalf("%+v", s2)
	}
	s3, _ := m.Start(StartRequest{InitiatedBy: "cli"})
	if !s3.NameAuto || !strings.HasPrefix(s3.Name, "cli ") || len(s3.Name) != len("cli 00:00:00") {
		t.Fatalf("%+v", s3)
	}
}

func TestTemplates(t *testing.T) {
	bs := []Binding{{BindHeader, "x-user-id", "act_bind_9"}, {BindJWTClaim, "sub", "u_1"}, {BindCookie, "sid", "ck"}}
	cases := map[string]string{
		"activity {bind}":              "activity act_bind_9",
		"{bind.X-USER-ID}":             "act_bind_9",
		"{bind.sub}":                   "u_1",
		"{bind.jwt:sub}":               "u_1",
		"{bind.sid}":                   "ck",
		"{bind.SID}":                   "?", // case-insensitive for headers only
		"{bind.nope}":                  "?",
		"{initiated_by}@{date} {time}": "dashboard@2026-10-11 14:32:07",
		"{unknown} {bind.}":            "{unknown} ?",
		"{{bind}}":                     "{act_bind_9}",
		"{bind":                        "{bind",
		"bind}":                        "bind}",
		"{ bind }":                     "{ bind }",
		"{}":                           "{}",
		"héllo {bind} 日本語 😀":           "héllo act_bind_9 日本語 😀",
		"{bind}{bind}":                 "act_bind_9act_bind_9",
	}
	for in, want := range cases {
		if got := ExpandNameTemplate(in, bs, "dashboard", t0); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
	if got := ExpandNameTemplate("x {bind}", nil, "api", t0); got != "x ?" {
		t.Fatalf("%q", got)
	}
	// expansion output is not re-expanded
	if got := ExpandNameTemplate("{bind}", []Binding{{BindHeader, "h", "{time}"}}, "api", t0); got != "{time}" {
		t.Fatalf("re-expanded: %q", got)
	}
	// through Start
	m := newMgr(t, newFakeCollector(t), nil)
	s, err := m.Start(StartRequest{Name: "activity {bind}", Bindings: []Binding{{BindHeader, "x-user-id", "act_bind_9"}}})
	if err != nil || s.Name != "activity act_bind_9" || s.NameAuto {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestTemplateHugeInput(t *testing.T) {
	huge := strings.Repeat("{bind}{", 100000) + strings.Repeat("}", 100000)
	start := time.Now()
	n := SanitizeName(ExpandNameTemplate(huge, []Binding{{BindHeader, "h", "v"}}, "api", t0))
	if utf8.RuneCountInString(n) > MaxNameRunes || time.Since(start) > 2*time.Second {
		t.Fatalf("len=%d took %v", utf8.RuneCountInString(n), time.Since(start))
	}
}

func TestSanitizeName(t *testing.T) {
	long := strings.Repeat("é", 500)
	got := SanitizeName(long)
	if utf8.RuneCountInString(got) != MaxNameRunes || !strings.HasSuffix(got, "…") || !utf8.ValidString(got) {
		t.Fatalf("%d %q", utf8.RuneCountInString(got), got)
	}
	cases := map[string]string{
		"  a   b\t\nc  ":         "a b c",
		"a\x00b\x07c":            "abc",
		"a‮b":                    "ab",
		"\x1b[31mred":            "[31mred",
		"bad\xffutf":             "badutf",
		"":                       "",
		" \n ":                   "",
		strings.Repeat("a", 120): strings.Repeat("a", 120),
	}
	for in, want := range cases {
		if got := SanitizeName(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
	// exactly 121 runes truncates; trailing space before ellipsis is trimmed
	s := strings.Repeat("a", 118) + " bc"
	if got := SanitizeName(s); utf8.RuneCountInString(got) > MaxNameRunes {
		t.Fatalf("too long %d", utf8.RuneCountInString(got))
	}
}

func TestStartSanitisesName(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), nil)
	s, _ := m.Start(StartRequest{Name: "  hello \x00 \n world  ", InitiatedBy: "a\nb"})
	if s.Name != "hello world" {
		t.Fatalf("%+v", s)
	}
}

func TestRenameHTTP(t *testing.T) {
	m := newMgr(t, newFakeCollector(t), nil)
	h := m.Handler()
	j := map[string]string{"Content-Type": "application/json"}
	w := do(t, h, "POST", "/v1/sessions", "127.0.0.1:7070", `{"bindings":[{"source":"header","name":"x-u","value":"v1"}]}`, j)
	var s Session
	json.Unmarshal(w.Body.Bytes(), &s)
	if !s.NameAuto || s.Name != "x-u=v1" {
		t.Fatalf("%s", w.Body)
	}
	w = do(t, h, "PATCH", "/v1/sessions/"+s.ID, "127.0.0.1:7070", `{"name":"  new   name for {bind} "}`, j)
	var r Session
	json.Unmarshal(w.Body.Bytes(), &r)
	if w.Code != 200 || r.Name != "new name for v1" || r.NameAuto || r.ID != s.ID {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if g, _ := m.Get(s.ID); g.Name != "new name for v1" || g.NameAuto {
		t.Fatalf("not persisted %+v", g)
	}
	if !strings.Contains(w.Body.String(), `"name_auto":false`) {
		t.Fatalf("name_auto must always be present: %s", w.Body)
	}
	for body, code := range map[string]int{`{"name":"  \n "}`: 400, `{"name":""}`: 400, `{}`: 400, `nope`: 400, `{"name":5}`: 400} {
		if w := do(t, h, "PATCH", "/v1/sessions/"+s.ID, "127.0.0.1:7070", body, j); w.Code != code {
			t.Errorf("%s: %d", body, w.Code)
		}
	}
	if w := do(t, h, "PATCH", "/v1/sessions/ts_nope", "127.0.0.1:7070", `{"name":"x"}`, j); w.Code != http.StatusNotFound {
		t.Fatalf("404: %d", w.Code)
	}
	// guard: no JSON content type, bad host, browser origin
	if w := do(t, h, "PATCH", "/v1/sessions/"+s.ID, "127.0.0.1:7070", `{"name":"x"}`, nil); w.Code < 400 {
		t.Fatalf("no content-type accepted: %d", w.Code)
	}
	if w := do(t, h, "PATCH", "/v1/sessions/"+s.ID, "evil.example", `{"name":"x"}`, j); w.Code < 400 {
		t.Fatalf("bad host accepted: %d", w.Code)
	}
	if w := do(t, h, "PATCH", "/v1/sessions/"+s.ID, "127.0.0.1:7070", `{"name":"x"}`, map[string]string{"Content-Type": "application/json", "Origin": "http://evil.example"}); w.Code < 400 {
		t.Fatalf("origin accepted: %d", w.Code)
	}
	if g, _ := m.Get(s.ID); g.Name != "new name for v1" {
		t.Fatalf("guarded request mutated: %q", g.Name)
	}
}

func TestRenameSealedAndAbortedAndPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	fs, _ := NewFileStore(path)
	f := newFakeCollector(t)
	m := newMgr(t, f, func(c *Config) { c.Store = fs; c.Retention = time.Hour })
	a, _ := m.Start(StartRequest{})
	m.Stop(a.ID)
	waitState(t, m, a.ID, StateSealed)
	r, err := m.Rename(a.ID, "labelled")
	if err != nil || r.State != StateSealed || r.Name != "labelled" || r.NameAuto {
		t.Fatalf("%+v %v", r, err)
	}
	b, _ := m.Start(StartRequest{})
	m.Abort(b.ID)
	if r, err := m.Rename(b.ID, "aborted-one"); err != nil || r.State != StateAborted {
		t.Fatalf("%+v %v", r, err)
	}
	// evict to archive, then rename from the archive
	m.Sweep(time.Now().Add(2 * time.Hour))
	if _, ok := fs.MemStore.Get(a.ID); ok {
		t.Fatal("expected eviction to archive")
	}
	if r, err := m.Rename(a.ID, "from archive"); err != nil || r.Name != "from archive" {
		t.Fatalf("%+v %v", r, err)
	}
	// re-evict: archive now holds two lines; reads must return the latest
	m.Sweep(time.Now().Add(4 * time.Hour))
	for _, s := range m.List("") {
		if s.ID == a.ID && s.Name != "from archive" {
			t.Fatalf("stale list entry %q", s.Name)
		}
	}
	// restart: new store + manager read renamed sealed session
	m.Close()
	fs2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	m2 := newMgr(t, f, func(c *Config) { c.Store = fs2 })
	if g, ok := m2.Get(a.ID); !ok || g.Name != "from archive" || g.NameAuto {
		t.Fatalf("after restart: %+v %v", g, ok)
	}
	if r, err := m2.Rename(a.ID, "after restart"); err != nil || r.State != StateSealed {
		t.Fatalf("%+v %v", r, err)
	}
	fs3, _ := NewFileStore(path)
	if g, _ := fs3.Get(a.ID); g.Name != "after restart" {
		t.Fatalf("not persisted: %q", g.Name)
	}
	if _, err := m2.Rename("ts_nope", "x"); err != ErrNotFound {
		t.Fatalf("%v", err)
	}
	if _, err := m2.Rename(a.ID, " \x00 "); err != ErrInvalid {
		t.Fatalf("%v", err)
	}
}

func TestRenameWhileDraining(t *testing.T) {
	f := newFakeCollector(t)
	f.set(1, time.Time{}, 0) // keep draining
	m := newMgr(t, f, nil)
	s, _ := m.Start(StartRequest{QuiesceSeconds: 1, MaxDrainSeconds: 1000})
	m.Stop(s.ID)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				if _, err := m.Rename(s.ID, "n"+string(rune('a'+i))); err != nil {
					t.Error(err)
					return
				}
				m.Get(s.ID)
				m.List("")
			}
		}()
	}
	wg.Wait()
	f.set(0, time.Time{}, 3) // let it seal while we keep renaming
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				m.Rename(s.ID, "during")
			}
		}
	}()
	got := waitState(t, m, s.ID, StateSealed)
	close(done)
	if got.DroppedEvents != 3 || got.EndedAt.IsZero() || got.Name == "" || got.NameAuto {
		t.Fatalf("drain result lost or name wrong: %+v", got)
	}
	if got.State != StateSealed {
		t.Fatal(got.State)
	}
}
