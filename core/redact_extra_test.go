package core

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRedactExtra(t *testing.T) {
	u, _ := url.Parse("https://u:pw@h/p?a=1&access_token=abc&api_key=k")
	got := RedactURL(u)
	for _, bad := range []string{"abc", "pw", "=k"} {
		if strings.Contains(got, bad) {
			t.Fatalf("leak %q in %s", bad, got)
		}
	}
	if !strings.Contains(got, "a=1") {
		t.Fatal(got)
	}
	if s := RedactQuery("user=bob&password=x%20y&n=1"); strings.Contains(s, "x%20y") || !strings.Contains(s, "user=bob") {
		t.Fatal(s)
	}
	if s := RedactText(`{"user":"bob","password":"hunter2","tok`); strings.Contains(s, "hunter2") {
		t.Fatal(s)
	}
	if s := RedactText("Authorization: Bearer abc.def.ghi"); strings.Contains(s, "abc.def") {
		t.Fatal(s)
	}
	m := RedactAny(map[string]any{"a": map[string]any{"password": "x", "n": 1}}).(map[string]any)
	if m["a"].(map[string]any)["password"] != RedactedValue {
		t.Fatal(m)
	}
	if RedactNamed("password", "x") != RedactedValue || RedactNamed("id", 7) != 7 {
		t.Fatal("named")
	}
}

func TestLoopbackGuard(t *testing.T) {
	h := LoopbackGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), GuardConfig{})
	do := func(method, host, origin, ct, body string) int {
		r := httptest.NewRequest(method, "http://"+host+"/x", strings.NewReader(body))
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if c := do("GET", "127.0.0.1:7070", "", "", ""); c != 204 {
		t.Fatal(c)
	}
	if c := do("GET", "evil.com", "", "", ""); c != 403 {
		t.Fatal(c)
	}
	if c := do("POST", "127.0.0.1:7070", "http://evil.com", "application/json", "{}"); c != 403 {
		t.Fatal(c)
	}
	if c := do("POST", "127.0.0.1:7070", "", "text/plain", "{}"); c != 415 {
		t.Fatal(c)
	}
	if c := do("POST", "127.0.0.1:7070", "", "application/json; charset=utf-8", "{}"); c != 204 {
		t.Fatal(c)
	}
}
