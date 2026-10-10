package core

import (
	"net/http"
	"strings"
	"testing"
)

func TestRedactJSON(t *testing.T) {
	in := `{"user":"u1","Password":"hunter2","n":12345678901234567890,"nested":{"api_key":"k","list":[{"access_token":"t"},{"ok":1}]},"classname":"x","ssn":"000-00-0000"}`
	out, red := RedactJSON([]byte(in))
	if !red {
		t.Fatal("expected redacted")
	}
	s := string(out)
	for _, leak := range []string{"hunter2", `"k"`, `"t"`, "000-00-0000"} {
		if strings.Contains(s, leak) {
			t.Errorf("leaked %q in %s", leak, s)
		}
	}
	for _, keep := range []string{`"classname":"x"`, "12345678901234567890", `"user":"u1"`} {
		if !strings.Contains(s, keep) {
			t.Errorf("lost %q in %s", keep, s)
		}
	}
}

func TestRedactNoop(t *testing.T) {
	for _, in := range []string{`{"a":1}`, `not json`, `"str"`} {
		out, red := RedactJSON([]byte(in))
		if red || string(out) != in {
			t.Errorf("%q changed: %s %v", in, out, red)
		}
	}
}

func TestPayloadUsesRedactor(t *testing.T) {
	p := Payload(map[string]string{"token": "abc"})
	if !p.Redacted || strings.Contains(string(p.Inline), "abc") {
		t.Fatalf("%+v", p)
	}
}

func TestRedactHeadersAndCustomKeys(t *testing.T) {
	h := http.Header{"Authorization": {"Bearer x"}, "Set-Cookie": {"a=b"}, "X-Id": {"1"}}
	m := RedactHeaders(h)
	if m["Authorization"][0] != RedactedValue || m["Set-Cookie"][0] != RedactedValue || m["X-Id"][0] != "1" {
		t.Fatalf("%v", m)
	}
	SetRedactKeys([]string{"foo"})
	defer SetRedactKeys(DefaultRedactKeys)
	if _, red := RedactJSON([]byte(`{"foo":1}`)); !red {
		t.Fatal("custom key")
	}
	if _, red := RedactJSON([]byte(`{"password":1}`)); red {
		t.Fatal("default key should be gone")
	}
}
