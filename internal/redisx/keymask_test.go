package redisx

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

const jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2lnbmF0dXJl"

func TestShowKey(t *testing.T) {
	flat := &Redactor{KeyMask: KeyMaskFlat, MaskKeys: []string{"cart:*:items", "magic:*"}}
	for key, want := range map[string]string{
		"session:tok_SECRET123":   "session:[REDACTED]",
		"auth:token:" + jwt:       "auth:token:[REDACTED]",
		"sess:9f86d081884c7d65":   "sess:[REDACTED]",
		"session:42:cart":         "session:[REDACTED]:cart",
		"cache:tok_SECRET123":     "cache:[REDACTED]", // names a secret itself
		"cache:" + jwt:            "cache:[REDACTED]",
		"otp:15551234567":         "otp:[REDACTED]",
		"cart:alice:items":        "cart:[REDACTED]:items", // WithMaskKeys
		"magic:abc:def":           "magic:[REDACTED]:[REDACTED]",
		"cart:alice":              "cart:alice",
		"user:1":                  "user:1",
		"user:1:password":         "user:1:password", // the name of the secret, not the secret
		"auth:token:index":        "auth:token:index",
		"secret:set":              "secret:set",
		"token_bucket:api":        "token_bucket:api",
		"orders:2024-05-01":       "orders:2024-05-01",
		"plain":                   "plain",
		"":                        "",
		"session:":                "session:",
		"user:550e8400-e29b-41d4": "user:550e8400-e29b-41d4",
	} {
		if got := flat.ShowKey("s1", key); got != want {
			t.Errorf("%q recorded as %q, want %q", key, got, want)
		}
	}
}

// What the default rules catch, miss and mask needlessly, as the package
// comment of contrib/tredis lists it.
func TestShowKeyDefaultRules(t *testing.T) {
	flat := &Redactor{KeyMask: KeyMaskFlat}
	const x = masked
	for key, want := range map[string]string{
		// caught: a secret name in any case is kept, what follows it is masked
		"ApiKey:Ab3dE9xYz":         "ApiKey:" + x,
		"Session:Ab3dE9xYz":        "Session:" + x,
		"SESSION:Ab3dE9xYz":        "SESSION:" + x,
		"sessions:Ab3dE9xYz":       "sessions:" + x, // plurals
		"tokens:Ab3dE9xYz":         "tokens:" + x,
		"api_keys:Ab3dE9xYz":       "api_keys:" + x,
		"session/Ab3dE9xYz":        "session/" + x, // a name joined by '/', '_', '-', '.', '='
		"session/Ab3d/E9xYz":       "session/" + x,
		"session_Ab3dE9xYz":        "session_" + x,
		"sess-9f86d081884c":        "sess-" + x,
		"api_key_Ab3dE9":           "api_key_" + x,
		"token=Ab3dE9":             "token=" + x,
		"{session}:Ab3dE9xYz":      "{session}:" + x, // hash tags
		"{session:Ab3dE9xYz}:data": "{session:" + x + "}:data",
		"sid:Ab3dE9:{Zz9}":         "sid:" + x + ":{" + x + "}",
		"session:Ab3d*":            "session:" + x, // a pattern is masked like a key
		"session:*":                "session:*",
		"session:Ab3dE9:*":         "session:" + x + ":*",
		// missed: left to WithMaskKeys
		"user:42:reset:Ab3dE9": "user:42:reset:Ab3dE9", // no secret name
		"session:abcdefgh":     "session:abcdefgh",     // a lower-case word reads as a name
		"user:ann@example.com": "user:ann@example.com",
		"user:+15551234567":    "user:+15551234567",
		"refresh:Ab3dE9":       "refresh:Ab3dE9",
		"csrf:Ab3dE9":          "csrf:Ab3dE9",
		"verify:123456":        "verify:123456",
		"code:123456":          "code:123456",
		"pin:1234":             "pin:1234",
		"oauth:state:Ab3dE9":   "oauth:state:Ab3dE9",
		"bearer:Ab3dE9":        "bearer:Ab3dE9",
		"magic_link:Ab3dE9":    "magic_link:Ab3dE9",
		"cache/session/Ab3dE9": "cache/session/Ab3dE9", // a name in the middle of a '/' path
		"sessionAb3dE9":        "sessionAb3dE9",
		// masked although no secret
		"auth:user:42":          "auth:user:" + x,
		"rate:session:10.0.0.1": "rate:session:" + x,
		// recorded once: what is masked already stays as it is
		"session:" + x:           "session:" + x,
		"session:#a1b2c3d4e5f6":  "session:#a1b2c3d4e5f6",
		"token=" + x:             "token=" + x,
		"session_#a1b2c3d4e5f6":  "session_#a1b2c3d4e5f6",
		"auth:token:" + x + ":x": "auth:token:" + x + ":x",
	} {
		if got := flat.ShowKey("s1", key); got != want {
			t.Errorf("%q recorded as %q, want %q", key, got, want)
		}
	}
}

// A string that is not known to be a key is masked like one when it reads
// like one.
func TestShowText(t *testing.T) {
	flat := &Redactor{KeyMask: KeyMaskFlat}
	for s, want := range map[string]string{
		"session:Ab3dE9xYz":       "session:" + masked,
		"v":                       "v",
		"the session: Ab3dE9xYz":  "the session: Ab3dE9xYz", // free text is for the value redaction
		`{"session":"Ab3dE9xYz"}`: `{"session":"Ab3dE9xYz"}`,
		`["session:Ab3dE9xYz"]`:   `["session:Ab3dE9xYz"]`,
		"{session}:Ab3dE9xYz":     "{session}:" + masked, // a hash tag, not JSON
		"":                        "",
	} {
		if got := flat.ShowText("s1", s); got != want {
			t.Errorf("%q recorded as %q, want %q", s, got, want)
		}
	}
	// Length is no reason to leave a name alone: a token can be kilobytes long.
	long := "session:" + strings.Repeat("Ab3dE9xYz", 300)
	if got := flat.ShowText("s1", long); got != "session:"+masked {
		t.Errorf("a 2.7 KB token after a secret name is recorded as %.60q", got)
	}
}

func TestMaskKeysGlob(t *testing.T) {
	flat := &Redactor{KeyMask: KeyMaskFlat, MaskKeys: []string{"reset:*", "cart:*:items", "u:[0-9]*:k"}}
	for key, want := range map[string]string{
		"reset:Ab3/dE9+xYz==": "reset:" + masked, // '*' crosses '/'
		"reset:a:b/c":         "reset:" + masked + ":" + masked,
		"cart:al/ice:items":   "cart:" + masked + ":items",
		"cart:alice:item":     "cart:alice:item",
		"u:42:k":              "u:" + masked + ":k",
		"u:x42:k":             "u:x42:k",
		"reset":               "reset",
	} {
		if got := flat.ShowKey("s1", key); got != want {
			t.Errorf("%q recorded as %q, want %q", key, got, want)
		}
	}
}

func TestMatch(t *testing.T) {
	for _, tc := range []struct {
		pattern, name string
		want          bool
	}{
		{"user:*", "user:1", true}, {"user:*", "user:", true}, {"user:*", "users:1", false},
		{"reset:*", "reset:a/b/c", true}, {"*", "", true}, {"*", "a/b", true}, {"", "", true}, {"", "a", false},
		{"a?c", "a/c", true}, {"a?c", "ac", false}, {"*:items", "cart:1:items", true}, {"*:items", "cart:items:1", false},
		{"a*b*c", "a/b/b/c", true}, {"a*b*c", "a/b/b", false}, {"a**", "a", true},
		{"u:[0-9]", "u:7", true}, {"u:[0-9]", "u:x", false}, {"u:[^0-9]", "u:x", true}, {"u:[^0-9]", "u:7", false},
		{"u:[a-cx]*", "u:xyz", true}, {`u:\*`, "u:*", true}, {`u:\*`, "u:x", false},
		{`u:[\]]`, "u:]", true}, {"ts:ctx:*", "ts:ctx:abc", true},
		// malformed: nothing matches
		{"u:[", "u:[", false}, {"u:[a-", "u:a", false}, {`u:\`, `u:\`, false}, {"u:[a-]", "u:a", false},
	} {
		if got := Match(tc.pattern, tc.name); got != tc.want {
			t.Errorf("Match(%q, %q) = %v", tc.pattern, tc.name, got)
		}
	}
	for pattern, bad := range map[string]bool{
		"user:*": false, "u:[0-9]*": false, `u:\[`: false, "u:[^a]": false,
		"u:[": true, "u:[a-": true, `u:\`: true, "u:[a-]": true, "u:[]": true,
	} {
		if err := CheckGlob(pattern); (err != nil) != bad {
			t.Errorf("CheckGlob(%q) = %v", pattern, err)
		}
		func() {
			defer func() {
				if (recover() != nil) != bad {
					t.Errorf("MustGlobs(%q): panic is %v", pattern, !bad)
				}
			}()
			MustGlobs("WithMaskKeys", []string{"ok:*", pattern})
		}()
	}
}

func TestShowKeyHash(t *testing.T) {
	h := &Redactor{}
	a := h.ShowKey("s1", "session:tok_SECRET123")
	if !strings.HasPrefix(a, "session:#") || len(a) != len("session:#")+keyHashLen || strings.Contains(a, "SECRET") {
		t.Fatalf("recorded as %q", a)
	}
	if again := h.ShowKey("s1", "session:tok_SECRET123"); again != a {
		t.Errorf("one key reads %q and %q within a session", a, again)
	}
	if other := h.ShowKey("s1", "session:tok_SECRET124"); other == a {
		t.Errorf("two keys read alike: %q", a)
	}
	if other := h.ShowKey("s2", "session:tok_SECRET123"); other == a {
		t.Errorf("the key reads %q in two sessions", a)
	}
	if got := KeyPattern(a); got != "session:*" {
		t.Errorf("entity %q", got)
	}
	if got := KeyPattern("session:" + masked); got != "session:*" {
		t.Errorf("entity %q", got)
	}
	off := &Redactor{KeyMask: KeyMaskOff, MaskKeys: []string{"session:*"}}
	if got := off.ShowKey("s1", "session:tok_SECRET123"); got != "session:tok_SECRET123" {
		t.Errorf("masking is off: %q", got)
	}
	if got := off.ShowText("s1", "session:tok_SECRET123"); got != "session:tok_SECRET123" {
		t.Errorf("masking is off: %q", got)
	}
}

// The default hash cannot be recomputed from an export: its key is not the
// session id, nor anything else an event carries. KeyMaskSession can be.
func TestKeyMaskHashIsNotKeyedByTheSession(t *testing.T) {
	sessionKeyed := func(session, seg string) string {
		mac := hmac.New(sha256.New, []byte(session))
		mac.Write([]byte(seg))
		return "#" + hex.EncodeToString(mac.Sum(nil))[:keyHashLen]
	}
	const seg = "123456" // a six-digit code: a million candidates
	if got := (&Redactor{KeyMask: KeyMaskSession}).ShowKey("s1", "otp:"+seg); got != "otp:"+sessionKeyed("s1", seg) {
		t.Errorf("KeyMaskSession records %q", got)
	}
	def := (&Redactor{}).ShowKey("s1", "otp:"+seg)
	if def == "otp:"+sessionKeyed("s1", seg) || def == "otp:"+sessionKeyed("", seg) {
		t.Errorf("the default hash %q is the session-keyed one", def)
	}
	// It is this, and nothing an event carries can stand in for the key.
	key := processKey()
	if len(key) != 32 || bytes.Equal(key, make([]byte, 32)) {
		t.Fatalf("process key %x", key)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("s1\x00" + seg))
	if want := "otp:#" + hex.EncodeToString(mac.Sum(nil))[:keyHashLen]; def != want {
		t.Errorf("the default hash is %q, want %q", def, want)
	}
}

// The examples of the "Entities and keys" section of contrib/tredis.
func TestKeyPattern(t *testing.T) {
	for key, want := range map[string]string{
		"user:1":                      "user:*",
		"order:42:items":              "order:*:items",
		"user:550e8400-e29b-41d4":     "user:*",
		"user:alice":                  "user:alice",
		"config":                      "config",
		"session:#a1b2c3d4e5f6":       "session:*",
		"orders:2024-05-01":           "orders:*",
		"user:abc1234":                "user:abc1234", // hex, but shorter than 8
		"session_#a1b2c3d4e5f6":       "session_*",
		"session/[REDACTED]":          "session/*",
		"{session}:{#a1b2c3d4e5f6}":   "{session}:{*}",
		"sid:[REDACTED]:{[REDACTED]}": "sid:*:{*}",
	} {
		if got := KeyPattern(key); got != want {
			t.Errorf("entity of %q is %q, want %q", key, got, want)
		}
	}
}
