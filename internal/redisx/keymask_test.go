package redisx

import (
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
}

// The examples of the "Entities and keys" section of contrib/tredis.
func TestKeyPattern(t *testing.T) {
	for key, want := range map[string]string{
		"user:1":                  "user:*",
		"order:42:items":          "order:*:items",
		"user:550e8400-e29b-41d4": "user:*",
		"user:alice":              "user:alice",
		"config":                  "config",
		"session:#a1b2c3d4":       "session:*",
		"orders:2024-05-01":       "orders:*",
		"user:abc1234":            "user:abc1234", // hex, but shorter than 8
	} {
		if got := KeyPattern(key); got != want {
			t.Errorf("entity of %q is %q, want %q", key, got, want)
		}
	}
}
