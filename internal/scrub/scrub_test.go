package scrub

import (
	"strings"
	"testing"
)

func TestTextKeepsNoCredential(t *testing.T) {
	for in, want := range map[string]string{
		`Post "https://collector.internal/v1/ingest?token=s3cret&x=1": EOF`:            `Post "https://collector.internal/v1/ingest": EOF`,
		`Post "https://user:p@ss@w0rd@collector.internal/v1/ingest": EOF`:              `Post "https://collector.internal/v1/ingest": EOF`,
		`Post "https://user:pa/ss@collector.internal:8443/v1/ingest#frag": EOF`:        `Post "https://collector.internal:8443/v1/ingest": EOF`,
		`post https://u:12/x@h.example/p?k=v failed, then http://t0ken@h2.example/ ok`: `post https://h.example/p failed, then http://h2.example/ ok`,
		`dial tcp 127.0.0.1:1: connect: connection refused`:                            `dial tcp 127.0.0.1:1: connect: connection refused`,
		`the collector answered HTTP 503`:                                              `the collector answered HTTP 503`,
		`Get "http://[::1": bad`:                                                       `Get "<url>": bad`,
		`http://[::1]:7071/v1/ingest`:                                                  `http://[::1]:7071/v1/ingest`,
		``:                                                                             ``,
	} {
		if got := Text(in); got != want {
			t.Errorf("Text(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	got := Text(`x https://user:p@ss@w0rd@h/a?token=s3cret y http://user:pa/ss@h/b z http://t0ken@h/`)
	for _, secret := range []string{"s3cret", "w0rd", "p@ss", "pa/ss", "t0ken", "user"} {
		if strings.Contains(got, secret) {
			t.Errorf("%q survives in %q", secret, got)
		}
	}
}

func TestURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://ingest:s3cr3tpw@collector:7071/base?token=tok_abc#frag": "http://collector:7071/base",
		"http://127.0.0.1:7071": "http://127.0.0.1:7071",
		"::not a url":           "<url>",
		"":                      "<url>",
		"http://":               "<url>",
	} {
		if got := URL(in); got != want {
			t.Errorf("URL(%q) = %q, want %q", in, got, want)
		}
	}
}
