// Package scrub removes what may be a credential from URLs, alone or inside a
// text such as an error message, before the text is stored, logged or sent
// on.
package scrub

import (
	"net/url"
	"regexp"
	"strings"
)

// inText matches a URL inside a text, up to the next blank, quote or bracket.
var inText = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]*://[^\s"'<>]+`)

// URL returns raw with its scheme, host and path only. A user name, a
// password, a query and a fragment may each be a credential. The userinfo is
// taken to end at the last "@" of the URL, not of what a parser takes for the
// authority: a password may hold "/" and "@" unescaped, and a parser would
// then leave part of it in the path. A raw that cannot be parsed comes back
// as "<url>".
func URL(raw string) string {
	scheme, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return "<url>"
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	u, err := url.Parse(scheme + "://" + rest)
	if err != nil || u.Host == "" {
		return "<url>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

// Text returns s with every URL in it replaced by URL of it.
func Text(s string) string { return inText.ReplaceAllStringFunc(s, URL) }
