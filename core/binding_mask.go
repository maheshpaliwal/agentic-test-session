package core

import (
	"net/http"
	"net/url"
	"strings"
)

// ActiveBindingKeys returns the attribute keys that active bindings extract
// (nil when the SessionSet has no bindings). Values of these attributes are
// the tester's identifier and must never be recorded.
func ActiveBindingKeys() []BindingKey {
	if kb, ok := activeSessions().(interface{ BindingKeys() []BindingKey }); ok {
		return kb.BindingKeys()
	}
	return nil
}

// maskKeys is ActiveBindingKeys plus the BindPriority keys: priority keys are
// identifiers and are masked whenever they appear, even with no active binding.
func maskKeys() []BindingKey {
	keys := ActiveBindingKeys()
	prio := BindPriority()
	if len(prio) == 0 {
		return keys
	}
	out := make([]BindingKey, 0, len(keys)+len(prio))
	out = append(out, keys...)
	return append(out, prio...)
}

// MaskBoundHeaders masks, in place, header values whose name matches an active
// header binding. Cookie bindings are covered by Cookie/Set-Cookie being
// redacted wholesale; jwt_claim by Authorization being redacted.
func MaskBoundHeaders(h map[string][]string) {
	keys := maskKeys()
	for _, k := range keys {
		if k.Source != SourceHeader {
			continue
		}
		for name := range h {
			if strings.EqualFold(name, k.Name) {
				h[name] = []string{RedactedValue}
			}
		}
	}
}

// MaskBoundQuery masks values of query params named by an active query binding.
func MaskBoundQuery(raw string) string {
	if raw == "" || !strings.Contains(raw, "=") {
		return raw
	}
	var names map[string]bool
	for _, k := range maskKeys() {
		if k.Source == SourceQuery {
			if names == nil {
				names = map[string]bool{}
			}
			names[k.Name] = true
		}
	}
	if names == nil {
		return raw
	}
	parts := strings.Split(raw, "&")
	for i, p := range parts {
		k, _, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		if dk, err := url.QueryUnescape(k); err == nil {
			k = dk
		}
		if names[k] {
			parts[i] = p[:strings.Index(p, "=")+1] + url.QueryEscape(RedactedValue)
		}
	}
	return strings.Join(parts, "&")
}

// RedactURLBound is RedactURL plus masking of bound query params.
func RedactURLBound(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	c.RawQuery = MaskBoundQuery(c.RawQuery)
	return RedactURL(&c)
}

// RedactHeadersBound is RedactHeaders plus masking of bound header values.
func RedactHeadersBound(h http.Header) map[string][]string {
	out := RedactHeaders(h)
	MaskBoundHeaders(out)
	return out
}
