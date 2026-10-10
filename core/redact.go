package core

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
)

// RedactedValue replaces the value of any sensitive key.
const RedactedValue = "[REDACTED]"

// DefaultRedactKeys is the built-in, case-insensitive list of sensitive keys.
var DefaultRedactKeys = []string{
	"authorization", "proxy-authorization", "cookie", "set-cookie",
	"password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
	"access_key", "private_key", "card_number", "cardnumber", "cvv", "cvc",
	"ssn", "x-api-key", "credentials",
}

type keyMatcher struct {
	exact  map[string]struct{}
	suffix []string
}

var matcher atomic.Pointer[keyMatcher]

func init() {
	SetRedactKeys(DefaultRedactKeys)
	Redactor = RedactJSON
}

func normKey(k string) string {
	k = strings.ToLower(k)
	return strings.NewReplacer("_", "", "-", "", " ", "").Replace(k)
}

// SetRedactKeys replaces the sensitive-key list. Keys are compared
// case-insensitively and ignoring '-', '_' and spaces. Short keys (<5 chars,
// e.g. "ssn") must match exactly; longer ones also match as a suffix
// ("access_token" matches "token").
func SetRedactKeys(keys []string) {
	m := &keyMatcher{exact: map[string]struct{}{}}
	for _, k := range keys {
		n := normKey(k)
		if n == "" {
			continue
		}
		m.exact[n] = struct{}{}
		if len(n) >= 5 {
			m.suffix = append(m.suffix, n)
		}
	}
	matcher.Store(m)
}

func sensitive(key string) bool {
	m := matcher.Load()
	n := normKey(key)
	if _, ok := m.exact[n]; ok {
		return true
	}
	for _, s := range m.suffix {
		if strings.HasSuffix(n, s) {
			return true
		}
	}
	return false
}

// RedactJSON masks the values of sensitive keys, recursively. Invalid JSON or
// JSON with nothing to mask is returned untouched.
func RedactJSON(b []byte) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return b, false
	}
	if !redactValue(v) {
		return b, false
	}
	out, err := json.Marshal(v)
	if err != nil {
		return b, false
	}
	return out, true
}

func redactValue(v any) bool {
	changed := false
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if sensitive(k) {
				t[k] = RedactedValue
				changed = true
			} else if redactValue(val) {
				changed = true
			}
		}
	case []any:
		for _, e := range t {
			if redactValue(e) {
				changed = true
			}
		}
	}
	return changed
}

// RedactHeaders returns a copy of h with sensitive header values masked.
func RedactHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		if sensitive(k) {
			out[k] = []string{RedactedValue}
			continue
		}
		out[k] = append([]string(nil), vs...)
	}
	return out
}
