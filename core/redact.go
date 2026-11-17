package core

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

// RedactedValue replaces the value of any sensitive key.
const RedactedValue = "[REDACTED]"

// DefaultRedactKeys is the built-in, case-insensitive list of sensitive keys.
var DefaultRedactKeys = []string{
	"authorization", "proxy-authorization", "cookie", "set-cookie",
	"password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
	"access_key", "private_key", "card_number", "cardnumber", "cvv", "cvc",
	"ssn", "x-api-key", "credentials",
	"passphrase", "auth", "jwt", "signature", "session_id", "sessionid", "otp",
}

// queryOnlyKeys are sensitive only as URL query or form parameter names
// (OAuth codes, signed-URL signatures, API keys passed as ?key=). As JSON
// fields or column names the same words are everyday data ("status_code",
// a Redis "key"), so they are not part of DefaultRedactKeys.
var queryOnlyKeys = map[string]struct{}{
	"key": {}, "code": {}, "sig": {}, "pin": {}, "session": {},
}

// maxKeyCache bounds the per-matcher memo of key verdicts. Field and column
// names repeat endlessly, but keys can also be data (a map keyed by user id),
// so the memo stops growing instead of following them.
const maxKeyCache = 4096

type keyMatcher struct {
	exact  map[string]struct{} // whole key, normalised by normKey
	suffix []string            // normalised keys of 5+ chars, matched as a suffix
	words  [][]string          // each key as words, matched as a contiguous run

	cache  sync.Map // raw key -> bool
	cached atomic.Int64
}

var matcher atomic.Pointer[keyMatcher]

func init() {
	SetRedactKeys(DefaultRedactKeys)
	Redactor = RedactJSON
}

var keyNormalizer = strings.NewReplacer("_", "", "-", "", " ", "")

func normKey(k string) string { return keyNormalizer.Replace(strings.ToLower(k)) }

// keyWords splits a field name into lower-case words at punctuation
// ("user[password]", "card.cvv", "x-api-key") and camelCase boundaries
// ("currentPasswordValue", "APIKey"). Trailing digits are dropped from each
// word so "password2" still reads as "password".
func keyWords(k string) []string {
	var words []string
	rs := []rune(k)
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		w := strings.TrimRightFunc(strings.ToLower(string(rs[start:end])), unicode.IsDigit)
		if w != "" {
			words = append(words, w)
		}
		start = -1
	}
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush(i)
			continue
		}
		if start >= 0 && unicode.IsUpper(r) {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				flush(i)
			}
		}
		if start < 0 {
			start = i
		}
	}
	flush(len(rs))
	return words
}

// SetRedactKeys replaces the sensitive-key list. A field name is sensitive
// when, compared case-insensitively,
//   - it equals a listed key ignoring '-', '_' and spaces ("Card-Number"
//     matches "card_number"), or ends with a listed key of five or more
//     characters ("useraccesstoken" matches "token"), or
//   - the words of a listed key appear in it as whole words, in order:
//     "user[password]", "password_confirmation", "currentPasswordValue" and
//     "card.cvv" match "password" and "cvv"; "classname", "tokenizer" and
//     "max_tokens" match nothing.
//
// Matching whole words is deliberately generous: a harmless "token_count"
// is masked too. Remove or replace keys here if that hides data you need.
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
		if w := keyWords(k); len(w) > 0 {
			m.words = append(m.words, w)
		}
	}
	matcher.Store(m)
}

func sensitive(key string) bool {
	m := matcher.Load()
	if v, ok := m.cache.Load(key); ok {
		return v.(bool)
	}
	v := m.match(key)
	if m.cached.Load() < maxKeyCache {
		if _, loaded := m.cache.LoadOrStore(key, v); !loaded {
			m.cached.Add(1)
		}
	}
	return v
}

func (m *keyMatcher) match(key string) bool {
	if strings.EqualFold(key, KeySession) {
		return false // tsession's own header is an identifier, not a credential
	}
	n := normKey(key)
	if _, ok := m.exact[n]; ok {
		return true
	}
	for _, s := range m.suffix {
		if strings.HasSuffix(n, s) {
			return true
		}
	}
	kw := keyWords(key)
	for _, want := range m.words {
		if containsRun(kw, want) {
			return true
		}
	}
	return false
}

// containsRun reports whether want occurs in words as a contiguous run.
func containsRun(words, want []string) bool {
	for i := 0; i+len(want) <= len(words); i++ {
		j := 0
		for j < len(want) && words[i+j] == want[j] {
			j++
		}
		if j == len(want) {
			return true
		}
	}
	return false
}

// sensitiveParam is sensitive for URL query and form parameter names, which
// carry a few secrets under names too generic to mask anywhere else.
func sensitiveParam(name string) bool {
	if sensitive(name) {
		return true
	}
	_, ok := queryOnlyKeys[normKey(name)]
	return ok
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
