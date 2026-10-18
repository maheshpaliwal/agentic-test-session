package core

import (
	"net/url"
	"regexp"
	"strings"
)

// IsSensitiveKey reports whether a field/column/param name is configured as sensitive.
func IsSensitiveKey(k string) bool { return sensitive(k) }

// RedactNamed returns RedactedValue when key is sensitive, otherwise v unchanged.
// Use it wherever a value is paired with a name (SQL columns, diff fields, Dynamo attrs).
func RedactNamed(key string, v any) any {
	if sensitive(key) {
		return RedactedValue
	}
	return v
}

// RedactAny returns a deep copy of maps/slices with sensitive keys masked.
// Scalars are returned as-is; non-JSON-shaped values pass through unchanged.
func RedactAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if sensitive(k) {
				out[k] = RedactedValue
			} else {
				out[k] = RedactAny(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = RedactAny(val)
		}
		return out
	default:
		return v
	}
}

// RedactQuery masks the values of sensitive keys in a raw query string or
// form-encoded body ("a=1&access_token=xyz"). Unparseable input is returned
// with RedactText applied.
func RedactQuery(raw string) string {
	if raw == "" || !strings.Contains(raw, "=") {
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
		if sensitive(k) {
			parts[i] = p[:strings.Index(p, "=")+1] + url.QueryEscape(RedactedValue)
		}
	}
	return strings.Join(parts, "&")
}

// RedactURL renders u without userinfo passwords and with sensitive query
// values masked. Safe for request URIs (path + query) as well as absolute URLs.
func RedactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	if c.User != nil {
		if _, has := c.User.Password(); has {
			c.User = url.UserPassword(c.User.Username(), "xxxxx")
		}
	}
	c.RawQuery = RedactQuery(c.RawQuery)
	return c.String()
}

var (
	reJSONish = regexp.MustCompile(`(?i)("?([a-z0-9_.\-]+)"?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,&;}\]]+)`)
	reBearer  = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[a-z0-9._~+/=\-]+`)
)

// RedactText is a best-effort scrubber for free text that is not valid JSON:
// truncated JSON, form bodies, "key=value"/"key: value" fragments and bearer
// tokens. It cannot be exhaustive; callers should prefer structured redaction.
func RedactText(s string) string {
	if s == "" {
		return s
	}
	s = reBearer.ReplaceAllString(s, "$1 "+RedactedValue)
	return reJSONish.ReplaceAllStringFunc(s, func(m string) string {
		sub := reJSONish.FindStringSubmatch(m)
		if len(sub) < 4 || !sensitive(sub[2]) {
			return m
		}
		q := ""
		if v := sub[3]; len(v) > 0 && (v[0] == '"' || v[0] == '\'') {
			q = string(v[0])
		}
		return sub[1] + q + RedactedValue + q
	})
}
