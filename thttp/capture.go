// Package thttp wraps net/http servers and clients so that requests join the
// active test session: inbound requests become api.in spans, outbound ones
// api.out spans, with trace context propagated through headers.
package thttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"unicode/utf8"

	"tsession/core"
)

// message is the payload recorded for a request or response.
type message struct {
	Method  string              `json:"method,omitempty"`
	URL     string              `json:"url,omitempty"`
	Status  int                 `json:"status,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    any                 `json:"body,omitempty"`
	// BodySize is the full body length seen (the captured Body may be capped).
	BodySize int `json:"body_size,omitempty"`
}

const redacted = "[REDACTED]"

var sensitiveExact = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
	"x-auth-token":        true,
	"x-csrf-token":        true,
	"x-xsrf-token":        true,
}

var sensitiveParts = []string{"token", "secret", "password", "api-key", "apikey"}

func sensitive(name string) bool {
	n := strings.ToLower(name)
	if sensitiveExact[n] {
		return true
	}
	for _, p := range sensitiveParts {
		if strings.Contains(n, p) {
			return true
		}
	}
	return false
}

// redactHeaders returns a copy of h with sensitive values masked. Values of
// non-sensitive headers are scrubbed too (Referer/Location query tokens, inline
// "token=..." fragments).
func redactHeaders(h http.Header) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, v := range h {
		if sensitive(k) || core.IsSensitiveKey(k) {
			out[k] = []string{redacted}
			continue
		}
		vs := make([]string, len(v))
		for i, x := range v {
			vs[i] = scrubHeaderValue(x)
		}
		out[k] = vs
	}
	core.MaskBoundHeaders(out)
	return out
}

func scrubHeaderValue(v string) string {
	if i := strings.IndexByte(v, '?'); i >= 0 && (strings.HasPrefix(v, "http") || strings.HasPrefix(v, "/")) {
		q, frag, _ := strings.Cut(v[i+1:], "#")
		v = v[:i+1] + core.MaskBoundQuery(core.RedactQuery(q))
		if frag != "" {
			v += "#" + core.RedactText(frag)
		}
	}
	return core.RedactText(v)
}

// limitBuf keeps the first max bytes written to it and notes overflow.
type limitBuf struct {
	buf       bytes.Buffer
	max       int
	truncated bool
	total     int
}

func newLimitBuf() *limitBuf { return &limitBuf{max: core.GetConfig().MaxInlineBytes} }

func (l *limitBuf) add(p []byte) {
	l.total += len(p)
	room := l.max - l.buf.Len()
	if room <= 0 {
		if len(p) > 0 {
			l.truncated = true
		}
		return
	}
	if len(p) > room {
		l.buf.Write(p[:room])
		l.truncated = true
		return
	}
	l.buf.Write(p)
}

// size is the total number of bytes seen, including those past the cap.
func (l *limitBuf) size() int {
	if l == nil {
		return 0
	}
	return l.total
}

// body renders the captured bytes: complete valid JSON stays JSON (redacted
// structurally); anything else (truncated JSON, form bodies, text) becomes a
// string redacted BEFORE it is capped by the payload layer.
func (l *limitBuf) body(contentType string) any {
	if l == nil || l.buf.Len() == 0 {
		return nil
	}
	b := l.buf.Bytes()
	if !utf8.Valid(b) {
		return "<binary>"
	}
	if !l.truncated && json.Valid(b) {
		out, _ := core.Redactor(append([]byte(nil), b...))
		return json.RawMessage(out)
	}
	s := string(b)
	if isForm(contentType, s) {
		return core.RedactQuery(s)
	}
	return core.RedactText(s)
}

func isForm(contentType, s string) bool {
	if strings.HasPrefix(strings.ToLower(contentType), "application/x-www-form-urlencoded") {
		return true
	}
	t := strings.TrimSpace(s)
	return t != "" && t[0] != '{' && t[0] != '[' && t[0] != '<' && !strings.ContainsAny(t, " \n\t") &&
		strings.Contains(t, "=") && strings.Contains(t, "&")
}

// teeBody records what downstream reads from an io.ReadCloser, capped.
type teeBody struct {
	rc  interface{ Read([]byte) (int, error) }
	cl  func() error
	cap *limitBuf
}

func (t *teeBody) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		t.cap.add(p[:n])
	}
	return n, err
}

func (t *teeBody) Close() error { return t.cl() }
