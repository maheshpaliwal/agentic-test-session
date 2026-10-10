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

// redactHeaders returns a copy of h with sensitive values masked.
func redactHeaders(h http.Header) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, v := range h {
		if sensitive(k) {
			out[k] = []string{redacted}
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}

// limitBuf keeps the first max bytes written to it and notes overflow.
type limitBuf struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func newLimitBuf() *limitBuf { return &limitBuf{max: core.GetConfig().MaxInlineBytes} }

func (l *limitBuf) add(p []byte) {
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

// body renders the captured bytes as JSON when complete and valid, else text.
func (l *limitBuf) body() any {
	if l == nil || l.buf.Len() == 0 {
		return nil
	}
	b := l.buf.Bytes()
	if !l.truncated && json.Valid(b) {
		return json.RawMessage(append([]byte(nil), b...))
	}
	if !utf8.Valid(b) {
		return "<binary>"
	}
	return string(b)
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
