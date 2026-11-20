// Package thttp wraps net/http servers and clients so that requests join the
// active test session: inbound requests become api.in spans, outbound ones
// api.out spans, with trace context propagated through headers.
package thttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/maheshpaliwal/agentic-test-session/core"
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

// limitBuf keeps the first max bytes written to it and notes overflow. It is
// written by whoever reads the body (for a client request that is the
// transport's write goroutine) and read when the span finishes, which can be
// another goroutine while the body is still being sent, hence the lock.
type limitBuf struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	max       int
	truncated bool
	total     int
}

func newLimitBuf() *limitBuf { return &limitBuf{max: core.GetConfig().MaxInlineBytes} }

func (l *limitBuf) add(p []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
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
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total
}

// cut reports whether bytes past the cap were dropped.
func (l *limitBuf) cut() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncated
}

// body renders the captured bytes. Complete valid JSON stays JSON (redacted
// structurally) whatever the declared type. Otherwise the content type decides:
// form bodies are redacted by key, JSON and text types are scrubbed as free
// text BEFORE the payload layer caps them, and anything with no redactor
// (multipart, XML, other application types) is replaced by a
// "<media/type, N bytes>" placeholder so it is never recorded verbatim.
func (l *limitBuf) body(contentType string) any {
	if l == nil {
		return nil
	}
	// Work on a private copy: the buffer may still be written to.
	l.mu.Lock()
	b, truncated, total := bytes.Clone(l.buf.Bytes()), l.truncated, l.total
	l.mu.Unlock()
	if len(b) == 0 {
		return nil
	}
	if !utf8.Valid(b) {
		return "<binary>"
	}
	if !truncated && json.Valid(b) {
		out, _ := core.Redactor(b)
		return json.RawMessage(out)
	}
	s := string(b)
	mt := effectiveType(contentType, b)
	switch {
	case mt == "application/x-www-form-urlencoded":
		return core.RedactQuery(s)
	case !redactable(mt):
		return fmt.Sprintf("<%s, %d bytes>", mt, total)
	case looksLikeForm(s):
		return core.RedactQuery(s)
	}
	return core.RedactText(s)
}

// maxMediaType bounds the (client-controlled) media type echoed in placeholders.
const maxMediaType = 64

// mediaType returns the lower-cased type/subtype of a Content-Type value,
// without parameters; "" when the header is absent.
func mediaType(contentType string) string {
	mt, _, _ := strings.Cut(contentType, ";")
	mt = strings.ToLower(strings.TrimSpace(mt))
	if len(mt) > maxMediaType {
		mt = mt[:maxMediaType]
	}
	return mt
}

// effectiveType is the media type used to pick a redactor: the declared one,
// or a sniffed one when the header is absent. Markup that is declared or
// sniffed as plain text is treated as XML, which has no redactor.
func effectiveType(contentType string, b []byte) string {
	mt := mediaType(contentType)
	if mt == "" {
		mt = mediaType(http.DetectContentType(b))
	}
	if mt == "text/plain" && bytes.HasPrefix(bytes.TrimSpace(b), []byte("<")) {
		return "text/xml"
	}
	return mt
}

// redactable reports whether free-text scrubbing (core.RedactText) is a
// meaningful redactor for the media type: JSON (truncated or invalid) and
// plain-text types. XML and binary/structured application types are not.
func redactable(mt string) bool {
	if strings.Contains(mt, "xml") {
		return false
	}
	return strings.HasPrefix(mt, "text/") || strings.Contains(mt, "json")
}

// looksLikeForm detects a form-encoded body sent under a text type.
func looksLikeForm(s string) bool {
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
