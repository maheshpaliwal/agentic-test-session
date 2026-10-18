package core

import (
	"encoding/json"
	"sync"
	"unicode/utf8"
)

// Redactor masks sensitive data in an already-marshalled JSON payload and
// reports whether anything was masked. Replaced by the runtime's real redactor.
var Redactor = func(b []byte) ([]byte, bool) { return b, false }

// Payload marshals v, redacts, and caps to Config.MaxInlineBytes. Redaction
// always runs on the FULL value before truncation, so a secret cannot survive
// by straddling the cut. Valid JSON is redacted structurally (Redactor); strings
// and non-JSON bytes are scrubbed with RedactText. Oversized payloads become a
// JSON string of the truncated redacted text with Truncated=true; Size is the
// original size.
func Payload(v any) PayloadRef {
	if v == nil {
		return PayloadRef{}
	}
	var b []byte
	text, isText := "", false
	switch t := v.(type) {
	case json.RawMessage:
		if json.Valid(t) {
			b = t
		} else {
			text, isText = string(t), true
		}
	case []byte:
		if json.Valid(t) {
			b = t
		} else {
			text, isText = string(t), true
		}
	case string:
		text, isText = t, true
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			text, isText = "<unmarshalable>", true
		}
	}
	max := GetConfig().MaxInlineBytes
	if isText {
		size := len(text)
		r := RedactText(text)
		red := r != text
		trunc := false
		if len(r) > max {
			r, trunc = cutUTF8(r, max), true
		}
		tb, _ := json.Marshal(r)
		return PayloadRef{Inline: tb, Size: size, Truncated: trunc, Redacted: red}
	}
	size := len(b)
	b, red := Redactor(b)
	if len(b) > max {
		tb, _ := json.Marshal(cutUTF8(string(b), max))
		return PayloadRef{Inline: tb, Size: size, Truncated: true, Redacted: red}
	}
	return PayloadRef{Inline: json.RawMessage(b), Size: size, Redacted: red}
}

// cutUTF8 returns at most n bytes of s without splitting a rune.
func cutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// MemRecorder collects events in memory; for tests.
type MemRecorder struct {
	mu sync.Mutex
	ev []Event
}

func (m *MemRecorder) Record(e Event) { m.mu.Lock(); m.ev = append(m.ev, e); m.mu.Unlock() }
func (m *MemRecorder) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Event(nil), m.ev...)
}

// StaticSessions is a SessionSet for tests.
type StaticSessions struct {
	Active      map[string]bool
	ExclusiveID string
	// Bindings are active attribute bindings (tests).
	Bindings []Binding
}

func (s StaticSessions) IsActive(id string) bool   { return s.Active[id] }
func (s StaticSessions) Exclusive() (string, bool) { return s.ExclusiveID, s.ExclusiveID != "" }

// BindingKeys lists the attribute keys the bindings need extracted.
func (s StaticSessions) BindingKeys() []BindingKey { return NewBindingSet(s.Bindings).Keys() }

// MatchAttrs implements Binder.
func (s StaticSessions) MatchAttrs(a Attributes) (string, bool) {
	id, _, ok := s.MatchAttrsKey(a)
	return id, ok
}

// MatchAttrsDetail implements DetailBinder.
func (s StaticSessions) MatchAttrsDetail(a Attributes) BindingMatch {
	if len(s.Bindings) == 0 {
		return BindingMatch{}
	}
	return NewBindingSet(s.Bindings).MatchDetail(a)
}

// MatchAttrsKey implements KeyBinder.
func (s StaticSessions) MatchAttrsKey(a Attributes) (string, BindingKey, bool) {
	if len(s.Bindings) == 0 {
		return "", BindingKey{}, false
	}
	return NewBindingSet(s.Bindings).Match(a)
}
