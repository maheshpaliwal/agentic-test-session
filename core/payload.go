package core

import (
	"encoding/json"
	"sync"
)

// Redactor masks sensitive data in an already-marshalled JSON payload and
// reports whether anything was masked. Replaced by the runtime's real redactor.
var Redactor = func(b []byte) ([]byte, bool) { return b, false }

// Payload marshals v, redacts, and caps to Config.MaxInlineBytes. Oversized
// payloads become a JSON string of the truncated text with Truncated=true.
func Payload(v any) PayloadRef {
	if v == nil {
		return PayloadRef{}
	}
	var b []byte
	switch t := v.(type) {
	case json.RawMessage:
		b = t
	case []byte:
		if json.Valid(t) {
			b = t
		} else {
			b, _ = json.Marshal(string(t))
		}
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			b, _ = json.Marshal("<unmarshalable>")
		}
	}
	size := len(b)
	b, red := Redactor(b)
	max := GetConfig().MaxInlineBytes
	if len(b) > max {
		tb, _ := json.Marshal(string(b[:max]))
		return PayloadRef{Inline: tb, Size: size, Truncated: true, Redacted: red}
	}
	return PayloadRef{Inline: json.RawMessage(b), Size: size, Redacted: red}
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
}

func (s StaticSessions) IsActive(id string) bool   { return s.Active[id] }
func (s StaticSessions) Exclusive() (string, bool) { return s.ExclusiveID, s.ExclusiveID != "" }
