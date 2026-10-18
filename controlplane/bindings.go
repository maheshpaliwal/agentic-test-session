package controlplane

import (
	"fmt"
	"strings"
)

// Binding sources (docs/BINDING.md). jwt_claim values are read from an
// unverified JWT payload: attribution only, never authentication.
const (
	BindHeader   = "header"
	BindJWTClaim = "jwt_claim"
	BindCookie   = "cookie"
	BindQuery    = "query"
)

const (
	MaxBindings     = 8
	MaxBindingName  = 64
	MaxBindingValue = 256
)

// Binding attributes requests to a session when the request carries
// (source, name) == value.
type Binding struct {
	Source string `json:"source"`
	Name   string `json:"name"`
	Value  string `json:"value"`
}

// ActiveBinding is a Binding tagged with its session, as published in the
// active set.
type ActiveBinding struct {
	SessionID string `json:"session_id"`
	Binding
}

// BindingConflictError is returned when a binding is already held by another
// ACTIVE session. The HTTP layer renders it as a 409 binding_conflict body.
type BindingConflictError struct {
	Session string
	Binding Binding
}

func (e *BindingConflictError) Error() string {
	return fmt.Sprintf("binding_conflict: %s:%s already bound by session %s", e.Binding.Source, e.Binding.Name, e.Session)
}

// NormalizeBindings validates bs and returns a normalized copy (header names
// lowercased, exact duplicates removed). nil/empty yields nil.
func NormalizeBindings(bs []Binding) ([]Binding, error) {
	if len(bs) == 0 {
		return nil, nil
	}
	if len(bs) > MaxBindings {
		return nil, fmt.Errorf("%w: at most %d bindings per session", ErrInvalid, MaxBindings)
	}
	out := make([]Binding, 0, len(bs))
	seen := map[Binding]bool{}
	for _, b := range bs {
		switch b.Source {
		case BindHeader, BindJWTClaim, BindCookie, BindQuery:
		default:
			return nil, fmt.Errorf("%w: binding source must be header|jwt_claim|cookie|query", ErrInvalid)
		}
		if b.Name == "" || b.Value == "" {
			return nil, fmt.Errorf("%w: binding name and value must be non-empty", ErrInvalid)
		}
		if len(b.Name) > MaxBindingName {
			return nil, fmt.Errorf("%w: binding name exceeds %d bytes", ErrInvalid, MaxBindingName)
		}
		if len(b.Value) > MaxBindingValue {
			return nil, fmt.Errorf("%w: binding value exceeds %d bytes", ErrInvalid, MaxBindingValue)
		}
		if b.Source == BindHeader {
			b.Name = strings.ToLower(b.Name)
			if strings.ContainsFunc(b.Name, func(r rune) bool { return r <= ' ' || r >= 0x7f || r == ':' }) {
				return nil, fmt.Errorf("%w: invalid header name", ErrInvalid)
			}
		}
		if seen[b] {
			continue
		}
		seen[b] = true
		out = append(out, b)
	}
	return out, nil
}

// bindingConflictLocked returns the first binding in bs already held by an
// ACTIVE session. Caller holds m.mu, which makes check-then-insert atomic.
func (m *Manager) bindingConflictLocked(bs []Binding) *BindingConflictError {
	if len(bs) == 0 {
		return nil
	}
	held := map[Binding]string{}
	for _, s := range m.live {
		if s.State != StateActive {
			continue
		}
		for _, b := range s.Bindings {
			if cur, ok := held[b]; !ok || s.ID < cur {
				held[b] = s.ID
			}
		}
	}
	for _, b := range bs {
		if id, ok := held[b]; ok {
			return &BindingConflictError{Session: id, Binding: b}
		}
	}
	return nil
}
