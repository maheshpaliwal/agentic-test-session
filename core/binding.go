package core

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Binding sources.
const (
	SourceHeader   = "header"
	SourceJWTClaim = "jwt_claim"
	SourceCookie   = "cookie"
	SourceQuery    = "query"
)

// Limits (docs/BINDING.md rule 5).
const (
	MaxBindingValue = 256
	MaxBindingName  = 64
	MaxJWTBytes     = 8 << 10
)

// Attributes exposes request attributes to bindings. Transports implement it
// lazily: Get must only do work (cookie parse, JWT decode) when called.
// source is one of the Source* constants; unsupported sources return "".
type Attributes interface {
	Get(source, name string) string
}

// Binder is an optional interface on SessionSet. MatchAttrs returns the
// session whose binding matches the attributes. Implementations must call
// a.Get only for keys that some active binding actually uses.
type Binder interface {
	MatchAttrs(a Attributes) (sessionID string, ok bool)
}

// KeyBinder is an optional richer Binder that also reports which key matched
// (used to record the bound_by attribute; the value is never reported).
type KeyBinder interface {
	MatchAttrsKey(a Attributes) (sessionID string, key BindingKey, ok bool)
}

// BindingKey identifies an attribute to extract.
type BindingKey struct{ Source, Name string }

// String renders "<source>:<name>".
func (k BindingKey) String() string { return k.Source + ":" + k.Name }

// Binding is one active (session, source, name, value) tuple.
type Binding struct {
	SessionID string `json:"session_id,omitempty"`
	Source    string `json:"source"`
	Name      string `json:"name"`
	Value     string `json:"value"`
}

// NormalizeBindingName lower-cases header names; others are exact.
func NormalizeBindingName(source, name string) string {
	if source == SourceHeader {
		return strings.ToLower(name)
	}
	return name
}

// BindingSet is an immutable index of bindings: the keys to extract and a
// value->session lookup. The zero value is empty and matches nothing.
type BindingSet struct {
	keys []BindingKey // sorted, deterministic
	idx  map[BindingKey]map[string]string
}

// NewBindingSet indexes bs, dropping invalid entries (unknown source, empty
// or oversized name/value). On duplicate (key,value) the first wins.
func NewBindingSet(bs []Binding) BindingSet {
	var s BindingSet
	for _, b := range bs {
		switch b.Source {
		case SourceHeader, SourceJWTClaim, SourceCookie, SourceQuery:
		default:
			continue
		}
		if b.Name == "" || b.Value == "" || len(b.Name) > MaxBindingName || len(b.Value) > MaxBindingValue || b.SessionID == "" {
			continue
		}
		k := BindingKey{b.Source, NormalizeBindingName(b.Source, b.Name)}
		if s.idx == nil {
			s.idx = map[BindingKey]map[string]string{}
		}
		m := s.idx[k]
		if m == nil {
			m = map[string]string{}
			s.idx[k] = m
			s.keys = append(s.keys, k)
		}
		if _, dup := m[b.Value]; !dup {
			m[b.Value] = b.SessionID
		}
	}
	sort.Slice(s.keys, func(i, j int) bool { return s.keys[i].String() < s.keys[j].String() })
	return s
}

// Empty reports whether no binding exists (no extraction needed).
func (s BindingSet) Empty() bool { return len(s.keys) == 0 }

// Keys returns the attribute keys to extract. Callers must not modify it.
func (s BindingSet) Keys() []BindingKey { return s.keys }

// Match extracts only the bound keys from a and returns the matching session
// and deciding key (see MatchDetail for priority and ambiguity semantics).
func (s BindingSet) Match(a Attributes) (string, BindingKey, bool) {
	m := s.MatchDetail(a)
	return m.SessionID, m.Key, m.OK
}

// ---- JWT claim extraction (UNVERIFIED) ----

const (
	jwtCacheTTL = 60 * time.Second
	jwtCacheMax = 1024
)

type jwtEntry struct {
	claims map[string]string
	exp    time.Time
}

var jwtCache = struct {
	sync.Mutex
	m map[[32]byte]jwtEntry
}{m: map[[32]byte]jwtEntry{}}

var jwtNow = time.Now // overridable in tests

// BearerToken returns the token from an "Authorization: Bearer <t>" value.
func BearerToken(authz string) string {
	if len(authz) < 8 || !strings.EqualFold(authz[:7], "bearer ") {
		return ""
	}
	return strings.TrimSpace(authz[7:])
}

// JWTClaim returns the top-level scalar claim from the JWT payload. The
// signature is NOT verified (and alg is ignored, so alg:none works): this
// attributes traffic, it does not authenticate it. Tokens over MaxJWTBytes,
// malformed tokens and missing/oversized/non-scalar claims yield "". Results
// are cached by token hash for 60s in a size-capped cache; the token itself is
// never stored.
func JWTClaim(token, claim string) string {
	if token == "" || len(token) > MaxJWTBytes || claim == "" {
		return ""
	}
	h := sha256.Sum256([]byte(token))
	now := jwtNow()
	jwtCache.Lock()
	e, ok := jwtCache.m[h]
	jwtCache.Unlock()
	if !ok || now.After(e.exp) {
		e = jwtEntry{claims: decodeJWTClaims(token), exp: now.Add(jwtCacheTTL)}
		jwtCache.Lock()
		if len(jwtCache.m) >= jwtCacheMax {
			for k, v := range jwtCache.m {
				if now.After(v.exp) {
					delete(jwtCache.m, k)
				}
			}
			if len(jwtCache.m) >= jwtCacheMax {
				clear(jwtCache.m)
			}
		}
		jwtCache.m[h] = e
		jwtCache.Unlock()
	}
	return e.claims[claim]
}

// JWTClaimFromAuthorization is JWTClaim(BearerToken(authz), claim).
func JWTClaimFromAuthorization(authz, claim string) string {
	return JWTClaim(BearerToken(authz), claim)
}

func decodeJWTClaims(token string) map[string]string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		var x any
		if json.Unmarshal(v, &x) != nil {
			continue
		}
		var s string
		switch t := x.(type) {
		case string:
			s = t
		case float64:
			s = strconv.FormatFloat(t, 'f', -1, 64)
		case bool:
			s = strconv.FormatBool(t)
		default:
			continue
		}
		if len(s) <= MaxBindingValue {
			out[k] = s
		}
	}
	return out
}
