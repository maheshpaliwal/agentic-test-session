package core

import (
	"log"
	"strings"
	"sync/atomic"
	"time"
)

// ParseBindPriority parses a comma-separated "source:name" list (the
// TSESSION_BIND_PRIORITY format). "jwt" is an alias for jwt_claim; header names
// are lower-cased. Invalid and duplicate entries are dropped; the first
// invalid entry is logged once per process. Names are split on the first colon.
func ParseBindPriority(s string) []BindingKey {
	var out []BindingKey
	seen := map[BindingKey]bool{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		src, name, ok := strings.Cut(p, ":")
		src, name = strings.ToLower(strings.TrimSpace(src)), strings.TrimSpace(name)
		if src == "jwt" {
			src = SourceJWTClaim
		}
		switch src {
		case SourceHeader, SourceJWTClaim, SourceCookie, SourceQuery:
		default:
			ok = false
		}
		if !ok || name == "" || len(name) > MaxBindingName {
			logOnce(&badPriorityLogged, "tsession: ignoring invalid TSESSION_BIND_PRIORITY entry (want source:name, source in header|jwt_claim|jwt|cookie|query)")
			continue
		}
		k := BindingKey{src, NormalizeBindingName(src, name)}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

var badPriorityLogged atomic.Bool

func logOnce(flag *atomic.Bool, msg string) {
	if flag.CompareAndSwap(false, true) {
		log.Print(msg)
	}
}

type prioBox struct{ keys []BindingKey }

var bindPriority atomic.Pointer[prioBox]

// BindPriority returns the configured ordered priority list (nil when unset).
// Callers must not modify it.
func BindPriority() []BindingKey {
	if b := bindPriority.Load(); b != nil {
		return b.keys
	}
	return nil
}

// SetBindPriority sets the process-wide priority list (normally via
// Config.BindPriority). Entries are normalized and de-duplicated.
func SetBindPriority(keys []BindingKey) {
	if len(keys) == 0 {
		bindPriority.Store(nil)
		return
	}
	norm := make([]BindingKey, 0, len(keys))
	seen := map[BindingKey]bool{}
	for _, k := range keys {
		if k.Source == "jwt" {
			k.Source = SourceJWTClaim
		}
		k.Name = NormalizeBindingName(k.Source, k.Name)
		if k.Name == "" || seen[k] {
			continue
		}
		seen[k] = true
		norm = append(norm, k)
	}
	bindPriority.Store(&prioBox{norm})
}

// ---- ambiguity ----

var (
	ambiguous   atomic.Uint64
	ambLogNanos atomic.Int64
)

// BindingAmbiguous returns how many requests matched bindings of two
// different sessions (OR path) and were therefore left untraced.
func BindingAmbiguous() uint64 { return ambiguous.Load() }

func noteAmbiguous() {
	ambiguous.Add(1)
	now := time.Now().UnixNano()
	last := ambLogNanos.Load()
	if now-last >= int64(30*time.Second) && ambLogNanos.CompareAndSwap(last, now) {
		log.Printf("tsession: request matched bindings of multiple sessions; left untraced (total=%d)", ambiguous.Load())
	}
}

// BindingMatch is the detailed result of a binding lookup.
type BindingMatch struct {
	SessionID string
	Key       BindingKey // key that decided
	OK        bool
	Fallback  bool // decided by a key other than the first priority key (priority list set)
	Ambiguous bool // OR path matched two different sessions; OK is false
}

// DetailBinder is an optional richer KeyBinder that reports fallback and
// ambiguity.
type DetailBinder interface {
	MatchAttrsDetail(a Attributes) BindingMatch
}

// MatchDetail is Match plus fallback/ambiguity reporting. Semantics:
//
//   - priority list set: the first priority key (that some binding uses)
//     with a non-empty request value decides. An unbound value means no match;
//     later keys are NOT consulted. If no priority key is present, remaining
//     bound keys are tried in OR manner.
//   - OR manner: matches of two different sessions are ambiguous (no match).
func (s BindingSet) MatchDetail(a Attributes) BindingMatch {
	if len(s.keys) == 0 || a == nil {
		return BindingMatch{}
	}
	prio := BindPriority()
	for i, k := range prio {
		m, used := s.idx[k]
		if !used {
			continue
		}
		v := a.Get(k.Source, k.Name)
		if v == "" || len(v) > MaxBindingValue {
			continue
		}
		id, ok := m[v]
		return BindingMatch{SessionID: id, Key: k, OK: ok, Fallback: i > 0}
	}
	var res BindingMatch
	for _, k := range s.keys {
		if len(prio) > 0 && inPrio(prio, k) {
			continue
		}
		v := a.Get(k.Source, k.Name)
		if v == "" || len(v) > MaxBindingValue {
			continue
		}
		id, ok := s.idx[k][v]
		if !ok {
			continue
		}
		if res.OK && res.SessionID != id {
			noteAmbiguous()
			return BindingMatch{Ambiguous: true}
		}
		if !res.OK {
			res = BindingMatch{SessionID: id, Key: k, OK: true, Fallback: len(prio) > 0}
		}
	}
	return res
}

func inPrio(prio []BindingKey, k BindingKey) bool {
	for _, p := range prio {
		if p == k {
			return true
		}
	}
	return false
}
