package redisx

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
	"strings"
	"sync"
)

// KeyMask says how the secret part of a key name is recorded.
type KeyMask int

const (
	// KeyMaskHash records a short hash, "session:#a1b2c3d4e5f6", keyed by a
	// random secret of this process that is never exported: one key reads the
	// same for as long as the process runs, so its commands still group as
	// one entity, and nobody can find the secret by hashing candidates. Two
	// processes (two services, or one service before and after a restart)
	// record the same key under different hashes.
	KeyMaskHash KeyMask = iota
	// KeyMaskFlat records the redaction marker, "session:[REDACTED]": nothing
	// is derived from the secret, and all such keys read alike.
	KeyMaskFlat
	// KeyMaskOff records key names as they are.
	KeyMaskOff
	// KeyMaskSession records a short hash keyed by the session id: one key
	// reads the same in every service of a session. The session id is part of
	// every exported event, so whoever reads an export can hash candidates
	// and find a secret that can be guessed or confirm one that is known.
	KeyMaskSession
)

// keyHashLen is the number of hex digits of a masked segment: 48 bits. Two
// different segments get the same hash with a chance of 1% once a process (or,
// for KeyMaskSession, a session) has masked about 2.4 million of them.
const keyHashLen = 12

// processKey keys KeyMaskHash. It lives in memory only.
var processKey = sync.OnceValue(func() []byte {
	k := make([]byte, 32)
	_, _ = rand.Read(k) // does not fail
	return k
})

// secretPrefixes are key segments that, besides core's sensitive names,
// announce a secret in the segment after them: session stores key their
// entries by the session token ("session:<token>", "sess:<sid>").
var secretPrefixes = []string{"session", "sess", "sid"}

// nameSeparators join a name to what it names inside one segment:
// "session_<token>", "session/<token>", "token=<value>".
const nameSeparators = "_-./="

// ShowKey returns key as it is recorded: with the parts that hold a secret
// masked. The real key is still what is sent to Redis. session scopes the hash.
//
// The key is cut into segments at ':'. A segment is a name when it is made of
// letters joined by '_', '-' or '.', in any case, and a secret name when it is
// a name that is sensitive per core, or session, sess or sid, or the plural of
// one of them ("tokens"); braces around a segment ("{session}") are ignored.
// A secret name is never masked. A segment is masked when
//
//   - a MaskKeys glob matches the key and the segment falls under a wildcard
//     of it ("auth:token:*" masks the third segment and any after it);
//   - it comes after a secret name and is not a lower-case word
//     ("Session:<token>", "auth:user:42:profile");
//   - it starts with a secret name joined to something that is not a
//     lower-case word by '_', '-', '.', '/' or '=': that part is masked
//     ("session_<token>", "session/<token>");
//   - it is not a name and holds a sensitive name ("tok_SECRET123"), or it is
//     a JWT.
//
// A lower-case word is taken for a name ("auth:token:index" is recorded
// whole), and so is a segment of wildcards only. This is a heuristic over key
// names: it does not know which keys of an application hold secrets, and a
// secret made of lower-case letters only passes it.
func (h *Redactor) ShowKey(session, key string) string {
	if h.KeyMask == KeyMaskOff || key == "" {
		return key
	}
	if len(h.MaskKeys) == 0 && !holdsSecret(key) {
		return key // the usual case, without an allocation
	}
	segs := strings.Split(key, ":")
	cuts := make([]cut, len(segs))
	after := false
	for i, s := range segs {
		cuts[i] = secretPart(s, &after)
	}
	for _, g := range h.MaskKeys {
		markGlob(g, segs, cuts)
	}
	var b strings.Builder
	for i, s := range segs {
		if i > 0 {
			b.WriteByte(':')
		}
		if c := cuts[i]; c.from < c.to {
			b.WriteString(s[:c.from])
			b.WriteString(h.maskSegment(session, s[c.from:c.to]))
			b.WriteString(s[c.to:])
		} else {
			b.WriteString(s)
		}
	}
	return b.String()
}

// ShowText is ShowKey for a string that is not known to be a key: any other
// argument, an element of a reply, a set member. Free text (it holds a space
// or a control character, or is very long) and JSON documents are left to the
// value redaction; anything else is masked as a key would be, so a key that
// travels as a value is recorded the way it is everywhere else.
func (h *Redactor) ShowText(session, s string) string {
	if h.KeyMask == KeyMaskOff || !keyLike(s) {
		return s
	}
	return h.ShowKey(session, s)
}

// Text returns a stored value, a member or an element of a reply as it is
// recorded: scrubbed as a value (RedactStr), and then masked as a key name if
// it still reads as one that holds a secret (ShowText).
func (h *Redactor) Text(session, s string) string { return h.ShowText(session, RedactStr(s)) }

// keyLike reports whether s can be a key name: no blank in it, and not a
// JSON document. Its length says nothing: a token after a secret name can be
// kilobytes long (a JWT with many claims), and it is cut off by its prefix
// like a short one.
func keyLike(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' {
			return false
		}
	}
	return !(s[0] == '{' || s[0] == '[') || !json.Valid([]byte(s))
}

func (h *Redactor) maskSegment(session, seg string) string {
	if h.KeyMask == KeyMaskFlat {
		return masked
	}
	var mac hash.Hash
	if h.KeyMask == KeyMaskSession {
		mac = hmac.New(sha256.New, []byte(session))
	} else {
		// Keying an HMAC costs two SHA-256 blocks and five allocations; the
		// process key never changes, so the keyed state is kept and reset.
		mac = processMAC.Get().(hash.Hash)
		defer func() { mac.Reset(); processMAC.Put(mac) }()
		mac.Write([]byte(session))
		mac.Write([]byte{0})
	}
	mac.Write([]byte(seg))
	var sum [sha256.Size]byte
	var out [1 + 2*sha256.Size]byte
	out[0] = '#'
	hex.Encode(out[1:], mac.Sum(sum[:0]))
	return string(out[:1+keyHashLen])
}

var processMAC = sync.Pool{New: func() any { return hmac.New(sha256.New, processKey()) }}

// maskedSegment reports whether p is what ShowKey writes for a masked part.
func maskedSegment(p string) bool {
	if p == masked {
		return true
	}
	if len(p) != keyHashLen+1 || p[0] != '#' {
		return false
	}
	for i := 1; i < len(p); i++ {
		if c := p[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// maskedEnd returns the index in p where a masked part that ends p starts:
// 0 for a masked segment, the index after the separator for "session_#a1b2…".
func maskedEnd(p string) (int, bool) {
	if maskedSegment(p) {
		return 0, true
	}
	if i := strings.LastIndexAny(p, nameSeparators); i >= 0 && maskedSegment(p[i+1:]) {
		return i + 1, true
	}
	return 0, false
}

// cut is the part of a segment that is masked: seg[from:to]. from == to
// means none.
type cut struct{ from, to int }

// holdsSecret reports whether the default rules mask any part of key.
func holdsSecret(key string) bool {
	after := false
	for {
		i := strings.IndexByte(key, ':')
		seg := key
		if i >= 0 {
			seg = key[:i]
		}
		if c := secretPart(seg, &after); c.from < c.to {
			return true
		}
		if i < 0 {
			return false
		}
		key = key[i+1:]
	}
}

// secretPart applies the default rules of ShowKey to one segment. after says
// that a secret name came before, and is set when seg is or starts with one.
func secretPart(seg string, after *bool) cut {
	lead := 0
	for lead < len(seg) && seg[lead] == '{' {
		lead++
	}
	end := len(seg)
	for end > lead && seg[end-1] == '}' {
		end--
	}
	s := seg[lead:end]
	if s == "" || wildcards(s) {
		return cut{}
	}
	if _, done := maskedEnd(s); done {
		return cut{} // already masked: masking is applied once
	}
	if secretName(s) {
		*after = true
		return cut{}
	}
	if plainWord(s) {
		return cut{}
	}
	if *after {
		return cut{lead, end}
	}
	for i := 1; i < len(s)-1; i++ {
		if strings.IndexByte(nameSeparators, s[i]) < 0 {
			continue
		}
		if !nameWord(s[:i]) {
			break
		}
		if secretName(s[:i]) && !plainWord(s[i+1:]) {
			*after = true
			return cut{lead + i + 1, end}
		}
	}
	if isJWT(s) {
		return cut{lead, end}
	}
	if !strings.Contains(s, masked) && hasLetter(s) && NameSensitive(s) {
		*after = true // the secret may be this segment or the next: mask both
		return cut{lead, end}
	}
	return cut{}
}

// markGlob marks the segments of a key that fall under a wildcard of glob,
// when glob matches the key. The glob is matched segment by segment; its last
// segment takes the rest of the key.
func markGlob(glob string, segs []string, cuts []cut) {
	gs := strings.Split(glob, ":")
	if len(gs) > len(segs) {
		return
	}
	last := len(gs) - 1
	for i, g := range gs {
		s := segs[i]
		if i == last {
			s = strings.Join(segs[i:], ":")
		}
		if !Match(g, s) {
			return
		}
	}
	for i, g := range gs {
		if !strings.ContainsAny(g, "*?[") {
			continue
		}
		to := i
		if i == last {
			to = len(segs) - 1
		}
		for j := i; j <= to; j++ {
			if _, done := maskedEnd(segs[j]); !done {
				cuts[j] = cut{0, len(segs[j])}
			}
		}
	}
}

// secretName reports whether the segment names a secret that follows it.
func secretName(s string) bool {
	if !nameWord(s) {
		return false
	}
	if secretWord(s) {
		return true
	}
	n := len(s) - 1 // a plural: tokens, sessions, api_keys
	return n > 1 && (s[n] == 's' || s[n] == 'S') && secretWord(s[:n])
}

func secretWord(s string) bool {
	last := s[strings.LastIndexAny(s, "_-.")+1:]
	for _, p := range secretPrefixes {
		if strings.EqualFold(s, p) || strings.EqualFold(last, p) {
			return true
		}
	}
	return NameSensitive(s)
}

// nameWord reports whether s reads as a name in any case: letters joined by
// '_', '-' or '.'.
func nameWord(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

// plainWord reports whether s reads as a name rather than a value: lower-case
// letters joined by '_', '-' or '.'.
func plainWord(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

func hasLetter(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i] | 0x20; c >= 'a' && c <= 'z' {
			return true
		}
	}
	return false
}

// wildcards reports whether s is made of glob wildcards only.
func wildcards(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '*' && s[i] != '?' {
			return false
		}
	}
	return true
}

func isJWT(s string) bool { return strings.HasPrefix(s, "eyJ") && strings.Count(s, ".") == 2 }
