package redisx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"slices"
	"strings"
)

// KeyMask says how the secret part of a key name is recorded.
type KeyMask int

const (
	// KeyMaskHash records a short keyed hash, "session:#a1b2c3d4": the same key
	// reads the same throughout one session, so its commands still group as
	// one entity, and differently in every other session.
	KeyMaskHash KeyMask = iota
	// KeyMaskFlat records the redaction marker, "session:[REDACTED]": nothing
	// of the secret is derived, and all such keys read alike.
	KeyMaskFlat
	// KeyMaskOff records key names as they are.
	KeyMaskOff
)

// keyHashLen is the number of hex digits of a masked segment. 32 bits keep
// two keys of one session apart and are too few to confirm a long random
// token; a short or guessable one can still be found by trying candidates.
const keyHashLen = 8

// secretPrefixes are key segments that, besides core's sensitive names,
// announce a secret in the segment after them: session stores key their
// entries by the session token ("session:<token>", "sess:<sid>").
var secretPrefixes = []string{"session", "sessions", "sess", "sid"}

// ShowKey returns key as it is recorded: with the segments that hold a secret
// masked. The real key is still what is sent to Redis. session keys the hash.
//
// A ':' separated segment is masked when
//   - a MaskKeys glob matches the key and the segment falls under a wildcard
//     of it ("auth:token:*" masks the third segment and any after it);
//   - it is not a plain lower-case word (letters joined by '_', '-' or '.')
//     and comes after a segment that names a secret: a sensitive name per
//     core, or "session", "sessions", "sess", "sid";
//   - it is not a plain lower-case word and holds a sensitive name itself
//     ("tok_SECRET123"), or it is a JWT.
//
// A plain lower-case word is taken for a name ("auth:token:index" is recorded
// whole). This is a heuristic over key names: it does not know which keys of
// an application hold secrets, and a secret made of lower-case letters only
// passes it.
func (h *Redactor) ShowKey(session, key string) string {
	if h.KeyMask == KeyMaskOff || key == "" {
		return key
	}
	segs := strings.Split(key, ":")
	mask := make([]bool, len(segs))
	for _, g := range h.MaskKeys {
		markGlob(g, segs, mask)
	}
	markSecrets(segs, mask)
	if !slices.Contains(mask, true) {
		return key
	}
	for i, s := range segs {
		if mask[i] && s != "" {
			segs[i] = h.maskSegment(session, s)
		}
	}
	return strings.Join(segs, ":")
}

func (h *Redactor) maskSegment(session, seg string) string {
	if h.KeyMask == KeyMaskFlat {
		return masked
	}
	mac := hmac.New(sha256.New, []byte(session))
	mac.Write([]byte(seg))
	return "#" + hex.EncodeToString(mac.Sum(nil))[:keyHashLen]
}

// maskedSegment reports whether p is a segment ShowKey masked.
func maskedSegment(p string) bool {
	if p == masked {
		return true
	}
	if len(p) != keyHashLen+1 || p[0] != '#' {
		return false
	}
	_, err := hex.DecodeString(p[1:])
	return err == nil
}

// markGlob marks the segments of a key that fall under a wildcard of glob,
// when glob matches the key. The glob is matched segment by segment; its last
// segment takes the rest of the key, as path.Match's * does.
func markGlob(glob string, segs []string, mask []bool) {
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
		if ok, _ := path.Match(g, s); !ok {
			return
		}
	}
	for i, g := range gs {
		if !strings.ContainsAny(g, "*?[") {
			continue
		}
		mask[i] = true
		if i == last {
			for j := i; j < len(segs); j++ {
				mask[j] = true
			}
		}
	}
}

// markSecrets applies the default rules of ShowKey.
func markSecrets(segs []string, mask []bool) {
	after := false // a segment naming a secret came before
	for i, s := range segs {
		switch {
		case s == "":
		case secretName(s):
			after = true
		case !plainWord(s) && (after || NameSensitive(s) || isJWT(s)):
			mask[i] = true
		}
	}
}

// secretName reports whether the segment names a secret that follows it.
func secretName(s string) bool {
	return plainWord(s) && (NameSensitive(s) || slices.Contains(secretPrefixes, s))
}

// plainWord reports whether s reads as a name rather than a value: lower-case
// letters joined by '_', '-' or '.'.
func plainWord(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

func isJWT(s string) bool { return strings.HasPrefix(s, "eyJ") && strings.Count(s, ".") == 2 }
