package redisx

import (
	"fmt"
	"slices"
)

// Match reports whether name matches the glob pattern. The syntax is that of
// path.Match with one difference: '*' and '?' match any byte, '/' included, as
// they do in a Redis KEYS pattern. A key can hold a '/' anywhere, in a base64
// token for one, and a glob that stops there would leave such keys out.
//
// A '*' matches any run of bytes, a '?' any one byte, "[a-c]" one byte of the
// class ("[^a-c]" one byte outside it) and "\c" the byte c.
//
// A malformed pattern matches nothing: check it with CheckGlob.
func Match(pattern, name string) bool {
	px, nx := 0, 0
	starP, starN := -1, 0 // where to resume after the last '*'
	for nx < len(name) || px < len(pattern) {
		if px < len(pattern) {
			switch c := pattern[px]; c {
			case '*':
				starP, starN = px, nx+1
				px++
				continue
			case '?':
				if nx < len(name) {
					px, nx = px+1, nx+1
					continue
				}
			case '[':
				if end, ok, valid := matchClass(pattern, px, name, nx); !valid {
					return false
				} else if ok {
					px, nx = end, nx+1
					continue
				}
			default:
				if c == '\\' {
					if px++; px == len(pattern) {
						return false
					}
				}
				if nx < len(name) && name[nx] == pattern[px] {
					px, nx = px+1, nx+1
					continue
				}
			}
		}
		if starP < 0 || starN > len(name) {
			return false
		}
		px, nx = starP+1, starN
		starN++
	}
	return true
}

// matchClass matches name[nx] against the class that starts at pattern[px]. It
// returns the index after the class; valid is false for a malformed class.
func matchClass(pattern string, px int, name string, nx int) (end int, ok, valid bool) {
	i := px + 1
	negate := i < len(pattern) && pattern[i] == '^'
	if negate {
		i++
	}
	var c byte
	have := nx < len(name)
	if have {
		c = name[nx]
	}
	for n := 0; ; n++ {
		if i >= len(pattern) {
			return 0, false, false
		}
		if pattern[i] == ']' && n > 0 {
			break
		}
		lo, next, good := classByte(pattern, i)
		if !good {
			return 0, false, false
		}
		hi := lo
		if next < len(pattern) && pattern[next] == '-' {
			if hi, next, good = classByte(pattern, next+1); !good {
				return 0, false, false
			}
		}
		if have && lo <= c && c <= hi {
			ok = true
		}
		i = next
	}
	return i + 1, have && ok != negate, true
}

// classByte reads one byte of a class, unescaping it.
func classByte(pattern string, i int) (c byte, next int, ok bool) {
	if i >= len(pattern) || pattern[i] == '-' || pattern[i] == ']' {
		return 0, 0, false
	}
	if pattern[i] == '\\' {
		if i++; i >= len(pattern) {
			return 0, 0, false
		}
	}
	return pattern[i], i + 1, true
}

// CheckGlob returns an error for a malformed pattern, which Match would let
// match nothing.
func CheckGlob(pattern string) error {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if i++; i == len(pattern) {
				return fmt.Errorf("malformed glob %q: it ends in a backslash", pattern)
			}
		case '[':
			end, _, valid := matchClass(pattern, i, "", 0)
			if !valid {
				return fmt.Errorf("malformed glob %q: the character class at byte %d is not closed or holds a bad range", pattern, i)
			}
			i = end - 1
		}
	}
	return nil
}

// MustGlobs panics when one of the globs of an option is malformed. A glob
// that protects data (WithMaskKeys, WithRedactKeys) and matches nothing would
// leave it unprotected without a word, so the mistake is reported where the
// option is written.
func MustGlobs(option string, globs []string) {
	for _, g := range globs {
		if err := CheckGlob(g); err != nil {
			panic(option + ": " + err.Error())
		}
	}
}

func matchAny(globs []string, key string) bool {
	return slices.ContainsFunc(globs, func(p string) bool { return Match(p, key) })
}
