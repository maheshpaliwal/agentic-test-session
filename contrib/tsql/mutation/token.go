// Package mutation is a small, dependency-free SQL statement classifier. It
// tokenizes Postgres/MySQL text (comments, quoted identifiers, strings,
// dollar quoting, placeholders) and extracts just enough structure for the
// tracing layer: statement kind, target table, WHERE predicate and
// single-row INSERT values. It is not a validating parser.
package mutation

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Dialect selects lexical details that differ between databases.
type Dialect int

const (
	Postgres Dialect = iota
	MySQL
)

type tokType uint8

const (
	tWord   tokType = iota // bare identifier or keyword
	tQuoted                // "ident" or `ident`
	tString
	tNumber
	tPlaceholder
	tPunct
)

type token struct {
	typ        tokType
	text       string // raw source text
	val        string // unquoted identifier / upper-cased word / string contents
	start, end int
	depth      int  // parenthesis depth at this token ('(' reports the outer depth)
	arg        int  // placeholder: 0-based argument index, -1 when named
	dollar     bool // placeholder written as $n
}

func (t token) isWord(w string) bool  { return t.typ == tWord && t.val == w }
func (t token) isPunct(p string) bool { return t.typ == tPunct && t.text == p }
func (t token) isIdent() bool         { return t.typ == tWord || t.typ == tQuoted }

// name is the identifier a word or quoted token denotes (bare words fold to
// lower case, as Postgres does; MySQL keeps them as written).
func (t token) name(d Dialect) string {
	if t.typ == tQuoted || d == MySQL {
		if t.typ == tQuoted {
			return t.val
		}
		return t.text
	}
	return strings.ToLower(t.text)
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }
func isIdentPart(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func tokenize(s string, d Dialect) (toks []token, named bool) {
	depth, seq := 0, 0
	i, n := 0, len(s)
	add := func(t token) {
		t.depth = depth
		toks = append(toks, t)
	}
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-', c == '#' && d == MySQL:
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			lvl := 1
			i += 2
			for i < n && lvl > 0 {
				switch {
				case s[i] == '/' && i+1 < n && s[i+1] == '*' && d == Postgres:
					lvl++
					i += 2
				case s[i] == '*' && i+1 < n && s[i+1] == '/':
					lvl--
					i += 2
				default:
					i++
				}
			}
		case c == '\'' || (c == '"' && d == MySQL):
			backslash := d == MySQL
			if k := len(toks); k > 0 && toks[k-1].typ == tWord && toks[k-1].end == i &&
				(toks[k-1].val == "E") {
				backslash = true
			}
			st := i
			i = scanQuoted(s, i, c, backslash)
			add(token{typ: tString, text: s[st:i], val: unquote(s[st:i], c), start: st, end: i})
		case c == '"' || c == '`':
			st := i
			i = scanQuoted(s, i, c, false)
			add(token{typ: tQuoted, text: s[st:i], val: unquote(s[st:i], c), start: st, end: i})
		case c == '$':
			st := i
			j := i + 1
			for j < n && s[j] >= '0' && s[j] <= '9' {
				j++
			}
			if j > i+1 {
				num, _ := strconv.Atoi(s[i+1 : j])
				i = j
				add(token{typ: tPlaceholder, text: s[st:i], start: st, end: i, arg: num - 1, dollar: true})
				break
			}
			// dollar-quoted string: $tag$ ... $tag$
			for j < n && (s[j] == '_' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] >= '0' && s[j] <= '9') {
				j++
			}
			if j < n && s[j] == '$' {
				tag := s[i : j+1]
				if e := strings.Index(s[j+1:], tag); e >= 0 {
					i = j + 1 + e + len(tag)
				} else {
					i = n
				}
				add(token{typ: tString, text: s[st:i], start: st, end: i})
				break
			}
			i++
			add(token{typ: tPunct, text: "$", start: st, end: i})
		case c == '?':
			add(token{typ: tPlaceholder, text: "?", start: i, end: i + 1, arg: seq})
			seq++
			i++
		case c == ':' && i+1 < n && s[i+1] == ':':
			add(token{typ: tPunct, text: "::", start: i, end: i + 2})
			i += 2
		case c == ':' && i+1 < n && (s[i+1] == '_' || s[i+1] >= 'a' && s[i+1] <= 'z' || s[i+1] >= 'A' && s[i+1] <= 'Z'):
			st := i
			i++
			for i < n && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || s[i] >= '0' && s[i] <= '9') {
				i++
			}
			named = true
			add(token{typ: tPlaceholder, text: s[st:i], start: st, end: i, arg: -1})
		case c >= '0' && c <= '9':
			st := i
			for i < n && (s[i] >= '0' && s[i] <= '9' || s[i] == '.' && i+1 < n && s[i+1] >= '0' && s[i+1] <= '9') {
				i++
			}
			add(token{typ: tNumber, text: s[st:i], val: s[st:i], start: st, end: i})
		default:
			r, sz := utf8.DecodeRuneInString(s[i:])
			if isIdentStart(r) {
				st := i
				for i < n {
					r, sz = utf8.DecodeRuneInString(s[i:])
					if !isIdentPart(r) {
						break
					}
					i += sz
				}
				add(token{typ: tWord, text: s[st:i], val: strings.ToUpper(s[st:i]), start: st, end: i})
				break
			}
			if sz == 0 {
				sz = 1
			}
			t := token{typ: tPunct, text: s[i : i+sz], start: i, end: i + sz}
			i += sz
			switch t.text {
			case "(":
				add(t)
				depth++
				continue
			case ")":
				if depth > 0 {
					depth--
				}
			}
			add(t)
		}
	}
	return toks, named
}

// scanQuoted returns the index just past the closing quote q (doubled quotes
// escape; backslash escapes when enabled). Unterminated input runs to the end.
func scanQuoted(s string, i int, q byte, backslash bool) int {
	i++
	for i < len(s) {
		switch {
		case backslash && s[i] == '\\':
			i += 2
		case s[i] == q && i+1 < len(s) && s[i+1] == q:
			i += 2
		case s[i] == q:
			return i + 1
		default:
			i++
		}
	}
	return len(s)
}

func unquote(raw string, q byte) string {
	if len(raw) < 2 || raw[len(raw)-1] != q {
		return strings.TrimPrefix(raw, string(q))
	}
	inner := raw[1 : len(raw)-1]
	return strings.ReplaceAll(inner, string([]byte{q, q}), string(q))
}
