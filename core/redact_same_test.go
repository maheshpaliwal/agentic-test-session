package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// RedactJSON and RedactText have fast paths that skip the parse, or a regular
// expression, for input that has nothing to mask. These are the plain
// implementations they replaced, word for word, and tests that both give the
// same bytes for generated input, valid and broken.

func plainRedactJSON(b []byte) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return b, false
	}
	if !redactValue(v) {
		return b, false
	}
	out, err := json.Marshal(v)
	if err != nil {
		return b, false
	}
	return out, true
}

func plainRedactText(s string) string {
	if s == "" {
		return s
	}
	s = reBearer.ReplaceAllString(s, "$1 "+RedactedValue)
	if strings.Contains(s, "</") {
		s = reXMLText.ReplaceAllStringFunc(s, func(m string) string {
			sub := reXMLText.FindStringSubmatch(m)
			if len(sub) < 5 || !sensitive(sub[2]) {
				return m
			}
			return sub[1] + RedactedValue + sub[4]
		})
	}
	return reJSONish.ReplaceAllStringFunc(s, func(m string) string {
		sub := reJSONish.FindStringSubmatch(m)
		if len(sub) < 4 || !sensitive(sub[2]) {
			return m
		}
		q := ""
		if v := sub[3]; len(v) > 0 && (v[0] == '"' || v[0] == '\'') {
			q = string(v[0])
		}
		return sub[1] + q + RedactedValue + q
	})
}

var sameKeys = []string{"user", "password", "token", "id", "note", "api_key", "max_tokens", "Authorization", "pass\\u0077ord", "p\\\"q", "clé", "secret", "items", "a", "b", "X-Api-Key", "user[password]", "card.cvv", "", "pwd"}
var sameAtoms = []string{`1`, `-2.50`, `1e9`, `true`, `false`, `null`, `"x"`, `"<a&b>"`, `"é\u00e9\/"`, `"tok=abc"`, `""`, `12345678901234567890`}

func sameJSON(r *rand.Rand, depth int) string {
	switch n := r.Intn(10); {
	case depth > 3 || n < 4:
		return sameAtoms[r.Intn(len(sameAtoms))]
	case n < 8:
		var b strings.Builder
		b.WriteString("{")
		for i, k := 0, r.Intn(5); i < k; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			if r.Intn(4) == 0 {
				b.WriteString(" ")
			}
			fmt.Fprintf(&b, `"%s"`, sameKeys[r.Intn(len(sameKeys))])
			if r.Intn(4) == 0 {
				b.WriteString(" \n\t")
			}
			b.WriteString(":")
			b.WriteString(sameJSON(r, depth+1))
		}
		b.WriteString("}")
		return b.String()
	default:
		var b strings.Builder
		b.WriteString("[")
		for i, k := 0, r.Intn(4); i < k; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(sameJSON(r, depth+1))
		}
		b.WriteString("]")
		return b.String()
	}
}

func TestRedactJSONFastPathIsTheSame(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for range sameRounds(t, 200000) {
		s := sameJSON(r, 0)
		switch r.Intn(6) { // also text that is not a JSON document
		case 0:
			if len(s) > 2 {
				s = s[:r.Intn(len(s))]
			}
		case 1:
			s += " " + sameJSON(r, 0)
		case 2:
			if len(s) > 2 {
				j := r.Intn(len(s))
				s = s[:j] + string(rune("\"\\:{}[],\xff x"[r.Intn(11)])) + s[j+1:]
			}
		}
		w, wc := plainRedactJSON([]byte(s))
		g, gc := RedactJSON([]byte(s))
		if wc != gc || !bytes.Equal(w, g) {
			panic(fmt.Sprintf("%q:\nold %q %v\nnew %q %v", s, w, wc, g, gc))
		}
	}
}

func TestRedactTextFastPathIsTheSame(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	frag := []string{"password", "token", "user", "=", ":", " ", "\"", "'", "Bearer ", "basic\t", "abc.def", "&", ",", ";", "}", "]", "[REDACTED]", "<secret>", "</secret>", "<a>", "</a>", "x", "\\", "\n", "{", "\"password\":\"hunter2\"", "tok=1", "api_key = 'v v'", "é", "Authorization: Bearer abc", "[", "-", "_", ".", "max_tokens=5", "\\\""}
	for range sameRounds(t, 300000) {
		var b strings.Builder
		for j, k := 0, r.Intn(12); j < k; j++ {
			b.WriteString(frag[r.Intn(len(frag))])
		}
		s := b.String()
		if w, g := plainRedactText(s), RedactText(s); w != g {
			panic(fmt.Sprintf("%q:\nold %q\nnew %q", s, w, g))
		}
	}
}

// sameRounds is n, or a tenth of it under -short.
func sameRounds(t *testing.T, n int) int {
	if testing.Short() {
		return n / 10
	}
	return n
}
