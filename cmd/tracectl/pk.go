package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// noKey is how the collector spells the key of a mutation recorded without one.
const noKey = "{}"

// resolveKey turns the <pk> typed for `history` into the key of one of the
// session's entities, spelled as the collector lists it (the only spelling it
// is certain to accept for a composite key). typed is tried, in this order, as
//
//   - the listed spelling itself, or nothing for the unknown key;
//   - a JSON object with the same columns and values, in any order and spacing;
//   - column=value pairs separated by commas;
//   - the bare value of a one-column key.
//
// The first reading that names an entity wins. One that names several (the
// bare value 1 where both {"id":1} and {"id":"1"} were recorded) is refused
// rather than guessed. Nothing is looked up from what was typed alone: the
// result is always a key the session recorded.
func resolveKey(ents []client.Entity, store, entity, typed string) (string, error) {
	var keys []string // of this store and entity, as listed
	for _, e := range ents {
		if e.Store == store && e.Entity == entity {
			keys = append(keys, canonKey(e.PK))
		}
	}
	if strings.TrimSpace(typed) == "" {
		typed = noKey
	}
	readings := []func(key string) bool{
		func(key string) bool { return key == typed },
	}
	if cols, ok := parseKey(typed); ok {
		want := pairsOf(cols)
		readings = append(readings, func(key string) bool { return samePairs(key, want) })
	}
	if want, ok := parsePairs(typed); ok {
		readings = append(readings, func(key string) bool { return samePairs(key, want) })
	}
	readings = append(readings, func(key string) bool {
		cols, ok := parseKey(key)
		if !ok || len(cols) != 1 {
			return false
		}
		for _, v := range cols {
			return keyValue(v) == typed
		}
		return false
	})
	for _, matches := range readings {
		var hit []string
		for _, key := range keys {
			if matches(key) {
				hit = append(hit, key)
			}
		}
		switch len(hit) {
		case 0:
		case 1:
			return hit[0], nil
		default:
			return "", fmt.Errorf("%s names %d keys of %s/%s: %s; pass one of them as JSON",
				printable(typed), len(hit), printable(store), printable(entity), keyList(hit))
		}
	}
	what := "key " + printable(typed)
	if typed == noKey {
		what = "an unknown key"
	}
	if len(keys) == 0 {
		return "", fmt.Errorf("the session changed nothing in %s/%s", printable(store), printable(entity))
	}
	return "", fmt.Errorf("the session changed nothing in %s/%s with %s; it recorded: %s",
		printable(store), printable(entity), what, keyList(keys))
}

// canonKey is the listed key; a collector that lists the unknown key as an
// empty string means "{}".
func canonKey(pk string) string {
	if pk == "" {
		return noKey
	}
	return pk
}

// parseKey reads a key written as a JSON object. Numbers keep their digits,
// so two large ids that differ only past float64 precision stay different.
func parseKey(s string) (map[string]any, bool) {
	if !strings.HasPrefix(strings.TrimSpace(s), "{") {
		return nil, false
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var cols map[string]any
	if err := dec.Decode(&cols); err != nil || cols == nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF { // nothing may follow the object
		return nil, false
	}
	return cols, true
}

// parsePairs reads "column=value,column=value". It refuses, rather than
// guesses at, anything else: a piece without "=", an empty or repeated column.
func parsePairs(s string) (map[string]string, bool) {
	out := map[string]string{}
	for _, piece := range strings.Split(s, ",") {
		col, val, ok := strings.Cut(piece, "=")
		col = strings.TrimSpace(col)
		if _, dup := out[col]; !ok || col == "" || dup {
			return nil, false
		}
		out[col] = strings.TrimSpace(val)
	}
	return out, true
}

// pairsOf renders each column's value as it is typed in the pairs form.
func pairsOf(cols map[string]any) map[string]string {
	out := make(map[string]string, len(cols))
	for col, v := range cols {
		out[col] = keyValue(v)
	}
	return out
}

// samePairs reports whether the listed key has exactly the columns and
// values of want.
func samePairs(key string, want map[string]string) bool {
	cols, ok := parseKey(key)
	if !ok || len(cols) != len(want) {
		return false
	}
	for col, v := range cols {
		if w, ok := want[col]; !ok || w != keyValue(v) {
			return false
		}
	}
	return true
}

// keyValue renders one key value the way a person types it: a string without
// its quotes, a number by its digits, anything else as compact JSON.
func keyValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprint(v)
	}
	return strings.TrimSpace(b.String())
}

// keyList renders a few keys for an error message.
func keyList(keys []string) string {
	const show = 5
	parts := make([]string, 0, show+1)
	for i, k := range keys {
		if i == show {
			parts = append(parts, fmt.Sprintf("and %d more", len(keys)-show))
			break
		}
		if k == noKey {
			k += " " + unknownKey
		}
		parts = append(parts, printable(k))
	}
	return strings.Join(parts, ", ")
}

// printable makes text that was typed or recorded safe to print in an error:
// one line, at most 80 runes, nothing a terminal would act on.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if !unicode.IsPrint(r) {
			return '?'
		}
		return r
	}, clip(s, 80))
}

// uncertainCount is the number of changes whose key or row image is
// incomplete or uncertain.
func uncertainCount(ms []client.EntityMutation) int {
	n := 0
	for _, m := range ms {
		if m.Uncertain() {
			n++
		}
	}
	return n
}
