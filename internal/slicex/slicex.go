// Package slicex holds the few generic slice helpers the standard slices package does not provide.
//
// Reach for the standard library first (slices.Sort, slices.Contains, slices.DeleteFunc, ...). The helpers here
// allocate their result, so keep plain loops on hot paths.
package slicex

import (
	"cmp"
	"slices"
	"strings"
)

// Filter returns the elements of s for which keep reports true, in order. It never aliases s, and the result is
// nil only when s is nil, so a filtered list still encodes as [] in JSON.
func Filter[S ~[]E, E any](s S, keep func(E) bool) S {
	out := s[:0:0]
	for _, v := range s {
		if keep(v) {
			out = append(out, v)
		}
	}
	return out
}

// Map returns fn applied to every element of s. The result is non-nil and has len(s) elements.
func Map[E, R any](s []E, fn func(E) R) []R {
	out := make([]R, len(s))
	for i, v := range s {
		out[i] = fn(v)
	}
	return out
}

// Count returns the number of elements of s for which pred reports true.
func Count[E any](s []E, pred func(E) bool) int {
	n := 0
	for _, v := range s {
		if pred(v) {
			n++
		}
	}
	return n
}

// FilterMap applies fn to every element of s and keeps the results for which fn reports true.
// The result is nil when nothing is kept.
func FilterMap[E, R any](s []E, fn func(E) (R, bool)) []R {
	var out []R
	for _, v := range s {
		if r, ok := fn(v); ok {
			out = append(out, r)
		}
	}
	return out
}

// ToMap indexes s by key. When two elements share a key the later one wins.
func ToMap[E any, K comparable](s []E, key func(E) K) map[K]E {
	out := make(map[K]E, len(s))
	for _, v := range s {
		out[key(v)] = v
	}
	return out
}

// SortBy sorts s in place, ascending by key. The sort is not stable.
func SortBy[S ~[]E, E any, K cmp.Ordered](s S, key func(E) K) {
	slices.SortFunc(s, func(a, b E) int { return cmp.Compare(key(a), key(b)) })
}

// SplitTrim splits s on sep, trims surrounding space from every part and drops the empty ones.
// The result is nil when no part remains.
func SplitTrim(s, sep string) []string {
	var out []string
	for p := range strings.SplitSeq(s, sep) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
