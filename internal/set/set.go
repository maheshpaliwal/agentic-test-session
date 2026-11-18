// Package set is a minimal generic set.
package set

import (
	"cmp"
	"iter"
	"maps"
	"slices"
)

// Set is a set of comparable values. The zero value is an empty set that can be read but not added to;
// build one with Of or make(Set[T]). It is a map, so len, range and delete work on it.
type Set[T comparable] map[T]struct{}

// Of returns a set holding items.
func Of[T comparable](items ...T) Set[T] {
	s := make(Set[T], len(items))
	s.Add(items...)
	return s
}

// Add inserts items.
func (s Set[T]) Add(items ...T) {
	for _, v := range items {
		s[v] = struct{}{}
	}
}

// AddSeq inserts every value of seq, e.g. maps.Keys(m).
func (s Set[T]) AddSeq(seq iter.Seq[T]) {
	for v := range seq {
		s[v] = struct{}{}
	}
}

// Has reports whether v is in the set.
func (s Set[T]) Has(v T) bool {
	_, ok := s[v]
	return ok
}

// Sorted returns the members of s in ascending order. The result is nil for an empty set.
func Sorted[T cmp.Ordered](s Set[T]) []T {
	return slices.Sorted(maps.Keys(s))
}
