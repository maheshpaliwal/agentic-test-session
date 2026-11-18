// Package mapx holds the few generic map helpers the standard maps package does not provide.
//
// Reach for the standard library first (maps.Clone, maps.Copy, maps.Keys, ...).
package mapx

import (
	"cmp"
	"slices"
)

// SortedKeys returns the keys of m in ascending order. The result is non-nil.
func SortedKeys[M ~map[K]V, K cmp.Ordered, V any](m M) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Values returns the values of m in unspecified order. The result is non-nil.
func Values[M ~map[K]V, K comparable, V any](m M) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// Pick returns a new map holding the entries of m whose key is in keys. Keys missing from m are skipped.
func Pick[M ~map[K]V, K comparable, V any](m M, keys ...K) M {
	out := make(M, len(keys))
	for _, k := range keys {
		if v, ok := m[k]; ok {
			out[k] = v
		}
	}
	return out
}
