// Package ident cleans the names an SDK gives itself (service, instance), so
// that the same process is known by the same name wherever it is named: on
// the active-set stream (a header, which HTTP trims), in a delivery report
// (JSON, which nothing trims) and on its events at the collector.
package ident

import "strings"

// MaxLen bounds a cleaned name.
const MaxLen = 128

// Clean returns s as it is compared and shown: printable ASCII only, without
// leading or trailing spaces, at most MaxLen bytes. It is idempotent.
func Clean(s string) string { return CleanN(s, MaxLen) }

// CleanN is Clean with another length limit.
func CleanN(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > limit {
		s = strings.TrimSpace(s[:limit])
	}
	return s
}
