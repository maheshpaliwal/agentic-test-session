package core

import "testing"

func BenchmarkNewSpanID(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = NewSpanID()
		}
	})
}

func BenchmarkNewTraceID(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = NewTraceID()
	}
}

// Ids are 16 and 32 lower-case hex digits and do not repeat: a span that
// shared its id with another would be taken for it in the call tree.
func TestNewIDs(t *testing.T) {
	seen := map[string]struct{}{}
	for range 200_000 {
		s, tr := NewSpanID(), NewTraceID()
		if len(s) != 16 || len(tr) != 32 {
			panic("lengths: " + s + " " + tr)
		}
		for _, id := range []string{s, tr} {
			for _, c := range id {
				if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
					panic("not lower-case hex: " + id)
				}
			}
			if _, dup := seen[id]; dup {
				panic("repeated id " + id)
			}
			seen[id] = struct{}{}
		}
	}
}
