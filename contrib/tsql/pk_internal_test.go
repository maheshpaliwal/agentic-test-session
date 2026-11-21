package tsql

import (
	"fmt"
	"testing"
)

// The per-handle key cache never holds more than maxPKTables entries.
func TestPKCacheIsBounded(t *testing.T) {
	var p pkCache
	for i := 0; i < maxPKTables+10; i++ {
		p.put(fmt.Sprintf("t%d", i), pkEntry{cols: []string{"id"}})
	}
	if n := len(p.m); n == 0 || n > maxPKTables {
		t.Fatalf("%d entries, want 1..%d", n, maxPKTables)
	}
	p.put("t0", pkEntry{})
	for i := len(p.m); i < maxPKTables; i++ {
		p.put(fmt.Sprintf("u%d", i), pkEntry{})
	}
	p.put("t0", pkEntry{cols: []string{"k"}}) // replacing an entry of a full cache keeps the rest
	if n := len(p.m); n != maxPKTables {
		t.Fatalf("%d entries after replacing one in a full cache, want %d", n, maxPKTables)
	}
}

// A table is asked about again after a failed lookup, maxPKLookups times.
func TestPKEntrySettled(t *testing.T) {
	var p pkCache
	if _, ok := p.get("t"); ok {
		t.Fatal("empty cache has an entry")
	}
	for i := 1; i <= maxPKLookups; i++ {
		p.failed("t")
		if e, _ := p.get("t"); e.settled() != (i == maxPKLookups) {
			t.Fatalf("after %d failures settled = %v", i, e.settled())
		}
	}
	p.put("t", pkEntry{})
	if e, ok := p.get("t"); !ok || !e.settled() {
		t.Fatal("a table without a key is not settled")
	}
	p.reset()
	if _, ok := p.get("t"); ok {
		t.Fatal("reset kept an entry")
	}
}
