package tsql

import (
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

func cacheHas(q string) bool {
	parseCache.Lock()
	defer parseCache.Unlock()
	_, ok := parseCache.m[parseKey{q, mutation.Postgres}]
	return ok
}

// Large statements (bulk inserts, inlined lists) are parsed but never pinned
// in the parse cache.
func TestParseCacheSkipsLargeStatements(t *testing.T) {
	small := "SELECT * FROM cache_probe WHERE id = $1"
	large := "INSERT INTO cache_probe (id, note) VALUES " + strings.Repeat("(1, 'some note'), ", 500) + "(2, 'x')"
	if len(large) <= maxCachedSQL {
		t.Fatalf("test statement too small: %d", len(large))
	}
	if in := parseCached(small, mutation.Postgres); in.Table != "cache_probe" || !cacheHas(small) {
		t.Fatalf("small statement not cached: %+v", in)
	}
	if in := parseCached(large, mutation.Postgres); in.Kind != mutation.Insert || !in.MultiRow {
		t.Fatalf("large statement not parsed: kind=%v", in.Kind)
	}
	if cacheHas(large) {
		t.Fatal("large statement was cached")
	}
}
