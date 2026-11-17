package tsql

import (
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

var parseCache = struct {
	sync.Mutex
	m map[parseKey]mutation.Info
}{m: map[parseKey]mutation.Info{}}

type parseKey struct {
	sql string
	d   mutation.Dialect
}

// maxCachedSQL bounds the statement text kept as a cache key. Longer texts
// (bulk inserts, inlined IN lists) are rarely repeated verbatim and would pin
// megabytes of SQL and literal offsets.
const maxCachedSQL = 4 << 10

func parseCached(q string, d mutation.Dialect) mutation.Info {
	if len(q) > maxCachedSQL {
		return mutation.Parse(q, d)
	}
	k := parseKey{q, d}
	parseCache.Lock()
	in, ok := parseCache.m[k]
	parseCache.Unlock()
	if ok {
		return in
	}
	in = mutation.Parse(q, d)
	parseCache.Lock()
	if len(parseCache.m) >= 1024 {
		clear(parseCache.m)
	}
	parseCache.m[k] = in
	parseCache.Unlock()
	return in
}
