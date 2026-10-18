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

func parseCached(q string, d mutation.Dialect) mutation.Info {
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
