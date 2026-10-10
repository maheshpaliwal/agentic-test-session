package tsql

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"tsession/core"
)

const maxCellLen = 1024

// toValue converts a driver value into something JSON-friendly. It copies
// byte slices because drivers reuse buffers.
func toValue(v any, max int) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if utf8.Valid(x) {
			return capStr(string(x), max)
		}
		return fmt.Sprintf("<%d bytes>", len(x))
	case string:
		return capStr(x, max)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case int64, float64, bool, int, int32, float32, uint64:
		return x
	default:
		return capStr(fmt.Sprint(x), max)
	}
}

func capStr(s string, max int) string {
	if max > 0 && len(s) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		return s[:cut] + "...[truncated]"
	}
	return s
}

func rowMap(cols []string, dest []driver.Value) map[string]any {
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		if i < len(dest) {
			m[c] = toValue(dest[i], maxCellLen)
		}
	}
	return m
}

func argValues(args []driver.NamedValue, cfg *config) []any {
	n := len(args)
	if n > cfg.maxArgs {
		n = cfg.maxArgs
	}
	out := make([]any, n, n+1)
	for i := 0; i < n; i++ {
		out[i] = toValue(args[i].Value, cfg.maxArgLen)
	}
	if len(args) > n {
		out = append(out, fmt.Sprintf("<%d more>", len(args)-n))
	}
	return out
}

func capSQL(s string, max int) string { return capStr(s, max) }

// pkOf returns the PK map of a row image, or nil unless every PK column is present.
func pkOf(row map[string]any, cols []string) map[string]any {
	if row == nil || len(cols) == 0 {
		return nil
	}
	pk := make(map[string]any, len(cols))
	for _, c := range cols {
		v, ok := lookupCI(row, c)
		if !ok {
			return nil
		}
		pk[c] = v
	}
	return pk
}

func lookupCI(row map[string]any, col string) (any, bool) {
	if v, ok := row[col]; ok {
		return v, true
	}
	for k, v := range row {
		if strings.EqualFold(k, col) {
			return v, true
		}
	}
	return nil, false
}

func pkKey(pk map[string]any, cols []string) string {
	var b strings.Builder
	for _, c := range cols {
		fmt.Fprintf(&b, "%v\x00", pk[c])
	}
	return b.String()
}

func pkString(pk map[string]any, cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprint(pk[c])
	}
	return strings.Join(parts, ",")
}

// diffMaps lists changed fields. A nil side means insert/delete.
func diffMaps(before, after map[string]any) []core.FieldChange {
	seen := map[string]struct{}{}
	var keys []string
	for k := range before {
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	for k := range after {
		if _, ok := seen[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []core.FieldChange
	for _, k := range keys {
		o, oOK := before[k]
		n, nOK := after[k]
		if before != nil && after != nil && oOK && nOK && fmt.Sprint(o) == fmt.Sprint(n) {
			continue
		}
		fc := core.FieldChange{Field: k}
		if oOK {
			fc.Old = o
		}
		if nOK {
			fc.New = n
		}
		out = append(out, fc)
	}
	return out
}
