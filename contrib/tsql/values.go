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

// argValues renders bind arguments; masked positions become core.RedactedValue.
func argValues(args []driver.NamedValue, cfg *config, mask []bool) []any {
	n := len(args)
	if n > cfg.maxArgs {
		n = cfg.maxArgs
	}
	out := make([]any, n, n+1)
	for i := 0; i < n; i++ {
		if i < len(mask) && mask[i] {
			out[i] = core.RedactedValue
			continue
		}
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
		parts[i] = fmt.Sprint(core.RedactNamed(c, pk[c]))
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
			fc.Old = core.RedactAny(core.RedactNamed(k, o))
		}
		if nOK {
			fc.New = core.RedactAny(core.RedactNamed(k, n))
		}
		out = append(out, fc)
	}
	return out
}

// img is a captured row: display values plus the faithful driver values of its
// primary-key columns (nil when unavailable).
type img struct {
	disp map[string]any
	raw  map[string]driver.Value
}

// pkRef identifies a row: display form for the event, raw values for binding.
type pkRef struct {
	disp map[string]any
	raw  map[string]driver.Value
}

func (m img) ref(pkCols []string) pkRef {
	d := pkOf(m.disp, pkCols)
	if d == nil {
		return pkRef{}
	}
	return pkRef{disp: d, raw: m.raw}
}

func rawKey(raw map[string]driver.Value, cols []string) string {
	var b strings.Builder
	for _, c := range cols {
		fmt.Fprintf(&b, "%T:%v\x00", raw[c], raw[c])
	}
	return b.String()
}

// rawPK copies the primary-key columns out of one result row; nil unless all are present.
func rawPK(cols []string, dest []driver.Value, pkCols []string) map[string]driver.Value {
	if len(pkCols) == 0 {
		return nil
	}
	out := make(map[string]driver.Value, len(pkCols))
	for _, pc := range pkCols {
		found := false
		for i, c := range cols {
			if i < len(dest) && strings.EqualFold(c, pc) {
				v := dest[i]
				if b, ok := v.([]byte); ok {
					v = append([]byte(nil), b...)
				}
				out[pc] = v
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return out
}

func redactMap(m map[string]any) map[string]any {
	if r, ok := core.RedactAny(m).(map[string]any); ok {
		return r
	}
	return m
}

func redactRows(rows []map[string]any) []any {
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = core.RedactAny(r)
	}
	return out
}
