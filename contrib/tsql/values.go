package tsql

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
)

const maxCellLen = 1024

// truncMark ends every value capStr shortened.
const truncMark = "...[truncated]"

// toValue converts a driver value into something JSON-friendly. It copies
// byte slices because drivers reuse buffers. Binary data is only counted
// ("<3 bytes>"); see keyValue for the values of key columns.
func toValue(v any, max int) any { return render(v, max, false) }

// Limits of a key value's display form. A binary key is shown whole, in hex,
// up to maxKeyBytes; a longer one, like a text key longer than maxCellLen,
// keeps a prefix and ends in its length and a hash of the whole value.
const (
	maxKeyBytes    = 64
	keyPrefixBytes = 32
	keyHashBytes   = 8
)

// keyValue is toValue for the value of a primary-key column. Rows are told
// apart and filed by this form, so two different keys must never look the
// same, which a count of bytes and a cut-off prefix do:
//
//   - binary data (bytes that are not UTF-8) is written in hex, Postgres
//     style: \xfffe0001. Beyond maxKeyBytes it is the first keyPrefixBytes
//     and "...[100 bytes sha256:0123456789abcdef]";
//   - text is kept as text up to maxCellLen bytes, then cut and given the
//     same ending;
//   - bytes that are text which itself starts with \x are written in hex, so
//     no text reads like the hex form of other bytes.
//
// The same value has the same form wherever it comes from (a row image, a
// bind argument), so before, after and PK of a mutation agree.
func keyValue(v any) any { return render(v, maxCellLen, true) }

func render(v any, max int, key bool) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return renderBytes(x, max, key)
	case string:
		if key {
			return capKey(x, max)
		}
		return capStr(x, max)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case int64, float64, bool, int, int32, float32, uint64:
		return x
	}
	// What a driver that checks its own arguments (pgx) lets through. A
	// Valuer says what it stands for (pgtype.UUID, pgtype.Text, sql.Null*);
	// one that renders itself is taken at its word (uuid.UUID); a byte array
	// or a named byte slice is bytes, and 16 of them in an array are a UUID.
	if vr, ok := v.(driver.Valuer); ok {
		if val, ok := valuerValue(vr); ok {
			if _, again := val.(driver.Valuer); !again {
				return render(val, max, key)
			}
		}
	} else if _, ok := v.(fmt.Stringer); !ok {
		if b, isUUID, ok := byteSeq(v); ok {
			if isUUID {
				return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
			}
			return renderBytes(b, max, key)
		}
	}
	if key {
		return capKey(fmt.Sprint(v), max)
	}
	return capStr(fmt.Sprint(v), max)
}

func renderBytes(b []byte, max int, key bool) any {
	switch {
	case !key && utf8.Valid(b):
		return capStr(string(b), max)
	case !key:
		return fmt.Sprintf("<%d bytes>", len(b))
	case utf8.Valid(b) && !strings.HasPrefix(string(b), `\x`):
		return capKey(string(b), max)
	case len(b) <= maxKeyBytes:
		return `\x` + hex.EncodeToString(b)
	}
	return `\x` + hex.EncodeToString(b[:keyPrefixBytes]) + keyTail(b)
}

// keyTail ends a key value that is not shown whole.
func keyTail[T string | []byte](whole T) string {
	sum := sha256.Sum256([]byte(whole))
	return fmt.Sprintf("...[%d bytes sha256:%x]", len(whole), sum[:keyHashBytes])
}

// capKey is capStr for a key value: what is cut off is replaced by the
// value's length and hash, so two long keys with the same beginning differ.
func capKey(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + keyTail(s)
}

// valuerValue calls v.Value like database/sql does: a nil pointer whose
// method has a value receiver is NULL, and a Valuer that fails or panics is
// rendered as it is.
func valuerValue(v driver.Valuer) (val driver.Value, ok bool) {
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, true
	}
	defer func() {
		if recover() != nil {
			val, ok = nil, false
		}
	}()
	val, err := v.Value()
	return val, err == nil
}

// byteSeq returns the bytes of a byte array or of a named byte slice.
func byteSeq(v any) (b []byte, uuid, ok bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Bytes(), false, true
		}
	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			b = make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(b), rv)
			return b, len(b) == 16, true
		}
	}
	return nil, false, false
}

func capStr(s string, max int) string {
	if max > 0 && len(s) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		return s[:cut] + truncMark
	}
	return s
}

// rowMap renders one result row. The columns named in pkCols are the row's
// key and get the faithful form of keyValue.
func rowMap(cols []string, dest []driver.Value, pkCols []string) map[string]any {
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		if i < len(dest) {
			m[c] = render(dest[i], maxCellLen, isKeyCol(c, pkCols))
		}
	}
	return m
}

// isKeyCol reports whether col is one of pkCols, compared as rawPK and
// lookupCI compare column names.
func isKeyCol(col string, pkCols []string) bool {
	for _, pc := range pkCols {
		if strings.EqualFold(col, pc) {
			return true
		}
	}
	return false
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

// diffMaps lists changed fields. A nil side means insert/delete. truncated
// reports that a field compared equal only on its captured prefix (both
// values were cut at maxCellLen), so a change beyond it may be missing.
func diffMaps(before, after map[string]any) (out []core.FieldChange, truncated bool) {
	keys := set.Set[string]{}
	keys.AddSeq(maps.Keys(before))
	keys.AddSeq(maps.Keys(after))
	for _, k := range set.Sorted(keys) {
		o, oOK := before[k]
		n, nOK := after[k]
		if before != nil && after != nil && oOK && nOK && fmt.Sprint(o) == fmt.Sprint(n) {
			if s, ok := o.(string); ok && strings.HasSuffix(s, truncMark) {
				truncated = true
			}
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
	return out, truncated
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
