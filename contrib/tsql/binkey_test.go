package tsql_test

// Binary and long key values: two different rows are never recorded under
// the same key. Fake driver; the same on a real Postgres in binkey_pg_test.go.

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// binDB is a table t (k, v, blob) keyed by k holding the given keys; every
// row's blob is three bytes that are not text.
func binDB(keyCol string, keys ...[]byte) *fakeDB {
	return &fakeDB{
		pk: func(string) result { return pkRows(keyCol) },
		handler: func(q string, _ []driver.Value) result {
			if !strings.HasPrefix(q, "SELECT") {
				return result{affected: int64(len(keys))}
			}
			r := result{cols: []string{keyCol, "v", "blob"}}
			for i, k := range keys {
				r.rows = append(r.rows, []driver.Value{k, "a", []byte{0xff, 0x00, byte(i)}})
			}
			return r
		},
	}
}

func TestBinaryKeysAreToldApart(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, binDB("k", []byte{0xff, 0xfe, 0x01}, []byte{0xff, 0xfe, 0x02}), "postgres")
	if _, err := db.ExecContext(ctx, "UPDATE t SET v = upper(v) WHERE v >= $1", "a"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 2 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	for i, want := range []string{`\xfffe01`, `\xfffe02`} {
		m := e.Mutations[i]
		if !reflect.DeepEqual(m.PK, map[string]any{"k": want}) {
			t.Errorf("mutation %d: pk %v, want k = %s", i, m.PK, want)
		}
		// before, after and pk carry the key in the same form; a binary
		// column that is not the key is still only counted
		for side, img := range map[string]map[string]any{"before": jsonMap(t, m.Before), "after": jsonMap(t, m.After)} {
			if img["k"] != want || img["blob"] != "<3 bytes>" {
				t.Errorf("mutation %d: %s image %v", i, side, img)
			}
		}
	}
}

// The key taken from a bind argument, when no image was read, has the form
// the key has in an image.
func TestBinaryKeyFromArgumentMatchesTheImage(t *testing.T) {
	key := []byte{0xff, 0xfe, 0x01}
	pkOf := func(opts ...tsql.Option) any {
		rec, ctx := setup(t)
		db := open(t, binDB("k", key), "postgres", opts...)
		if _, err := db.ExecContext(ctx, "DELETE FROM t WHERE k = $1", key); err != nil {
			panic(err)
		}
		e := lastWrite(t, rec)
		if len(e.Mutations) != 1 {
			panic(fmt.Sprintf("mutations %+v", e.Mutations))
		}
		return e.Mutations[0].PK["k"]
	}
	if img, arg := pkOf(), pkOf(tsql.WithImages(false), tsql.WithPKColumns("t", "k")); img != `\xfffe01` || arg != img {
		panic(fmt.Sprintf("pk from the image %v, from the argument %v", img, arg))
	}
}

// A long key is shown bounded, and two that differ only beyond the shown
// part are still different.
func TestLongKeysAreBoundedAndToldApart(t *testing.T) {
	long := func(fill, last byte, n int) []byte { return append(bytes.Repeat([]byte{fill}, n-1), last) }
	for name, keys := range map[string][2][]byte{
		"binary": {long(0xff, 1, 5000), long(0xff, 2, 5000)},
		"text":   {long('a', 'b', 5000), long('a', 'c', 5000)},
	} {
		rec, ctx := setup(t)
		db := open(t, binDB("k", keys[0], keys[1]), "postgres")
		for range 2 {
			if _, err := db.ExecContext(ctx, "UPDATE t SET v = upper(v) WHERE v >= $1", "a"); err != nil {
				panic(err)
			}
		}
		ws := byKind(rec.Events(), core.KindDBWrite)
		ms := ws[0].Mutations
		if len(ms) != 2 {
			panic(fmt.Sprintf("%s: mutations %+v flags %v", name, ms, ws[0].Flags))
		}
		a, b := ms[0].PK["k"].(string), ms[1].PK["k"].(string)
		if a == b || len(a) > 1100 || !strings.Contains(a, "...[5000 bytes sha256:") {
			t.Errorf("%s: keys %.80q... (%d bytes) and %.80q...", name, a, len(a), b)
		}
		if again := ws[1].Mutations[0].PK["k"]; again != a {
			t.Errorf("%s: the same key was recorded as %v and as %v", name, a, again)
		}
		if hasFlag(ws[0], "diff_truncated") {
			t.Errorf("%s: flags %v", name, ws[0].Flags)
		}
	}
}

// A key column with a sensitive name is masked whatever its form.
func TestBinaryKeyInSensitiveColumnIsMasked(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, binDB("api_key", []byte{0xff, 0xfe, 0x01}), "postgres")
	if _, err := db.ExecContext(ctx, "UPDATE t SET v = upper(v) WHERE v >= $1", "a"); err != nil {
		panic(err)
	}
	m := lastWrite(t, rec).Mutations[0]
	if m.PK["api_key"] != core.RedactedValue || jsonMap(t, m.Before)["api_key"] != core.RedactedValue || jsonMap(t, m.After)["api_key"] != core.RedactedValue {
		panic(fmt.Sprintf("pk %v before %s after %s", m.PK, m.Before.Inline, m.After.Inline))
	}
}
