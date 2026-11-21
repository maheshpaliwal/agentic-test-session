package tsql_test

// Binary and uuid keys against a real Postgres through pgx (set
// TSESSION_TEST_PG_DSN; skipped otherwise).

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Rows of a table keyed by bytea are recorded under their own key, in the
// same form in pk, before and after, and whether the key came from a row
// image or, with images off, from the bind argument.
func TestPGBinaryKeysAreToldApart(t *testing.T) {
	rtAdmin(t, "DROP TABLE IF EXISTS tsql_bin", "CREATE TABLE tsql_bin (id bytea PRIMARY KEY, v text, blob bytea)",
		`INSERT INTO tsql_bin VALUES ('\xfffe0001','a','\xff0001'),('\xfffe0002','b','\xff0002'),('abc','c','\xff0003')`)
	t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_bin") })
	k1, k3 := []byte{0xff, 0xfe, 0x00, 0x01}, []byte{0xff, 0xfe, 0x00, 0x03}

	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []struct {
		q    string
		args []any
	}{
		{"UPDATE tsql_bin SET v = upper(v) WHERE v < 'c'", nil},
		{"UPDATE tsql_bin SET v = 'x' WHERE id = $1", []any{[]byte("abc")}},
		{"INSERT INTO tsql_bin (id, v) VALUES ($1, $2)", []any{k3, "n"}},
		{"DELETE FROM tsql_bin WHERE id = $1", []any{k1}},
	} {
		if _, err := db.ExecContext(ctx, s.q, s.args...); err != nil {
			t.Fatalf("%s: %v", s.q, err)
		}
	}
	ws := byKind(rec.Events(), core.KindDBWrite)
	if len(ws) != 4 {
		t.Fatalf("%d write events", len(ws))
	}
	var got [][]any
	for _, e := range ws {
		if len(e.Flags) != 0 {
			t.Errorf("%s: flags %v", e.Name, e.Flags)
		}
		var keys []any
		for _, m := range e.Mutations {
			keys = append(keys, m.PK["id"])
			for side, p := range map[string]core.PayloadRef{"before": m.Before, "after": m.After} {
				if p.IsZero() {
					continue
				}
				if img := jsonMap(t, p); img["id"] != m.PK["id"] || (img["blob"] != nil && img["blob"] != "<3 bytes>") {
					t.Errorf("%s: pk %v, %s image %v", e.Name, m.PK, side, img)
				}
			}
		}
		got = append(got, keys)
	}
	// bytes that are text are shown as the text they are
	want := [][]any{{`\xfffe0001`, `\xfffe0002`}, {"abc"}, {`\xfffe0003`}, {`\xfffe0001`}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys recorded %v, want %v", got, want)
	}

	// Without images the key is the bind argument's: the same form.
	rec, ctx = setup(t)
	plain, err := tsql.Open("pgx", rtDSN(t), tsql.WithImages(false), tsql.WithPKColumns("tsql_bin", "id"))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.ExecContext(ctx, "UPDATE tsql_bin SET v = 'y' WHERE id = $1", k3); err != nil {
		t.Fatal(err)
	}
	if m := lastWrite(t, rec).Mutations; len(m) != 1 || m[0].PK["id"] != `\xfffe0003` {
		t.Fatalf("key from the argument: %+v", m)
	}
}

// A uuid key reads the same whether it reached tsql as the text the server
// returns, as 16 bytes or as a pgtype value in a bind argument.
func TestPGUUIDKeyHasOneForm(t *testing.T) {
	rtAdmin(t, "DROP TABLE IF EXISTS tsql_uuid", "CREATE TABLE tsql_uuid (id uuid PRIMARY KEY, v text)")
	t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_uuid") })
	const text = "01234567-89ab-cdef-0123-456789abcdef"
	raw := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	for name, opts := range map[string][]tsql.Option{
		"key_from_images":    nil,
		"key_from_arguments": {tsql.WithImages(false), tsql.WithPKColumns("tsql_uuid", "id")},
	} {
		t.Run(name, func(t *testing.T) {
			rtAdmin(t, "TRUNCATE tsql_uuid")
			rec, ctx := setup(t)
			db, err := tsql.Open("pgx", rtDSN(t), opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, s := range []struct {
				q    string
				args []any
			}{
				{"INSERT INTO tsql_uuid (id, v) VALUES ($1, $2)", []any{raw, "a"}},
				{"UPDATE tsql_uuid SET v = 'b' WHERE id = $1", []any{pgtype.UUID{Bytes: raw, Valid: true}}},
				{"UPDATE tsql_uuid SET v = 'c' WHERE id = $1", []any{raw}},
				{"UPDATE tsql_uuid SET v = 'd' WHERE id = $1", []any{text}},
				{"DELETE FROM tsql_uuid WHERE id = $1", []any{pgtype.UUID{Bytes: raw, Valid: true}}},
			} {
				if _, err := db.ExecContext(ctx, s.q, s.args...); err != nil {
					t.Fatalf("%s: %v", s.q, err)
				}
				e := lastWrite(t, rec)
				if len(e.Mutations) != 1 || e.Mutations[0].PK["id"] != text || hasFlag(e, "after_failed") || hasFlag(e, "image_failed") {
					t.Fatalf("%s (%T): mutations %+v flags %v", s.q, s.args[0], e.Mutations, e.Flags)
				}
			}
		})
	}
}
