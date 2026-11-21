package tsql_test

// What the handle remembers about primary keys can go out of date. These
// tests, against a real Postgres (TSESSION_TEST_PG_DSN; skipped otherwise),
// hold tsql to one rule: a write is recorded with its true key and change, or
// it is flagged; it is never recorded with another row's values.

import (
	"reflect"
	"slices"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func rtStaleTables(t *testing.T, create ...string) {
	t.Helper()
	drop := []string{"DROP SCHEMA IF EXISTS tsql_st_a CASCADE", "DROP SCHEMA IF EXISTS tsql_st_b CASCADE", "DROP SCHEMA IF EXISTS tsql_st CASCADE"}
	rtAdmin(t, drop...)
	rtAdmin(t, append([]string{"CREATE SCHEMA tsql_st", "CREATE SCHEMA tsql_st_a", "CREATE SCHEMA tsql_st_b"}, create...)...)
	t.Cleanup(func() { rtAdmin(t, drop...) })
}

// The same unqualified name is a different table, with a different key, in
// each of the two schemas.
var twoSchemas = []string{
	"CREATE TABLE tsql_st_a.t (a int PRIMARY KEY, b int, v text)", "INSERT INTO tsql_st_a.t VALUES (1, 1, 'a-row')",
	"CREATE TABLE tsql_st_b.t (a int, b int PRIMARY KEY, v text)", "INSERT INTO tsql_st_b.t VALUES (1, 1, 'x'), (1, 2, 'y')",
}

// rtWantChange checks that e is one mutation with the given key whose only
// changed field is field, and no flags.
func rtWantChange(t *testing.T, e core.Event, pk map[string]any, field string) {
	t.Helper()
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
	m := e.Mutations[0]
	var changed []string
	for _, d := range m.Diff {
		changed = append(changed, d.Field)
	}
	if !reflect.DeepEqual(m.PK, pk) || !slices.Equal(changed, []string{field}) {
		t.Fatalf("recorded key %v and changed fields %v, want %v and [%s]", m.PK, changed, pk, field)
	}
}

func TestPGPKSearchPathRevertsWithItsTransaction(t *testing.T) {
	rtStaleTables(t, twoSchemas...)
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	db.SetMaxOpenConns(1)
	rtMustExec(t, db, "SET search_path TO tsql_st_b")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path TO tsql_st_a"); err != nil {
		t.Fatal(err)
	}
	rtWantChange(t, rtWrite(t, rec, tx, ctx, "UPDATE t SET v = $1 WHERE a = $2", "a2", 1), map[string]any{"a": int64(1)}, "v")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rtWantChange(t, rtWrite(t, rec, db, ctx, "UPDATE t SET v = $1 WHERE b = $2", "z", 1), map[string]any{"b": int64(1)}, "v")
}

func TestPGPKSearchPathSetOtherWays(t *testing.T) {
	for _, how := range []string{"SELECT set_config('search_path', 'tsql_st_b', false)", "SET SCHEMA 'tsql_st_b'", "SET search_path TO tsql_st_b"} {
		t.Run(how, func(t *testing.T) {
			rtStaleTables(t, twoSchemas...)
			rec, ctx := setup(t)
			db := rtOpen(t, &rtDriver{})
			db.SetMaxOpenConns(1)
			rtMustExec(t, db, "SET search_path TO tsql_st_a")
			rtWantChange(t, rtWrite(t, rec, db, ctx, "UPDATE t SET v = $1 WHERE a = $2", "a2", 1), map[string]any{"a": int64(1)}, "v")
			if how[0] == 'S' && how[1] == 'E' && how[2] == 'L' {
				var s string
				if err := db.QueryRowContext(ctx, how).Scan(&s); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, how); err != nil {
				t.Fatal(err)
			}
			rtWantChange(t, rtWrite(t, rec, db, ctx, "UPDATE t SET v = $1 WHERE b = $2", "z", 1), map[string]any{"b": int64(1)}, "v")
		})
	}
}

func TestPGPKChangedInARolledBackTransaction(t *testing.T) {
	rtStaleTables(t, "CREATE TABLE tsql_st.c (a int PRIMARY KEY, b int NOT NULL, v text)", "INSERT INTO tsql_st.c VALUES (1, 5, 'x'), (2, 5, 'y')")
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"DELETE FROM tsql_st.c WHERE a = 2", "ALTER TABLE tsql_st.c DROP CONSTRAINT c_pkey", "ALTER TABLE tsql_st.c ADD PRIMARY KEY (b)"} {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	rtWantChange(t, rtWrite(t, rec, tx, ctx, "UPDATE tsql_st.c SET v = $1 WHERE a = $2", "in-tx", 1), map[string]any{"b": int64(5)}, "v")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	rtWantChange(t, rtWrite(t, rec, db, ctx, "UPDATE tsql_st.c SET v = $1 WHERE a = $2", "z", 1), map[string]any{"a": int64(1)}, "v")
}

// No primary key, and the default key column "id" does not identify a row:
// the after image of another row must not be recorded as this one's.
func TestPGPKDefaultKeyThatIsNotUnique(t *testing.T) {
	rtStaleTables(t, "CREATE TABLE tsql_st.n (id int, name text)", "INSERT INTO tsql_st.n VALUES (1, 'a'), (1, 'b')")
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	e := rtWrite(t, rec, db, ctx, "UPDATE tsql_st.n SET name = $1 WHERE name = $2", "z", "b")
	if !hasFlag(e, "pk_not_unique") || len(e.Mutations) != 1 || len(e.Mutations[0].Diff) != 0 || !e.Mutations[0].After.IsZero() {
		t.Fatalf("flags %v mutations %+v", e.Flags, e.Mutations)
	}
}

// A statement that fails because its table does not exist yet must not fix
// "no key" for the table that is created afterwards.
func TestPGPKTableCreatedAfterAFailedStatement(t *testing.T) {
	rtStaleTables(t)
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	if _, err := db.ExecContext(ctx, "INSERT INTO tsql_st.late (sku, qty) VALUES ($1, $2)", "a", 1); err == nil {
		t.Fatal("expected an error: the table does not exist")
	}
	rtAdmin(t, "CREATE TABLE tsql_st.late (sku text PRIMARY KEY, qty int)", "INSERT INTO tsql_st.late VALUES ('w', 1)")
	rtWantChange(t, rtWrite(t, rec, db, ctx, "UPDATE tsql_st.late SET qty = qty + 1 WHERE qty = $1", 1), map[string]any{"sku": "w"}, "qty")
}
