package tsql_test

// Primary keys read from the catalog of a real Postgres (set
// TSESSION_TEST_PG_DSN; skipped otherwise).

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// rtPKTables creates the tables of these tests: none of them has an "id".
func rtPKTables(t *testing.T) {
	t.Helper()
	drop := []string{
		"DROP VIEW IF EXISTS tsql_pk_view", "DROP TABLE IF EXISTS tsql_pk_stock", "DROP TABLE IF EXISTS tsql_pk_lines",
		"DROP TABLE IF EXISTS tsql_pk_nokey", `DROP TABLE IF EXISTS "tsql PK Items"`, "DROP SCHEMA IF EXISTS tsql_pk_s CASCADE",
	}
	rtAdmin(t, drop...)
	rtAdmin(t,
		"CREATE TABLE tsql_pk_stock (sku text PRIMARY KEY, qty int)",
		"INSERT INTO tsql_pk_stock VALUES ('widget', 10), ('gadget', 5)",
		// key order (tenant, sku) differs from column order
		"CREATE TABLE tsql_pk_lines (sku text, qty int, tenant text, PRIMARY KEY (tenant, sku))",
		"INSERT INTO tsql_pk_lines VALUES ('widget', 10, 'acme'), ('widget', 20, 'globex')",
		"CREATE TABLE tsql_pk_nokey (sku text, qty int)",
		"INSERT INTO tsql_pk_nokey VALUES ('widget', 10)",
		"CREATE VIEW tsql_pk_view AS SELECT * FROM tsql_pk_nokey",
		`CREATE TABLE "tsql PK Items" ("SKU" text, "order" int, qty int, PRIMARY KEY ("SKU", "order"))`,
		`INSERT INTO "tsql PK Items" VALUES ('widget', 1, 10)`,
		"CREATE SCHEMA tsql_pk_s",
		"CREATE TABLE tsql_pk_s.tsql_pk_stock (code int PRIMARY KEY, qty int)",
		"INSERT INTO tsql_pk_s.tsql_pk_stock VALUES (7, 10)",
	)
	t.Cleanup(func() { rtAdmin(t, drop...) })
}

// rtLookups counts the catalog lookups among the statements the driver saw.
func rtLookups(cs []rtCall) int {
	n := 0
	for _, c := range cs {
		if isPKLookup(c.SQL) {
			n++
		}
	}
	return n
}

// rtWrite runs one traced write and returns its event.
func rtWrite(t *testing.T, rec *core.MemRecorder, ex rtExecer, ctx context.Context, q string, args ...any) core.Event {
	t.Helper()
	if _, err := ex.ExecContext(ctx, q, args...); err != nil {
		panic(fmt.Sprintf("%s: %v", q, err))
	}
	return lastWrite(t, rec)
}

// rtWantUpdate checks an UPDATE recorded with the given key and exactly one
// changed field, qty.
func rtWantUpdate(t *testing.T, e core.Event, pk map[string]any, oldQty, newQty int64) {
	t.Helper()
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	m := e.Mutations[0]
	if !reflect.DeepEqual(m.PK, pk) {
		panic(fmt.Sprintf("pk %v, want %v", m.PK, pk))
	}
	if m.Before.IsZero() || m.After.IsZero() || len(m.Diff) != 1 || m.Diff[0].Field != "qty" || m.Diff[0].Old != oldQty || m.Diff[0].New != newQty {
		panic(fmt.Sprintf("mutation %+v, want qty %d -> %d", m, oldQty, newQty))
	}
}

// The walkthrough defect, on the real server: a table keyed by sku.
func TestPGPKNonIDKey(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	d := &rtDriver{}
	db := rtOpen(t, d)
	const q = "UPDATE tsql_pk_stock SET qty = qty - 1 WHERE sku = $1"
	rtWantUpdate(t, rtWrite(t, rec, db, ctx, q, "widget"), map[string]any{"sku": "widget"}, 10, 9)
	first := d.take()
	rtWantUpdate(t, rtWrite(t, rec, db, ctx, q, "widget"), map[string]any{"sku": "widget"}, 9, 8)
	steady := d.take()
	t.Logf("non-id key: first statement on the handle %d round trips (%d lookup), steady state %d (%d lookups)",
		len(first), rtLookups(first), len(steady), rtLookups(steady))
	if rtLookups(first) != 1 || rtLookups(steady) != 0 || len(first) != 10 || len(steady) != 7 {
		panic(fmt.Sprintf("first:\n%s\nsteady:\n%s", strings.Join(rtSQLs(first), "\n"), strings.Join(rtSQLs(steady), "\n")))
	}
	// DELETE and INSERT get the key too.
	e := rtWrite(t, rec, db, ctx, "INSERT INTO tsql_pk_stock (sku, qty) VALUES ($1, $2)", "sprocket", 3)
	if m := e.Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"sku": "sprocket"}) || hasFlag(e, "after_from_args") {
		panic(fmt.Sprintf("insert %+v flags %v", m, e.Flags))
	}
	e = rtWrite(t, rec, db, ctx, "DELETE FROM tsql_pk_stock WHERE qty = $1", 3)
	if m := e.Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"sku": "sprocket"}) || len(e.Flags) != 0 {
		panic(fmt.Sprintf("delete %+v flags %v", m, e.Flags))
	}
	// A multi-row update pairs every row with its own after image.
	e = rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_stock SET qty = qty + 100 WHERE qty < $1", 100)
	if len(e.Mutations) != 2 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("multi-row: %+v flags %v", e.Mutations, e.Flags))
	}
	for _, m := range e.Mutations {
		if len(m.PK) != 1 || len(m.Diff) != 1 || m.Diff[0].New.(int64)-m.Diff[0].Old.(int64) != 100 {
			panic(fmt.Sprintf("multi-row mutation %+v", m))
		}
	}
	if n := rtLookups(d.take()); n != 0 {
		panic(fmt.Sprintf("%d more lookups for a table already known", n))
	}
}

func TestPGPKCompositeKey(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	// The predicate names only part of the key: the rest comes from the row.
	e := rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_lines SET qty = qty - 1 WHERE tenant = $1", "globex")
	rtWantUpdate(t, e, map[string]any{"tenant": "globex", "sku": "widget"}, 20, 19)
	e = rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_lines SET qty = qty - 1 WHERE sku = $1 AND tenant = $2", "widget", "acme")
	rtWantUpdate(t, e, map[string]any{"tenant": "acme", "sku": "widget"}, 10, 9)
}

// Names that need quoting: a table with spaces and capitals, a key column in
// capitals and one that is a reserved word.
func TestPGPKQuotedIdentifiers(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	db := rtOpen(t, &rtDriver{})
	e := rtWrite(t, rec, db, ctx, `UPDATE "tsql PK Items" SET qty = qty - 1 WHERE "SKU" = $1`, "widget")
	rtWantUpdate(t, e, map[string]any{"SKU": "widget", "order": int64(1)}, 10, 9)
	e = rtWrite(t, rec, db, ctx, `INSERT INTO "tsql PK Items" ("SKU", "order", qty) VALUES ($1, $2, $3)`, "widget", 2, 4)
	if m := e.Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"SKU": "widget", "order": int64(2)}) || len(e.Flags) != 0 {
		panic(fmt.Sprintf("insert %+v flags %v", m, e.Flags))
	}
}

// A schema-qualified name and an unqualified one that search_path sends to
// another schema each get the key of the table the server resolves them to.
func TestPGPKSchemaQualifiedAndSearchPath(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	d := &rtDriver{}
	db := rtOpen(t, d) // one connection, so SET search_path applies to what follows
	e := rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_s.tsql_pk_stock SET qty = qty - 1 WHERE qty = $1", 10)
	rtWantUpdate(t, e, map[string]any{"code": int64(7)}, 10, 9)
	e = rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_stock SET qty = qty - 1 WHERE qty = $1", 10)
	rtWantUpdate(t, e, map[string]any{"sku": "widget"}, 10, 9)

	if _, err := db.ExecContext(ctx, "SET search_path TO tsql_pk_s, public"); err != nil {
		panic(err)
	}
	defer db.ExecContext(ctx, "RESET search_path")
	d.take()
	// (Another text than above: pgx caches a prepared SELECT by its text, and
	// the server refuses a cached plan whose result columns changed.)
	e = rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_stock SET qty = qty - 1 WHERE qty IN ($1)", 9)
	rtWantUpdate(t, e, map[string]any{"code": int64(7)}, 9, 8)
	if n := rtLookups(d.take()); n != 1 {
		panic(fmt.Sprintf("%d lookups after search_path changed, want 1", n))
	}
}

// A table without a primary key and a view have no key to find: the record
// says the after image is unavailable and fabricates no diff. The catalog is
// asked once.
func TestPGPKNoKeyAndView(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	d := &rtDriver{}
	db := rtOpen(t, d)
	for _, table := range []string{"tsql_pk_nokey", "tsql_pk_view"} {
		d.take()
		for i := 0; i < 2; i++ {
			e := rtWrite(t, rec, db, ctx, "UPDATE "+table+" SET qty = qty - 1 WHERE sku = $1", "widget")
			if !hasFlag(e, "pk_unknown") || !hasFlag(e, "after_unavailable") || len(e.Mutations) != 1 {
				panic(fmt.Sprintf("%s: flags %v mutations %+v", table, e.Flags, e.Mutations))
			}
			if m := e.Mutations[0]; m.PK != nil || m.Before.IsZero() || !m.After.IsZero() || len(m.Diff) != 0 || !m.AfterUnavailable() {
				panic(fmt.Sprintf("%s: mutation %+v", table, m))
			}
		}
		if n := rtLookups(d.take()); n != 1 {
			panic(fmt.Sprintf("%s: %d lookups for two statements, want 1", table, n))
		}
	}
	// WithPKColumns is how such a table gets a key.
	db = rtOpen(t, d, tsql.WithPKColumns("tsql_pk_nokey", "sku"))
	d.take()
	e := rtWrite(t, rec, db, ctx, "UPDATE tsql_pk_nokey SET qty = qty - 1 WHERE sku = $1", "widget")
	if m := e.Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) || len(m.Diff) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("declared key: %+v flags %v", m, e.Flags))
	}
	if n := rtLookups(d.take()); n != 0 {
		panic(fmt.Sprintf("%d lookups for a table with a declared key", n))
	}
}

// The first traced write of a table inside an application transaction looks
// the key up there. The transaction's settings, its savepoints and its data
// are exactly what the application made them.
func TestPGPKLookupInsideTransactionLeavesItUntouched(t *testing.T) {
	rtPKTables(t)
	rec, ctx := setup(t)
	d := &rtDriver{}
	db := rtOpen(t, d)
	state := func(q rtQueryer) string {
		return strings.Join([]string{
			rtShow(t, q, "SHOW statement_timeout"), rtShow(t, q, "SHOW transaction_isolation"), rtShow(t, q, "SHOW transaction_read_only"),
			rtShow(t, q, "SHOW search_path"), rtShow(t, q, "SELECT string_agg(sku || '=' || qty, ',' ORDER BY sku) FROM tsql_pk_stock"),
		}, " | ")
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	rtMustExec(t, tx, "SET LOCAL statement_timeout = '7s'", "UPDATE tsql_pk_stock SET qty = 11 WHERE sku = 'gadget'", "SAVEPOINT app")
	before := state(tx)
	d.take()
	e := rtWrite(t, rec, tx, ctx, "UPDATE tsql_pk_stock SET qty = qty - 1 WHERE sku = $1", "widget")
	if n := rtLookups(d.take()); n != 1 {
		panic(fmt.Sprintf("%d lookups in the transaction, want 1", n))
	}
	rtWantUpdate(t, e, map[string]any{"sku": "widget"}, 10, 9)
	if got, want := state(tx), strings.Replace(before, "widget=10", "widget=9", 1); got != want {
		panic(fmt.Sprintf("transaction state after the lookup:\n%s\nwant:\n%s", got, want))
	}
	// The application's savepoint is still its own, and still there.
	rtMustExec(t, tx, "ROLLBACK TO SAVEPOINT app")
	if got := state(tx); got != before {
		panic(fmt.Sprintf("after ROLLBACK TO the application's savepoint:\n%s\nwant:\n%s", got, before))
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("commit: %v", err))
	}
	if got := rtShow(t, db, "SELECT string_agg(sku || '=' || qty, ',' ORDER BY sku) FROM tsql_pk_stock"); got != "gadget=11,widget=10" {
		panic(fmt.Sprintf("committed rows %q", got))
	}
	if got := rtShow(t, db, "SHOW statement_timeout"); got != "0" {
		panic(fmt.Sprintf("statement_timeout after the transaction: %q", got))
	}
}

// A user who may not read pg_index: the lookup fails inside the fence, the
// application's transaction goes on and commits, and after three refusals the
// catalog is left alone. Runs in its own database, because revoking a catalog
// privilege from PUBLIC is per database.
func TestPGPKLookupDeniedKeepsTransactionUsable(t *testing.T) {
	admin, err := url.Parse(rtDSN(t))
	if err != nil || admin.Scheme == "" {
		t.Skip("TSESSION_TEST_PG_DSN is not a URL")
	}
	const dbName, role = "tsql_pk_denied", "tsql_pk_limited"
	dropAll := func() {
		adm, err := sql.Open("pgx", admin.String())
		if err != nil {
			return
		}
		defer adm.Close()
		adm.Exec("DROP DATABASE IF EXISTS " + dbName)
		adm.Exec("DROP ROLE IF EXISTS " + role)
	}
	dropAll()
	adm, err := sql.Open("pgx", admin.String())
	if err != nil {
		panic(err)
	}
	defer adm.Close()
	if _, err := adm.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Skipf("cannot create a database: %v", err)
	}
	t.Cleanup(dropAll)
	if _, err := adm.Exec("CREATE ROLE " + role + " LOGIN"); err != nil {
		t.Skipf("cannot create a role: %v", err)
	}
	inDB := *admin
	inDB.Path = "/" + dbName
	setupDB, err := sql.Open("pgx", inDB.String())
	if err != nil {
		panic(err)
	}
	for _, s := range []string{
		"REVOKE SELECT ON pg_catalog.pg_index FROM PUBLIC",
		"CREATE TABLE stock (sku text PRIMARY KEY, qty int)",
		"INSERT INTO stock VALUES ('widget', 10)",
		"GRANT SELECT, UPDATE ON stock TO " + role,
	} {
		if _, err := setupDB.Exec(s); err != nil {
			setupDB.Close()
			panic(fmt.Sprintf("%s: %v", s, err))
		}
	}
	setupDB.Close()
	limited := inDB
	limited.User = url.User(role)

	rec, ctx := setup(t)
	d := &rtDriver{}
	name := uniqueName("pgx-rt")
	sql.Register(name, d)
	db, err := tsql.Open(name, limited.String(), tsql.WithStore("postgres"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 5; i++ {
		e := rtWrite(t, rec, tx, ctx, "UPDATE stock SET qty = qty - 1 WHERE sku = $1", "widget")
		// The key was not found, so this is the "after image unavailable" record.
		if hasFlag(e, "pk_lookup_failed") != (i < 3) || !hasFlag(e, "after_unavailable") || len(e.Mutations) != 1 || len(e.Mutations[0].Diff) != 0 {
			panic(fmt.Sprintf("statement %d: flags %v mutations %+v", i, e.Flags, e.Mutations))
		}
	}
	if got := rtShow(t, tx, "SELECT qty::text FROM stock"); got != "5" {
		panic(fmt.Sprintf("qty inside the transaction %q, want 5", got))
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("commit after refused lookups: %v", err))
	}
	if n := rtLookups(d.take()); n != 3 {
		panic(fmt.Sprintf("%d lookups, want 3 and then none", n))
	}
	if got := rtShow(t, db, "SELECT qty::text FROM stock"); got != "5" {
		panic(fmt.Sprintf("committed qty %q, want 5", got))
	}
}
