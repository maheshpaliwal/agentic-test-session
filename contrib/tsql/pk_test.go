package tsql_test

// Primary keys read from the catalog, against the fake driver. The same
// behaviour is checked on a real Postgres in pk_pg_test.go.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// stockDB is a fake "stock" table keyed by sku (no id column) holding one
// row; an UPDATE takes one off qty. keys is what the catalog says per table.
func stockDB(keys map[string][]string) *fakeDB {
	qty := int64(10)
	return &fakeDB{
		pk: func(table string) result { return pkRows(keys[table]...) },
		handler: func(q string, _ []driver.Value) result {
			switch {
			case strings.HasPrefix(q, "SELECT"):
				return result{cols: []string{"sku", "qty"}, rows: [][]driver.Value{{"widget", qty}}}
			case strings.HasPrefix(q, "UPDATE"):
				qty--
			}
			return result{affected: 1}
		},
	}
}

const stockUpdate = "UPDATE stock SET qty = qty - 1 WHERE sku = $1"

func lastWrite(t *testing.T, rec *core.MemRecorder) core.Event {
	t.Helper()
	ws := byKind(rec.Events(), core.KindDBWrite)
	if len(ws) == 0 {
		panic("no write event recorded")
	}
	return ws[len(ws)-1]
}

// The walkthrough defect: a table keyed by something other than "id" is
// recorded with its real key and a diff of what changed.
func TestPKLookupNonIDKey(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if got := f.PKLookups(); len(got) != 1 || !reflect.DeepEqual(got[0].Args, []driver.Value{"stock"}) {
		panic(fmt.Sprintf("lookups %+v", got))
	}
	calls := f.Calls()
	if len(calls) != 3 || calls[2].SQL != `SELECT * FROM stock WHERE "sku" IN ($1)` || !reflect.DeepEqual(calls[2].Args, []driver.Value{"widget"}) {
		panic(fmt.Sprintf("calls:\n%s", sqls(calls)))
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	m := e.Mutations[0]
	if !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) || m.After.IsZero() || m.Before.IsZero() {
		panic(fmt.Sprintf("mutation %+v", m))
	}
	if len(m.Diff) != 1 || m.Diff[0].Field != "qty" || m.Diff[0].Old != int64(10) || m.Diff[0].New != int64(9) {
		panic(fmt.Sprintf("diff %+v", m.Diff))
	}
}

// One lookup per table per handle, whatever the connection, a table without
// a key included.
func TestPKLookupOncePerTablePerHandle(t *testing.T) {
	_, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "postgres")
	c1, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	defer c2.Close()
	for i := 0; i < 3; i++ {
		for _, c := range []*sql.Conn{c1, c2} {
			if _, err := c.ExecContext(ctx, stockUpdate, "widget"); err != nil {
				panic(err)
			}
			if _, err := c.ExecContext(ctx, "DELETE FROM nokey WHERE sku = $1", "widget"); err != nil {
				panic(err)
			}
		}
	}
	var tables []string
	for _, c := range f.PKLookups() {
		tables = append(tables, c.Args[0].(string))
	}
	if !reflect.DeepEqual(tables, []string{"stock", "nokey"}) {
		panic(fmt.Sprintf("lookups for %v, want one for stock and one for nokey", tables))
	}
	// Another handle on the same database does not share them.
	db2 := open(t, f, "postgres")
	if _, err := db2.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if n := len(f.PKLookups()); n != 3 {
		panic(fmt.Sprintf("%d lookups after a second handle wrote, want 3", n))
	}
}

// The lookup is fenced like an image read: its own read-only transaction in
// autocommit, a savepoint inside the application's transaction.
func TestPKLookupRunsInsideTheFence(t *testing.T) {
	_, ctx := setup(t)
	raw := func(f *fakeDB) string {
		var b strings.Builder
		for _, c := range f.RawCalls() {
			if isPKLookup(c.SQL) {
				b.WriteString("<pk lookup>\n")
				break
			}
			b.WriteString(c.SQL + "\n")
		}
		rest := f.RawCalls()
		for i, c := range rest {
			if isPKLookup(c.SQL) && i+1 < len(rest) {
				b.WriteString(rest[i+1].SQL + "\n")
			}
		}
		return b.String()
	}
	f := stockDB(map[string][]string{"stock": {"sku"}})
	if _, err := open(t, f, "postgres").ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if got, want := raw(f), "BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500\n<pk lookup>\nROLLBACK\n"; got != want {
		panic(fmt.Sprintf("autocommit:\n%s\nwant:\n%s", got, want))
	}
	f = stockDB(map[string][]string{"stock": {"sku"}})
	tx, err := open(t, f, "postgres").BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	if _, err := tx.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	want := "SAVEPOINT ts_capture\n" + isoAsk + "\n<pk lookup>\nROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture\n"
	if got := raw(f); got != want {
		panic(fmt.Sprintf("transaction:\n%s\nwant:\n%s", got, want))
	}
}

// A lookup the server refuses (no permission on the catalog) leaves the
// application's transaction usable, is retried only maxPKLookups times, and
// the write is still recorded.
func TestPKLookupFailureIsIsolatedAndBounded(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(nil)
	f.abortModel = true
	f.pk = func(string) result {
		return result{err: errors.New("permission denied for table pg_index (SQLSTATE 42501)")}
	}
	db := open(t, f, "postgres")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := tx.ExecContext(ctx, stockUpdate, "widget"); err != nil {
			panic(fmt.Sprintf("statement %d failed after a refused lookup: %v", i, err))
		}
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("commit after refused lookups: %v", err))
	}
	if n := len(f.PKLookups()); n != 3 {
		panic(fmt.Sprintf("%d lookups, want 3 and then none", n))
	}
	ws := byKind(rec.Events(), core.KindDBWrite)
	if len(ws) != 5 {
		panic(fmt.Sprintf("%d write events", len(ws)))
	}
	for i, e := range ws {
		if hasFlag(e, "pk_lookup_failed") != (i < 3) || e.Err != "" || len(e.Mutations) != 1 {
			panic(fmt.Sprintf("event %d: flags %v err %q mutations %d", i, e.Flags, e.Err, len(e.Mutations)))
		}
	}
}

// An answer that is not a list of column names counts as a failed lookup.
func TestPKLookupOddAnswerIsIgnored(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(nil)
	f.pk = func(string) result {
		return result{cols: []string{"a", "b"}, rows: [][]driver.Value{{"sku", "x"}}}
	}
	if _, err := open(t, f, "postgres").ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if e := lastWrite(t, rec); !hasFlag(e, "pk_lookup_failed") || !hasFlag(e, "pk_unknown") {
		panic(fmt.Sprintf("flags %v", e.Flags))
	}
}

// WithPKColumns wins and the catalog is not asked; neither is it with the
// lookup or the images switched off.
func TestPKLookupExplicitKeyWins(t *testing.T) {
	for name, opts := range map[string][]tsql.Option{
		"WithPKColumns":           {tsql.WithPKColumns("stock", "qty")},
		"WithPKColumns qualified": {tsql.WithPKColumns("public.stock", "qty")},
		"WithPKLookup(false)":     {tsql.WithPKLookup(false), tsql.WithDefaultPK("qty")},
		"WithImages(false)":       {tsql.WithImages(false)},
	} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t)
			f := stockDB(map[string][]string{"stock": {"sku"}, "public.stock": {"sku"}})
			db := open(t, f, "postgres", opts...)
			if _, err := db.ExecContext(ctx, "UPDATE public.stock SET qty = qty - 1 WHERE sku = $1", "widget"); err != nil {
				panic(err)
			}
			if n := len(f.PKLookups()); n != 0 {
				panic(fmt.Sprintf("%d lookups, want none", n))
			}
			if name == "WithImages(false)" {
				return
			}
			if m := lastWrite(t, rec).Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"qty": int64(10)}) {
				panic(fmt.Sprintf("pk %v, want the declared key qty", m.PK))
			}
		})
	}
}

// Nothing is looked up for a statement outside a session, and a read never
// asks: it uses a key an earlier write found.
func TestPKLookupOnlyForTracedWrites(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(context.Background(), stockUpdate, "widget"); err != nil {
		panic(err)
	}
	read := func() {
		rows, err := db.QueryContext(ctx, "SELECT * FROM stock WHERE qty > $1", 0)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
		}
		rows.Close()
	}
	read()
	if got := f.RawCalls(); len(got) != 2 {
		panic(fmt.Sprintf("untraced write and a read sent:\n%s", sqls(got)))
	}
	if _, err := db.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	read()
	if n := len(f.PKLookups()); n != 1 {
		panic(fmt.Sprintf("%d lookups, want 1", n))
	}
	rs := byKind(rec.Events(), core.KindDBRead)
	if len(rs) != 2 || len(rs[0].Reads[0].PKs) != 0 || !reflect.DeepEqual(rs[1].Reads[0].PKs, []string{"widget"}) {
		panic(fmt.Sprintf("read keys: %+v", rs))
	}
}

// A traced DDL statement, or one that sets search_path, drops the cached keys.
func TestPKLookupCacheDroppedOnSchemaChange(t *testing.T) {
	for _, stmt := range []string{"ALTER TABLE stock ADD COLUMN note text", "SET search_path TO other, public"} {
		_, ctx := setup(t)
		f := stockDB(map[string][]string{"stock": {"sku"}})
		db := open(t, f, "postgres")
		for _, q := range []string{stockUpdate, stockUpdate, stmt, stockUpdate, stockUpdate} {
			args := []any{"widget"}
			if q == stmt {
				args = nil
			}
			if _, err := db.ExecContext(ctx, q, args...); err != nil {
				panic(err)
			}
		}
		if n := len(f.PKLookups()); n != 2 {
			panic(fmt.Sprintf("%d lookups around %q, want 2", n, stmt))
		}
	}
}

// Inside a SERIALIZABLE transaction nothing is read, and nothing is
// remembered: the next statement outside it looks the key up.
func TestPKLookupSkippedUnderSerializable(t *testing.T) {
	_, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "postgres")
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		panic(err)
	}
	if _, err := tx.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	if got := f.RawCalls(); len(got) != 1 {
		panic(fmt.Sprintf("serializable transaction sent:\n%s", sqls(got)))
	}
	if _, err := db.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if n := len(f.PKLookups()); n != 1 {
		panic(fmt.Sprintf("%d lookups, want 1", n))
	}
}

// In a transaction that is already aborted the fence cannot open. That says
// nothing about the table, so it does not use up the attempts, and the
// statement makes one refused attempt, not one per capture.
func TestPKLookupInAbortedTransaction(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	f.abortModel = true
	inner := f.handler
	f.handler = func(q string, a []driver.Value) result {
		if q == "SELECT 1/0" {
			return result{err: errors.New("division by zero")}
		}
		return inner(q, a)
	}
	db := open(t, f, "postgres")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	if _, err := tx.ExecContext(context.Background(), "SELECT 1/0"); err == nil {
		panic("expected an error")
	}
	before := len(f.RawCalls())
	for i := 0; i < 4; i++ {
		if _, err := tx.ExecContext(ctx, stockUpdate, "widget"); err == nil {
			panic("statement in an aborted transaction succeeded")
		}
	}
	if got := f.RawCalls()[before:]; len(got) != 8 || got[0].SQL != "SAVEPOINT ts_capture" || got[1].SQL != stockUpdate {
		panic(fmt.Sprintf("aborted transaction saw:\n%s", sqls(got)))
	}
	if e := lastWrite(t, rec); !hasFlag(e, "image_failed") || hasFlag(e, "pk_lookup_failed") {
		panic(fmt.Sprintf("flags %v", e.Flags))
	}
	tx.Rollback()
	f.mu.Lock()
	f.aborted = false
	f.mu.Unlock()
	if _, err := db.ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	if e := lastWrite(t, rec); len(f.PKLookups()) != 1 || len(e.Flags) != 0 || e.Mutations[0].PK["sku"] != "widget" {
		panic(fmt.Sprintf("after the transaction: %d lookups, flags %v, mutations %+v", len(f.PKLookups()), e.Flags, e.Mutations))
	}
}

// A composite key, in key order, with names spelled as the catalog has them.
func TestPKLookupCompositeAndQuotedKey(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{
		pk: func(string) result { return pkRows("Tenant", "order") },
		handler: func(q string, _ []driver.Value) result {
			if strings.HasPrefix(q, "SELECT") {
				return result{cols: []string{"Tenant", "order", "v"}, rows: [][]driver.Value{{"acme", int64(7), "x"}}}
			}
			return result{affected: 1}
		},
	}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, `UPDATE "Lines" SET v = $1 WHERE "Tenant" = $2 AND "order" = $3`, "y", "acme", 7); err != nil {
		panic(err)
	}
	if got := f.PKLookups(); len(got) != 1 || got[0].Args[0] != `"Lines"` {
		panic(fmt.Sprintf("lookup %+v", got))
	}
	calls := f.Calls()
	if want := `SELECT * FROM "Lines" WHERE ("Tenant" = $1 AND "order" = $2)`; calls[len(calls)-1].SQL != want {
		panic(fmt.Sprintf("re-read %q, want %q", calls[len(calls)-1].SQL, want))
	}
	if m := lastWrite(t, rec).Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"Tenant": "acme", "order": int64(7)}) {
		panic(fmt.Sprintf("pk %v", m.PK))
	}
	// An INSERT takes the key from its VALUES, whatever the case it is written in.
	if _, err := db.ExecContext(ctx, `INSERT INTO "Lines" ("Tenant", "order", v) VALUES ($1, $2, $3)`, "acme", 8, "z"); err != nil {
		panic(err)
	}
	calls = f.Calls()
	if got := calls[len(calls)-1]; !strings.HasPrefix(got.SQL, `SELECT * FROM "Lines" WHERE ("Tenant" = $1`) || got.Args[1] != int64(8) {
		panic(fmt.Sprintf("insert re-read %+v", got))
	}
}

// MySQL asks information_schema, in the current database or the named one.
func TestPKLookupMySQL(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "mysql")
	if _, err := db.ExecContext(ctx, "UPDATE stock SET qty = qty - 1 WHERE sku = ?", "widget"); err != nil {
		panic(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE shop.stock SET qty = qty - 1 WHERE sku = ?", "widget"); err != nil {
		panic(err)
	}
	got := f.PKLookups()
	if len(got) != 2 || !strings.Contains(got[0].SQL, "TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?") || !reflect.DeepEqual(got[0].Args, []driver.Value{"stock"}) ||
		!strings.Contains(got[1].SQL, "TABLE_SCHEMA = ? AND TABLE_NAME = ?") || !reflect.DeepEqual(got[1].Args, []driver.Value{"shop", "stock"}) {
		panic(fmt.Sprintf("lookups %+v", got))
	}
	if raw := f.RawCalls(); len(raw) != 8 { // per table: lookup, before, update, after; MySQL needs no fence
		panic(fmt.Sprintf("statements:\n%s", sqls(raw)))
	}
	calls := f.Calls()
	if !strings.HasSuffix(calls[2].SQL, "FROM stock WHERE `sku` IN (?)") {
		panic(fmt.Sprintf("re-read %q", calls[2].SQL))
	}
	if m := lastWrite(t, rec).Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) || len(m.Diff) != 1 {
		panic(fmt.Sprintf("mutation %+v", m))
	}
}

// A database that is neither Postgres nor MySQL is not asked.
func TestPKLookupOnlyForKnownDatabases(t *testing.T) {
	_, ctx := setup(t)
	f := stockDB(map[string][]string{"stock": {"sku"}})
	db := open(t, f, "sqlite")
	if _, err := db.ExecContext(ctx, "UPDATE stock SET qty = qty - 1 WHERE sku = ?", "widget"); err != nil {
		panic(err)
	}
	if n := len(f.PKLookups()); n != 0 {
		panic(fmt.Sprintf("%d lookups on an unknown database", n))
	}
}

// Where no key is known the row cannot be read back. The record then says
// that the after image is unavailable; it does not list every column as
// changed to nothing.
func TestUpdateWithoutKeyRecordsNoDiff(t *testing.T) {
	rec, ctx := setup(t)
	f := stockDB(nil) // the catalog knows no key and there is no "id" column
	if _, err := open(t, f, "postgres").ExecContext(ctx, stockUpdate, "widget"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if !hasFlag(e, "pk_unknown") || !hasFlag(e, "after_unavailable") || len(e.Mutations) != 1 {
		panic(fmt.Sprintf("flags %v mutations %+v", e.Flags, e.Mutations))
	}
	m := e.Mutations[0]
	if m.Before.IsZero() || !m.After.IsZero() || len(m.Diff) != 0 || !m.AfterUnavailable() {
		panic(fmt.Sprintf("mutation %+v", m))
	}
	// A DELETE has no after image by nature: its diff stays.
	if _, err := open(t, f, "postgres").ExecContext(ctx, "DELETE FROM stock WHERE sku = $1", "widget"); err != nil {
		panic(err)
	}
	if m := lastWrite(t, rec).Mutations[0]; len(m.Diff) != 2 || m.AfterUnavailable() {
		panic(fmt.Sprintf("delete %+v", m))
	}
}
