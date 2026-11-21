package tsql_test

// Upserts against a real Postgres (set TSESSION_TEST_PG_DSN; skipped
// otherwise): what is recorded for a row that was inserted, one that was
// updated and one whose outcome is not known, and that the application's
// transaction is what it is untraced.

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// upPGReset (re)creates the tables of the upsert tests: tsql_up keyed by sku,
// holding "widget" and "bolt"; tsql_up_users with a generated id and a unique
// email, holding one user.
func upPGReset(t *testing.T) {
	t.Helper()
	rtAdmin(t,
		"DROP VIEW IF EXISTS tsql_up_view", "DROP TABLE IF EXISTS tsql_up", "DROP TABLE IF EXISTS tsql_up_users", "DROP TABLE IF EXISTS tsql_up_part",
		"CREATE TABLE tsql_up (sku text PRIMARY KEY, qty int, name text)",
		"INSERT INTO tsql_up VALUES ('widget', 10, 'Widget'), ('bolt', 3, 'Bolt')",
		"CREATE TABLE tsql_up_users (id serial PRIMARY KEY, email text UNIQUE, name text)",
		"INSERT INTO tsql_up_users (email, name) VALUES ('a@example.com', 'Ann')",
		"CREATE TABLE tsql_up_part (sku text PRIMARY KEY, qty int) PARTITION BY HASH (sku)",
		"CREATE TABLE tsql_up_part0 PARTITION OF tsql_up_part FOR VALUES WITH (MODULUS 1, REMAINDER 0)",
		"INSERT INTO tsql_up_part VALUES ('widget', 10)",
		"CREATE VIEW tsql_up_view AS SELECT * FROM tsql_up")
	t.Cleanup(func() {
		rtAdmin(t, "DROP VIEW IF EXISTS tsql_up_view", "DROP TABLE IF EXISTS tsql_up", "DROP TABLE IF EXISTS tsql_up_users", "DROP TABLE IF EXISTS tsql_up_part")
	})
}

const upPGStmt = "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty"

// upPGOne runs one upsert through tsql and returns its write event.
func upPGOne(t *testing.T, opts []tsql.Option, stmt string, args ...any) core.Event {
	t.Helper()
	upPGReset(t)
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", rtDSN(t), opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
		t.Fatal(err)
	}
	return lastWrite(t, rec)
}

// upPGClean asserts one mutation and no flag.
func upPGClean(t *testing.T, e core.Event) core.Mutation {
	t.Helper()
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
	return e.Mutations[0]
}

func TestPGUpsert(t *testing.T) {
	rtDSN(t)
	t.Run("insert", func(t *testing.T) {
		m := upPGClean(t, upPGOne(t, nil, upPGStmt, "gadget", 5, "Gadget"))
		upCheck(t, m, "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
		if !reflect.DeepEqual(m.PK, map[string]any{"sku": "gadget"}) || jsonMap(t, m.After)["qty"] != float64(5) {
			t.Errorf("mutation %+v", m)
		}
	})
	t.Run("update", func(t *testing.T) {
		m := upPGClean(t, upPGOne(t, nil, upPGStmt, "widget", 7, "ignored"))
		upCheck(t, m, "update", true, "qty:10->7")
		if b, a := jsonMap(t, m.Before), jsonMap(t, m.After); b["qty"] != float64(10) || a["qty"] != float64(7) || a["name"] != "Widget" {
			t.Errorf("before %v after %v", b, a)
		}
		if !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) {
			t.Errorf("pk %v", m.PK)
		}
	})
	t.Run("do_nothing", func(t *testing.T) {
		const stmt = "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO NOTHING"
		if e := upPGOne(t, nil, stmt, "widget", 7, "x"); len(e.Mutations) != 0 || len(e.Flags) != 0 {
			t.Errorf("a row that was left alone: mutations %+v flags %v", e.Mutations, e.Flags)
		}
		m := upPGClean(t, upPGOne(t, nil, stmt, "gadget", 5, "Gadget"))
		upCheck(t, m, "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	})
	t.Run("multi_row_mix", func(t *testing.T) {
		e := upPGOne(t, nil, "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty",
			"widget", 7, "x", "gadget", 5, "Gadget", "bolt", 4, "y")
		if core.HasUncertainFlag(e.Flags) || len(e.Mutations) != 3 {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		got := map[any]core.Mutation{}
		for _, m := range e.Mutations {
			got[m.PK["sku"]] = m
		}
		upCheck(t, got["widget"], "update", true, "qty:10->7")
		upCheck(t, got["bolt"], "update", true, "qty:3->4")
		upCheck(t, got["gadget"], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	})
	// Some rows of the statement are left as they are by the WHERE of DO
	// UPDATE (and locked all the same): they are no mutation.
	t.Run("multi_row_some_untouched", func(t *testing.T) {
		e := upPGOne(t, nil, "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty WHERE tsql_up.qty > 5",
			"widget", 7, "x", "bolt", 4, "y")
		if core.HasUncertainFlag(e.Flags) || len(e.Mutations) != 1 {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], "update", true, "qty:10->7")
	})
	t.Run("conflict_target_not_primary_key", func(t *testing.T) {
		const stmt = "INSERT INTO tsql_up_users (email, name) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"
		m := upPGClean(t, upPGOne(t, nil, stmt, "a@example.com", "Anna"))
		upCheck(t, m, "update", true, "name:Ann->Anna")
		if !reflect.DeepEqual(m.PK, map[string]any{"id": int64(1)}) {
			t.Errorf("pk %v, want the id of the row that was there", m.PK)
		}
		m = upPGClean(t, upPGOne(t, nil, stmt, "b@example.com", "Bob"))
		if m.Op != "upsert" || !m.Before.IsZero() || jsonMap(t, m.After)["name"] != "Bob" || m.PK["id"] == nil {
			t.Errorf("mutation %+v", m)
		}
	})
	// The values name key 'gadget', the constraint finds 'widget' by another
	// column: which row was written cannot be told from the statement.
	t.Run("on_constraint", func(t *testing.T) {
		rtDSN(t)
		e := upPGOne(t, nil, "INSERT INTO tsql_up_users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT tsql_up_users_email_key DO UPDATE SET name = EXCLUDED.name",
			99, "a@example.com", "Anna")
		upUnknown(t, e)
		if !e.Mutations[0].After.IsZero() {
			t.Errorf("an image of a row that may not be the one written: %s", e.Mutations[0].After.Inline)
		}
	})
	t.Run("partitioned_table", func(t *testing.T) {
		m := upPGClean(t, upPGOne(t, nil, "INSERT INTO tsql_up_part (sku, qty) VALUES ($1, $2) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty", "widget", 7))
		upCheck(t, m, "update", true, "qty:10->7")
	})
	// The upsert moves the row to another key: it is not found under the old
	// one afterwards, so its new values are not known.
	t.Run("changes_the_key", func(t *testing.T) {
		e := upPGOne(t, nil, "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO UPDATE SET sku = 'moved'", "widget", 7, "x")
		if len(e.Mutations) != 1 || !hasFlag(e, "after_unavailable") {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], "update", true, "")
		if !e.Mutations[0].After.IsZero() {
			t.Errorf("after image %s", e.Mutations[0].After.Inline)
		}
	})
}

// upPGTxRun is an application transaction around an upsert, with settings and
// savepoints of its own, then an upsert in a transaction that is aborted. It
// returns everything the application can observe.
func upPGTxRun(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	var out []string
	note := func(what string, err error) {
		if err != nil {
			what += ": " + err.Error()
		}
		out = append(out, what)
	}
	show := func(tx *sql.Tx, q string) {
		var s sql.NullString
		err := tx.QueryRowContext(context.Background(), q).Scan(&s)
		note(q+" = "+s.String, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rtMustExec(t, tx, "SET LOCAL statement_timeout = '3s'", "SAVEPOINT app")
	res, err := tx.ExecContext(ctx, upPGStmt, "widget", 7, "x")
	note("upsert", err)
	if err == nil {
		n, _ := res.RowsAffected()
		note(fmt.Sprint("affected ", n), nil)
	}
	show(tx, "SHOW statement_timeout")
	show(tx, "SHOW transaction_read_only")
	show(tx, "SELECT qty::text FROM tsql_up WHERE sku = 'widget'")
	_, err = tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT app")
	note("rollback to the application's savepoint", err)
	show(tx, "SELECT qty::text FROM tsql_up WHERE sku = 'widget'")
	_, err = tx.ExecContext(ctx, upPGStmt, "gadget", 5, "Gadget")
	note("second upsert", err)
	note("commit", tx.Commit())

	// an aborted transaction stays aborted, and the upsert fails as it does untraced
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(context.Background(), "SELECT 1/0")
	note("division", err)
	_, err = tx.ExecContext(ctx, upPGStmt, "widget", 8, "x")
	note("upsert in aborted tx", err)
	show(tx, "SHOW statement_timeout")
	note("commit of aborted tx", tx.Commit())
	var qty, rows string
	if err := db.QueryRowContext(context.Background(), "SELECT (SELECT qty FROM tsql_up WHERE sku = 'widget')::text, (SELECT count(*) FROM tsql_up)::text").Scan(&qty, &rows); err != nil {
		t.Fatal(err)
	}
	return append(out, "widget qty "+qty, "rows "+rows)
}

// Inside an application transaction the traced run leaves exactly what the
// untraced run leaves: results, settings, savepoints, an aborted transaction.
func TestPGUpsertInTransaction(t *testing.T) {
	rtDSN(t)
	upPGReset(t)
	plain, err := sql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	want := upPGTxRun(t, context.Background(), plain)

	upPGReset(t)
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	got := upPGTxRun(t, ctx, db)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("traced:\n%s\nuntraced:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	t.Log(strings.Join(got, "\n"))
	ws := byKind(rec.Events(), core.KindDBWrite)
	var ups []core.Event
	for _, e := range ws {
		if strings.HasPrefix(e.Name, "INSERT") {
			ups = append(ups, e)
		}
	}
	if len(ups) != 3 {
		t.Fatalf("%d upsert events", len(ups))
	}
	upCheck(t, upPGClean(t, ups[0]), "update", true, "qty:10->7")
	upCheck(t, upPGClean(t, ups[1]), "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	if len(ups[2].Mutations) != 0 || ups[2].Err == "" {
		t.Errorf("upsert in the aborted transaction: mutations %+v error %q", ups[2].Mutations, ups[2].Err)
	}
}

// Another connection writes the row between tsql's read and the statement:
// the read and the row disagree about what the statement did, and the record
// says that it is not known instead of taking either side.
func TestPGUpsertConcurrentWriteBetweenReadAndStatement(t *testing.T) {
	rtDSN(t)
	for _, tc := range []struct{ name, sku, other string }{
		// tsql saw no row; the statement met the other connection's and updated it
		{"insert_by_another_connection", "gadget", "INSERT INTO tsql_up VALUES ('gadget', 1, 'Theirs')"},
		// tsql saw the row; the statement found none and inserted
		{"delete_by_another_connection", "widget", "DELETE FROM tsql_up WHERE sku = 'widget'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upPGReset(t)
			rec, ctx := setup(t)
			d := &rtDriver{}
			d.hook = func(via, q string) {
				if strings.HasPrefix(q, "INSERT INTO tsql_up") {
					rtAdmin(t, tc.other)
				}
			}
			db := rtOpen(t, d)
			if _, err := db.ExecContext(ctx, upPGStmt, tc.sku, 7, "Ours"); err != nil {
				t.Fatal(err)
			}
			e := lastWrite(t, rec)
			upUnknown(t, e)
			if len(e.Mutations) != 1 || jsonMap(t, e.Mutations[0].After)["qty"] != float64(7) {
				t.Fatalf("mutations %+v", e.Mutations)
			}
		})
	}
}

// A view has no xmax: the first upsert through it learns that (the read
// fails inside the fence and is made again without; the statement and the
// transaction are untouched). The rows are told apart by the read before
// the statement.
func TestPGUpsertThroughView(t *testing.T) {
	rtDSN(t)
	upPGReset(t)
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", rtDSN(t), tsql.WithPKColumns("tsql_up_view", "sku"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const stmt = "INSERT INTO tsql_up_view (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, stmt, "widget", 7, "x"); err != nil {
		t.Fatal(err)
	}
	upCheck(t, upPGClean(t, lastWrite(t, rec)), "update", true, "qty:10->7")
	if _, err := tx.ExecContext(ctx, stmt, "widget", 8, "x"); err != nil {
		t.Fatalf("the transaction after the failed read: %v", err)
	}
	upCheck(t, upPGClean(t, lastWrite(t, rec)), "update", true, "qty:7->8")
	if _, err := tx.ExecContext(ctx, stmt, "gadget", 5, "Gadget"); err != nil {
		t.Fatal(err)
	}
	upCheck(t, upPGClean(t, lastWrite(t, rec)), "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// WithUpsertBefore(false): the row after the statement alone says whether it
// was inserted. A row whose version is not fresh was updated, or inserted and
// then locked: that is not known without the read before, and is flagged.
func TestPGUpsertBeforeOff(t *testing.T) {
	rtDSN(t)
	off := []tsql.Option{tsql.WithUpsertBefore(false)}
	m := upPGClean(t, upPGOne(t, off, upPGStmt, "gadget", 5, "Gadget"))
	upCheck(t, m, "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	e := upPGOne(t, off, upPGStmt, "widget", 7, "x")
	upUnknown(t, e)
	if len(e.Mutations) != 1 || jsonMap(t, e.Mutations[0].After)["qty"] != float64(7) {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
}

// A row that was there before the statement and has xmax set is not thereby
// a row the statement updated: xmax stays set on any row that was ever locked
// or updated. Where the row version is stored says whether it was written.
// Each case below was recorded as an update of the wrong row, with before
// equal to after and no flag.
func TestPGUpsertDoesNotClaimARowItDidNotWrite(t *testing.T) {
	rtDSN(t)
	// noRecordOf fails if any mutation claims the row with this key was changed.
	untouched := func(t *testing.T, e core.Event, col string, val any) {
		t.Helper()
		for _, m := range e.Mutations {
			if reflect.DeepEqual(m.PK[col], val) && (m.Op == "update" || !m.After.IsZero()) {
				t.Errorf("the statement did not write %s=%v, yet it is recorded: %+v (flags %v)", col, val, m, e.Flags)
			}
		}
	}
	t.Run("key rounded by the server", func(t *testing.T) {
		upPGReset(t)
		rtAdmin(t, "DROP TABLE IF EXISTS tsql_up_n", "CREATE TABLE tsql_up_n (id int PRIMARY KEY, v text)",
			"INSERT INTO tsql_up_n VALUES (1, 'one'), (2, 'two')", "UPDATE tsql_up_n SET v = v") // both rows carry an xmax
		t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_up_n") })
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		// 1.6 is looked for as 1.6 (no row; 1 is not read either), and stored as 2.
		if _, err := db.ExecContext(ctx, "INSERT INTO tsql_up_n (id, v) VALUES (1.6, 'x') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v"); err != nil {
			t.Fatal(err)
		}
		e := lastWrite(t, rec)
		untouched(t, e, "id", float64(1))
		untouched(t, e, "id", int64(1))
		if !hasFlag(e, "upsert_unknown") && !hasFlag(e, "image_skipped") {
			t.Errorf("row 2 was written and is not found under the key in VALUES: the record must say so; flags %v mutations %+v", e.Flags, e.Mutations)
		}
	})
	t.Run("DO UPDATE WHERE false through Query", func(t *testing.T) {
		upPGReset(t)
		rtAdmin(t, "UPDATE tsql_up SET qty = qty WHERE sku = 'widget'") // widget carries an xmax
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		rows, err := db.QueryContext(ctx, upPGStmt+" WHERE false", "widget", 7, "x")
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		e := lastWrite(t, rec)
		for _, m := range e.Mutations {
			if m.Op == "update" {
				t.Errorf("nothing was written, yet an update is recorded: %+v (flags %v)", m, e.Flags)
			}
		}
	})
	t.Run("a fresh insert that something locked, no read before", func(t *testing.T) {
		upPGReset(t)
		rtAdmin(t, "DROP TABLE IF EXISTS tsql_up_child",
			"CREATE TABLE tsql_up_child (id serial PRIMARY KEY, sku text REFERENCES tsql_up(sku))",
			`CREATE OR REPLACE FUNCTION tsql_up_child_fn() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN INSERT INTO tsql_up_child (sku) VALUES (NEW.sku); RETURN NEW; END $f$`,
			"CREATE TRIGGER tsql_up_child_tr AFTER INSERT ON tsql_up FOR EACH ROW EXECUTE FUNCTION tsql_up_child_fn()")
		t.Cleanup(func() {
			rtAdmin(t, "DROP TABLE IF EXISTS tsql_up_child", "DROP FUNCTION IF EXISTS tsql_up_child_fn() CASCADE")
		})
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t), tsql.WithUpsertBefore(false))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, upPGStmt, "gadget", 5, "Gadget"); err != nil {
			t.Fatal(err)
		}
		// The foreign-key check of the trigger's insert locks the new row: its xmax is set.
		e := lastWrite(t, rec)
		for _, m := range e.Mutations {
			if m.Op == "update" {
				t.Errorf("an inserted row is recorded as an update: %+v (flags %v)", m, e.Flags)
			}
		}
		upUnknown(t, e)
	})
	t.Run("images off, another row's id in VALUES", func(t *testing.T) {
		upPGReset(t)
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t), tsql.WithImages(false))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		// The conflict is on email: the row that is updated is Ann's (id 1), not id 99.
		if _, err := db.ExecContext(ctx, "INSERT INTO tsql_up_users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name", 99, "a@example.com", "Anna"); err != nil {
			t.Fatal(err)
		}
		e := lastWrite(t, rec)
		untouched(t, e, "id", int64(99))
		for _, m := range e.Mutations {
			if len(m.PK) != 0 {
				t.Errorf("filed under %v, a row that was not written: %+v", m.PK, m)
			}
		}
		if !hasFlag(e, "upsert_unknown") {
			t.Errorf("flags %v, want upsert_unknown", e.Flags)
		}
	})
}

// An upsert with RETURNING: the application gets the rows and columns it
// gets untraced, and the rows are recorded as what happened to them.
func TestPGUpsertReturning(t *testing.T) {
	rtDSN(t)
	const stmt = "INSERT INTO tsql_up (sku, qty, name) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty RETURNING sku, qty"
	run := func(ctx context.Context, db *sql.DB) []string {
		upPGReset(t)
		rs, err := db.QueryContext(ctx, stmt, "widget", 7, "x", "gadget", 5, "Gadget")
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		cols, _ := rs.Columns()
		out := []string{strings.Join(cols, ",")}
		for rs.Next() {
			var sku string
			var qty int
			if err := rs.Scan(&sku, &qty); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(sku, qty))
		}
		return out
	}
	plain, err := sql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	want := run(context.Background(), plain)
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := run(ctx, db); !reflect.DeepEqual(got, want) || len(got) != 3 {
		t.Fatalf("traced %v, untraced %v", got, want)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 2 || hasFlag(e, "upsert_unknown") {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
	got := map[any]core.Mutation{}
	for _, m := range e.Mutations {
		got[m.PK["sku"]] = m
	}
	upCheck(t, got["widget"], "update", true, "qty:10->7")
	upCheck(t, got["gadget"], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
}

// rtUpsertCases are the upserts measured like rtCases: one that updates a
// row and one that inserts cost what an UPDATE costs (a read before and one
// after); DO NOTHING, which writes new rows only, what an INSERT costs.
var rtUpsertCases = []rtCase{
	{"upsert_updates", 2, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
		func(i int) []any { return []any{1, fmt.Sprintf("u%d", i), "n"} }},
	{"upsert_inserts", 2, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
		func(i int) []any { return []any{2000000 + i, "new", "n"} }},
	{"upsert_do_nothing", 1, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
		func(i int) []any { return []any{3000000 + i, "new", "n"} }},
	{"upsert_multi_row", 2, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
		func(i int) []any { return []any{1, fmt.Sprintf("m%d", i), "n", 4000000 + i, "new", "n"} }},
	// the statement does not say by which values the row is found: no read
	{"upsert_on_constraint", 0, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT tsql_rt_pkey DO UPDATE SET status = EXCLUDED.status",
		func(i int) []any { return []any{1, fmt.Sprintf("c%d", i), "n"} }},
}

// TestPGUpsertRoundTrips pins the round trips of a traced upsert, in
// autocommit and inside a transaction, on first use and in the steady state,
// and with the read before the statement off.
func TestPGUpsertRoundTrips(t *testing.T) {
	rtDSN(t)
	defer rtDrop(t)
	for _, c := range rtUpsertCases {
		for _, inTx := range []bool{false, true} {
			for _, before := range []bool{true, false} {
				name := c.name
				if inTx {
					name += "/tx"
				} else {
					name += "/autocommit"
				}
				want := c
				if !before {
					name += "/before_off"
					want.captures = min(c.captures, 1)
				}
				t.Run(name, func(t *testing.T) {
					m := rtMeasure(t, c, inTx, false, tsql.WithUpsertBefore(before))
					t.Logf("%-18s first on connection: %d round trips, steady state: %d", name, len(m[0].calls), len(m[1].calls))
					for n, s := range rtSQLs(m[1].calls) {
						t.Logf("  steady %d. %s", n+1, s)
					}
					t.Logf("  steady recorded: %s", m[1].recorded)
					first, steady := want.rtWant(inTx, false)
					if len(m[0].calls) != first || len(m[1].calls) != steady {
						t.Errorf("round trips: first %d, steady %d; want %d and %d", len(m[0].calls), len(m[1].calls), first, steady)
					}
				})
			}
		}
	}
}
