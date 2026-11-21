package tsql_test

// Captures inside a SERIALIZABLE transaction, whichever way the level was
// set, against the fake driver. The same is checked on a real Postgres in
// iso_pg_test.go.

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// isoCase is one way an application makes its next transaction SERIALIZABLE.
type isoCase struct {
	name   string
	before []string // on the connection, before the transaction
	begin  string   // the statement that opens it; "" for database/sql's BeginTx
	inTx   []string // first statements of the transaction
	after  []string // puts the connection back to its default level
}

var pgIsoCases = []isoCase{
	{name: "begin", begin: "BEGIN ISOLATION LEVEL SERIALIZABLE"},
	{name: "begin_modes", begin: "begin transaction read write, isolation level serializable"},
	{name: "start_transaction", begin: "START TRANSACTION ISOLATION LEVEL SERIALIZABLE"},
	{name: "begin_and_set_in_one_text", begin: "BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"},
	{name: "set_transaction", inTx: []string{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"}},
	{name: "set_transaction_after_begin", begin: "BEGIN", inTx: []string{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ WRITE"}},
	{name: "set_transaction_isolation", begin: "BEGIN", inTx: []string{"SET transaction_isolation = 'serializable'"}},
	{name: "session_characteristics", before: []string{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"},
		after: []string{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"}},
	{name: "session_characteristics_raw_begin", before: []string{"set session characteristics as transaction isolation level serializable"},
		begin: "BEGIN", after: []string{"RESET default_transaction_isolation"}},
	{name: "default_transaction_isolation", before: []string{"SET default_transaction_isolation = 'serializable'"},
		after: []string{"SET default_transaction_isolation TO DEFAULT"}},
	{name: "default_transaction_isolation_to", before: []string{"SET SESSION default_transaction_isolation TO serializable"},
		begin: "START TRANSACTION", after: []string{"RESET ALL"}},
}

type isoExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func isoExec(t *testing.T, ctx context.Context, ex isoExecer, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := ex.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// isoRun runs the case's transaction on c with the traced statements stmts in
// it and returns what the driver was sent from the first of them on.
// Everything else runs with ctl.
func isoRun(t *testing.T, c *sql.Conn, ic isoCase, ctx, ctl context.Context, take func() []string, stmts ...string) []string {
	t.Helper()
	return isoRunCheck(t, c, ic, ctx, ctl, take, nil, stmts...)
}

// isoRunCheck is isoRun with a look at the transaction, once the traced
// statements have run, that is not part of what is returned.
func isoRunCheck(t *testing.T, c *sql.Conn, ic isoCase, ctx, ctl context.Context, take func() []string, check func(rtQueryer), stmts ...string) []string {
	t.Helper()
	isoExec(t, ctl, c, ic.before...)
	var ex rtQueryer = c
	end := func() error { _, err := c.ExecContext(ctl, "COMMIT"); return err }
	if ic.begin != "" {
		isoExec(t, ctl, c, ic.begin)
	} else {
		tx, err := c.BeginTx(ctl, nil)
		if err != nil {
			t.Fatal(err)
		}
		ex, end = tx, tx.Commit
	}
	isoExec(t, ctl, ex, ic.inTx...)
	take()
	isoExec(t, ctx, ex, stmts...)
	got := take()
	if check != nil {
		check(ex)
	}
	if err := end(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	return got
}

func fakeTake(f *fakeDB) func() []string {
	return func() []string {
		f.mu.Lock()
		defer f.mu.Unlock()
		var out []string
		for _, c := range f.calls {
			out = append(out, c.SQL)
		}
		f.calls = nil
		return out
	}
}

// isoReads counts the image reads among the statements a driver was sent.
func isoReads(qs []string) (n int) {
	for _, q := range qs {
		if strings.HasPrefix(q, "SELECT ") && strings.Contains(q, "* FROM ") {
			n++
		}
	}
	return n
}

var isoTwoUpdates = []string{fenceUpdate, fenceUpdate}

// Inside a transaction a statement made SERIALIZABLE nothing is sent but the
// application's own statements: no key lookup, no image read, no fence. The
// statement that sets the level may itself be traced or not.
func TestSerializableSetByStatementSendsNoCapture(t *testing.T) {
	for _, ic := range pgIsoCases {
		for _, traced := range []bool{true, false} {
			name := ic.name + "/control_untraced"
			if traced {
				name = ic.name + "/control_traced"
			}
			t.Run(name, func(t *testing.T) {
				rec, ctx := setup(t)
				ctl := context.Background()
				if traced {
					ctl = ctx
				}
				f := fenceFake(&fakeDB{})
				db := open(t, f, "postgres")
				c, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				take := fakeTake(f)
				if got := isoRun(t, c, ic, ctx, ctl, take, isoTwoUpdates...); strings.Join(got, "\n") != fenceUpdate+"\n"+fenceUpdate {
					t.Fatalf("driver saw:\n%s\nwant only the application's two statements", strings.Join(got, "\n"))
				}
				for _, e := range byKind(rec.Events(), core.KindDBWrite) {
					if !hasFlag(e, "image_skipped:serializable") || len(e.Mutations) != 1 || !e.Mutations[0].Before.IsZero() {
						t.Fatalf("event %q: flags %v mutations %+v", e.Name, e.Flags, e.Mutations)
					}
				}
				// The level is the transaction's, or the session's until it is
				// changed back: the next transaction is captured again.
				isoExec(t, ctl, c, ic.after...)
				next := isoCase{begin: ic.begin}
				if strings.Contains(strings.ToUpper(ic.begin), "SERIALIZABLE") {
					next.begin = "BEGIN ISOLATION LEVEL REPEATABLE READ"
				}
				if got := isoRun(t, c, next, ctx, ctl, take, isoTwoUpdates...); isoReads(got) != 4 {
					t.Fatalf("the transaction after it ran %d image reads, want 4:\n%s", isoReads(got), strings.Join(got, "\n"))
				}
			})
		}
	}
}

// A level no statement shows (the session default from the role, the database
// or the connection string): the first fence of a transaction asks the server
// in the text that opens it. A SERIALIZABLE answer closes the fence without
// reading anything, and is remembered for the connection's later transactions.
func TestSerializableDefaultIsAskedOncePerConnection(t *testing.T) {
	rec, ctx := setup(t)
	f := fenceFake(&fakeDB{serializable: true})
	db := open(t, f, "postgres")
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, fenceUpdate); err != nil { // the connection now takes two statements in one text
		t.Fatal(err)
	}
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	take := fakeTake(f)
	got := isoRun(t, c, isoCase{}, ctx, ctx, take, isoTwoUpdates...)
	want := []string{
		"SAVEPOINT ts_capture; " + isoAsk,
		"ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture", fenceUpdate, fenceUpdate,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("first transaction, driver saw:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, ic := range []isoCase{{}, {begin: "BEGIN"}, {begin: "START TRANSACTION READ WRITE"}} {
		if got := isoRun(t, c, ic, ctx, ctx, take, isoTwoUpdates...); strings.Join(got, "\n") != fenceUpdate+"\n"+fenceUpdate {
			t.Fatalf("later transaction (%q), driver saw:\n%s", ic.begin, strings.Join(got, "\n"))
		}
	}
	for _, e := range byKind(rec.Events(), core.KindDBWrite)[1:] {
		if !hasFlag(e, "image_skipped:serializable") {
			t.Fatalf("event %q: flags %v", e.Name, e.Flags)
		}
	}
	// A transaction that states another level is read as before, and is not
	// asked about; so is one at the default level once that has changed.
	got = isoRun(t, c, isoCase{begin: "BEGIN ISOLATION LEVEL READ COMMITTED"}, ctx, ctx, take, isoTwoUpdates...)
	if isoReads(got) != 4 || strings.Contains(strings.Join(got, "\n"), "transaction_isolation") {
		t.Fatalf("READ COMMITTED transaction:\n%s", strings.Join(got, "\n"))
	}
	f.mu.Lock()
	f.serializable = false
	f.mu.Unlock()
	isoExec(t, context.Background(), c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	got = isoRun(t, c, isoCase{}, ctx, ctx, take, isoTwoUpdates...)
	if isoReads(got) != 4 || strings.Count(strings.Join(got, "\n"), "transaction_isolation") != 1 {
		t.Fatalf("after the default changed:\n%s", strings.Join(got, "\n"))
	}
}

// A default set inside a transaction that is rolled back is not the
// session's: the next transaction is asked about, not taken for SERIALIZABLE.
func TestSerializableDefaultSetInRolledBackTransaction(t *testing.T) {
	_, ctx := setup(t)
	f := fenceFake(&fakeDB{})
	db := open(t, f, "postgres")
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	isoExec(t, ctx, c, "BEGIN", "SET default_transaction_isolation = 'serializable'", "ROLLBACK")
	if got := isoRun(t, c, isoCase{}, ctx, ctx, fakeTake(f), isoTwoUpdates...); isoReads(got) != 4 {
		t.Fatalf("%d image reads, want 4:\n%s", isoReads(got), strings.Join(got, "\n"))
	}
}

// MySQL: in a SERIALIZABLE transaction a plain SELECT is a locking read, so
// nothing is read in one. The level is known only from the statements the
// connection saw; one a statement sets without showing it counts as
// SERIALIZABLE.
func TestMySQLSerializableSetByStatementSendsNoCapture(t *testing.T) {
	for _, tc := range []struct {
		set    string
		skips  int  // transactions after it in which nothing is read
		sticky bool // it is the session's level until set again
	}{
		{"SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", 2, true},
		{"SET @@session.transaction_isolation = 'SERIALIZABLE'", 2, true},
		{"SET SESSION tx_isolation = 'serializable'", 2, true},
		{"SET transaction_isolation = @lvl", 2, true},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", 1, false},
		{"SET @@transaction_isolation = 'SERIALIZABLE'", 1, false},
		{"SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE", 0, false},
		{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", 0, false},
	} {
		t.Run(tc.set, func(t *testing.T) {
			_, ctx := setup(t)
			f := fenceFake(&fakeDB{})
			db := open(t, f, "mysql", noPKLookup)
			c, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			take := fakeTake(f)
			reads := func(ic isoCase) int {
				return isoReads(isoRun(t, c, ic, ctx, context.Background(), take, isoTwoUpdates...))
			}
			for i, ic := range []isoCase{{before: []string{tc.set}}, {begin: "START TRANSACTION"}} {
				if n := reads(ic); (n == 0) != (i < tc.skips) {
					t.Fatalf("transaction %d: %d image reads (%d transactions are to be skipped)", i+1, n, tc.skips)
				}
			}
			if tc.sticky {
				if n := reads(isoCase{before: []string{"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"}}); n == 0 {
					t.Fatalf("after the session level was set back: %d image reads", n)
				}
			}
		})
	}
}
