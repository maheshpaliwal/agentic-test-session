package tsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// txCorpus is a set of statement texts around transaction control: every
// keyword trackTx reacts to, in every position the tokenizer can find it, and
// the look-alikes it must not react to.
func txCorpus() []string {
	// One statement each.
	stmts := []string{
		"BEGIN", "begin", "Begin", "BEGIN WORK", "BEGIN TRANSACTION", "BEGIN ISOLATION LEVEL SERIALIZABLE",
		"BEGIN READ ONLY", "START TRANSACTION", "start transaction read only", "START SLAVE", "START",
		"COMMIT", "commit", "COMMIT WORK", "COMMIT TRANSACTION", "COMMIT AND CHAIN", "COMMIT AND NO CHAIN",
		"COMMIT PREPARED 'x'", "END", "end", "END WORK", "END AND CHAIN",
		"ROLLBACK", "rollback", "ROLLBACK WORK", "ROLLBACK TRANSACTION", "ROLLBACK AND CHAIN",
		"ROLLBACK WORK AND CHAIN", "ROLLBACK TO SAVEPOINT s", "ROLLBACK TO s", "ROLLBACK WORK TO SAVEPOINT s",
		"ROLLBACK TRANSACTION TO s", "ROLLBACK PREPARED 'x'", "ABORT", "abort work", "ABORT AND CHAIN",
		"PREPARE TRANSACTION 'x'", "prepare transaction 'x'", "PREPARE p AS SELECT 1", "PREPARE",
		"SAVEPOINT s", "RELEASE SAVEPOINT s", "RELEASE s", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET autocommit = 0", "LOCK TABLE t", "XA START 'x'", "XA COMMIT 'x'",
		// Isolation levels: of the transaction, the next one, the session.
		"START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "begin isolation level repeatable read",
		"set transaction isolation level serializable", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET default_transaction_isolation = 'serializable'", "Set Default_Transaction_Isolation To Default",
		"SET transaction_isolation = 'serializable'", "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"SET @@session.transaction_isolation = 'READ-COMMITTED'", "SET @@transaction_isolation = 'SERIALIZABLE'",
		"RESET ALL", "reset all", "DISCARD ALL", "RESET default_transaction_isolation",
		"SELECT set_config('default_transaction_isolation', 'serializable', false)", "SHOW transaction_isolation",
		// Look-alikes: a keyword that is not the first token, or only a prefix
		// or suffix of it.
		"SELECT 1", "SELECT 'BEGIN'", "SELECT 'BEGIN; COMMIT'", "SELECT 1 AS begin", "SELECT commit FROM t",
		"UPDATE t SET a = 1 WHERE id = 2", "INSERT INTO t (note) VALUES ('x; BEGIN')",
		"INSERT INTO t (note) VALUES ('it''s; COMMIT; done')", `INSERT INTO t (note) VALUES ("a; ROLLBACK")`,
		"INSERT INTO t (note) VALUES (E'a\\'; BEGIN')", "INSERT INTO t (note) VALUES ('a\\'; BEGIN; --')",
		"SELECT $$ ; BEGIN $$", "SELECT $q$ ; COMMIT $q$, 1", "SELECT $q$ ; COMMIT", "SELECT 1 -- ; BEGIN",
		"SELECT 1 /* ; BEGIN */", "SELECT 1 /* ; BEGIN", "SELECT 1 # ; COMMIT", "SELECT `a;BEGIN` FROM t",
		`SELECT "a; END" FROM t`, "BEGINNING", "BEGI", "BEGIN_", "BEGIN$", "BEGIN1", "_BEGIN", "XBEGIN", "COMMITS",
		"ENDS", "ENDED", "STARTS TRANSACTION", "ROLLBACKS", "ABORTED", "PREPARED", "(BEGIN)", "(COMMIT", "BEGIN(",
		"1BEGIN", "$1 BEGIN", ":x BEGIN", "?BEGIN", "'BEGIN'", `"BEGIN"`, "`COMMIT`", "E'x' BEGIN", "-BEGIN", "- -BEGIN",
		"/BEGIN", "/ * x */ BEGIN", "#BEGIN", "$BEGIN", "$$BEGIN$$", "$a$BEGIN", ".COMMIT", "::END", "*/ COMMIT", "\\BEGIN",
		// Words the tokenizer upper-cases into a keyword, or does not.
		"\u017ftart transaction", "comm\u0131t", "rollbac\u212a", "BEGIN\u00e9", "\u00e9BEGIN", "BEG\u0131N", "\u00a0BEGIN",
		"\u2003COMMIT", "\ufeffBEGIN", "BEGIN\u00a0", "COMMIT\u200b", "\xffBEGIN", "BEGIN\xff", "BEG\xffIN", "\xc3COMMIT",
		// Empty and degenerate texts.
		"", " ", ";", ";;", "--", "/*", "/**/", "-- only a comment", "/* only a comment */", "#", "\x00BEGIN", "\x00",
	}
	// What can stand before the first token of a statement.
	leads := []string{
		"", " ", "\n\t ", "\r\n", "\f", "\v", "-- c\n", "--\n", "-- c", "--c\n", "--\tc\n", "-- a\n-- b\n", "# c\n", "#c\n", "#",
		"/* c */", "/* c */ ", "/**/", "/* a */ /* b */", "/* /* nested */ still */", "/* /* nested */ ", "/* a */ -- b\n",
		"/* ; */", "-- ; \n", "/* unterminated ", "/*/ ", "/*/*/ ", "/* * / */", "- ", "-", "--", "/", "(", ";", " ; ", ";;",
	}
	var out []string
	for _, l := range leads {
		for _, s := range stmts {
			out = append(out, l+s, l+s+";", l+s+" ; ")
		}
	}
	// Several statements in one text, transaction control in every position.
	parts := []string{
		"BEGIN", "commit", "END", "ROLLBACK", "abort", "START TRANSACTION", "PREPARE TRANSACTION 'x'", "COMMIT AND CHAIN",
		"ROLLBACK TO s", "COMMIT PREPARED 'x'", "SAVEPOINT s", "SELECT 1", "INSERT INTO t (note) VALUES ('a; BEGIN')",
		"UPDATE t SET a = 1 /* ; COMMIT */ WHERE id = 2", "DELETE FROM t -- ; ROLLBACK", "SELECT $$ ; END $$", "",
		"/* c */ begin", "-- c\nCOMMIT", "(ROLLBACK)", "comm\u0131t", "\u017ftart transaction",
	}
	seps := []string{";", "; ", " ;\n", ";;", "; /* c */ ", ";-- c\n", ";--c\n", ";#c\n", "; /* ; */ ", ";\t-- ;\n"}
	for _, a := range parts {
		for _, b := range parts {
			for _, sep := range seps {
				out = append(out, a+sep+b)
			}
			for _, c := range []string{"BEGIN", "COMMIT", "SELECT 1"} {
				out = append(out, a+"; "+b+"; "+c, a+";"+b+";"+c+";")
			}
		}
	}
	// Bulk statements, as outside-session traffic has them.
	big := "INSERT INTO t (id, note) VALUES " + strings.Repeat("(1, 'a; b'), ", 3000) + "(2, 'x')"
	out = append(out, big, "BEGIN; "+big, big+"; COMMIT", big+";", big+"; -- done", "/* "+strings.Repeat("; ", 3000)+"*/ COMMIT",
		strings.Repeat(";--", 3000), strings.Repeat(";/*", 3000), strings.Repeat(";--", 3000)+"\nBEGIN", strings.Repeat("; ", 3000)+"begin",
		"SELECT 1 WHERE id IN ("+strings.Repeat("1,", 3000)+"1)")
	return out
}

// The untraced path decides with a cheap scan whether a statement has to be
// parsed for its transaction effect. For every text of the corpus, in both
// dialects, the decision it reaches must be the one the full parse reaches
// (which is what every statement went through before).
func TestTxScanAgreesWithParse(t *testing.T) {
	corpus := txCorpus()
	hits, maybes := 0, 0
	for _, d := range []mutation.Dialect{mutation.Postgres, mutation.MySQL} {
		for _, q := range corpus {
			old := mutation.Parse(q, d).Tx // the decision on the base commit
			scan := mutation.MayControlTx(q, d)
			if old != mutation.TxNone && !scan {
				t.Errorf("dialect %d: scan missed transaction control %d in %q", d, old, q)
			}
			now := mutation.TxNone // the decision of trackTxUntraced
			if scan {
				now = mutation.Parse(q, d).Tx
			}
			if now != old {
				t.Errorf("dialect %d: %q: tx effect %d, want %d", d, q, now, old)
			}
			if old != mutation.TxNone {
				hits++
			} else if scan {
				maybes++
			}
		}
	}
	if hits < 1000 {
		t.Fatalf("corpus has only %d texts with transaction control", hits)
	}
	t.Logf("%d texts x 2 dialects: %d with transaction control, %d parsed needlessly", len(corpus), hits, maybes)
}

// The scan must say no, without parsing, for the statements that make up the
// traffic outside a session; otherwise the fast path would be vacuous.
func TestTxScanSkipsOrdinaryStatements(t *testing.T) {
	for _, q := range []string{
		"SELECT id FROM t WHERE id = $1",
		"/* app:orders */ SELECT id FROM t WHERE note = 'a; b'",
		"INSERT INTO t (id, note) VALUES " + strings.Repeat("(1, 'a; b'), ", 3000) + "(2, 'x')",
		"UPDATE t SET a = 1 WHERE id = 2;",
		"SAVEPOINT s",
		"RELEASE SAVEPOINT s",
		"SELECT 1; SELECT 2",
	} {
		for _, d := range []mutation.Dialect{mutation.Postgres, mutation.MySQL} {
			if mutation.MayControlTx(q, d) {
				t.Errorf("dialect %d: %.60q would be parsed", d, q)
			}
		}
	}
}

// Feeding the same texts to a conn through the untraced path and to another
// through the old one (parse, then trackTx) leaves both with the same
// transaction and isolation state after every statement.
func TestTrackTxUntracedMatchesTrackTx(t *testing.T) {
	for _, store := range []string{"postgres", "mysql"} {
		cfg := buildConfig(nil)
		cfg.store = store
		oldC, newC := newConn(nil, cfg, &pkCache{}), newConn(nil, cfg, &pkCache{})
		serial := 0
		for i, q := range txCorpus() {
			info, _ := oldC.parse(q)
			oldC.trackTx(info)
			newC.trackTxUntraced(q)
			if oldC.rawTx != newC.rawTx {
				t.Fatalf("%s: statement %d %q: rawTx = %v, want %v", store, i, q, newC.rawTx, oldC.rawTx)
			}
			type iso struct {
				iso                                    isoState
				isoDefault, defSerial, defInTx, nextTx bool
			}
			got := iso{newC.iso, newC.isoDefault, newC.defSerial, newC.defInTx, newC.nextSerial}
			if want := (iso{oldC.iso, oldC.isoDefault, oldC.defSerial, oldC.defInTx, oldC.nextSerial}); got != want {
				t.Fatalf("%s: statement %d %q: isolation state %+v, want %+v", store, i, q, got, want)
			}
			if got.iso == isoSerial {
				serial++
			}
		}
		if serial < 100 {
			t.Fatalf("%s: only %d statements of the corpus left a SERIALIZABLE transaction open", store, serial)
		}
	}
}

// nopConn is a driver connection that does nothing and allocates nothing.
type nopConn struct{}

func (nopConn) Prepare(string) (driver.Stmt, error) { return nopStmt{}, nil }
func (nopConn) Close() error                        { return nil }
func (nopConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (nopConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.ResultNoRows, nil
}
func (nopConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nopRows{}, nil
}

type nopStmt struct{}

func (nopStmt) Close() error  { return nil }
func (nopStmt) NumInput() int { return -1 }
func (nopStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.ResultNoRows, nil
}
func (nopStmt) Query([]driver.Value) (driver.Rows, error) { return nopRows{}, nil }
func (nopStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return driver.ResultNoRows, nil
}
func (nopStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return nopRows{}, nil
}

type nopRows struct{}

func (nopRows) Columns() []string         { return nil }
func (nopRows) Close() error              { return nil }
func (nopRows) Next([]driver.Value) error { return io.EOF }

// A statement outside a session costs no allocation in tsql, whatever its
// size: it is neither parsed nor redacted. The SDK is enabled and a session is
// active elsewhere.
func TestUntracedStatementsDoNotAllocate(t *testing.T) {
	core.Configure(core.Config{Service: "svc", Instance: "i1", Enabled: true})
	rec := &core.MemRecorder{}
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() {
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})

	var b strings.Builder
	b.WriteString("INSERT INTO public.orders (id, status, amount) VALUES ")
	for i := range 2000 {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(%d, 'status-%04d', %d.50)", 100000+i, i, 1000+i)
	}
	insert := b.String()
	if len(insert) < 54<<10 {
		t.Fatalf("statement is only %d bytes", len(insert))
	}
	selectLit := "SELECT id, status FROM public.orders WHERE tenant = 'acme-000001' AND status = 'shipped; held' AND amount > 100"

	cfg := buildConfig(nil)
	cfg.store = "postgres"
	c := newConn(nopConn{}, cfg, &pkCache{})
	st, err := c.PrepareContext(context.Background(), insert)
	if err != nil {
		t.Fatal(err)
	}
	se, sq := st.(driver.StmtExecContext), st.(driver.StmtQueryContext)

	contexts := map[string]context.Context{
		"no session":       context.Background(),
		"inactive session": core.ContextWith(context.Background(), core.SpanContext{SessionID: "other", TraceID: "t9"}),
	}
	// Nothing at all: the closures stay on the stack and nothing is parsed.
	const maxAllocs = 0
	for name, ctx := range contexts {
		ops := map[string]func(){
			"exec 54KB insert":          func() { _, _ = c.ExecContext(ctx, insert, nil) },
			"query 54KB insert":         func() { _, _ = c.QueryContext(ctx, insert, nil) },
			"prepared exec 54KB insert": func() { _, _ = se.ExecContext(ctx, nil) },
			"prepared query":            func() { _, _ = sq.QueryContext(ctx, nil) },
			"query literal select":      func() { _, _ = c.QueryContext(ctx, selectLit, nil) },
		}
		for op, f := range ops {
			if n := testing.AllocsPerRun(200, f); n > maxAllocs {
				t.Errorf("%s, %s: %v allocations per statement, want at most %d", name, op, n, maxAllocs)
			}
		}
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("%d events recorded outside a session", n)
	}
	if cacheHas(insert) || cacheHas(selectLit) {
		t.Fatal("a statement outside a session was parsed")
	}
	if c.rawTx {
		t.Fatal("an INSERT opened a transaction")
	}
}
