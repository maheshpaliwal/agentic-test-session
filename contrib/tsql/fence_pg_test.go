package tsql_test

// What the capture fence relies on, checked against a real Postgres (set
// TSESSION_TEST_PG_DSN; skipped otherwise). The TestPGFact* tests use the
// plain pgx driver without tsql: they pin down the server and driver
// behaviour the fence is built on. The TestPGFence* tests check, through
// tsql, that the application's transaction, session settings and pooled
// connection are exactly as they would be without it.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// rtRawConn returns one plain pgx connection (no tsql).
func rtRawConn(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("pgx", rtDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type rtQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func rtShow(t *testing.T, q rtQueryer, query string) string {
	t.Helper()
	var s sql.NullString
	if err := q.QueryRowContext(context.Background(), query).Scan(&s); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if !s.Valid {
		return "NULL"
	}
	return s.String
}

func rtMustExec(t *testing.T, q rtQueryer, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := q.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// A SET LOCAL issued after a savepoint is reverted by ROLLBACK TO that
// savepoint, to whatever the application had (session value or its own SET
// LOCAL); RELEASE alone keeps it until the end of the transaction. So the
// fence always rolls back and never needs to read or restore the old value.
func TestPGFactRollbackToSavepointRevertsSetLocal(t *testing.T) {
	c := rtRawConn(t)
	for _, app := range []struct{ name, set, want string }{
		{"default", "", "0"},
		{"session value", "SET statement_timeout = '7s'", "7s"},
		{"application SET LOCAL", "", "3s"},
	} {
		t.Run(app.name, func(t *testing.T) {
			rtMustExec(t, c, "RESET statement_timeout")
			if app.set != "" {
				rtMustExec(t, c, app.set)
			}
			rtMustExec(t, c, "BEGIN")
			if app.name == "application SET LOCAL" {
				rtMustExec(t, c, "SET LOCAL statement_timeout = '3s'")
			}
			if got := rtShow(t, c, "SHOW statement_timeout"); got != app.want {
				t.Fatalf("before: %q", got)
			}
			rtMustExec(t, c, "SAVEPOINT ts_capture", "SET LOCAL statement_timeout = 500")
			if got := rtShow(t, c, "SHOW statement_timeout"); got != "500ms" {
				t.Fatalf("inside: %q", got)
			}
			rtMustExec(t, c, "ROLLBACK TO SAVEPOINT ts_capture", "RELEASE SAVEPOINT ts_capture")
			if got := rtShow(t, c, "SHOW statement_timeout"); got != app.want {
				t.Fatalf("after ROLLBACK TO + RELEASE: %q, want %q", got, app.want)
			}
			// The contrast: RELEASE without the rollback keeps the capture's value.
			rtMustExec(t, c, "SAVEPOINT ts_capture", "SET LOCAL statement_timeout = 500", "RELEASE SAVEPOINT ts_capture")
			if got := rtShow(t, c, "SHOW statement_timeout"); got != "500ms" {
				t.Fatalf("after RELEASE only: %q, want the leaked 500ms", got)
			}
			rtMustExec(t, c, "ROLLBACK")
		})
	}
	rtMustExec(t, c, "RESET statement_timeout")
}

// The two statements that open the fence, and the two that close it inside a
// transaction, can each go in one argument-less Exec (pgx sends it as one
// simple-protocol message): the timeout is in force for the next statement,
// and afterwards the transaction and its settings are as before.
func TestPGFactMultiStatementFence(t *testing.T) {
	c := rtRawConn(t)
	ctx := context.Background()
	sleep := func() error {
		var v sql.NullString
		return c.QueryRowContext(ctx, "SELECT pg_sleep($1)::text", 0.5).Scan(&v)
	}

	// inside a transaction
	rtMustExec(t, c, "BEGIN", "SAVEPOINT ts_capture; SET LOCAL statement_timeout = 100")
	if got := rtShow(t, c, "SHOW statement_timeout"); got != "100ms" {
		t.Fatalf("inside: %q", got)
	}
	start := time.Now()
	if err := sleep(); err == nil || !strings.Contains(err.Error(), "57014") {
		t.Fatalf("pg_sleep under the fence: %v, want a statement timeout", err)
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("timeout fired after %v", d)
	}
	// the failed SELECT aborted the subtransaction only
	rtMustExec(t, c, "ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture")
	if got := rtShow(t, c, "SHOW statement_timeout"); got != "0" {
		t.Fatalf("after: %q", got)
	}
	if _, err := c.ExecContext(ctx, "ROLLBACK TO SAVEPOINT ts_capture"); err == nil {
		t.Fatal("the savepoint must be gone")
	}
	rtMustExec(t, c, "ROLLBACK")

	// autocommit
	rtMustExec(t, c, "BEGIN READ ONLY; SET LOCAL statement_timeout = 100")
	if err := sleep(); err == nil || !strings.Contains(err.Error(), "57014") {
		t.Fatalf("pg_sleep under the fence: %v, want a statement timeout", err)
	}
	rtMustExec(t, c, "ROLLBACK")
	if got := rtShow(t, c, "SHOW statement_timeout"); got != "0" {
		t.Fatalf("after: %q", got)
	}
	if got := rtShow(t, c, "SHOW transaction_read_only"); got != "off" {
		t.Fatalf("read only after: %q", got)
	}
}

// A timeout set by the capture SELECT itself (set_config in a CTE that is
// evaluated first) is not in force for that statement: the timer is armed
// when the statement starts. And outside a savepoint it would stay for the
// rest of the transaction. So the timeout needs its own statement.
func TestPGFactTimeoutSetInsideTheStatementIsNotInForce(t *testing.T) {
	c := rtRawConn(t)
	ctx := context.Background()
	rtMustExec(t, c, "BEGIN")
	start := time.Now()
	var v sql.NullString
	err := c.QueryRowContext(ctx,
		"WITH ts AS MATERIALIZED (SELECT set_config('statement_timeout', '100', true) AS v) SELECT pg_sleep($1)::text FROM ts", 0.5).Scan(&v)
	if err != nil {
		t.Fatalf("expected the sleep to finish (the timeout is not in force): %v", err)
	}
	if d := time.Since(start); d < 450*time.Millisecond {
		t.Fatalf("slept only %v", d)
	}
	if got := rtShow(t, c, "SHOW statement_timeout"); got != "100ms" {
		t.Fatalf("the setting applies only from the next statement on: %q", got)
	}
	rtMustExec(t, c, "ROLLBACK")
}

// A driver that prepares every statement cannot send two in one text, and
// inside a transaction the rejected text aborts it. So tsql merges the
// statements that open the fence inside a transaction only on a connection
// that already accepted such a text (the first one it tries there is the
// pair closing the fence, sent under the fence's own savepoint).
func TestPGFactMultiStatementNeedsSimpleProtocol(t *testing.T) {
	c := rtRawConn(t)
	ctx := context.Background()
	rtMustExec(t, c, "BEGIN")
	if _, err := c.PrepareContext(ctx, "SAVEPOINT ts_capture; SET LOCAL statement_timeout = 100"); err == nil || !strings.Contains(err.Error(), "42601") {
		t.Fatalf("prepare of two statements: %v, want a syntax error", err)
	}
	if _, err := c.ExecContext(ctx, "SELECT 1"); err == nil || !strings.Contains(err.Error(), "25P02") {
		t.Fatalf("the transaction should be aborted: %v", err)
	}
	rtMustExec(t, c, "ROLLBACK")
}

// One fence cannot cover the before image and the after image: the
// application's statement runs between them, and inside the fence it would be
// cut by the capture timeout and undone by the rollback that closes the fence.
func TestPGFactOneFenceCannotCoverTheApplicationStatement(t *testing.T) {
	rtAdmin(t, "DROP TABLE IF EXISTS tsql_fact", "CREATE TABLE tsql_fact (id int PRIMARY KEY, v text)", "INSERT INTO tsql_fact VALUES (1, 'a')")
	defer rtAdmin(t, "DROP TABLE IF EXISTS tsql_fact")
	c := rtRawConn(t)
	ctx := context.Background()
	rtMustExec(t, c, "BEGIN", "SAVEPOINT ts_capture; SET LOCAL statement_timeout = 100")
	if _, err := c.ExecContext(ctx, "UPDATE tsql_fact SET v = 'b' WHERE id = 1 AND (SELECT pg_sleep(0.3)) IS NOT NULL"); err == nil || !strings.Contains(err.Error(), "57014") {
		t.Fatalf("a slow application statement inside the fence: %v, want it cut by the capture timeout", err)
	}
	rtMustExec(t, c, "ROLLBACK TO SAVEPOINT ts_capture", "UPDATE tsql_fact SET v = 'b' WHERE id = 1",
		"ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture")
	if got := rtShow(t, c, "SELECT v FROM tsql_fact WHERE id = 1"); got != "a" {
		t.Fatalf("v = %q: closing the fence must have undone the application's write", got)
	}
	rtMustExec(t, c, "ROLLBACK")
}

// ---- through tsql ----

func rtFenceTable(t *testing.T) {
	t.Helper()
	rtAdmin(t, "DROP TABLE IF EXISTS tsql_fence", "CREATE TABLE tsql_fence (id int PRIMARY KEY, v text)",
		"INSERT INTO tsql_fence VALUES (1, 'a'), (2, 'b')")
	t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_fence") })
}

// rtTxState is what the application can see of its transaction.
func rtTxState(t *testing.T, q rtQueryer) string {
	t.Helper()
	return fmt.Sprintf("statement_timeout=%s isolation=%s read_only=%s xid_assigned=%s rows=%s",
		rtShow(t, q, "SHOW statement_timeout"), rtShow(t, q, "SHOW transaction_isolation"), rtShow(t, q, "SHOW transaction_read_only"),
		rtShow(t, q, "SELECT (txid_current_if_assigned() IS NOT NULL)::text"),
		rtShow(t, q, "SELECT string_agg(id || '=' || v, ',' ORDER BY id) FROM tsql_fence"))
}

// rtFenceScript runs one application transaction that has its own SET LOCAL
// and its own open savepoint, and returns everything the application observes
// along the way. sctx decides whether the writes are traced.
func rtFenceScript(t *testing.T, db *sql.DB, sctx context.Context, iso sql.IsolationLevel) []string {
	t.Helper()
	bg := context.Background()
	var obs []string
	see := func(label string, q rtQueryer) { obs = append(obs, label+": "+rtTxState(t, q)) }

	tx, err := db.BeginTx(sctx, &sql.TxOptions{Isolation: iso})
	if err != nil {
		t.Fatal(err)
	}
	rtMustExec(t, tx, "SET LOCAL statement_timeout = '7s'", "SAVEPOINT app_sp")
	see("start", tx)
	step := func(label, q string, args ...any) {
		_, err := tx.ExecContext(sctx, q, args...)
		obs = append(obs, fmt.Sprintf("%s err=%v", label, err))
		see(label, tx)
	}
	// matches no row: only the before image is read, and no xid may be assigned by it
	step("delete nothing", "DELETE FROM tsql_fence WHERE id = $1", -1)
	step("update", "UPDATE tsql_fence SET v = $1 WHERE id = $2", "x", 1)
	step("insert", "INSERT INTO tsql_fence (id, v) VALUES ($1, $2)", 3, "c")
	// the before image of this one runs into the capture timeout (a failed statement inside the fence)
	step("slow update", "UPDATE tsql_fence SET v = 'y' WHERE id = 2 AND (SELECT pg_sleep(0.4)) IS NOT NULL")
	step("delete", "DELETE FROM tsql_fence WHERE id = $1", 1)

	// no savepoint of the capture is left open
	rtMustExec(t, tx, "SAVEPOINT probe")
	_, err = tx.ExecContext(bg, "ROLLBACK TO SAVEPOINT ts_capture")
	obs = append(obs, fmt.Sprintf("rollback to ts_capture: %v", err))
	rtMustExec(t, tx, "ROLLBACK TO SAVEPOINT probe", "RELEASE SAVEPOINT probe")
	// the application's own savepoint still works and takes all writes with it
	rtMustExec(t, tx, "ROLLBACK TO SAVEPOINT app_sp")
	see("after rollback to app_sp", tx)
	step("update again", "UPDATE tsql_fence SET v = $1 WHERE id = $2", "z", 2)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	obs = append(obs, "after commit: "+rtTxState(t, db))
	return obs
}

// The application's transaction is exactly as it would be without tsql: the
// same script run traced and untraced observes the same settings, isolation,
// xid assignment, savepoints and data at every step.
func TestPGFenceLeavesTransactionStateUntouched(t *testing.T) {
	rtDSN(t)
	for _, iso := range []sql.IsolationLevel{sql.LevelDefault, sql.LevelRepeatableRead} {
		for _, legacy := range []bool{false, true} {
			if legacy && iso != sql.LevelDefault {
				continue // a driver.Conn without BeginTx has no isolation levels
			}
			t.Run(fmt.Sprintf("%s/legacy=%v", iso, legacy), func(t *testing.T) {
				run := func(traced bool) ([]string, []core.Event) {
					rtFenceTable(t)
					rec, ctx := setup(t)
					db := rtOpen(t, &rtDriver{legacy: legacy}, tsql.WithCaptureTimeout(150*time.Millisecond))
					if !traced {
						ctx = context.Background()
					}
					return rtFenceScript(t, db, ctx, iso), byKind(rec.Events(), core.KindDBWrite)
				}
				want, none := run(false)
				got, evs := run(true)
				if len(none) != 0 {
					t.Fatalf("untraced run recorded %d events", len(none))
				}
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					t.Fatalf("traced run observed:\n%s\n\nuntraced run observed:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
				}
				t.Logf("observed (traced == untraced):\n%s", strings.Join(got, "\n"))
				// the captures did run: images where expected, the timeout flagged
				if len(evs) != 6 {
					t.Fatalf("write events: %d", len(evs))
				}
				up, slow := evs[1], evs[3]
				if len(up.Mutations) != 1 || jsonMap(t, up.Mutations[0].Before)["v"] != "a" || jsonMap(t, up.Mutations[0].After)["v"] != "x" {
					t.Fatalf("update images: %+v flags %v", up.Mutations, up.Flags)
				}
				if !hasFlag(slow, "image_timeout") {
					t.Fatalf("slow update flags %v", slow.Flags)
				}
				for _, e := range evs {
					if e.Err != "" || hasFlag(e, "image_failed") || hasFlag(e, "after_failed") {
						t.Fatalf("event %q: err %q flags %v", e.Name, e.Err, e.Flags)
					}
				}
			})
		}
	}
}

// In a transaction that is already aborted the capture gives up at its first
// statement, which the server refuses like any other: nothing follows it, in
// particular no ROLLBACK TO SAVEPOINT (it could revive the transaction if the
// application had a savepoint of the same name).
func TestPGFenceInAbortedTransactionSendsNothingMore(t *testing.T) {
	rtFenceTable(t)
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi_statement_proven=%v", warm), func(t *testing.T) {
			rec, ctx := setup(t)
			d := &rtDriver{}
			db := rtOpen(t, d)
			bg := context.Background()
			if warm { // a capture in autocommit shows this connection accepts two statements in one text
				if _, err := db.ExecContext(ctx, "UPDATE tsql_fence SET v = v WHERE id = $1", 2); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			rtMustExec(t, tx, "SAVEPOINT ts_capture") // the application's own, same name
			if _, err := tx.ExecContext(bg, "SELECT 1/0"); err == nil {
				t.Fatal("expected an error")
			}
			d.take()
			_, err = tx.ExecContext(ctx, "UPDATE tsql_fence SET v = $1 WHERE id = $2", "x", 1)
			if err == nil || !strings.Contains(err.Error(), "25P02") {
				t.Fatalf("application statement in an aborted transaction: %v", err)
			}
			calls := rtSQLs(d.take())
			if len(calls) != 2 || !strings.HasPrefix(calls[0], "exec: SAVEPOINT ts_capture") || !strings.HasPrefix(calls[1], "exec: UPDATE") {
				t.Fatalf("driver saw:\n%s", strings.Join(calls, "\n"))
			}
			if _, err := tx.ExecContext(bg, "SELECT 1"); err == nil || !strings.Contains(err.Error(), "25P02") {
				t.Fatalf("the transaction must still be aborted: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			ws := byKind(rec.Events(), core.KindDBWrite)
			if e := ws[len(ws)-1]; !hasFlag(e, "image_failed") || e.Err == "" {
				t.Fatalf("flags %v err %q", e.Flags, e.Err)
			}
			if got := rtShow(t, db, "SELECT v FROM tsql_fence WHERE id = 1"); got != "a" {
				t.Fatalf("v = %q", got)
			}
		})
	}
}

// A capture cancelled midway leaves the pooled connection as it was: no
// transaction open, no changed statement_timeout.
func TestPGFenceCancelledCaptureLeavesConnectionClean(t *testing.T) {
	rtFenceTable(t)
	clean := func(t *testing.T, db *sql.DB) {
		t.Helper()
		if got := rtShow(t, db, "SHOW statement_timeout"); got != "0" {
			t.Fatalf("statement_timeout = %q", got)
		}
		if got := rtShow(t, db, "SHOW transaction_read_only"); got != "off" {
			t.Fatalf("transaction_read_only = %q", got)
		}
		if _, err := db.ExecContext(context.Background(), "SAVEPOINT outside_tx"); err == nil || !strings.Contains(err.Error(), "25P01") {
			t.Fatalf("a transaction is still open on the connection (SAVEPOINT: %v)", err)
		}
		if _, err := db.ExecContext(context.Background(), "UPDATE tsql_fence SET v = v WHERE id = 1"); err != nil {
			t.Fatalf("connection not usable for a write: %v", err)
		}
	}

	// Cancelled between the fence and the capture SELECT: the driver refuses
	// the SELECT without touching the connection, so the fence must close what
	// it opened. The backend is looked at from a second connection at the
	// moment the application's statement arrives (which pgx then refuses too,
	// giving the connection up, as it does without tsql).
	t.Run("between statements", func(t *testing.T) {
		_, ctx := setup(t)
		d := &rtDriver{}
		db := rtOpen(t, d)
		pid := rtShow(t, db, "SELECT pg_backend_pid()::text")
		admin := rtRawConn(t)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		state := "application statement never reached the driver"
		d.hook = func(via, q string) {
			switch {
			case via == "query" && strings.HasPrefix(q, "SELECT * FROM tsql_fence"):
				cancel()
			case strings.HasPrefix(q, "UPDATE tsql_fence"):
				state = rtShow(t, admin, "SELECT state FROM pg_stat_activity WHERE pid = "+pid)
			}
		}
		_, err := db.ExecContext(cctx, "UPDATE tsql_fence SET v = $1 WHERE id = $2", "x", 1)
		if err == nil {
			t.Fatal("the application's statement ran with a cancelled context")
		}
		d.hook = nil
		t.Logf("driver saw:\n%s", strings.Join(rtSQLs(d.take()), "\n"))
		if state != "idle" {
			t.Fatalf("backend state when the application's statement arrived: %q, want idle (no transaction left open)", state)
		}
		clean(t, db)
	})

	// Cancelled while the capture SELECT is running: pgx gives the connection
	// up; the pool must hand out a working one.
	t.Run("during the capture", func(t *testing.T) {
		_, ctx := setup(t)
		db := rtOpen(t, &rtDriver{}, tsql.WithCaptureTimeout(5*time.Second))
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		time.AfterFunc(150*time.Millisecond, cancel)
		start := time.Now()
		_, err := db.ExecContext(cctx, "UPDATE tsql_fence SET v = 'x' WHERE id = 1 AND (SELECT pg_sleep(3)) IS NOT NULL")
		if err == nil {
			t.Fatal("the application's statement ran with a cancelled context")
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("the capture outlived the cancellation: %v", d)
		}
		clean(t, db)
	})

	// A capture cut by its own server-side timeout: the connection survives.
	t.Run("capture timeout", func(t *testing.T) {
		rec, ctx := setup(t)
		db := rtOpen(t, &rtDriver{}, tsql.WithCaptureTimeout(100*time.Millisecond))
		pid := rtShow(t, db, "SELECT pg_backend_pid()::text")
		if _, err := db.ExecContext(ctx, "UPDATE tsql_fence SET v = 'q' WHERE id = 2 AND (SELECT pg_sleep(0.4)) IS NOT NULL"); err != nil {
			t.Fatalf("the application's statement must not be cut by the capture timeout: %v", err)
		}
		if e := byKind(rec.Events(), core.KindDBWrite)[0]; !hasFlag(e, "image_timeout") {
			t.Fatalf("flags %v", e.Flags)
		}
		if got := rtShow(t, db, "SELECT pg_backend_pid()::text"); got != pid {
			t.Fatalf("connection replaced (%s -> %s)", pid, got)
		}
		clean(t, db)
	})
}
