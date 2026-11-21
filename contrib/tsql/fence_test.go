package tsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// fenceFake answers the two captures of "UPDATE t SET a = 1 WHERE id = 2".
func fenceFake(f *fakeDB) *fakeDB {
	n := 0
	f.handler = func(q string, _ []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			n++
			return result{cols: []string{"id", "a"}, rows: [][]driver.Value{{int64(2), int64(n % 2)}}}
		}
		return result{affected: 1}
	}
	return f
}

const fenceUpdate = "UPDATE t SET a = 1 WHERE id = 2"

func fenceRecorded(t *testing.T, rec *core.MemRecorder) string {
	t.Helper()
	var out []string
	for _, e := range byKind(rec.Events(), core.KindDBWrite) {
		b, err := json.Marshal(struct {
			M []core.Mutation
			F []string
		}{e.Mutations, e.Flags})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(b))
	}
	return strings.Join(out, "\n")
}

func fenceWant(t *testing.T, f *fakeDB, want ...string) {
	t.Helper()
	if got := sqls(f.RawCalls()); got != strings.Join(want, "\n")+"\n" {
		t.Fatalf("driver saw:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}

// In autocommit the fence is three round trips per capture: its read-only
// transaction and the timeout open in one text.
func TestCaptureFenceAutocommitSequence(t *testing.T) {
	_, ctx := setup(t)
	f := fenceFake(&fakeDB{})
	db := open(t, f, "postgres", noPKLookup) // the lookup has its own fence tests
	if _, err := db.ExecContext(ctx, fenceUpdate); err != nil {
		t.Fatal(err)
	}
	fenceWant(t, f,
		"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500",
		"SELECT * FROM t WHERE id = 2 LIMIT 51",
		"ROLLBACK",
		fenceUpdate,
		"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500",
		"SELECT * FROM t WHERE id IN ($1)",
		"ROLLBACK",
	)
}

// A driver that cannot send two statements in one text gets the fence
// statement by statement, after one refused attempt per connection made where
// the refusal is harmless; what is recorded is the same either way.
func TestCaptureFenceWithoutMultiStatementSupport(t *testing.T) {
	run := func(t *testing.T, noMulti, inTx bool) (*fakeDB, string) {
		rec, ctx := setup(t)
		// the fake models an aborted transaction, not an aborted statement in autocommit
		f := fenceFake(&fakeDB{noMulti: noMulti, abortModel: inTx})
		db := open(t, f, "postgres", noPKLookup)
		db.SetMaxOpenConns(1)
		var ex interface {
			ExecContext(context.Context, string, ...any) (sql.Result, error)
		} = db
		var tx *sql.Tx
		if inTx {
			tx, _ = db.BeginTx(ctx, nil)
			ex = tx
		}
		for range 2 {
			if _, err := ex.ExecContext(ctx, fenceUpdate); err != nil {
				t.Fatalf("application statement: %v", err)
			}
		}
		if inTx {
			if err := tx.Commit(); err != nil {
				t.Fatalf("COMMIT: %v", err)
			}
		}
		return f, fenceRecorded(t, rec)
	}

	t.Run("transaction", func(t *testing.T) {
		f, got := run(t, true, true)
		one := []string{
			"SAVEPOINT ts_capture", "SET LOCAL statement_timeout = 500", "SELECT * FROM t WHERE id = 2 LIMIT 51",
			"ROLLBACK TO SAVEPOINT ts_capture", "RELEASE SAVEPOINT ts_capture",
			fenceUpdate,
			"SAVEPOINT ts_capture", "SET LOCAL statement_timeout = 500", "SELECT * FROM t WHERE id IN ($1)",
			"ROLLBACK TO SAVEPOINT ts_capture", "RELEASE SAVEPOINT ts_capture",
		}
		// the one attempt: the closing pair of the first capture, under its savepoint
		// (the first fence of a transaction also asks for its isolation level)
		first := []string{one[0], isoAsk, one[2]}
		first = append(first, "ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture")
		first = append(first, one[3:]...)
		fenceWant(t, f, append(first, one...)...)
		if _, want := run(t, false, true); got != want {
			t.Fatalf("recorded:\n%s\nwith multi-statement support:\n%s", got, want)
		}
	})
	t.Run("autocommit", func(t *testing.T) {
		f, got := run(t, true, false)
		one := []string{
			"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED", "SET LOCAL statement_timeout = 500", "SELECT * FROM t WHERE id = 2 LIMIT 51", "ROLLBACK",
			fenceUpdate,
			"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED", "SET LOCAL statement_timeout = 500", "SELECT * FROM t WHERE id IN ($1)", "ROLLBACK",
		}
		// the one attempt: the opening pair, outside any transaction, then a
		// ROLLBACK in case its BEGIN did run
		first := append([]string{"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500", "ROLLBACK"}, one...)
		fenceWant(t, f, append(first, one...)...)
		if _, want := run(t, false, false); got != want {
			t.Fatalf("recorded:\n%s\nwith multi-statement support:\n%s", got, want)
		}
	})
}

// When the opening text fails inside a transaction that is already aborted,
// nothing else is sent: no ROLLBACK TO SAVEPOINT that could reach a savepoint
// of the application.
func TestCaptureFenceSendsNothingAfterRefusalInAbortedTx(t *testing.T) {
	rec, ctx := setup(t)
	aborted := false
	f := fenceFake(&fakeDB{})
	inner := f.handler
	f.handler = func(q string, a []driver.Value) result {
		if aborted {
			return result{err: errors.New("ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)")}
		}
		return inner(q, a)
	}
	db := open(t, f, "postgres")
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, fenceUpdate); err != nil { // the connection now takes two statements in one text
		t.Fatal(err)
	}
	tx, _ := db.BeginTx(ctx, nil)
	f.mu.Lock()
	f.calls, aborted = nil, true
	f.mu.Unlock()
	if _, err := tx.ExecContext(ctx, fenceUpdate); err == nil {
		t.Fatal("the application's statement must fail")
	}
	fenceWant(t, f, "SAVEPOINT ts_capture; "+isoAsk, fenceUpdate)
	ws := byKind(rec.Events(), core.KindDBWrite)
	if e := ws[len(ws)-1]; !hasFlag(e, "image_failed") {
		t.Fatalf("flags %v", e.Flags)
	}
}

// Any other failure of the opening text inside a transaction may have left
// the savepoint behind with its subtransaction aborted. The transaction no
// longer takes a statement then, which is how tsql knows, and the fence is
// closed. (When it still does, see TestFenceThatNeverOpenedTouchesNothing.)
func TestCaptureFenceClosesAfterFailedOpenInTx(t *testing.T) {
	_, ctx := setup(t)
	fail := false
	f := fenceFake(&fakeDB{abortModel: true})
	inner := f.handler
	f.handler = func(q string, a []driver.Value) result {
		if fail && strings.HasPrefix(q, "SAVEPOINT ts_capture; ") {
			fail = false
			return result{err: errors.New("ERROR: canceling statement due to user request (SQLSTATE 57014)")}
		}
		return inner(q, a)
	}
	db := open(t, f, "postgres")
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, fenceUpdate); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.BeginTx(ctx, nil)
	f.mu.Lock()
	f.calls, fail = nil, true
	f.mu.Unlock()
	if _, err := tx.ExecContext(ctx, fenceUpdate); err != nil {
		t.Fatalf("application statement after a failed capture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	fenceWant(t, f,
		"SAVEPOINT ts_capture; "+isoAsk,
		"SHOW transaction_isolation",
		"ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture",
		fenceUpdate,
		"SAVEPOINT ts_capture; "+isoAsk, // the first one was not answered
		"SELECT * FROM t WHERE id IN ($1)",
		"ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture",
	)
}
