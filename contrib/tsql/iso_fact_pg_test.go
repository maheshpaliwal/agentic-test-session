package tsql_test

// What asking for the isolation level relies on, checked against a real
// Postgres with the plain pgx driver (set TSESSION_TEST_PG_DSN).

import (
	"context"
	"strings"
	"testing"
)

// The statement that sets the fence's timeout in a transaction of unknown
// level: outside SERIALIZABLE it sets the timeout exactly as SET LOCAL does
// (in force for the next statement, reverted by ROLLBACK TO the savepoint)
// and reports one row; inside SERIALIZABLE it sets nothing and reports none.
// The count is the one an Exec returns, alone and as the last statement of a
// text. It takes no predicate lock: it reads no table.
func TestPGFactAskingTheLevelWhileSettingTheTimeout(t *testing.T) {
	rtDSN(t)
	for _, tc := range []struct {
		begin string
		rows  int64
		set   string // statement_timeout inside the fence
	}{
		{"BEGIN", 1, "500ms"},
		{"BEGIN ISOLATION LEVEL REPEATABLE READ", 1, "500ms"},
		{"BEGIN ISOLATION LEVEL SERIALIZABLE", 0, "7s"},
	} {
		for _, text := range []string{isoAsk, "SAVEPOINT s; " + isoAsk} {
			c := rtRawConn(t)
			rtMustExec(t, c, "SET statement_timeout = '7s'", tc.begin)
			if !strings.HasPrefix(text, "SAVEPOINT") {
				rtMustExec(t, c, "SAVEPOINT s")
			}
			res, err := c.ExecContext(context.Background(), text)
			if err != nil {
				t.Fatalf("%s: %v", tc.begin, err)
			}
			if n, err := res.RowsAffected(); err != nil || n != tc.rows {
				t.Fatalf("%s: %q reported %d rows (%v), want %d", tc.begin, text, n, err, tc.rows)
			}
			if got := rtShow(t, c, "SHOW statement_timeout"); got != tc.set {
				t.Fatalf("%s: statement_timeout inside the fence = %s, want %s", tc.begin, got, tc.set)
			}
			if tc.rows == 1 {
				if _, err := c.ExecContext(context.Background(), "SELECT pg_sleep(2)"); err == nil || !strings.Contains(err.Error(), "57014") {
					t.Fatalf("%s: the timeout is not in force: %v", tc.begin, err)
				}
			}
			rtMustExec(t, c, "ROLLBACK TO SAVEPOINT s; RELEASE SAVEPOINT s")
			if got := rtShow(t, c, "SHOW statement_timeout"); got != "7s" {
				t.Fatalf("%s: statement_timeout after the fence = %s, want 7s", tc.begin, got)
			}
			if got := rtShow(t, c, "SELECT count(*) FROM pg_locks WHERE mode = 'SIReadLock' AND pid = pg_backend_pid()"); got != "0" {
				t.Fatalf("%s: %s predicate locks", tc.begin, got)
			}
			rtMustExec(t, c, "COMMIT")
		}
	}
}
