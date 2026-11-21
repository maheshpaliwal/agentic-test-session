package tsql

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Error shapes of the Postgres drivers, without the drivers.

// pgx v5 (*pgconn.PgError) and lib/pq since 1.9 (*pq.Error).
type errWithSQLState struct{ code, msg string }

func (e *errWithSQLState) Error() string    { return e.msg }
func (e *errWithSQLState) SQLState() string { return e.code }

// lib/pq before 1.9: pq.Error{Code pq.ErrorCode; Message string} with
// Get(byte) and an Error() that prints the message only.
type pqErrorCode string

type errLikeOldPQ struct {
	Code    pqErrorCode
	Message string
}

func (e *errLikeOldPQ) Error() string { return "pq: " + e.Message }
func (e *errLikeOldPQ) Get(k byte) string {
	if k == 'C' {
		return string(e.Code)
	}
	return ""
}

// pgx v3: pgx.PgError, a value with a Code field and no method for it.
type errLikePgxV3 struct {
	Severity, Code, Message string
}

func (e errLikePgxV3) Error() string { return e.Severity + ": " + e.Message }

func TestTxAbortedReadsTheCodeHoweverTheDriverCarriesIt(t *testing.T) {
	// The server's message under lc_messages = de_DE: nothing in it to match.
	const german = "aktuelle Transaktion wurde abgebrochen, Befehle werden bis zum Ende der Transaktion ignoriert"
	const english = "current transaction is aborted, commands ignored until end of transaction block"
	for _, tc := range []struct {
		name    string
		err     error
		aborted bool
		state   string
	}{
		{"SQLState method", &errWithSQLState{"25P02", german}, true, "25P02"},
		{"SQLState method, other code", &errWithSQLState{"57014", english}, false, "57014"},
		{"SQLState method, wrapped", fmt.Errorf("exec: %w", &errWithSQLState{"25P02", german}), true, "25P02"},
		{"Get('C'), message without the code", &errLikeOldPQ{"25P02", german}, true, "25P02"},
		{"Get('C'), other code, message that reads aborted", &errLikeOldPQ{"42601", english}, false, "42601"},
		{"Code field", errLikePgxV3{"FEHLER", "25P02", german}, true, "25P02"},
		{"Code field, wrapped twice", fmt.Errorf("a: %w", fmt.Errorf("b: %w", errLikePgxV3{"ERROR", "25P02", german})), true, "25P02"},
		{"Code field, other code", errLikePgxV3{"ERROR", "40001", english}, false, "40001"},
		{"no code, code in the text", errors.New("FEHLER: " + german + " (SQLSTATE 25P02)"), true, ""},
		{"no code, English text", errors.New("pq: " + english), true, ""},
		{"no code, other text", errors.New("FEHLER: " + german), false, ""}, // nothing to go by
		{"client side", context.DeadlineExceeded, false, ""},
	} {
		state, ok := sqlState(tc.err)
		if state != tc.state || ok != (tc.state != "") {
			t.Errorf("%s: sqlState = %q, %v; want %q", tc.name, state, ok, tc.state)
		}
		if got := txAborted(tc.err); got != tc.aborted {
			t.Errorf("%s: txAborted = %v, want %v", tc.name, got, tc.aborted)
		}
	}
	if st := classify(&errLikeOldPQ{"57014", "Anfrage wurde storniert"}, context.Background()); st != capTimeout {
		t.Errorf("a statement timeout reported by its code only: status %d", st)
	}
}
