package tsql_test

// A fence that fails to open, against a real Postgres (set
// TSESSION_TEST_PG_DSN; skipped otherwise): whether or not its savepoint was
// taken, the application's transaction, its own savepoints included, is
// exactly what it is without tsql.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestPGFenceThatFailedToOpenLeavesTheTransactionAlone(t *testing.T) {
	rtDSN(t)
	// How the text that opens the fence fails, once.
	opening := func(q string) bool { return strings.HasPrefix(q, "SAVEPOINT ts_capture") && strings.Contains(q, "; ") }
	modes := map[string]func(q string) (string, error){
		// in the driver, before anything is sent: no savepoint, transaction untouched
		"never_sent": func(string) (string, error) { return "", errors.New("injected: the statement was not sent") },
		// on the server, after the savepoint was taken: its subtransaction is aborted
		"fails_after_the_savepoint": func(q string) (string, error) { return q + "; SELECT 1/0", nil },
	}
	for mode, interfere := range modes {
		for _, appSavepoint := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/application_savepoint=%v", mode, appSavepoint), func(t *testing.T) {
				run := func(traced bool) ([]string, []core.Event, []string) {
					rtAdmin(t, "DROP TABLE IF EXISTS tsql_fn", "CREATE TABLE tsql_fn (id int PRIMARY KEY, v text)", "INSERT INTO tsql_fn VALUES (1,'a')")
					rec, ctx := setup(t)
					bg := context.Background()
					if !traced {
						ctx = bg
					}
					var armed atomic.Bool
					d := &rtDriver{}
					d.rewrite = func(q string) (string, error) {
						if opening(q) && armed.CompareAndSwap(true, false) {
							return interfere(q)
						}
						return q, nil
					}
					db := rtOpen(t, d)
					// the connection now takes two statements in one text
					if _, err := db.ExecContext(ctx, "UPDATE tsql_fn SET v = v WHERE id = 1"); err != nil {
						panic(err)
					}
					var obs []string
					note := func(what string, err error) { obs = append(obs, fmt.Sprintf("%s: %v", what, err)) }
					rows := func(q rtQueryer) string {
						return rtShow(t, q, "SELECT string_agg(id || '=' || v, ' ' ORDER BY id) FROM tsql_fn")
					}
					tx, err := db.BeginTx(bg, nil)
					if err != nil {
						panic(err)
					}
					rtMustExec(t, tx, "INSERT INTO tsql_fn VALUES (2,'b')")
					if appSavepoint {
						rtMustExec(t, tx, "SAVEPOINT ts_capture") // the application's own
					}
					rtMustExec(t, tx, "INSERT INTO tsql_fn VALUES (3,'c')")
					d.take()
					armed.Store(true)
					_, err = tx.ExecContext(ctx, "UPDATE tsql_fn SET v = 'x' WHERE id = 1")
					note("update", err)
					sent := rtSQLs(d.take())
					if traced && armed.Load() {
						panic("the fence was not interfered with")
					}
					armed.Store(false)
					if err == nil {
						obs = append(obs, "rows after the update: "+rows(tx))
					}
					if appSavepoint {
						_, err = tx.ExecContext(bg, "ROLLBACK TO SAVEPOINT ts_capture")
						note("the application's rollback to its savepoint", err)
						if err == nil {
							obs = append(obs, "rows after it: "+rows(tx))
						}
					}
					note("commit", tx.Commit())
					c := rtRawConn(t)
					obs = append(obs, "rows in the end: "+rows(c))
					rtAdmin(t, "DROP TABLE IF EXISTS tsql_fn")
					return obs, byKind(rec.Events(), core.KindDBWrite), sent
				}
				want, _, _ := run(false)
				got, evs, sent := run(true)
				t.Logf("traced, the driver was sent:\n  %s", strings.Join(sent, "\n  "))
				if strings.Join(got, "\n") != strings.Join(want, "\n") {
					panic(fmt.Sprintf("traced run observed:\n%s\n\nuntraced run observed:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n")))
				}
				t.Logf("observed (traced == untraced):\n%s", strings.Join(got, "\n"))
				e := evs[len(evs)-1]
				if e.Err != "" || !hasFlag(e, "image_failed") || len(e.Mutations) != 1 || !e.Mutations[0].Before.IsZero() {
					panic(fmt.Sprintf("the update's event: err %q flags %v mutations %+v", e.Err, e.Flags, e.Mutations))
				}
			})
		}
	}
}

// The name tsql sends to the server: its own, per connection.
func TestPGFenceSavepointName(t *testing.T) {
	rtFenceTable(t)
	_, ctx := setup(t)
	d := &rtDriver{}
	db := rtOpen(t, d)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE tsql_fence SET v = 'x' WHERE id = 1"); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	names := map[string]bool{}
	for _, q := range d.raw {
		for _, m := range regexp.MustCompile(`SAVEPOINT (\w+)`).FindAllStringSubmatch(q, -1) {
			names[m[1]] = true
		}
	}
	if len(names) != 1 {
		panic(fmt.Sprintf("savepoint names sent: %v", names))
	}
	for n := range names {
		if !regexp.MustCompile(`^ts_capture_[0-9a-f]{16}$`).MatchString(n) {
			panic(fmt.Sprintf("savepoint name %q", n))
		}
	}
}
