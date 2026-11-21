package tsql_test

// The fence's savepoint is tsql's own: its name cannot be an application's,
// and after a fence that failed to open nothing is rolled back that tsql did
// not take. Fake driver (which keeps the savepoints, like the server); the
// same on a real Postgres in fence_name_pg_test.go.

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
)

var fenceSavepointStmt = regexp.MustCompile(`SAVEPOINT (\w+)`)

// The savepoint is named per connection, with a random part: a fixed name
// could be the name of a savepoint of the application.
func TestFenceSavepointNameIsRandomPerConnection(t *testing.T) {
	_, ctx := setup(t)
	f := fenceFake(&fakeDB{})
	db := open(t, f, "postgres", noPKLookup)
	names := map[string]int{}
	var conns []*sql.Conn
	for range 3 {
		c, err := db.Conn(ctx)
		if err != nil {
			panic(err)
		}
		conns = append(conns, c) // held, so that every one is a connection of its own
		isoExec(t, ctx, c, "BEGIN", fenceUpdate, fenceUpdate, "COMMIT")
		f.mu.Lock()
		seen := map[string]bool{}
		for _, q := range f.rawSQL {
			for _, m := range fenceSavepointStmt.FindAllStringSubmatch(q, -1) {
				seen[m[1]] = true
			}
		}
		f.rawSQL = nil
		f.mu.Unlock()
		if len(seen) != 1 {
			panic(fmt.Sprintf("one connection used the savepoint names %v", seen))
		}
		for n := range seen {
			if !regexp.MustCompile(`^ts_capture_[0-9a-f]{16}$`).MatchString(n) {
				panic(fmt.Sprintf("savepoint name %q", n))
			}
			names[n]++
		}
	}
	for _, c := range conns {
		c.Close()
	}
	if len(names) != 3 {
		panic(fmt.Sprintf("three connections used the names %v", names))
	}
}

// fenceOpenNeverSent makes the next text that opens a fence in a transaction
// fail in the driver, before anything is sent: the transaction is untouched
// and tsql's savepoint does not exist.
func fenceOpenNeverSent(f *fakeDB) {
	f.mu.Lock()
	defer f.mu.Unlock()
	armed := true
	f.slow = func(q string) time.Duration {
		if armed && strings.HasPrefix(q, "SAVEPOINT ts_capture") && strings.Contains(q, "; ") {
			armed = false
			return time.Minute // the capture's context ends first
		}
		return 0
	}
}

// The review finding: the fence's opening text fails without the savepoint
// having been taken. Rolling back "to the fence's savepoint" then hit a
// savepoint of that name the application had, and undid the application's
// work; with no such savepoint it failed and aborted the transaction. Now the
// transaction is first seen to be alive, and then nothing is sent.
func TestFenceThatNeverOpenedTouchesNothing(t *testing.T) {
	for _, appSavepoint := range []bool{true, false} {
		name := "no_application_savepoint"
		if appSavepoint {
			name = "application_savepoint_named_ts_capture"
		}
		t.Run(name, func(t *testing.T) {
			_, ctx := setup(t)
			bg := context.Background()
			f := fenceFake(&fakeDB{abortModel: true})
			db := open(t, f, "postgres", noPKLookup, tsql.WithCaptureTimeout(20*time.Millisecond))
			db.SetMaxOpenConns(1)
			if _, err := db.ExecContext(ctx, fenceUpdate); err != nil { // the connection now takes two statements in one text
				panic(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				panic(err)
			}
			if appSavepoint {
				isoExec(t, bg, tx, "SAVEPOINT ts_capture")
			}
			isoExec(t, bg, tx, "INSERT INTO t (id, a) VALUES (9, 9)")
			take := fakeTake(f)
			take()
			fenceOpenNeverSent(f)
			if _, err := tx.ExecContext(ctx, fenceUpdate); err != nil {
				panic(fmt.Sprintf("the application's statement after a fence that did not open: %v", err))
			}
			got := take()
			want := []string{
				"SHOW transaction_isolation", // is the transaction alive? it is: there is nothing to close
				fenceUpdate,
				"SAVEPOINT ts_capture; " + strings.Replace(isoAsk, "'500'", "'20'", 1), "SELECT * FROM t WHERE id IN ($1)", "ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture",
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				panic(fmt.Sprintf("driver saw:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n")))
			}
			if appSavepoint {
				// the application's savepoint is still there, and still its own
				if _, err := tx.ExecContext(bg, "ROLLBACK TO SAVEPOINT ts_capture"); err != nil {
					panic(fmt.Sprintf("the application's ROLLBACK TO its savepoint: %v", err))
				}
				isoExec(t, bg, tx, "RELEASE SAVEPOINT ts_capture")
			}
			if err := tx.Commit(); err != nil {
				panic(fmt.Sprintf("COMMIT: %v", err))
			}
		})
	}
}
