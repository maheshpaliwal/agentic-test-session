package tsql_test

// An UPDATE recorded with its after image only, against a real Postgres (set
// TSESSION_TEST_PG_DSN; skipped otherwise).

import (
	"fmt"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
)

func TestPGUpdateWithoutBeforeImageRecordsNoDiff(t *testing.T) {
	rtDSN(t)
	reset := func(t *testing.T) {
		rtAdmin(t, "DROP TABLE IF EXISTS tsql_nb", "CREATE TABLE tsql_nb (id int PRIMARY KEY, v text, n int)",
			"INSERT INTO tsql_nb VALUES (1,'a',1),(2,'b',2)")
		t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_nb") })
	}
	// The before image cannot be read by replaying the statement (it has no
	// predicate), and the statement returns the rows it wrote.
	t.Run("returning_rows", func(t *testing.T) {
		reset(t)
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t))
		if err != nil {
			panic(err)
		}
		defer db.Close()
		rs, err := db.QueryContext(ctx, "UPDATE tsql_nb SET v = v || 'x' RETURNING *")
		if err != nil {
			panic(err)
		}
		for rs.Next() {
		}
		if err := rs.Close(); err != nil {
			panic(err)
		}
		noBeforeCheck(t, lastWrite(t, rec), "image_skipped", 2)
	})
	// The read before the write runs into the capture timeout; the row is
	// read back by its key afterwards.
	t.Run("before_image_times_out", func(t *testing.T) {
		reset(t)
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", rtDSN(t), tsql.WithCaptureTimeout(50*time.Millisecond))
		if err != nil {
			panic(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "UPDATE tsql_nb SET v = 'z' WHERE id = 1 AND pg_sleep(0.3) IS NOT NULL"); err != nil {
			panic(err)
		}
		e := lastWrite(t, rec)
		noBeforeCheck(t, e, "image_timeout", 1)
		if got := jsonMap(t, e.Mutations[0].After)["v"]; got != "z" {
			panic(fmt.Sprintf("after image v = %v", got))
		}
	})
}
