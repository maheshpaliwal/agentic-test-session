package tsql_test

// SERIALIZABLE transactions against a real Postgres (set TSESSION_TEST_PG_DSN;
// skipped otherwise): whichever way the level is set, tsql reads nothing
// inside one, and a workload that succeeds without tsql succeeds with it.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// isoDSN returns the test DSN changed by f.
func isoDSN(t testing.TB, f func(u *url.URL, q url.Values)) string {
	t.Helper()
	u, err := url.Parse(rtDSN(t))
	if err != nil {
		panic(err)
	}
	q := u.Query()
	f(u, q)
	u.RawQuery = q.Encode()
	return u.String()
}

// isoSerializableDSN makes SERIALIZABLE the default of every session opened
// with it, as a connection string's options do.
func isoSerializableDSN(t testing.TB) string {
	return isoDSN(t, func(_ *url.URL, q url.Values) { q.Set("options", "-c default_transaction_isolation=serializable") })
}

// isoOpen opens dsn through tsql over a counting driver.
func isoOpen(t *testing.T, d *rtDriver, dsn string, opts ...tsql.Option) *sql.DB {
	t.Helper()
	name := uniqueName("pgx-iso")
	sql.Register(name, d)
	db, err := tsql.Open(name, dsn, append([]tsql.Option{tsql.WithStore("postgres")}, opts...)...)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func rtTake(d *rtDriver) func() []string {
	return func() []string {
		var out []string
		for _, c := range d.take() {
			out = append(out, c.SQL)
		}
		return out
	}
}

func isoTable(t *testing.T) {
	t.Helper()
	rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso", "CREATE TABLE tsql_iso (id int PRIMARY KEY, v text)",
		"INSERT INTO tsql_iso VALUES (1,'a'),(2,'b')")
	t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso") })
}

// The writes of one transaction: an UPDATE, an INSERT with its key and a
// DELETE, which between them make tsql look up the key and read before and
// after images.
var isoWrites = []string{
	"UPDATE tsql_iso SET v = v || 'x' WHERE id = 1",
	"INSERT INTO tsql_iso (id, v) VALUES (7, 'n')",
	"DELETE FROM tsql_iso WHERE id = 7",
}

func isoLevel(t *testing.T, want string) func(rtQueryer) {
	return func(q rtQueryer) {
		t.Helper()
		if got := rtShow(t, q, "SHOW transaction_isolation"); got != want {
			panic(fmt.Sprintf("the transaction runs at %q, want %q", got, want))
		}
	}
}

// Every way a statement can make a transaction SERIALIZABLE: the driver is
// sent the application's statements and nothing else.
func TestPGSerializableSetByStatementSendsNoCapture(t *testing.T) {
	rtDSN(t)
	for _, ic := range pgIsoCases {
		for _, traced := range []bool{true, false} {
			name := ic.name + "/control_untraced"
			if traced {
				name = ic.name + "/control_traced"
			}
			t.Run(name, func(t *testing.T) {
				isoTable(t)
				rec, ctx := setup(t)
				ctl := context.Background()
				if traced {
					ctl = ctx
				}
				d := &rtDriver{}
				c, err := isoOpen(t, d, rtDSN(t)).Conn(ctx)
				if err != nil {
					panic(err)
				}
				defer c.Close()
				take := rtTake(d)
				got := isoRunCheck(t, c, ic, ctx, ctl, take, isoLevel(t, "serializable"), isoWrites...)
				if strings.Join(got, "\n") != strings.Join(isoWrites, "\n") {
					panic(fmt.Sprintf("driver saw:\n%s\nwant only the application's statements", strings.Join(got, "\n")))
				}
				ws := byKind(rec.Events(), core.KindDBWrite)
				if len(ws) != len(isoWrites) {
					panic(fmt.Sprintf("%d write events, want %d", len(ws), len(isoWrites)))
				}
				for _, e := range ws {
					if e.Err != "" || len(e.Mutations) != 1 {
						panic(fmt.Sprintf("event %q: err %q mutations %+v", e.Name, e.Err, e.Mutations))
					}
					if e.Mutations[0].Op != "insert" && !hasFlag(e, "image_skipped:serializable") {
						panic(fmt.Sprintf("event %q: flags %v", e.Name, e.Flags))
					}
				}
				// the next transaction, at another level, is read again
				isoExec(t, ctl, c, ic.after...)
				next := isoCase{begin: ic.begin}
				if strings.Contains(strings.ToUpper(ic.begin), "SERIALIZABLE") {
					next.begin = "BEGIN ISOLATION LEVEL REPEATABLE READ"
				}
				got = isoRun(t, c, next, ctx, ctl, take, isoWrites...)
				if isoReads(got) != 4 {
					panic(fmt.Sprintf("the transaction after it ran %d image reads, want 4:\n%s", isoReads(got), strings.Join(got, "\n")))
				}
			})
		}
	}
}

// A SERIALIZABLE default that no statement shows: set by the connection
// string's options, or for the role. The first transaction on a connection
// opens one fence that asks the server and reads nothing; after that nothing
// is sent, also once the connection has been back in the pool.
func TestPGSerializableDefaultSendsNoCapture(t *testing.T) {
	rtDSN(t)
	const role = "tsql_iso_role"
	for _, how := range []string{"connection_options", "role_setting"} {
		t.Run(how, func(t *testing.T) {
			isoTable(t)
			dsn := isoSerializableDSN(t)
			if how == "role_setting" {
				rtAdmin(t, "DROP ROLE IF EXISTS "+role, "CREATE ROLE "+role+" LOGIN",
					"ALTER ROLE "+role+" SET default_transaction_isolation = 'serializable'", "GRANT ALL ON tsql_iso TO "+role)
				t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso", "DROP ROLE IF EXISTS "+role) })
				dsn = isoDSN(t, func(u *url.URL, _ url.Values) { u.User = url.User(role) })
			}
			rec, ctx := setup(t)
			d := &rtDriver{}
			db := isoOpen(t, d, dsn)
			db.SetMaxOpenConns(1)
			take := rtTake(d)
			run := func(raw bool) []string {
				t.Helper()
				c, err := db.Conn(ctx)
				if err != nil {
					panic(err)
				}
				defer c.Close() // back to the pool
				ic := isoCase{}
				if raw {
					ic.begin = "BEGIN"
				}
				return isoRunCheck(t, c, ic, ctx, ctx, take, isoLevel(t, "serializable"), isoWrites...)
			}
			got := run(false)
			var app []string
			asks := 0
			for _, q := range got {
				switch {
				case strings.Contains(q, isoAsk):
					asks++
				case isFence(q):
				default:
					app = append(app, q)
				}
			}
			if asks != 1 || strings.Join(app, "\n") != strings.Join(isoWrites, "\n") {
				panic(fmt.Sprintf("first transaction on the connection, driver saw:\n%s\nwant one fence that asks, and the application's statements", strings.Join(got, "\n")))
			}
			t.Logf("first transaction on the connection:\n  %s", strings.Join(got, "\n  "))
			for i, raw := range []bool{false, true, false} {
				if got := run(raw); strings.Join(got, "\n") != strings.Join(isoWrites, "\n") {
					panic(fmt.Sprintf("transaction %d, driver saw:\n%s\nwant only the application's statements", i+2, strings.Join(got, "\n")))
				}
			}
			for _, e := range byKind(rec.Events(), core.KindDBWrite) {
				if e.Err != "" || e.Mutations[0].Op != "insert" && !hasFlag(e, "image_skipped:serializable") {
					panic(fmt.Sprintf("event %q: err %q flags %v", e.Name, e.Err, e.Flags))
				}
			}
			// In autocommit the capture has a transaction of its own and the
			// images are read (see
			// TestPGAutocommitCaptureUnderSerializableDefaultIsHarmless).
			take()
			if _, err := db.ExecContext(ctx, isoWrites[0]); err != nil {
				panic(err)
			}
			got = take()
			e := lastWrite(t, rec)
			if isoReads(got) != 2 || !strings.HasPrefix(got[0], "BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED") ||
				len(e.Mutations) != 1 || e.Mutations[0].Before.IsZero() || e.Mutations[0].After.IsZero() || len(e.Flags) != 0 {
				panic(fmt.Sprintf("autocommit: flags %v, driver saw:\n%s", e.Flags, strings.Join(got, "\n")))
			}
			// A transaction that states its level is read, without asking.
			c, err := db.Conn(ctx)
			if err != nil {
				panic(err)
			}
			defer c.Close()
			got = isoRunCheck(t, c, isoCase{begin: "BEGIN ISOLATION LEVEL READ COMMITTED"}, ctx, ctx, take, isoLevel(t, "read committed"), isoWrites...)
			if isoReads(got) != 4 || strings.Contains(strings.Join(got, "\n"), isoAsk) {
				panic(fmt.Sprintf("READ COMMITTED transaction, driver saw:\n%s", strings.Join(got, "\n")))
			}
		})
	}
}

// isoWay is one way to open a SERIALIZABLE transaction on a connection.
type isoWay struct {
	name string
	dsn  func(tb testing.TB) string
	open func(t *testing.T, c *sql.Conn) (rtQueryer, func() error)
}

func isoBeginTx(opts *sql.TxOptions, first ...string) func(*testing.T, *sql.Conn) (rtQueryer, func() error) {
	return func(t *testing.T, c *sql.Conn) (rtQueryer, func() error) {
		t.Helper()
		tx, err := c.BeginTx(context.Background(), opts)
		if err != nil {
			panic(err)
		}
		rtMustExec(t, tx, first...)
		return tx, tx.Commit
	}
}

var isoWays = []isoWay{
	{"begin_tx_option", rtDSN, isoBeginTx(&sql.TxOptions{Isolation: sql.LevelSerializable})},
	{"begin_statement", rtDSN, func(t *testing.T, c *sql.Conn) (rtQueryer, func() error) {
		rtMustExec(t, c, "BEGIN ISOLATION LEVEL SERIALIZABLE")
		return c, func() error { _, err := c.ExecContext(context.Background(), "COMMIT"); return err }
	}},
	{"set_transaction", rtDSN, isoBeginTx(nil, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")},
	{"session_characteristics", rtDSN, func(t *testing.T, c *sql.Conn) (rtQueryer, func() error) {
		rtMustExec(t, c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		return isoBeginTx(nil)(t, c)
	}},
	{"default_from_connection_options", isoSerializableDSN, isoBeginTx(nil)},
}

// Two SERIALIZABLE transactions that each insert a row of their own do not
// conflict: neither reads. With tsql's read of the inserted row in each they
// would (each read misses the other's row), and one of them would fail with a
// serialization error. Traced, they succeed as they do untraced.
func TestPGSerializableConcurrentTransactionsSucceedTraced(t *testing.T) {
	rtDSN(t)
	for _, w := range isoWays {
		for _, traced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/traced=%v", w.name, traced), func(t *testing.T) {
				// no primary key: the re-read by the default key "id" is a scan of the table
				rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso_c", "CREATE TABLE tsql_iso_c (id int, v text)")
				t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso_c") })
				rec, ctx := setup(t)
				if !traced {
					ctx = context.Background()
				}
				db := isoOpen(t, &rtDriver{}, w.dsn(t))
				var ex [2]rtQueryer
				var commit [2]func() error
				for i := range ex {
					c, err := db.Conn(context.Background())
					if err != nil {
						panic(err)
					}
					defer c.Close()
					ex[i], commit[i] = w.open(t, c)
				}
				for i := range ex {
					if _, err := ex[i].ExecContext(ctx, "INSERT INTO tsql_iso_c (id, v) VALUES ($1, $2)", i+1, "x"); err != nil {
						panic(fmt.Sprintf("transaction %d: INSERT: %v", i+1, err))
					}
					isoLevel(t, "serializable")(ex[i])
				}
				for i := range commit {
					if err := commit[i](); err != nil {
						panic(fmt.Sprintf("transaction %d: COMMIT: %v", i+1, err))
					}
				}
				if n := len(byKind(rec.Events(), core.KindDBWrite)); traced && n != 2 || !traced && n != 0 {
					panic(fmt.Sprintf("%d write events", n))
				}
			})
		}
	}
}

// In autocommit a capture runs in a transaction of its own, which a
// SERIALIZABLE session default makes SERIALIZABLE. Its read does not fail a
// concurrent transaction, because the capture's transaction is rolled back
// and the reads of a transaction that rolled back count for nothing. The
// scenario is the one in which a committed read-only transaction would (the
// read-only anomaly: T reads ctl, U updates ctl and commits, the capture
// reads r, T inserts into r); T commits with tsql as without.
func TestPGAutocommitCaptureUnderSerializableDefaultIsHarmless(t *testing.T) {
	rtDSN(t)
	for _, traced := range []bool{false, true} {
		t.Run(fmt.Sprintf("traced=%v", traced), func(t *testing.T) {
			rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso_r", "DROP TABLE IF EXISTS tsql_iso_ctl",
				"CREATE TABLE tsql_iso_r (id int, v text)", "CREATE TABLE tsql_iso_ctl (v int)", "INSERT INTO tsql_iso_ctl VALUES (1)")
			t.Cleanup(func() { rtAdmin(t, "DROP TABLE IF EXISTS tsql_iso_r", "DROP TABLE IF EXISTS tsql_iso_ctl") })
			rec, ctx := setup(t)
			if !traced {
				ctx = context.Background()
			}
			db := isoOpen(t, &rtDriver{}, isoSerializableDSN(t))
			other, third := rtRawConn(t), rtRawConn(t)
			rtMustExec(t, other, "BEGIN ISOLATION LEVEL SERIALIZABLE")
			if got := rtShow(t, other, "SELECT v FROM tsql_iso_ctl"); got != "1" {
				panic(fmt.Sprintf("ctl = %s", got))
			}
			rtMustExec(t, third, "BEGIN ISOLATION LEVEL SERIALIZABLE", "UPDATE tsql_iso_ctl SET v = v + 1", "COMMIT")
			if _, err := db.ExecContext(ctx, "INSERT INTO tsql_iso_r (id, v) VALUES ($1, $2)", 1, "a"); err != nil {
				panic(err)
			}
			if _, err := other.ExecContext(context.Background(), "INSERT INTO tsql_iso_r (id, v) VALUES (2, 'b')"); err != nil {
				panic(fmt.Sprintf("the other connection's INSERT: %v", err))
			}
			if _, err := other.ExecContext(context.Background(), "COMMIT"); err != nil {
				panic(fmt.Sprintf("the other connection's COMMIT: %v", err))
			}
			if traced {
				e := lastWrite(t, rec)
				if len(e.Mutations) != 1 || e.Mutations[0].After.IsZero() || hasFlag(e, "after_from_args") {
					panic(fmt.Sprintf("the INSERT's row was not read back: flags %v", e.Flags))
				}
			}
		})
	}
}
