package tsql_test

// Round trips per traced statement against a real Postgres. Set
// TSESSION_TEST_PG_DSN; skipped otherwise.
//
// rtDriver wraps the pgx stdlib driver and records every statement that
// reaches it (ExecContext, QueryContext, Prepare), so the exact sequence tsql
// sends around an application statement is visible:
//
//	TSESSION_TEST_PG_DSN=... go test -run TestPGRoundTrips -v .
//	TSESSION_TEST_PG_DSN=... go test -run xxx -bench BenchmarkPGRoundTrips .
//
// One driver call is one statement text handed to the driver. With pgx an
// argument-less Exec is one simple-protocol message; a Query with arguments is
// one extended-protocol exchange, preceded the first time a text is seen on a
// connection by a prepare exchange pgx keeps in its statement cache (not
// visible at this level; the same for every case below).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func rtDSN(tb testing.TB) string {
	tb.Helper()
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		tb.Skip("TSESSION_TEST_PG_DSN not set")
	}
	return dsn
}

// rtAdmin runs statements on a plain (untraced, uncounted) connection.
func rtAdmin(tb testing.TB, stmts ...string) {
	tb.Helper()
	db, err := sql.Open("pgx", rtDSN(tb))
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			tb.Fatalf("%s: %v", s, err)
		}
	}
}

// rtCall is one statement that reached the real driver.
type rtCall struct {
	Via string // exec | query | prepare
	SQL string
}

// rtDriver counts the statements reaching the pgx stdlib driver. With legacy
// set its connections expose only the driver.Conn methods (Prepare, Close,
// Begin), like a minimal driver: every statement then goes through Prepare,
// where Postgres rejects a text holding more than one statement.
type rtDriver struct {
	legacy bool
	hook   func(via, q string) // called before a statement is forwarded

	mu    sync.Mutex
	calls []rtCall
}

func (d *rtDriver) add(via, q string) {
	if d.hook != nil {
		d.hook(via, q)
	}
	d.mu.Lock()
	d.calls = append(d.calls, rtCall{via, q})
	d.mu.Unlock()
}

// take returns the calls seen since the last take.
func (d *rtDriver) take() []rtCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	cs := d.calls
	d.calls = nil
	return cs
}

func (d *rtDriver) Open(dsn string) (driver.Conn, error) {
	c, err := stdlib.GetDefaultDriver().Open(dsn)
	if err != nil {
		return nil, err
	}
	rc := &rtConn{d: d, in: c.(rtInner)}
	if d.legacy {
		return rtLegacyConn{rc}, nil
	}
	return rc, nil
}

// rtInner is what the pgx stdlib connection implements.
type rtInner interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.NamedValueChecker
}

type rtConn struct {
	d  *rtDriver
	in rtInner
}

func (c *rtConn) Close() error { return c.in.Close() }
func (c *rtConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *rtConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.in.BeginTx(ctx, o)
}
func (c *rtConn) Prepare(q string) (driver.Stmt, error) {
	c.d.add("prepare", q)
	return c.in.Prepare(q)
}
func (c *rtConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.d.add("prepare", q)
	return c.in.PrepareContext(ctx, q)
}
func (c *rtConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	c.d.add("exec", q)
	return c.in.ExecContext(ctx, q, a)
}
func (c *rtConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	c.d.add("query", q)
	return c.in.QueryContext(ctx, q, a)
}
func (c *rtConn) Ping(ctx context.Context) error              { return c.in.Ping(ctx) }
func (c *rtConn) ResetSession(ctx context.Context) error      { return c.in.ResetSession(ctx) }
func (c *rtConn) CheckNamedValue(nv *driver.NamedValue) error { return c.in.CheckNamedValue(nv) }

type rtLegacyConn struct{ c *rtConn }

func (l rtLegacyConn) Prepare(q string) (driver.Stmt, error) { return l.c.Prepare(q) }
func (l rtLegacyConn) Close() error                          { return l.c.Close() }
func (l rtLegacyConn) Begin() (driver.Tx, error)             { return l.c.Begin() }

// rtOpen opens the real Postgres through tsql over a counting driver, on a
// single pooled connection so every statement is seen in order.
func rtOpen(tb testing.TB, d *rtDriver, opts ...tsql.Option) *sql.DB {
	tb.Helper()
	dsn := rtDSN(tb)
	name := uniqueName("pgx-rt")
	sql.Register(name, d)
	db, err := tsql.Open(name, dsn, append([]tsql.Option{tsql.WithStore("postgres")}, opts...)...)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	tb.Cleanup(func() { db.Close() })
	return db
}

func rtSQLs(cs []rtCall) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Via + ": " + c.SQL
	}
	return out
}

// rtCase is one traced statement to measure; args returns the arguments of
// the i-th execution.
type rtCase struct {
	name     string
	captures int // image reads tsql adds: before and/or after
	stmt     string
	args     func(i int) []any
}

var rtCases = []rtCase{
	{"update", 2, "UPDATE tsql_rt SET status = $1 WHERE id = $2", func(i int) []any { return []any{fmt.Sprintf("s%d", i), 1} }},
	{"delete", 1, "DELETE FROM tsql_rt WHERE id = $1", func(i int) []any { return []any{10 + i} }},
	{"insert_explicit_key", 1, "INSERT INTO tsql_rt (id, status, note) VALUES ($1, $2, $3)", func(i int) []any { return []any{1000000 + i, "new", "n"} }},
	{"insert_serial_key", 0, "INSERT INTO tsql_rt_serial (status) VALUES ($1)", func(i int) []any { return []any{"new"} }},
	{"update_multi_row", 2, "UPDATE tsql_rt SET status = $1 WHERE note = $2", func(i int) []any { return []any{fmt.Sprintf("m%d", i), "grp"} }},
}

// rtWant is the number of statements a case sends to the driver: the
// application's own plus the fenced captures. A capture is three round trips
// (open the fence, SELECT, close it). Inside a transaction the first capture
// on a connection opens the fence in two, until the connection has shown
// that it takes two statements in one text. A driver that prepares every
// statement (legacy) cannot: there a capture is five statements in a
// transaction and four in autocommit, plus the one refused attempt per
// connection (followed in autocommit by a ROLLBACK).
func (c rtCase) rtWant(inTx, legacy bool) (first, steady int) {
	per, probe := 3, 0
	switch {
	case legacy && inTx:
		per, probe = 5, 1
	case legacy:
		per, probe = 4, 2
	case inTx:
		probe = 1
	}
	steady = 1 + per*c.captures
	if c.captures == 0 {
		probe = 0
	}
	return steady + probe, steady
}

// rtReset (re)creates the tables the cases write to: ids 1..3 with note
// "grp" (the multi-row update), ids 10.. for the deletes.
func rtReset(tb testing.TB, deletable int) {
	tb.Helper()
	rtAdmin(tb,
		"DROP TABLE IF EXISTS tsql_rt", "DROP TABLE IF EXISTS tsql_rt_serial",
		"CREATE TABLE tsql_rt (id int PRIMARY KEY, status text, note text)",
		"CREATE TABLE tsql_rt_serial (id serial PRIMARY KEY, status text)",
		"INSERT INTO tsql_rt VALUES (1,'new','grp'),(2,'new','grp'),(3,'new','grp')",
		fmt.Sprintf("INSERT INTO tsql_rt SELECT g, 'new', 'del' FROM generate_series(10, %d) g", 10+deletable))
}

func rtDrop(tb testing.TB) {
	rtAdmin(tb, "DROP TABLE IF EXISTS tsql_rt", "DROP TABLE IF EXISTS tsql_rt_serial")
}

type rtExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// rtMeasured is what one traced statement cost and recorded.
type rtMeasured struct {
	calls    []rtCall
	recorded string // mutations and flags of the write event, as JSON
}

// rtMeasure runs c twice on one connection (in one transaction when inTx) and
// returns what each execution sent to the driver: the first statement on a
// connection and the steady state can differ.
func rtMeasure(t *testing.T, c rtCase, inTx bool, legacy bool, opts ...tsql.Option) [2]rtMeasured {
	t.Helper()
	rtReset(t, 4)
	rec, ctx := setup(t)
	d := &rtDriver{legacy: legacy}
	db := rtOpen(t, d, opts...)
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var ex rtExecer = db
	var tx *sql.Tx
	if inTx {
		var err error
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			t.Fatal(err)
		}
		ex = tx
	}
	var out [2]rtMeasured
	for i := range out {
		d.take()
		before := len(byKind(rec.Events(), core.KindDBWrite))
		if _, err := ex.ExecContext(ctx, c.stmt, c.args(i)...); err != nil {
			t.Fatalf("%s: %v", c.stmt, err)
		}
		out[i].calls = d.take()
		ws := byKind(rec.Events(), core.KindDBWrite)
		if len(ws) != before+1 {
			t.Fatalf("%s recorded %d write events, want 1", c.name, len(ws)-before)
		}
		e := ws[len(ws)-1]
		js, err := json.Marshal(struct {
			Mutations []core.Mutation `json:"mutations"`
			Flags     []string        `json:"flags"`
		}{e.Mutations, e.Flags})
		if err != nil {
			t.Fatal(err)
		}
		out[i].recorded = string(js)
	}
	if inTx {
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// TestPGRoundTrips logs, for every case in autocommit and inside a
// transaction, the exact statements tsql sends to the driver around the
// application's statement, and what was recorded. Run with -v to see the table.
func TestPGRoundTrips(t *testing.T) {
	rtDSN(t)
	defer rtDrop(t)
	for _, c := range rtCases {
		for _, inTx := range []bool{false, true} {
			mode := "autocommit"
			if inTx {
				mode = "tx"
			}
			var recorded [2]string
			for _, legacy := range []bool{false, true} {
				name := c.name + "/" + mode
				if legacy {
					name += "/legacy_driver"
				}
				t.Run(name, func(t *testing.T) {
					m := rtMeasure(t, c, inTx, legacy)
					t.Logf("%-20s %-10s first on connection: %d round trips, steady state: %d", c.name, mode, len(m[0].calls), len(m[1].calls))
					for i, label := range []string{"first", "steady"} {
						if i == 1 && strings.Join(rtSQLs(m[0].calls), "\n") == strings.Join(rtSQLs(m[1].calls), "\n") {
							t.Logf("  steady: same sequence")
						} else {
							for n, s := range rtSQLs(m[i].calls) {
								t.Logf("  %s %d. %s", label, n+1, s)
							}
						}
						t.Logf("  %s recorded: %s", label, m[i].recorded)
					}
					first, steady := c.rtWant(inTx, legacy)
					if len(m[0].calls) != first || len(m[1].calls) != steady {
						t.Errorf("round trips: first %d, steady %d; want %d and %d", len(m[0].calls), len(m[1].calls), first, steady)
					}
					// what is recorded does not depend on how the fence is sent
					if !legacy {
						recorded = [2]string{m[0].recorded, m[1].recorded}
					} else if recorded != [2]string{m[0].recorded, m[1].recorded} {
						t.Errorf("recorded differs from the run with multi-statement support:\n%s\n%s", m[0].recorded, recorded[0])
					}
				})
			}
		}
	}
	t.Run("update/images_off", func(t *testing.T) {
		m := rtMeasure(t, rtCases[0], false, false, tsql.WithImages(false))
		t.Logf("update with WithImages(false): %d round trips: %v", len(m[1].calls), rtSQLs(m[1].calls))
		if len(m[0].calls) != 1 || len(m[1].calls) != 1 {
			t.Errorf("round trips without images: %d and %d, want 1", len(m[0].calls), len(m[1].calls))
		}
	})
}

// BenchmarkPGRoundTrips reports latency and driver round trips per traced
// statement against the real server. "untraced" runs the same statement with
// a context that belongs to no session.
func BenchmarkPGRoundTrips(b *testing.B) {
	rtDSN(b)
	defer rtDrop(b)
	for _, c := range rtCases {
		for _, mode := range []string{"untraced", "autocommit", "tx"} {
			b.Run(c.name+"/"+mode, func(b *testing.B) {
				rtReset(b, b.N+2)
				ctx := benchSetup()
				defer benchTeardown()
				d := &rtDriver{}
				db := rtOpen(b, d)
				var ex rtExecer = db
				var tx *sql.Tx
				if mode == "tx" {
					var err error
					if tx, err = db.BeginTx(ctx, nil); err != nil {
						b.Fatal(err)
					}
					ex = tx
				}
				sctx := ctx
				if mode == "untraced" {
					sctx = context.Background()
				}
				// The first statement warms the connection (and pgx's statement cache).
				if _, err := ex.ExecContext(sctx, c.stmt, c.args(0)...); err != nil {
					b.Fatal(err)
				}
				d.take()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := ex.ExecContext(sctx, c.stmt, c.args(i+1)...); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(d.take()))/float64(b.N), "rt/op")
				if tx != nil {
					if err := tx.Rollback(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
