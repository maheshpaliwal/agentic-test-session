package tsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"tsession/contrib/tsql"
	"tsession/core"
)

// call is one statement the fake database saw.
type call struct {
	SQL  string
	Args []driver.Value
	Via  string // exec | query | prepare
}

type result struct {
	cols     []string
	rows     [][]driver.Value
	affected int64
	lastID   int64
	err      error
}

// fakeDB is a tiny in-process database/sql driver: it records every statement
// and answers from a handler.
type fakeDB struct {
	mu      sync.Mutex
	calls   []call
	handler func(q string, args []driver.Value) result
	plain   bool // conn implements only the legacy Prepare path
	skipCtx bool // ExecerContext/QueryerContext answer ErrSkip
	commits int
	rbacks  int
}

func (f *fakeDB) record(c call) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
}

func (f *fakeDB) Calls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeDB) answer(q string, args []driver.Value) result {
	if f.handler == nil {
		return result{}
	}
	return f.handler(q, args)
}

func (f *fakeDB) Open(string) (driver.Conn, error) {
	c := &fakeConn{db: f}
	if f.plain {
		return plainConn{c}, nil
	}
	return c, nil
}

type plainConn struct{ c *fakeConn }

func (p plainConn) Prepare(q string) (driver.Stmt, error) { return p.c.Prepare(q) }
func (p plainConn) Close() error                          { return nil }
func (p plainConn) Begin() (driver.Tx, error)             { return p.c.Begin() }

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(q string) (driver.Stmt, error) { return &fakeStmt{c: c, q: q}, nil }
func (c *fakeConn) Close() error                          { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)             { return fakeTx{c.db}, nil }
func (c *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return fakeTx{c.db}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	if c.db.skipCtx {
		return nil, driver.ErrSkip
	}
	vals := values(a)
	c.db.record(call{q, vals, "exec"})
	r := c.db.answer(q, vals)
	return fakeResult{r}, r.err
}

func (c *fakeConn) QueryContext(_ context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if c.db.skipCtx {
		return nil, driver.ErrSkip
	}
	vals := values(a)
	c.db.record(call{q, vals, "query"})
	r := c.db.answer(q, vals)
	if r.err != nil {
		return nil, r.err
	}
	return &fakeRows{r: r}, nil
}

type fakeTx struct{ db *fakeDB }

func (t fakeTx) Commit() error   { t.db.mu.Lock(); t.db.commits++; t.db.mu.Unlock(); return nil }
func (t fakeTx) Rollback() error { t.db.mu.Lock(); t.db.rbacks++; t.db.mu.Unlock(); return nil }

type fakeStmt struct {
	c *fakeConn
	q string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(a []driver.Value) (driver.Result, error) {
	s.c.db.record(call{s.q, a, "prepare"})
	r := s.c.db.answer(s.q, a)
	return fakeResult{r}, r.err
}
func (s *fakeStmt) Query(a []driver.Value) (driver.Rows, error) {
	s.c.db.record(call{s.q, a, "prepare"})
	r := s.c.db.answer(s.q, a)
	if r.err != nil {
		return nil, r.err
	}
	return &fakeRows{r: r}, nil
}

type fakeResult struct{ r result }

func (f fakeResult) LastInsertId() (int64, error) { return f.r.lastID, nil }
func (f fakeResult) RowsAffected() (int64, error) { return f.r.affected, nil }

type fakeRows struct {
	r result
	i int
}

func (f *fakeRows) Columns() []string { return f.r.cols }
func (f *fakeRows) Close() error      { return nil }
func (f *fakeRows) Next(dest []driver.Value) error {
	if f.i >= len(f.r.rows) {
		return io.EOF
	}
	copy(dest, f.r.rows[f.i])
	f.i++
	return nil
}

func values(a []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(a))
	for i, v := range a {
		out[i] = v.Value
	}
	return out
}

// ---- harness ----

var driverSeq atomic.Int64

// setup enables the SDK with an in-memory recorder and one active session.
func setup(t *testing.T) (*core.MemRecorder, context.Context) {
	t.Helper()
	core.Configure(core.Config{Service: "svc", Instance: "i1", Enabled: true})
	rec := &core.MemRecorder{}
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() {
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})
	return rec, core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: "t1"})
}

// open registers fake under a unique name and opens it through tsql.
func open(t *testing.T, f *fakeDB, driverName string, opts ...tsql.Option) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("%s-fake-%d", driverName, driverSeq.Add(1))
	sql.Register(name, f)
	db, err := tsql.Open(name, "dsn", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func byKind(evs []core.Event, k core.Kind) []core.Event {
	var out []core.Event
	for _, e := range evs {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

func hasFlag(e core.Event, f string) bool {
	for _, x := range e.Flags {
		if x == f {
			return true
		}
	}
	return false
}

func sqls(cs []call) string {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.SQL + "\n")
	}
	return b.String()
}
