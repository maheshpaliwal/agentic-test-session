package tsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// conn wraps a driver.Conn. database/sql serializes use of a driver conn, so
// the only concurrent access is the tx bookkeeping, guarded by mu.
type conn struct {
	inner driver.Conn
	cfg   *config

	mu     sync.Mutex
	txID   string
	serial bool // open tx runs at SERIALIZABLE
	rawTx  bool // the app opened a transaction with a plain BEGIN statement
}

func newConn(inner driver.Conn, cfg *config) *conn { return &conn{inner: inner, cfg: cfg} }

func (c *conn) currentTx() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txID
}

func (c *conn) setTx(id string, serializable bool) {
	c.mu.Lock()
	c.txID = id
	c.serial = serializable
	c.mu.Unlock()
}

// txState reports whether a transaction is open on this conn and whether it is SERIALIZABLE.
func (c *conn) txState() (open, serializable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txID != "" || c.rawTx, c.serial
}

// trackTxControl notices plain BEGIN/COMMIT/ROLLBACK statements so capture
// never opens its own transaction inside one the app started by hand.
func (c *conn) trackTxControl(query string) {
	begin, end := txControl(query)
	if !begin && !end {
		return
	}
	c.mu.Lock()
	c.rawTx = begin
	c.mu.Unlock()
}

// txControl classifies the leading keyword of a statement.
func txControl(q string) (begin, end bool) {
	i, n := 0, len(q)
	for i < n {
		switch {
		case q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r':
			i++
		case strings.HasPrefix(q[i:], "--"):
			for i < n && q[i] != '\n' {
				i++
			}
		case strings.HasPrefix(q[i:], "/*"):
			e := strings.Index(q[i+2:], "*/")
			if e < 0 {
				return false, false
			}
			i += e + 4
		default:
			goto word
		}
	}
	return false, false
word:
	words := strings.Fields(strings.ToUpper(q[i:min(n, i+48)]))
	if len(words) == 0 {
		return false, false
	}
	w1 := strings.TrimRight(words[0], ";")
	w2 := ""
	if len(words) > 1 {
		w2 = strings.TrimRight(words[1], ";")
	}
	switch w1 {
	case "BEGIN":
		return true, false
	case "START":
		return w2 == "TRANSACTION", false
	case "COMMIT", "END", "ABORT":
		return false, w2 != "PREPARED"
	case "ROLLBACK":
		return false, w2 != "TO" && w2 != "PREPARED" && w2 != "SAVEPOINT"
	}
	return false, false
}

func (c *conn) clearTx(id string) {
	c.mu.Lock()
	if c.txID == id {
		c.txID = ""
		c.serial = false
	}
	c.mu.Unlock()
}

func (c *conn) Close() error { return c.inner.Close() }

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	st, err := c.inner.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{inner: st, c: c, query: query}, nil
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	st, err := c.prepareInner(ctx, query)
	if err != nil {
		return nil, err
	}
	return &stmt{inner: st, c: c, query: query}, nil
}

func (c *conn) prepareInner(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := c.inner.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	st, err := c.inner.Prepare(query)
	if err == nil {
		select {
		case <-ctx.Done():
			st.Close()
			return nil, ctx.Err()
		default:
		}
	}
	return st, err
}

// Begin implements the legacy driver.Conn method.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		err error
	)
	if bt, ok := c.inner.(driver.ConnBeginTx); ok {
		tx, err = bt.BeginTx(ctx, opts)
	} else {
		if driver.IsolationLevel(opts.Isolation) != driver.IsolationLevel(sql.LevelDefault) {
			return nil, errors.New("sql: driver does not support non-default isolation level")
		}
		if opts.ReadOnly {
			return nil, errors.New("sql: driver does not support read-only transactions")
		}
		tx, err = c.inner.Begin() //nolint:staticcheck // legacy fallback
		if err == nil {
			select {
			case <-ctx.Done():
				tx.Rollback()
				return nil, ctx.Err()
			default:
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if !core.Enabled() {
		return tx, nil
	}
	id := "tx_" + core.NewSpanID()
	c.setTx(id, driver.IsolationLevel(opts.Isolation) == driver.IsolationLevel(sql.LevelSerializable))
	safe(func() {
		_, sp := core.Begin(ctx, core.KindDBTxBegin, "BEGIN")
		sp.SetTx(id)
		if opts.ReadOnly {
			sp.SetAttr("read_only", "true")
		}
		if opts.Isolation != 0 {
			sp.SetAttr("isolation", sql.IsolationLevel(opts.Isolation).String())
		}
		sp.End()
	})
	return &txWrap{inner: tx, c: c, ctx: ctx, id: id}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.doExec(ctx, query, args, func() (driver.Result, error) { return c.execInner(ctx, query, args) })
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.doQuery(ctx, query, args, true, func(q string) (driver.Rows, func(), error) {
		return c.queryInner(ctx, q, args)
	})
}

// execInner runs on the wrapped conn, replicating database/sql's fallback to
// prepare+exec when the driver answers ErrSkip (so ErrSkip never escapes after
// a span has started).
func (c *conn) execInner(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ex, ok := c.inner.(driver.ExecerContext); ok {
		res, err := ex.ExecContext(ctx, query, args)
		if err != driver.ErrSkip {
			return res, err
		}
	}
	st, err := c.prepareInner(ctx, query)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	return stmtExec(ctx, st, args)
}

func (c *conn) queryInner(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, func(), error) {
	if q, ok := c.inner.(driver.QueryerContext); ok {
		rows, err := q.QueryContext(ctx, query, args)
		if err != driver.ErrSkip {
			return rows, nil, err
		}
	}
	st, err := c.prepareInner(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	rows, err := stmtQuery(ctx, st, args)
	if err != nil {
		st.Close()
		return nil, nil, err
	}
	return rows, func() { st.Close() }, nil
}

func (c *conn) Ping(ctx context.Context) error {
	if p, ok := c.inner.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *conn) ResetSession(ctx context.Context) error {
	if r, ok := c.inner.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (c *conn) IsValid() bool {
	if v, ok := c.inner.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if ck, ok := c.inner.(driver.NamedValueChecker); ok {
		return ck.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// stmtExec mirrors database/sql's ctxDriverStmtExec.
func stmtExec(ctx context.Context, st driver.Stmt, args []driver.NamedValue) (driver.Result, error) {
	if se, ok := st.(driver.StmtExecContext); ok {
		return se.ExecContext(ctx, args)
	}
	dargs, err := namedToValues(args)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return st.Exec(dargs) //nolint:staticcheck // legacy fallback
}

func stmtQuery(ctx context.Context, st driver.Stmt, args []driver.NamedValue) (driver.Rows, error) {
	if sq, ok := st.(driver.StmtQueryContext); ok {
		return sq.QueryContext(ctx, args)
	}
	dargs, err := namedToValues(args)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return st.Query(dargs) //nolint:staticcheck // legacy fallback
}

func namedToValues(args []driver.NamedValue) ([]driver.Value, error) {
	out := make([]driver.Value, len(args))
	for i, nv := range args {
		if nv.Name != "" {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		out[i] = nv.Value
	}
	return out, nil
}

// txWrap records commit/rollback boundaries.
type txWrap struct {
	inner driver.Tx
	c     *conn
	ctx   context.Context
	id    string
}

func (t *txWrap) Commit() error {
	err := t.inner.Commit()
	if err != nil {
		t.end(core.KindDBTxRollbck, "ROLLBACK", err, true)
	} else {
		t.end(core.KindDBTxCommit, "COMMIT", nil, false)
	}
	return err
}

func (t *txWrap) Rollback() error {
	err := t.inner.Rollback()
	t.end(core.KindDBTxRollbck, "ROLLBACK", err, false)
	return err
}

func (t *txWrap) end(kind core.Kind, name string, err error, commitFailed bool) {
	t.c.clearTx(t.id)
	safe(func() {
		_, sp := core.Begin(t.ctx, kind, name)
		sp.SetTx(t.id)
		if commitFailed {
			sp.SetAttr("commit_failed", "true")
		}
		sp.SetErr(err)
		sp.End()
	})
}

// safe runs f, swallowing panics: tracing must never break the host app.
func safe(f func()) {
	defer core.Recover()
	f()
}
