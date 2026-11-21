package tsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// conn wraps a driver.Conn. database/sql serializes use of a driver conn, so
// the only concurrent access is the tx bookkeeping, guarded by mu.
type conn struct {
	inner driver.Conn
	cfg   *config
	pks   *pkCache // primary keys found in the catalog, shared by the handle's connections

	mu    sync.Mutex
	txID  string
	rawTx bool // the app opened a transaction with a plain BEGIN statement
	// Isolation bookkeeping (see iso.go).
	iso        isoState // level of the open transaction
	isoDefault bool     // the open transaction took the session's default level
	defSerial  bool     // the session's default level is SERIALIZABLE
	defInTx    bool     // the default was set inside the open transaction
	nextSerial bool     // MySQL: SET TRANSACTION made the next transaction SERIALIZABLE
	// Primary-key cache bookkeeping for the open transaction (see endPKTx).
	pkSchemaDirty bool
	pkTxTables    []string

	// multi is whether this connection takes two statements in one text (see
	// capture). Only the statement path touches it, which database/sql
	// serializes per connection.
	multi multiStmt
	// fence holds the statements of this connection's savepoint fence, made
	// when the first one is needed (see fenceStmts).
	fence *fenceStmts
}

func newConn(inner driver.Conn, cfg *config, pks *pkCache) *conn {
	return &conn{inner: inner, cfg: cfg, pks: pks}
}

func (c *conn) currentTx() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txID
}

// setTx notes the transaction database/sql opened with the given level.
func (c *conn) setTx(id string, level driver.IsolationLevel) {
	lvl := mutation.IsoOther
	switch sql.IsolationLevel(level) {
	case sql.LevelDefault:
		lvl = mutation.IsoUnset
	case sql.LevelSerializable:
		lvl = mutation.IsoSerializable
	}
	c.mu.Lock()
	c.txID = id
	c.beginIso(lvl)
	c.mu.Unlock()
}

// txState reports whether a transaction is open on this conn and what is
// known of its isolation level.
func (c *conn) txState() (open bool, iso isoState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txID != "" || c.rawTx, c.iso
}

// trackTx notices hand-written BEGIN/COMMIT/ROLLBACK statements (anywhere in
// a multi-statement text) so capture never opens its own transaction inside
// one the app started by hand, and the statements that set an isolation
// level, so it never reads inside a SERIALIZABLE one.
func (c *conn) trackTx(in mutation.Info) {
	tc := in.Tx
	if tc == mutation.TxNone && in.TxIso == mutation.IsoUnset && in.DefIso == mutation.IsoUnset {
		return
	}
	c.mu.Lock()
	c.noteIso(in)
	if tc != mutation.TxNone {
		c.rawTx = tc.Opens()
	}
	c.mu.Unlock()
	if tc != mutation.TxNone && tc != mutation.TxBegin {
		c.endPKTx(tc == mutation.TxCommit)
	}
}

// trackTxUntraced is trackTx for a statement that is not traced and so was
// not parsed. The transaction state must stay exact for it too: a later
// statement on this conn may belong to a session. Only a text that can
// contain transaction control (a cheap scan of the first keyword of each
// statement) or names an isolation level is parsed, and then trackTx sees
// exactly what it sees for a traced statement.
func (c *conn) trackTxUntraced(query string) {
	if !mutation.MayControlTx(query, c.dialect()) && !mutation.MaySetIsolation(query, c.dialect()) {
		return
	}
	info, _ := c.parse(query)
	c.trackTx(info)
}

func (c *conn) dialect() mutation.Dialect {
	if c.cfg.store == "mysql" {
		return mutation.MySQL
	}
	return mutation.Postgres
}

// parse classifies query; ok is false when the parser panicked.
func (c *conn) parse(query string) (info mutation.Info, ok bool) {
	defer func() {
		if recover() != nil {
			info, ok = mutation.Info{}, false
		}
	}()
	return parseCached(query, c.dialect()), true
}

func (c *conn) clearTx(id string, committed bool) {
	c.mu.Lock()
	if c.txID == id {
		c.txID = ""
		c.endIso(committed)
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
		//lint:ignore SA1019 legacy fallback for drivers without ConnBeginTx
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
	c.setTx(id, opts.Isolation)
	// The id is kept even outside a session: statements of this transaction
	// may run with a context that is in one.
	if core.Active(ctx) {
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
	}
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
	//lint:ignore SA1019 legacy fallback for drivers without StmtExecContext
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
	//lint:ignore SA1019 legacy fallback for drivers without StmtQueryContext
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
	t.c.clearTx(t.id, kind == core.KindDBTxCommit)
	t.c.endPKTx(kind == core.KindDBTxCommit)
	if !core.Active(t.ctx) {
		return // no span: nothing to render or redact
	}
	safe(func() {
		_, sp := core.Begin(t.ctx, kind, name)
		sp.SetTx(t.id)
		if commitFailed {
			sp.SetAttr("commit_failed", "true")
		}
		sp.SetErr(redactErr(err, false))
		sp.End()
	})
}

// safe runs f, swallowing panics: tracing must never break the host app.
func safe(f func()) {
	defer core.Recover()
	f()
}
