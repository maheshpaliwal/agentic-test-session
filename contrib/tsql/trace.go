package tsql

import (
	"context"
	"database/sql/driver"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// stmtTrace is the per-statement capture state; its span is never nil.
type stmtTrace struct {
	c     *conn
	cfg   *config
	span  *core.Span
	ctx   context.Context
	info  mutation.Info
	store string
	query string
	args  []driver.NamedValue
	write bool
	skip  bool

	sqlText string // query with sensitive literals masked; the only SQL text ever recorded
	mask    []bool // args that must be masked
	flagged map[string]bool

	retAll   bool // result rows are full-row RETURNING * images
	before   []img
	beforeOK bool
}

// flag adds a span flag once.
func (t *stmtTrace) flag(f string) {
	if t.flagged == nil {
		t.flagged = map[string]bool{}
	}
	if !t.flagged[f] {
		t.flagged[f] = true
		t.span.AddFlag(f)
	}
}

func (t *stmtTrace) pg() bool { return t.store == "postgres" || (t.store == "sql" && t.info.Dollar) }

// startTrace returns nil when nothing should be recorded (SDK off, no active
// session in ctx) so callers fall straight through to the driver.
func (c *conn) startTrace(ctx context.Context, query string, args []driver.NamedValue) (t *stmtTrace) {
	if !core.Enabled() {
		return nil
	}
	defer func() {
		if recover() != nil {
			t = nil
		}
	}()
	d := mutation.Postgres
	if c.cfg.store == "mysql" {
		d = mutation.MySQL
	}
	info := parseCached(query, d)
	kind := core.KindDBRead
	if info.Kind.IsWrite() {
		kind = core.KindDBWrite
	}
	sqlText := redactSQL(query, info)
	_, sp := core.Begin(ctx, kind, capSQL(sqlText, 2000))
	if sp == nil {
		return nil
	}
	t = &stmtTrace{c: c, cfg: c.cfg, span: sp, ctx: ctx, info: info, query: query, sqlText: sqlText, args: args, write: info.Kind.IsWrite()}
	t.store = c.cfg.store
	if t.store == "" {
		switch {
		case info.Dollar:
			t.store = "postgres"
		case info.QMark:
			t.store = "mysql"
		default:
			t.store = "sql"
		}
	}
	t.skip = c.cfg.skipped(info.Schema, info.Table)
	for _, tb := range info.Tables {
		if c.cfg.skipped("", tb) {
			t.skip = true
		}
	}
	t.retAll = info.ReturningAll
	req := map[string]any{"sql": capSQL(sqlText, 8000)}
	if t.skip {
		t.flag("table_skipped")
	} else {
		var cons bool
		t.mask, cons = argMask(info, args)
		if cons {
			t.flag("args_redacted_conservatively")
		}
		req["args"] = argValues(args, c.cfg, t.mask)
	}
	sp.SetReq(req)
	sp.SetTx(c.currentTx())
	sp.SetAttr("db.store", t.store)
	if info.Table != "" {
		sp.SetAttr("db.entity", info.Table)
	}
	return t
}

func (c *conn) doExec(ctx context.Context, query string, args []driver.NamedValue, run func() (driver.Result, error)) (driver.Result, error) {
	t := c.startTrace(ctx, query, args)
	if t == nil {
		res, err := run()
		if err == nil {
			c.trackTxControl(query)
		}
		return res, err
	}
	safe(t.preImage)
	res, err := run()
	if err == nil {
		c.trackTxControl(query)
	}
	safe(func() { t.finishExec(res, err) })
	t.span.End()
	return res, err
}

// doQuery runs a Query-path statement. run receives the (possibly rewritten)
// query text and may return a release func to call when the rows are closed.
func (c *conn) doQuery(ctx context.Context, query string, args []driver.NamedValue, canRewrite bool, run func(q string) (driver.Rows, func(), error)) (driver.Rows, error) {
	t := c.startTrace(ctx, query, args)
	if t == nil {
		r, rel, err := run(query)
		if err == nil {
			c.trackTxControl(query)
		}
		return wrapPlain(r, rel), err
	}
	safe(t.preImage)
	q := query
	if canRewrite {
		q = t.rewrite()
	}
	r, rel, err := run(q)
	if err != nil {
		t.span.SetErr(err)
		t.span.End()
		return nil, err
	}
	c.trackTxControl(query)
	rw := &rows{inner: r, release: rel, onClose: t.finishRows, pkCols: t.cfg.pk(t.info.Schema, t.info.Table)}
	switch {
	case t.write:
		rw.cap = t.cfg.maxRows
		rw.drain = t.info.HasReturning || q != query
		if t.skip {
			rw.cap = 0
		}
	case !t.skip:
		rw.cap = t.cfg.sampleRows
	}
	return rw, nil
}

// wrapPlain keeps the release hook working when tracing is off.
func wrapPlain(r driver.Rows, rel func()) driver.Rows {
	if rel == nil || r == nil {
		return r
	}
	return &rows{inner: r, release: rel}
}

// rewrite appends RETURNING * for Postgres writes when the option is on.
func (t *stmtTrace) rewrite() string {
	in := t.info
	if !t.cfg.captureAfter || t.store != "postgres" || t.skip || in.HasReturning || in.Multi ||
		in.WritesInCTE || in.End == 0 || (in.Verb != "INSERT" && in.Verb != "UPDATE") {
		return t.query
	}
	t.span.AddFlag("returning_appended")
	t.retAll = true
	return t.query[:in.End] + " RETURNING *"
}

func (t *stmtTrace) preImage() {
	in := t.info
	if !t.write || !t.cfg.images || t.skip || (in.Kind != mutation.Update && in.Kind != mutation.Delete) {
		return
	}
	q, idx, reason := in.ImageSelect(t.cfg.maxRows + 1)
	if reason != "" {
		t.flag("image_skipped")
		t.span.SetAttr("image_skip_reason", reason)
		return
	}
	vals := make([]driver.Value, len(idx))
	for i, ai := range idx {
		if ai < 0 || ai >= len(t.args) {
			t.flag("image_skipped")
			t.span.SetAttr("image_skip_reason", "arg_mismatch")
			return
		}
		vals[i] = t.args[ai].Value
	}
	rs, ok := t.capture(q, vals, t.cfg.maxRows+1, "image_failed")
	if !ok {
		return
	}
	if len(rs) > t.cfg.maxRows {
		rs = rs[:t.cfg.maxRows]
		t.flag("image_truncated")
	}
	t.before, t.beforeOK = rs, true
}

// capture runs a capture query and translates its outcome into span flags.
func (t *stmtTrace) capture(q string, vals []driver.Value, limit int, failFlag string) ([]img, bool) {
	rs, st := t.c.capture(t.ctx, t.pg(), q, vals, limit, t.cfg.pk(t.info.Schema, t.info.Table))
	switch st {
	case capOK:
		return rs, true
	case capSerializable:
		t.flag("image_skipped")
		t.flag("image_skipped:serializable")
		t.span.SetAttr("image_skip_reason", "serializable")
	case capTimeout:
		t.flag("image_timeout")
	default:
		t.flag(failFlag)
	}
	return nil, false
}

func (t *stmtTrace) finishExec(res driver.Result, err error) {
	if err != nil {
		t.span.SetErr(err)
		return
	}
	affected := int64(-1)
	if res != nil {
		if n, e := res.RowsAffected(); e == nil {
			affected = n
		}
	}
	if affected >= 0 {
		t.span.SetResp(map[string]any{"rows_affected": affected})
	}
	t.addMutations(res, nil, affected)
}

// finishRows runs once the application closed its rows.
func (t *stmtTrace) finishRows(r *rows) {
	defer t.span.End()
	if r.err != nil {
		t.span.SetErr(r.err)
		return
	}
	if t.write {
		affected := int64(-1)
		if r.drain {
			affected = int64(r.count)
		}
		t.span.SetResp(map[string]any{"rows": r.count})
		if t.info.HasReturning && !t.info.ReturningAll && !t.skip {
			t.span.AddFlag("returning_partial")
		}
		t.addMutations(nil, imgsOf(r.data, r.raws), affected)
		return
	}
	rs := core.ReadSummary{
		Store:     t.store,
		Entity:    t.info.Table,
		Statement: capSQL(t.sqlText, 512),
		RowCount:  r.count,
	}
	if !t.skip {
		rs.Args = core.Payload(argValues(t.args, t.cfg, t.mask))
		pkCols := t.cfg.pk(t.info.Schema, t.info.Table)
		for _, row := range r.data {
			if pk := pkOf(row, pkCols); pk != nil && len(rs.PKs) < 20 {
				rs.PKs = append(rs.PKs, pkString(pk, pkCols))
			}
		}
		if len(r.data) > 0 {
			rs.Sample = core.Payload(redactRows(r.data))
		}
	}
	t.span.AddRead(rs)
}
