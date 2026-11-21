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
	argVals []any  // rendered (masked) args, shared by the request and the read summary
	flagged map[string]bool

	pk        []string // primary-key columns of the statement's table
	pkCatalog bool     // pk is the catalog's own spelling (see lookupPK)
	aborted   bool     // the key lookup found the transaction aborted: no capture can run

	retAll   bool // result rows are full-row RETURNING * images
	before   []img
	beforeOK bool
	ups      *upsertRead // an upsert whose rows can be found by a key (upsert.go)
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
func (c *conn) startTrace(ctx context.Context, query string, args []driver.NamedValue, info mutation.Info) (t *stmtTrace) {
	if !core.Enabled() {
		return nil
	}
	defer func() {
		if recover() != nil {
			t = nil
		}
	}()
	kind := core.KindDBRead
	if info.Kind.IsWrite() {
		kind = core.KindDBWrite
	}
	_, sp := core.Begin(ctx, kind, "")
	if sp == nil {
		return nil
	}
	// Redaction walks the whole statement; it only happens once there is a
	// span to put the text on.
	sqlText := redactSQL(query, info)
	sp.SetName(capSQL(sqlText, 2000))
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
	t.pk, t.pkCatalog = c.knownPK(info)
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
		t.argVals = argValues(args, c.cfg, t.mask)
		req["args"] = t.argVals
	}
	sp.SetReq(req)
	sp.SetTx(c.currentTx())
	sp.SetAttr("db.store", t.store)
	if info.Table != "" {
		sp.SetAttr("db.entity", info.Table)
	}
	return t
}

// doExec runs an Exec-path statement. A statement whose ctx belongs to no
// active session is neither parsed nor redacted: it goes straight to the
// driver and only its transaction control, if any, is noted.
func (c *conn) doExec(ctx context.Context, query string, args []driver.NamedValue, run func() (driver.Result, error)) (driver.Result, error) {
	if !core.Active(ctx) {
		res, err := run()
		if err == nil {
			c.trackTxUntraced(query)
		}
		return res, err
	}
	info, ok := c.parse(query)
	var t *stmtTrace
	if ok {
		t = c.startTrace(ctx, query, args, info)
	}
	if t == nil {
		res, err := run()
		if err == nil {
			c.trackTx(info)
		}
		return res, err
	}
	safe(t.lookupPK)
	safe(t.preImage)
	res, err := run()
	if err == nil {
		c.trackTx(info)
		c.noteSchema(info)
	} else {
		c.pks.drop(info.TableSQL) // the table may not exist yet: what the lookup said about it is not kept
	}
	safe(func() { t.finishExec(res, err) })
	t.span.End()
	return res, err
}

// doQuery runs a Query-path statement. run receives the (possibly rewritten)
// query text and may return a release func to call when the rows are closed.
// Outside an active session it does as little as doExec.
func (c *conn) doQuery(ctx context.Context, query string, args []driver.NamedValue, canRewrite bool, run func(q string) (driver.Rows, func(), error)) (driver.Rows, error) {
	if !core.Active(ctx) {
		r, rel, err := run(query)
		if err == nil {
			c.trackTxUntraced(query)
		}
		return wrapPlain(r, rel), err
	}
	info, ok := c.parse(query)
	var t *stmtTrace
	if ok {
		t = c.startTrace(ctx, query, args, info)
	}
	if t == nil {
		r, rel, err := run(query)
		if err == nil {
			c.trackTx(info)
		}
		return wrapPlain(r, rel), err
	}
	safe(t.lookupPK)
	safe(t.preImage)
	q := query
	if canRewrite {
		q = t.rewrite()
	}
	r, rel, err := run(q)
	if err != nil {
		c.pks.drop(info.TableSQL)
		t.setErr(err)
		t.span.End()
		return nil, err
	}
	c.trackTx(info)
	c.noteSchema(info)
	rw := &rows{inner: r, release: rel, onClose: t.finishRows, pkCols: t.pk}
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
	if in.Kind == mutation.Upsert {
		t.upsertBefore()
		return
	}
	if !t.write || !t.cfg.images || t.skip || (in.Kind != mutation.Update && in.Kind != mutation.Delete) {
		return
	}
	if t.aborted {
		t.flag("image_failed")
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
	rs, st := t.captureKeyed(q, vals, limit, failFlag, t.pk)
	return rs, st == capOK
}

// captureKeyed is capture for rows that are told apart by keyCols, which need
// not be the primary key; it also says how the capture ended. With no
// failFlag a capture that failed is left to the caller to flag.
func (t *stmtTrace) captureKeyed(q string, vals []driver.Value, limit int, failFlag string, keyCols []string) ([]img, capStatus) {
	rs, st := t.c.capture(t.ctx, t.pg(), q, vals, limit, keyCols)
	switch st {
	case capOK:
		return rs, st
	case capSerializable:
		t.flag("image_skipped")
		t.flag("image_skipped:serializable")
		t.span.SetAttr("image_skip_reason", "serializable")
	case capDeadline:
		t.flag("image_skipped")
		t.flag("image_skipped:deadline")
		t.span.SetAttr("image_skip_reason", "deadline")
	case capTimeout:
		t.flag("image_timeout")
	default:
		if failFlag != "" {
			t.flag(failFlag)
		}
	}
	return nil, st
}

func (t *stmtTrace) finishExec(res driver.Result, err error) {
	if err != nil {
		t.setErr(err)
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
		t.setErr(r.err)
		return
	}
	if !t.skip {
		t.maskAliased(r.data)
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
		ret := imgsOf(r.data, r.raws)
		switch {
		case t.info.WritesInCTE:
			ret = nil // the rows belong to the outer query, they are not row images
		case !t.skip && (t.info.HasReturning || t.retAll) && r.count > len(r.data):
			t.flag("mutations_truncated")
		}
		t.addMutations(nil, ret, affected)
		return
	}
	rs := core.ReadSummary{
		Store:     t.store,
		Entity:    t.info.Table,
		Statement: capSQL(t.sqlText, 512),
		RowCount:  r.count,
	}
	if !t.skip {
		rs.Args = core.Payload(t.argVals)
		pkCols := t.pk
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
