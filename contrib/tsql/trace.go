package tsql

import (
	"context"
	"database/sql/driver"
	"strconv"
	"strings"
	"sync"

	"tsession/contrib/tsql/mutation"
	"tsession/core"
)

var parseCache = struct {
	sync.Mutex
	m map[parseKey]mutation.Info
}{m: map[parseKey]mutation.Info{}}

type parseKey struct {
	sql string
	d   mutation.Dialect
}

func parseCached(q string, d mutation.Dialect) mutation.Info {
	k := parseKey{q, d}
	parseCache.Lock()
	in, ok := parseCache.m[k]
	parseCache.Unlock()
	if ok {
		return in
	}
	in = mutation.Parse(q, d)
	parseCache.Lock()
	if len(parseCache.m) >= 1024 {
		clear(parseCache.m)
	}
	parseCache.m[k] = in
	parseCache.Unlock()
	return in
}

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

// resolve evaluates an operand to its display form and the faithful driver
// value (used for binding, never the lossy display form).
func (t *stmtTrace) resolve(v mutation.Value) (disp any, raw driver.Value, ok bool) {
	if v.Arg >= 0 {
		if v.Arg >= len(t.args) {
			return nil, nil, false
		}
		raw = t.args[v.Arg].Value
		return toValue(raw, maxCellLen), raw, true
	}
	return v.Literal, v.Literal, true
}

// pkFromValues extracts a full PK from "col -> operand" maps (WHERE equalities
// or INSERT values). The zero pkRef means "not fully known".
func (t *stmtTrace) pkFromValues(m map[string]mutation.Value, pkCols []string) pkRef {
	if len(m) == 0 || len(pkCols) == 0 {
		return pkRef{}
	}
	ref := pkRef{disp: make(map[string]any, len(pkCols)), raw: make(map[string]driver.Value, len(pkCols))}
	for _, c := range pkCols {
		v, ok := m[c]
		if !ok {
			return pkRef{}
		}
		d, r, ok := t.resolve(v)
		if !ok {
			return pkRef{}
		}
		ref.disp[c], ref.raw[c] = d, r
	}
	return ref
}

func (t *stmtTrace) newMutation(op string) core.Mutation {
	return core.Mutation{Store: t.store, Entity: t.info.Table, Op: op, Source: "sdk"}
}

// imgsOf pairs the display rows a rows wrapper captured with their raw PKs.
func imgsOf(data []map[string]any, raws []map[string]driver.Value) []img {
	if data == nil {
		return nil
	}
	out := make([]img, len(data))
	for i, d := range data {
		out[i].disp = d
		if i < len(raws) {
			out[i].raw = raws[i]
		}
	}
	return out
}

func (t *stmtTrace) addMutations(res driver.Result, ret []img, affected int64) {
	in := t.info
	if !in.Kind.IsMutation() {
		return
	}
	if in.Table == "" {
		t.flag("entity_unknown")
		return
	}
	if affected == 0 && in.Kind != mutation.Truncate {
		return
	}
	op := in.Kind.Op()
	if t.skip || in.Kind == mutation.Truncate {
		t.span.AddMutation(t.newMutation(op))
		return
	}
	pkCols := t.cfg.pk(in.Schema, in.Table)
	var muts []core.Mutation
	emit := func(before, after *img, fallback map[string]any) {
		m := t.newMutation(op)
		var bd, ad map[string]any
		if before != nil {
			bd = before.disp
		}
		if after != nil {
			ad = after.disp
		}
		m.PK = pkOf(ad, pkCols)
		if m.PK == nil {
			m.PK = pkOf(bd, pkCols)
		}
		if m.PK == nil {
			m.PK = fallback
		}
		if m.PK == nil {
			t.flag("pk_unknown")
		} else {
			m.PK = redactMap(m.PK)
		}
		if bd != nil {
			m.Before = core.Payload(core.RedactAny(bd))
		}
		if ad != nil {
			m.After = core.Payload(core.RedactAny(ad))
		}
		if bd != nil || ad != nil {
			m.Diff = diffMaps(bd, ad)
		}
		muts = append(muts, m)
	}

	switch in.Kind {
	case mutation.Update, mutation.Delete:
		eq := t.pkFromValues(in.Equals, pkCols)
		var after []img
		if in.Kind == mutation.Update && t.cfg.images {
			switch {
			case t.retAll && ret != nil:
				after = ret
			case t.beforeOK:
				var refs []pkRef
				for _, b := range t.before {
					if r := b.ref(pkCols); r.disp != nil {
						refs = append(refs, r)
					}
				}
				if len(refs) == len(t.before) {
					after = t.reread(refs, pkCols)
				}
			case eq.disp != nil:
				after = t.reread([]pkRef{eq}, pkCols)
			}
		}
		switch {
		case t.beforeOK:
			for i := range t.before {
				b := t.before[i]
				emit(&b, matchAfter(after, b, i, pkCols, len(t.before)), eq.disp)
			}
		case len(after) > 0:
			for i := range after {
				emit(nil, &after[i], eq.disp)
			}
		default:
			emit(nil, nil, eq.disp)
		}
		if in.Kind == mutation.Update && t.cfg.images && len(muts) > 0 && muts[0].After.IsZero() {
			t.flag("after_unavailable")
		}
	default: // insert, upsert
		var after []img
		if in.Bulk {
			t.flag("bulk_copy")
		} else if t.retAll && ret != nil {
			after = ret
		} else if t.cfg.images {
			row := map[string]any{}
			for col, v := range in.Values {
				if d, _, ok := t.resolve(v); ok {
					row[col] = d
				}
			}
			ref := t.pkFromValues(in.Values, pkCols)
			// App-written partial RETURNING (e.g. "RETURNING id"): take the PK from the returned rows.
			if ref.disp == nil {
				var retRefs []pkRef
				for _, rr := range ret {
					if r := rr.ref(pkCols); r.disp != nil {
						retRefs = append(retRefs, r)
					}
				}
				if len(retRefs) == 1 {
					ref = retRefs[0]
				} else if len(retRefs) > 1 {
					if rr := t.reread(retRefs, pkCols); len(rr) == len(retRefs) {
						after = rr
					}
				}
			}
			if ref.disp == nil && len(pkCols) == 1 && t.store == "mysql" && res != nil && affected == 1 {
				if id, err := res.LastInsertId(); err == nil && id > 0 {
					ref = pkRef{disp: map[string]any{pkCols[0]: id}, raw: map[string]driver.Value{pkCols[0]: id}}
				}
			}
			if ref.disp != nil && after == nil {
				if rr := t.reread([]pkRef{ref}, pkCols); len(rr) == 1 {
					after = rr
				}
			}
			if after == nil && len(row) > 0 {
				after = []img{{disp: row}}
				t.flag("after_from_args")
			}
		}
		if in.MultiRow {
			t.flag("multi_row_insert")
		}
		if len(after) == 0 {
			emit(nil, nil, t.pkFromValues(in.Values, pkCols).disp)
		}
		for i := range after {
			emit(nil, &after[i], nil)
		}
	}
	if len(muts) > t.cfg.maxRows {
		muts = muts[:t.cfg.maxRows]
		t.flag("mutations_truncated")
	}
	for _, m := range muts {
		t.span.AddMutation(m)
	}
}

func matchAfter(after []img, before img, idx int, pkCols []string, nBefore int) *img {
	if len(after) == 0 {
		return nil
	}
	if before.raw != nil {
		k := rawKey(before.raw, pkCols)
		for i := range after {
			if after[i].raw != nil && rawKey(after[i].raw, pkCols) == k {
				return &after[i]
			}
		}
		return nil
	}
	if bp := pkOf(before.disp, pkCols); bp != nil {
		k := pkKey(bp, pkCols)
		for i := range after {
			if ap := pkOf(after[i].disp, pkCols); ap != nil && pkKey(ap, pkCols) == k {
				return &after[i]
			}
		}
		return nil
	}
	if len(after) == nBefore && idx < len(after) {
		return &after[idx]
	}
	return nil
}

// reread loads the current rows for the given primary keys on the statement's
// connection. Keys are bound with their original driver values; if any key
// part cannot be bound faithfully (NULL) nothing is read. Errors only drop the
// image (flagged); they never propagate.
func (t *stmtTrace) reread(refs []pkRef, pkCols []string) []img {
	if len(refs) == 0 || len(pkCols) == 0 || t.info.TableSQL == "" {
		return nil
	}
	if len(refs) > t.cfg.maxRows {
		refs = refs[:t.cfg.maxRows]
	}
	for _, r := range refs {
		for _, c := range pkCols {
			if v, ok := r.raw[c]; !ok || v == nil {
				t.flag("image_skipped")
				t.flag("image_skipped:pk_unbindable")
				return nil
			}
		}
	}
	dollar := t.pg()
	var vals []driver.Value
	ph := func(v driver.Value) string {
		vals = append(vals, v)
		if dollar {
			return "$" + strconv.Itoa(len(vals))
		}
		return "?"
	}
	q := quoter(t.store)
	var b strings.Builder
	b.WriteString("SELECT * FROM " + t.info.TableSQL + " WHERE ")
	if len(pkCols) == 1 {
		b.WriteString(q(pkCols[0]) + " IN (")
		for i, r := range refs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(ph(r.raw[pkCols[0]]))
		}
		b.WriteString(")")
	} else {
		for i, r := range refs {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("(")
			for j, c := range pkCols {
				if j > 0 {
					b.WriteString(" AND ")
				}
				b.WriteString(q(c) + " = " + ph(r.raw[c]))
			}
			b.WriteString(")")
		}
	}
	rs, ok := t.capture(b.String(), vals, t.cfg.maxRows, "after_failed")
	if !ok {
		return nil
	}
	return rs
}

func quoter(store string) func(string) string {
	if store == "mysql" {
		return func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	}
	return func(s string) string {
		if simpleIdent(s) {
			return s // unquoted folds to lower case, matching how it was written
		}
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
}

func simpleIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if !(r == '_' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
