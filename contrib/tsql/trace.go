package tsql

import (
	"context"
	"database/sql/driver"
	"fmt"
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

	retAll   bool // result rows are full-row RETURNING * images
	before   []map[string]any
	beforeOK bool
}

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
	_, sp := core.Begin(ctx, kind, capSQL(query, 2000))
	if sp == nil {
		return nil
	}
	t = &stmtTrace{c: c, cfg: c.cfg, span: sp, ctx: ctx, info: info, query: query, args: args, write: info.Kind.IsWrite()}
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
	t.retAll = info.ReturningAll
	req := map[string]any{"sql": capSQL(query, 8000)}
	if t.skip {
		sp.AddFlag("table_skipped")
	} else {
		req["args"] = argValues(args, c.cfg)
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
		return run()
	}
	safe(t.preImage)
	res, err := run()
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
	rw := &rows{inner: r, release: rel, onClose: t.finishRows}
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
		t.span.AddFlag("image_skipped")
		t.span.SetAttr("image_skip_reason", reason)
		return
	}
	vals := make([]driver.Value, len(idx))
	for i, ai := range idx {
		if ai < 0 || ai >= len(t.args) {
			t.span.AddFlag("image_skipped")
			t.span.SetAttr("image_skip_reason", "arg_mismatch")
			return
		}
		vals[i] = t.args[ai].Value
	}
	rs, err := t.c.fetch(t.ctx, q, vals, t.cfg.maxRows+1)
	if err != nil {
		t.span.AddFlag("image_failed")
		return
	}
	if len(rs) > t.cfg.maxRows {
		rs = rs[:t.cfg.maxRows]
		t.span.AddFlag("image_truncated")
	}
	t.before, t.beforeOK = rs, true
}

// fetch runs a capture query on the same connection as the application's
// statement and returns up to limit rows.
func (c *conn) fetch(ctx context.Context, q string, vals []driver.Value, limit int) (out []map[string]any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("tsql: capture panic: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, c.cfg.captureTO)
	defer cancel()
	nv := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	r, rel, err := c.queryInner(ctx, q, nv)
	if err != nil {
		return nil, err
	}
	defer func() {
		r.Close()
		if rel != nil {
			rel()
		}
	}()
	cols := r.Columns()
	dest := make([]driver.Value, len(cols))
	for len(out) < limit {
		if err := r.Next(dest); err != nil {
			break
		}
		out = append(out, rowMap(cols, dest))
	}
	return out, nil
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
		t.addMutations(nil, r.data, affected)
		return
	}
	rs := core.ReadSummary{
		Store:     t.store,
		Entity:    t.info.Table,
		Statement: capSQL(t.query, 512),
		RowCount:  r.count,
	}
	if !t.skip {
		rs.Args = core.Payload(argValues(t.args, t.cfg))
		pkCols := t.cfg.pk(t.info.Schema, t.info.Table)
		for _, row := range r.data {
			if pk := pkOf(row, pkCols); pk != nil && len(rs.PKs) < 20 {
				rs.PKs = append(rs.PKs, pkString(pk, pkCols))
			}
		}
		if len(r.data) > 0 {
			rs.Sample = core.Payload(r.data)
		}
	}
	t.span.AddRead(rs)
}

func (t *stmtTrace) resolve(v mutation.Value) (any, bool) {
	if v.Arg >= 0 {
		if v.Arg >= len(t.args) {
			return nil, false
		}
		return toValue(t.args[v.Arg].Value, maxCellLen), true
	}
	return v.Literal, true
}

// pkFromValues extracts a full PK from "col -> operand" maps (WHERE equalities
// or INSERT values).
func (t *stmtTrace) pkFromValues(m map[string]mutation.Value, pkCols []string) map[string]any {
	if len(m) == 0 || len(pkCols) == 0 {
		return nil
	}
	pk := make(map[string]any, len(pkCols))
	for _, c := range pkCols {
		v, ok := m[c]
		if !ok {
			return nil
		}
		val, ok := t.resolve(v)
		if !ok {
			return nil
		}
		pk[c] = val
	}
	return pk
}

func (t *stmtTrace) newMutation(op string) core.Mutation {
	return core.Mutation{Store: t.store, Entity: t.info.Table, Op: op, Source: "sdk"}
}

func (t *stmtTrace) addMutations(res driver.Result, ret []map[string]any, affected int64) {
	in := t.info
	if !in.Kind.IsMutation() {
		return
	}
	if in.Table == "" {
		t.span.AddFlag("entity_unknown")
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
	emit := func(before, after map[string]any, fallback map[string]any) {
		m := t.newMutation(op)
		m.PK = pkOf(after, pkCols)
		if m.PK == nil {
			m.PK = pkOf(before, pkCols)
		}
		if m.PK == nil {
			m.PK = fallback
		}
		if before != nil {
			m.Before = core.Payload(before)
		}
		if after != nil {
			m.After = core.Payload(after)
		}
		if before != nil || after != nil {
			m.Diff = diffMaps(before, after)
		}
		muts = append(muts, m)
	}

	switch in.Kind {
	case mutation.Update, mutation.Delete:
		eqPK := t.pkFromValues(in.Equals, pkCols)
		var after []map[string]any
		if in.Kind == mutation.Update && t.cfg.images {
			switch {
			case t.retAll && ret != nil:
				after = ret
			case t.beforeOK:
				var pks []map[string]any
				for _, b := range t.before {
					if pk := pkOf(b, pkCols); pk != nil {
						pks = append(pks, pk)
					}
				}
				if len(pks) == len(t.before) {
					after = t.reread(pks, pkCols)
				}
			case eqPK != nil:
				after = t.reread([]map[string]any{eqPK}, pkCols)
			}
		}
		switch {
		case t.beforeOK:
			for i, b := range t.before {
				emit(b, matchAfter(after, b, i, pkCols, len(t.before)), eqPK)
			}
		case len(after) > 0:
			for _, a := range after {
				emit(nil, a, eqPK)
			}
		default:
			emit(nil, nil, eqPK)
		}
		if in.Kind == mutation.Update && t.cfg.images && len(muts) > 0 && muts[0].After.IsZero() {
			t.span.AddFlag("after_unavailable")
		}
	default: // insert, upsert
		var after []map[string]any
		if t.retAll && ret != nil {
			after = ret
		} else if t.cfg.images {
			row := map[string]any{}
			for col, v := range in.Values {
				if val, ok := t.resolve(v); ok {
					row[col] = val
				}
			}
			pk := t.pkFromValues(in.Values, pkCols)
			if pk == nil && len(pkCols) == 1 && t.store == "mysql" && res != nil && affected == 1 {
				if id, err := res.LastInsertId(); err == nil && id > 0 {
					pk = map[string]any{pkCols[0]: id}
				}
			}
			if pk != nil {
				if rr := t.reread([]map[string]any{pk}, pkCols); len(rr) == 1 {
					after = rr
				}
			}
			if after == nil && len(row) > 0 {
				after = []map[string]any{row}
				t.span.AddFlag("after_from_args")
			}
		}
		if in.MultiRow {
			t.span.AddFlag("multi_row_insert")
		}
		if len(after) == 0 {
			emit(nil, nil, t.pkFromValues(in.Values, pkCols))
		}
		for _, a := range after {
			emit(nil, a, nil)
		}
	}
	if len(muts) > t.cfg.maxRows {
		muts = muts[:t.cfg.maxRows]
		t.span.AddFlag("mutations_truncated")
	}
	for _, m := range muts {
		t.span.AddMutation(m)
	}
}

func matchAfter(after []map[string]any, before map[string]any, idx int, pkCols []string, nBefore int) map[string]any {
	if len(after) == 0 {
		return nil
	}
	if bp := pkOf(before, pkCols); bp != nil {
		k := pkKey(bp, pkCols)
		for _, a := range after {
			if ap := pkOf(a, pkCols); ap != nil && pkKey(ap, pkCols) == k {
				return a
			}
		}
		return nil
	}
	if len(after) == nBefore && idx < len(after) {
		return after[idx]
	}
	return nil
}

// reread loads the current rows for the given primary keys on the statement's
// connection. Errors only drop the image (flagged); they never propagate.
func (t *stmtTrace) reread(pks []map[string]any, pkCols []string) []map[string]any {
	if len(pks) == 0 || len(pkCols) == 0 || t.info.TableSQL == "" {
		return nil
	}
	if len(pks) > t.cfg.maxRows {
		pks = pks[:t.cfg.maxRows]
	}
	dollar := t.store == "postgres" || (t.store == "sql" && t.info.Dollar)
	var vals []driver.Value
	ph := func() string {
		vals = append(vals, nil)
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
		for i, pk := range pks {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(ph())
			vals[len(vals)-1] = pk[pkCols[0]]
		}
		b.WriteString(")")
	} else {
		for i, pk := range pks {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("(")
			for j, c := range pkCols {
				if j > 0 {
					b.WriteString(" AND ")
				}
				b.WriteString(q(c) + " = " + ph())
				vals[len(vals)-1] = pk[c]
			}
			b.WriteString(")")
		}
	}
	rs, err := t.c.fetch(t.ctx, b.String(), vals, t.cfg.maxRows)
	if err != nil {
		t.span.AddFlag("after_failed")
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
