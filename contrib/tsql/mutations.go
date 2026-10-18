package tsql

import (
	"database/sql/driver"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

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
