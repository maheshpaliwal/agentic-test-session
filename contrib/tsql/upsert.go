package tsql

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

// xmaxCol is the column the read after a Postgres upsert adds to the row: the
// row version's xmax, as text. A version that an INSERT wrote has xmax 0; the
// one ON CONFLICT DO UPDATE wrote carries the transaction that locked the
// row it replaced. That tells the two apart without touching the statement
// (a RETURNING clause would need SELECT rights the application may not have,
// fails on a partitioned table, and changes what the application receives).
const xmaxCol = "ts_capture_xmax"

// verCol is read with it, before the statement and after: where the row
// version is stored (its table, for a partitioned one, and its ctid). xmax
// alone does not say that THIS statement wrote the row: it stays set on a
// row that was ever locked or updated. A row whose version is where it was
// before the statement was not written by it, whatever its xmax says: the
// WHERE of DO UPDATE left it alone, or the statement wrote another row (a
// trigger changed the key, the server rounded it).
const verCol = "ts_capture_ver"

// noSysCols reports whether the last capture's read failed because the
// relation has no system columns (a view, a database that only speaks the
// protocol): SQLSTATE 42703, undefined column. Any other failure (no SELECT
// right, a timeout) says nothing about them.
func (c *conn) noSysCols() bool {
	if c.readErr == nil {
		return false
	}
	if state, coded := sqlState(c.readErr); coded {
		return state == "42703"
	}
	m := c.readErr.Error()
	return strings.Contains(m, "42703") || strings.Contains(m, "does not exist") &&
		(strings.Contains(m, "xmax") || strings.Contains(m, "ctid") || strings.Contains(m, "tableoid"))
}

// takeSys removes the columns a read with sys added from a row image and
// returns them. has is false for a row read without them.
func takeSys(m *img) (xmax, ver string, has bool) {
	x, ok := m.disp[xmaxCol]
	if !ok {
		return "", "", false
	}
	v := m.disp[verCol]
	delete(m.disp, xmaxCol)
	delete(m.disp, verCol)
	delete(m.raw, xmaxCol)
	delete(m.raw, verCol)
	return fmt.Sprint(x), fmt.Sprint(v), true
}

// upsertRead is what is read around an upsert that can update a row: the
// rows with the keys in VALUES, before the statement and after it.
type upsertRead struct {
	// cols are the columns the statement finds a row that is in its way by:
	// the conflict target on Postgres (which need not be the primary key),
	// the primary key on MySQL. exact says they are the catalog's spelling.
	cols  []string
	exact bool
	// keyCols are cols and the primary key: the columns whose values are
	// kept faithfully in a row image.
	keyCols []string
	keys    []pkRef // the values of cols, one per row of VALUES

	before   []img
	beforeOK bool // the rows were read before the statement
	// ver is where the version of each row of before was stored (verCol),
	// by the row's key; empty when the database does not say.
	ver map[string]string
	// notKey: cols matched more rows than there are keys, so they do not
	// identify a row.
	notKey bool
}

// key is the value of u.cols in a row image, comparable between two reads.
func (u *upsertRead) key(m img) string {
	if m.raw != nil {
		return rawKey(m.raw, u.cols)
	}
	var b strings.Builder
	for _, c := range u.cols {
		v, _ := lookupCI(m.disp, c)
		fmt.Fprintf(&b, "%v\x00", v)
	}
	return b.String()
}

// upsertPlan says by which columns and values the rows of an upsert are
// found, or why they cannot be: a guess at the row would be recorded as the
// row the statement wrote.
func (t *stmtTrace) upsertPlan() (u *upsertRead, reason string) {
	in := t.info
	u = &upsertRead{}
	switch c := in.Conflict; {
	case len(c.Target) > 0 && t.pg():
		u.cols = c.Target
	case c.AnyKey && t.store == "mysql" && !in.MultiRow:
		// Whichever unique key has the row's values is the one it collides
		// on; only the primary key is known here.
		u.cols, u.exact = t.pk, t.pkCatalog
	case c.AnyKey && t.store == "mysql":
		return nil, "multi_row"
	default:
		return nil, "conflict_target"
	}
	rows := in.Rows
	if !in.MultiRow && in.Values != nil {
		rows = []map[string]mutation.Value{in.Values}
	}
	switch {
	case in.TableSQL == "" || len(u.cols) == 0:
		return nil, "no_table"
	case len(rows) == 0:
		return nil, "no_values"
	case len(rows) > t.cfg.maxRows:
		return nil, "too_many_rows"
	}
	for _, row := range rows {
		ref := t.pkFromValues(row, u.cols)
		if ref.disp == nil {
			return nil, "key_not_in_values"
		}
		for _, c := range u.cols {
			if ref.raw[c] == nil { // NULL collides with nothing, and cannot be looked for with "="
				return nil, "pk_unbindable"
			}
		}
		u.keys = append(u.keys, ref)
	}
	u.keyCols = append(u.keyCols, u.cols...)
	for _, c := range t.pk {
		if !isKeyCol(c, u.cols) {
			u.keyCols = append(u.keyCols, c)
		}
	}
	return u, ""
}

// upsertBefore reads the rows an upsert can update before it runs. DO
// NOTHING needs no such read: every row it writes is new.
func (t *stmtTrace) upsertBefore() {
	if !t.write || !t.cfg.images || t.skip || t.info.Conflict.DoNothing {
		return
	}
	u, reason := t.upsertPlan()
	if u == nil {
		t.flag("image_skipped")
		if reason == "pk_unbindable" {
			t.flag("image_skipped:pk_unbindable")
		}
		t.span.SetAttr("image_skip_reason", reason)
		return
	}
	t.ups = u
	if !t.cfg.upsertBefore {
		return
	}
	if t.aborted {
		t.flag("image_failed")
		return
	}
	sys := t.pg() && !t.c.pks.noSys(t.info.TableSQL)
	q, vals := t.keySelect(u.cols, u.exact, u.keys, sys)
	failFlag := "image_failed"
	if sys {
		failFlag = "" // a view has no system columns: asked again without them below
	}
	rs, st := t.captureKeyed(q, vals, len(u.keys)+1, failFlag, u.keyCols)
	switch {
	case sys && st == capFailed && t.c.noSysCols():
		t.c.pks.setNoSys(t.info.TableSQL)
		q, vals = t.keySelect(u.cols, u.exact, u.keys, false)
		rs, st = t.captureKeyed(q, vals, len(u.keys)+1, "image_failed", u.keyCols)
	case sys && (st == capFailed || st == capAborted):
		t.flag("image_failed")
	}
	if st != capOK {
		return
	}
	if len(rs) > len(u.keys) {
		t.upsertNotKey(u)
		return
	}
	u.ver = map[string]string{}
	for i := range rs {
		if _, ver, has := takeSys(&rs[i]); has {
			u.ver[u.key(rs[i])] = ver
		}
	}
	u.before, u.beforeOK = rs, true
}

// upsertNotKey notes that u.cols matched more rows than keys: they are a
// guessed key (the default "id", or one that changed since it was looked up).
func (t *stmtTrace) upsertNotKey(u *upsertRead) {
	u.notKey = true
	t.flag("pk_not_unique")
	t.c.pks.drop(t.info.TableSQL)
}

// What an upsert did to one row.
const (
	upUnknown = iota
	upInserted
	upUpdated
	upUntouched // the row is where it was: the statement did not write it
)

// upsertRow is one row read after an upsert.
type upsertRow struct {
	after  *img
	before *img // the same row before the statement, when it was read and there
	// fresh: 1 when the row version is one an INSERT wrote (xmax 0), 0 when
	// it is not, -1 when the database does not say.
	fresh int
	kind  int
}

// upsertKind says what the statement did to r.
//
// With xmax (Postgres): a fresh version is a row the statement inserted, any
// other a row it updated. The read before the statement must agree: a row
// that was there and is now fresh, or one that was not there and is not
// fresh, was written by someone else in between (deleted, or inserted), and
// which values the statement met is not known.
//
// Without it (MySQL; a Postgres view) only the read before the statement
// says whether the row was there. On MySQL the affected-row count has to
// agree, in either client convention: 1 for a row inserted, 2 for a row
// updated (or 1, with CLIENT_FOUND_ROWS, for one set to the values it had);
// REPLACE counts 1 and one more for every row it deleted.
func (t *stmtTrace) upsertKind(r upsertRow, u *upsertRead, affected int64) int {
	was := r.before != nil
	switch {
	case r.fresh == 1 && !was:
		return upInserted
	case r.fresh == 0 && was:
		return upUpdated
	case r.fresh == 0 && !u.beforeOK:
		// No read before the statement (WithUpsertBefore(false)): a version
		// that is not fresh is a row the statement updated, or one it
		// inserted and something then locked (a foreign-key check, a
		// trigger). Not known.
		return upUnknown
	case r.fresh >= 0 || !u.beforeOK:
		return upUnknown
	case t.pg():
		if was {
			return upUpdated
		}
		return upInserted
	case !was && affected == 1:
		return upInserted
	case was && (affected == 2 || affected == 1 && !t.info.Conflict.Replace):
		return upUpdated
	}
	return upUnknown
}

// addUpserts records the rows of an upsert that can update (see doc.go,
// "Upserts"): a row it inserted as the new row, a row it updated as an
// update with the row before it, and a row of which that is not known
// without before image and diff, under the flag "upsert_unknown".
func (t *stmtTrace) addUpserts(res driver.Result, ret []img, affected int64, emit func(op string, before, after *img, fallback map[string]any, noDiff bool)) {
	in, u := t.info, t.ups
	if in.MultiRow {
		t.flag("multi_row_insert")
	}
	if !t.cfg.images {
		// The key in VALUES is the written row's key only if it is what the
		// statement collides on: with another conflict target the row that
		// is updated can be a different one.
		pk := t.pkFromValues(in.Values, t.pk).disp
		if t.pg() && !in.Conflict.DoNothing && !sameCols(in.Conflict.Target, t.pk) {
			pk = nil
			t.flag("upsert_unknown")
		}
		emit("upsert", nil, nil, pk, true)
		return
	}
	// unknown records rows of which it is not known whether they were
	// there: their after images, or one mutation without image, filed under
	// pk if that is known to be the row's key.
	unknown := func(pk map[string]any, after ...img) {
		t.flag("upsert_unknown")
		for i := range after {
			emit("upsert", nil, &after[i], nil, true)
		}
		if len(after) == 0 {
			emit("upsert", nil, nil, pk, true)
		}
	}
	if u == nil || u.notKey {
		unknown(nil, t.upsertWritten(res, ret, affected)...)
		return
	}
	read := func(sys bool, failFlag string) ([]img, capStatus) {
		q, vals := t.keySelect(u.cols, u.exact, u.keys, sys)
		return t.captureKeyed(q, vals, len(u.keys)+1, failFlag, u.keyCols)
	}
	sys := t.pg() && !t.c.pks.noSys(in.TableSQL)
	rs, st := read(sys, "")
	if sys && st == capFailed && t.c.noSysCols() {
		// A view has no xmax, nor has every database that speaks the
		// protocol. The read failed inside the fence; it is made again
		// without, and for a while the table is not asked for it. A read
		// that failed for another reason (no SELECT right) is not a sign of
		// that, and must not cost later statements the columns.
		t.c.pks.setNoSys(in.TableSQL)
		rs, st = read(false, "")
	}
	if st != capOK {
		if st == capFailed || st == capAborted {
			t.flag("after_failed")
		}
		unknown(t.upsertPK())
		return
	}
	if len(rs) > len(u.keys) {
		t.upsertNotKey(u)
		unknown(nil)
		return
	}

	was := make(map[string]*img, len(u.before))
	for i := range u.before {
		was[u.key(u.before[i])] = &u.before[i]
	}
	rows := make([]upsertRow, len(rs))
	for i := range rs {
		r := upsertRow{after: &rs[i], fresh: -1}
		xmax, ver, hasSys := takeSys(&rs[i])
		if hasSys {
			r.fresh = 0
			if xmax == "0" {
				r.fresh = 1
			}
		}
		k := u.key(rs[i])
		r.before = was[k]
		delete(was, k)
		r.kind = t.upsertKind(r, u, affected)
		if r.before != nil && hasSys && ver != "" && ver == u.ver[k] {
			r.kind = upUntouched
		}
		rows[i] = r
	}
	// Rows the statement did not write are not its mutations. If it wrote
	// as many rows as are accounted for without them, that is all: DO
	// UPDATE ... WHERE left them alone. If its row count is not known, or it
	// wrote a row that is not found under the keys in VALUES, what it wrote
	// is not known (the count check below says so).
	untouched := 0
	written := rows[:0:0]
	for _, r := range rows {
		if r.kind == upUntouched {
			untouched++
			continue
		}
		written = append(written, r)
	}
	rows = written
	if untouched > 0 && affected < 0 {
		unknown(nil)
	}

	// The statement's row count must account for the rows that were found
	// and those that are gone from under their key (an update moved them).
	// On MySQL it counts an updated row twice; upsertKind has looked at it.
	missing := false
	switch extra := int64(len(rows)+len(was)) - affected; {
	case !t.pg():
		if len(rows) == 0 && len(was) != 1 {
			unknown(nil, t.upsertWritten(res, ret, affected)...)
			return
		}
	case affected < 0 || extra == 0:
	case extra < 0:
		missing = true // rows were written that are not found under the keys in VALUES
	default:
		// More rows than were written: some were left as they were by the
		// WHERE of DO UPDATE, which locks them all the same. They are the
		// ones that did not change, if that many did not.
		keep := rows[:0:0]
		for _, r := range rows {
			if r.kind == upUpdated && r.before != nil {
				if d, hidden := diffMaps(r.before.disp, r.after.disp); len(d) == 0 && !hidden {
					continue
				}
			}
			keep = append(keep, r)
		}
		if int64(len(rows)-len(keep)) == extra {
			rows = keep
			break
		}
		for i := range rows {
			rows[i].kind = upUnknown
		}
		was = nil
	}
	for _, r := range rows {
		switch r.kind {
		case upInserted:
			emit("upsert", nil, r.after, nil, false)
		case upUpdated:
			emit("update", r.before, r.after, nil, false)
			if r.before == nil && !t.flagged["image_skipped"] && !t.flagged["image_timeout"] && !t.flagged["image_failed"] {
				t.flag("image_skipped")
				t.span.SetAttr("image_skip_reason", "upsert_before_off")
			}
		default:
			unknown(nil, *r.after)
		}
	}
	// A row that was there and is no longer found under its key: the update
	// moved it to another, and its new values are not known.
	for _, b := range u.before {
		if was[u.key(b)] != nil {
			emit("update", &b, nil, nil, false)
			t.flag("after_unavailable")
		}
	}
	if missing {
		unknown(nil)
	}
}

// upsertPK is the key in VALUES when it is the key the statement collides
// on: the row the statement wrote has it, whether it was inserted or updated
// (unless the update moved it, which only a read after the statement shows).
func (t *stmtTrace) upsertPK() map[string]any {
	u := t.ups
	if u == nil || t.info.MultiRow || len(u.cols) != len(t.pk) {
		return nil
	}
	for _, c := range t.pk {
		if !isKeyCol(c, u.cols) {
			return nil
		}
	}
	return t.pkFromValues(t.info.Values, t.pk).disp
}

// upsertWritten returns rows the statement is known to have written although
// they could not be looked for by a key: the rows of its own RETURNING *,
// and on MySQL the row with the generated key the server reported.
func (t *stmtTrace) upsertWritten(res driver.Result, ret []img, affected int64) []img {
	switch {
	case t.retAll && len(ret) > 0:
		return ret
	case len(ret) > 0: // RETURNING with some columns: the keys, if they are among them
		if rr := t.reread(refsOf(ret, t.pk), t.pk); len(rr) == len(ret) {
			return rr
		}
	case t.store == "mysql" && len(t.pk) == 1 && res != nil && affected == 1 && !t.info.MultiRow:
		if id, err := res.LastInsertId(); err == nil && id > 0 {
			ref := pkRef{disp: map[string]any{t.pk[0]: id}, raw: map[string]driver.Value{t.pk[0]: id}}
			if rr := t.reread([]pkRef{ref}, t.pk); len(rr) == 1 {
				return rr
			}
		}
	}
	return nil
}

// sameCols reports whether a and b name the same columns, in any order.
func sameCols(a, b []string) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	for _, c := range a {
		if !isKeyCol(c, b) {
			return false
		}
	}
	return true
}
