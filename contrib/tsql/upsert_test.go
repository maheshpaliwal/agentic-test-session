package tsql_test

// Upserts (INSERT ... ON CONFLICT DO UPDATE, ON DUPLICATE KEY UPDATE,
// REPLACE): a row that was already there is recorded as an update, with the
// row before it and only the fields that changed; one whose outcome is not
// known carries no diff. Fake driver; upsert_pg_test.go has a real Postgres.

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// upTable is a table of the fake database: rows found by one column (key),
// which need not be the primary key. locked marks the rows whose xmax is set,
// as it is on the row an ON CONFLICT DO UPDATE wrote.
type upTable struct {
	cols   []string
	key    int // index in cols of the column the statements find a row by
	rows   [][]driver.Value
	locked map[string]bool
	ver    map[string]int // how often the row was written: a write stores a new row version elsewhere
	noXmax bool           // a view: it has no system columns
	nextID int64
}

func (u *upTable) find(k driver.Value) int {
	for i, r := range u.rows {
		if fmt.Sprint(r[u.key]) == fmt.Sprint(k) {
			return i
		}
	}
	return -1
}

// sel answers a read of the rows whose key is one of args.
func (u *upTable) sel(q string, args []driver.Value) result {
	sys := strings.Contains(q, "xmax")
	if sys && u.noXmax {
		return result{err: fmt.Errorf(`ERROR: column "xmax" does not exist (SQLSTATE 42703)`)}
	}
	r := result{cols: append([]string(nil), u.cols...)}
	if sys {
		r.cols = append(r.cols, "ts_capture_xmax", "ts_capture_ver")
	}
	for _, row := range u.rows {
		for _, a := range args {
			if fmt.Sprint(a) == fmt.Sprint(row[u.key]) {
				out := append([]driver.Value(nil), row...)
				if sys {
					x := "0"
					if u.locked[fmt.Sprint(row[u.key])] {
						x = "731"
					}
					out = append(out, x, fmt.Sprintf("16384:(0,%d)", u.ver[fmt.Sprint(row[u.key])]+1))
				}
				r.rows = append(r.rows, out)
				break
			}
		}
	}
	return r
}

// put is what ON CONFLICT (key) DO UPDATE does with one row: set the columns
// in set on the row that is there, or insert.
func (u *upTable) put(row []driver.Value, set ...int) {
	if u.locked == nil {
		u.locked = map[string]bool{}
	}
	k := fmt.Sprint(row[u.key])
	if u.ver == nil {
		u.ver = map[string]int{}
	}
	u.ver[k]++
	if i := u.find(row[u.key]); i >= 0 {
		for _, c := range set {
			u.rows[i][c] = row[c]
		}
		u.locked[k] = true
		return
	}
	u.rows = append(u.rows, append([]driver.Value(nil), row...))
	u.locked[k] = false
}

// stockTable is stock(sku PRIMARY KEY, qty, name) holding "widget".
func stockTable() *upTable {
	return &upTable{cols: []string{"sku", "qty", "name"}, rows: [][]driver.Value{{"widget", int64(10), "Widget"}}}
}

const stockUpsert = "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty"

// upFake is a fake database with one table. Every statement that is not a
// SELECT is an upsert of the rows in its arguments (width columns each) that
// sets the columns in set on a row that is there.
func upFake(u *upTable, set ...int) *fakeDB {
	return &fakeDB{
		pk: func(string) result { return pkRows(u.cols[0]) },
		handler: func(q string, args []driver.Value) result {
			if strings.HasPrefix(q, "SELECT") {
				return u.sel(q, args)
			}
			if !strings.HasPrefix(q, "INSERT") && !strings.HasPrefix(q, "REPLACE") {
				return result{affected: 1}
			}
			n := int64(0)
			for w := len(u.cols); len(args) >= w; args = args[w:] {
				u.put(args[:w], set...)
				n++
			}
			return result{affected: n}
		},
	}
}

// upDiff is a mutation's diff as "field:old->new", in order.
func upDiff(m core.Mutation) string {
	var out []string
	for _, d := range m.Diff {
		out = append(out, fmt.Sprintf("%s:%v->%v", d.Field, d.Old, d.New))
	}
	return strings.Join(out, " ")
}

// upCheck asserts what one mutation of an upsert says: op, whether it has a
// before image, and its diff.
func upCheck(t *testing.T, m core.Mutation, op string, before bool, diff string) {
	t.Helper()
	if m.Op != op || m.Before.IsZero() == before || upDiff(m) != diff {
		t.Errorf("mutation op %q, before image %v, diff %q; want %q, %v, %q", m.Op, !m.Before.IsZero(), upDiff(m), op, before, diff)
	}
}

// An upsert of a row that is not there is recorded as it always was: the new
// row, every field set from nothing.
func TestUpsertInsertPath(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, upFake(stockTable(), 1), "postgres")
	if _, err := db.ExecContext(ctx, stockUpsert, "gadget", 5, "Gadget"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	m := e.Mutations[0]
	upCheck(t, m, "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	if !reflect.DeepEqual(m.PK, map[string]any{"sku": "gadget"}) || jsonMap(t, m.After)["qty"] != float64(5) {
		t.Errorf("mutation %+v", m)
	}
}

// The defect: an upsert that updated a row is an update of that row, with
// the row as it was and only the changed field in the diff.
func TestUpsertUpdatePath(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, upFake(stockTable(), 1), "postgres")
	if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7, "ignored"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	m := e.Mutations[0]
	upCheck(t, m, "update", true, "qty:10->7")
	if !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) {
		t.Errorf("pk %v", m.PK)
	}
	if b, a := jsonMap(t, m.Before), jsonMap(t, m.After); b["qty"] != float64(10) || a["qty"] != float64(7) || a["name"] != "Widget" {
		t.Errorf("before %v after %v", b, a)
	}
}

// DO NOTHING writes new rows only: nothing is read before it, a statement
// that met a row records no mutation, and one that inserted is an insert.
func TestUpsertDoNothing(t *testing.T) {
	const stmt = "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) DO NOTHING"
	rec, ctx := setup(t)
	u := stockTable()
	f := upFake(u)
	inner := f.handler
	f.handler = func(q string, args []driver.Value) result {
		if strings.HasPrefix(q, "INSERT") && u.find(args[0]) >= 0 {
			return result{affected: 0}
		}
		return inner(q, args)
	}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, stmt, "widget", 7, "x"); err != nil {
		panic(err)
	}
	if e := lastWrite(t, rec); len(e.Mutations) != 0 || core.HasUncertainFlag(e.Flags) {
		panic(fmt.Sprintf("a row that was left alone: mutations %+v flags %v", e.Mutations, e.Flags))
	}
	if calls := f.Calls(); len(calls) != 1 {
		panic(fmt.Sprintf("calls:\n%s", sqls(calls)))
	}
	if _, err := db.ExecContext(ctx, stmt, "gadget", 5, "Gadget"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	upCheck(t, e.Mutations[0], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
	if calls := f.Calls(); len(calls) != 3 || !strings.HasPrefix(calls[2].SQL, "SELECT") {
		panic(fmt.Sprintf("calls, want the statement and one read after it:\n%s", sqls(calls)))
	}
}

// A multi-row upsert: each row is recorded as what happened to it.
func TestUpsertMultiRowMix(t *testing.T) {
	rec, ctx := setup(t)
	u := stockTable()
	u.rows = append(u.rows, []driver.Value{"bolt", int64(3), "Bolt"})
	db := open(t, upFake(u, 1), "postgres")
	_, err := db.ExecContext(ctx, "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty",
		"widget", 7, "x", "gadget", 5, "Gadget", "bolt", 4, "y")
	if err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if core.HasUncertainFlag(e.Flags) || len(e.Mutations) != 3 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	got := map[any]core.Mutation{}
	for _, m := range e.Mutations {
		got[m.PK["sku"]] = m
	}
	upCheck(t, got["widget"], "update", true, "qty:10->7")
	upCheck(t, got["bolt"], "update", true, "qty:3->4")
	upCheck(t, got["gadget"], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
}

// usersTable is users(id PRIMARY KEY generated, email UNIQUE, name): the
// statements find a row by email, and its key is not in VALUES.
func usersTable() *upTable {
	return &upTable{cols: []string{"id", "email", "name"}, key: 1, nextID: 8,
		rows: [][]driver.Value{{int64(7), "a@example.com", "Ann"}}}
}

// usersFake upserts (email, name) into usersTable; the database gives the id.
func usersFake(u *upTable) *fakeDB {
	f := upFake(u)
	inner := f.handler
	f.handler = func(q string, args []driver.Value) result {
		if !strings.HasPrefix(q, "INSERT") {
			return inner(q, args)
		}
		if u.find(args[0]) < 0 {
			defer func() { u.nextID++ }()
		}
		u.put([]driver.Value{u.nextID, args[0], args[1]}, 2)
		return result{affected: 1}
	}
	return f
}

const usersUpsert = "INSERT INTO users (email, name) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name"

// The conflict target is not the primary key: the row is found by the
// target's columns and filed under the key it has.
func TestUpsertConflictTargetNotPrimaryKey(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, usersFake(usersTable()), "postgres")
	if _, err := db.ExecContext(ctx, usersUpsert, "a@example.com", "Anna"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	upCheck(t, e.Mutations[0], "update", true, "name:Ann->Anna")
	if !reflect.DeepEqual(e.Mutations[0].PK, map[string]any{"id": int64(7)}) {
		t.Errorf("pk %v, want the row's id", e.Mutations[0].PK)
	}
	if _, err := db.ExecContext(ctx, usersUpsert, "b@example.com", "Bob"); err != nil {
		panic(err)
	}
	e = lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		panic(fmt.Sprintf("mutations %+v flags %v", e.Mutations, e.Flags))
	}
	upCheck(t, e.Mutations[0], "upsert", false, "email:<nil>->b@example.com id:<nil>->8 name:<nil>->Bob")
	if !reflect.DeepEqual(e.Mutations[0].PK, map[string]any{"id": int64(8)}) {
		t.Errorf("pk %v, want the generated id", e.Mutations[0].PK)
	}
}

// upUnknown asserts that every mutation of e says no more than is known: no
// before image and no diff, under a flag that marks the record uncertain.
func upUnknown(t *testing.T, e core.Event) {
	t.Helper()
	if len(e.Mutations) == 0 {
		panic(fmt.Sprintf("no mutation (flags %v)", e.Flags))
	}
	for _, m := range e.Mutations {
		if m.Op != "upsert" || !m.Before.IsZero() || len(m.Diff) != 0 {
			t.Errorf("mutation %+v: want an upsert without before image and diff", m)
		}
	}
	if !hasFlag(e, "upsert_unknown") || !core.UncertainFlag("upsert_unknown") {
		t.Errorf("flags %v, want upsert_unknown, an uncertain flag", e.Flags)
	}
}

// Where the row the statement can collide with cannot be told from its text,
// what it did is not known and nothing is made up.
func TestUpsertOutcomeUnknown(t *testing.T) {
	for _, tc := range []struct{ name, stmt string }{
		{"on constraint", "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT stock_pkey DO UPDATE SET qty = EXCLUDED.qty"},
		{"partial index", "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku) WHERE qty > 0 DO UPDATE SET qty = EXCLUDED.qty"},
		{"expression", "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (lower(sku)) DO UPDATE SET qty = EXCLUDED.qty"},
		{"collation", `INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3) ON CONFLICT (sku COLLATE "C") DO UPDATE SET qty = EXCLUDED.qty`},
		{"key is an expression", "INSERT INTO stock (sku, qty, name) VALUES (lower($1), $2, $3) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty"},
		{"insert from select", "INSERT INTO stock (sku, qty, name) SELECT $1, $2, $3 ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty"},
		{"merge", "MERGE INTO stock USING (SELECT $1 AS sku, $2 AS qty, $3 AS name) s ON stock.sku = s.sku WHEN MATCHED THEN UPDATE SET qty = s.qty WHEN NOT MATCHED THEN INSERT VALUES (s.sku, s.qty, s.name)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t)
			f := upFake(stockTable(), 1)
			db := open(t, f, "postgres")
			if _, err := db.ExecContext(ctx, tc.stmt, "widget", 7, "x"); err != nil {
				panic(err)
			}
			upUnknown(t, lastWrite(t, rec))
			for _, c := range f.Calls() {
				if strings.HasPrefix(c.SQL, "SELECT") {
					t.Errorf("a row was read although the statement does not say which: %s", c.SQL)
				}
			}
		})
	}
}

// Under SERIALIZABLE nothing is read, so nothing is known about the row.
func TestUpsertSerializable(t *testing.T) {
	rec, ctx := setup(t)
	f := upFake(stockTable(), 1)
	db := open(t, f, "postgres", noPKLookup, tsql.WithPKColumns("stock", "sku"))
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, stockUpsert, "widget", 7, "x"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if !hasFlag(e, "image_skipped:serializable") || len(f.RawCalls()) != 1 {
		panic(fmt.Sprintf("flags %v, driver saw:\n%s", e.Flags, sqls(f.RawCalls())))
	}
}

// Sensitive columns are masked in both images and in the diff.
func TestUpsertRedactsBothImages(t *testing.T) {
	rec, ctx := setup(t)
	u := &upTable{cols: []string{"id", "password", "name"}, rows: [][]driver.Value{{int64(1), "old-secret", "Ann"}}}
	db := open(t, upFake(u, 1, 2), "postgres")
	_, err := db.ExecContext(ctx, "INSERT INTO accounts (id, password, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET password = EXCLUDED.password, name = EXCLUDED.name",
		1, "new-secret", "Anna")
	if err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 {
		panic(fmt.Sprintf("mutations %+v", e.Mutations))
	}
	m := e.Mutations[0]
	upCheck(t, m, "update", true, fmt.Sprintf("name:Ann->Anna password:%v->%v", core.RedactedValue, core.RedactedValue))
	if b, a := jsonMap(t, m.Before), jsonMap(t, m.After); b["password"] != core.RedactedValue || a["password"] != core.RedactedValue {
		t.Errorf("before %v after %v", b, a)
	}
	if js := fenceRecorded(t, rec); strings.Contains(js, "secret") {
		t.Errorf("a secret was recorded: %s", js)
	}
}

// Outside a session, and with images off, an upsert is the application's
// statement and nothing else.
func TestUpsertSendsNothingWhenNotCapturing(t *testing.T) {
	rec, ctx := setup(t)
	f := upFake(stockTable(), 1)
	db := open(t, f, "postgres")
	if _, err := db.Exec(stockUpsert, "widget", 7, "x"); err != nil {
		panic(err)
	}
	if got := f.RawCalls(); len(got) != 1 || len(rec.Events()) != 0 {
		panic(fmt.Sprintf("outside a session: %d events, driver saw:\n%s", len(rec.Events()), sqls(got)))
	}
	f = upFake(stockTable(), 1)
	db = open(t, f, "postgres", tsql.WithImages(false))
	if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7, "x"); err != nil {
		panic(err)
	}
	e := lastWrite(t, rec)
	if got := f.RawCalls(); len(got) != 1 || len(e.Mutations) != 1 {
		panic(fmt.Sprintf("images off: mutations %+v, driver saw:\n%s", e.Mutations, sqls(got)))
	}
	if m := e.Mutations[0]; m.Op != "upsert" || !m.Before.IsZero() || !m.After.IsZero() || len(m.Diff) != 0 {
		panic(fmt.Sprintf("images off: %+v", m))
	}
}
