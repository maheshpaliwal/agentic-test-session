package tsql_test

// MySQL upserts (ON DUPLICATE KEY UPDATE, REPLACE) against the fake driver
// only: there is no MySQL to run them on. The rows are found by the primary
// key in VALUES, and the outcome is taken as known only where the read
// before the statement and the affected-row count agree in either client
// convention (CLIENT_FOUND_ROWS or not).

import (
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

const (
	myODKU    = "INSERT INTO stock (sku, qty, name) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE qty = VALUES(qty)"
	myReplace = "REPLACE INTO stock (sku, qty, name) VALUES (?, ?, ?)"
)

// myFake is upFake for MySQL: the statement reports affected rows, and
// lastID as the generated key.
func myFake(u *upTable, affected, lastID int64, set ...int) *fakeDB {
	f := upFake(u, set...)
	inner := f.handler
	f.handler = func(q string, args []driver.Value) result {
		r := inner(q, args)
		if !strings.HasPrefix(q, "SELECT") {
			r.affected, r.lastID = affected, lastID
		}
		return r
	}
	return f
}

func TestMySQLUpsert(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stmt     string
		sku      string
		affected int64
		set      []int
		op       string // "" when the outcome is not known
		diff     string
	}{
		{"inserted", myODKU, "gadget", 1, []int{1}, "upsert", "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget"},
		{"updated", myODKU, "widget", 2, []int{1}, "update", "qty:10->5"},
		// CLIENT_FOUND_ROWS: 1 for a row that was there and keeps its values
		{"unchanged, found rows", myODKU, "widget", 1, nil, "update", ""},
		// no row with this key before, yet more than an insert was counted:
		// the statement collided on another unique key
		{"collided on another key", myODKU, "gadget", 2, []int{1}, "", ""},
		{"replace inserted", myReplace, "gadget", 1, []int{1, 2}, "upsert", "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget"},
		{"replace replaced", myReplace, "widget", 2, []int{1, 2}, "update", "name:Widget->Gadget qty:10->5"},
		// REPLACE deleted a second row, through another unique key
		{"replace deleted more", myReplace, "widget", 3, []int{1, 2}, "", ""},
		{"replace met another key", myReplace, "gadget", 2, []int{1, 2}, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t)
			f := myFake(stockTable(), tc.affected, 0, tc.set...)
			db := open(t, f, "mysql")
			if _, err := db.ExecContext(ctx, tc.stmt, tc.sku, 5, "Gadget"); err != nil {
				t.Fatal(err)
			}
			e := lastWrite(t, rec)
			if got := f.Calls(); len(got) != 3 || !strings.Contains(got[0].SQL, "* FROM stock WHERE `sku` IN (?)") || got[2].SQL != got[0].SQL {
				t.Fatalf("calls, want one read before and one after:\n%s", sqls(got))
			}
			if tc.op == "" {
				upUnknown(t, e)
				return
			}
			if len(e.Mutations) != 1 || len(e.Flags) != 0 {
				t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
			}
			upCheck(t, e.Mutations[0], tc.op, tc.op == "update", tc.diff)
		})
	}
}

// The key is generated, so the statement does not say which row it can meet:
// the row the server reports as inserted is read back, and whether it was
// there before is not known.
func TestMySQLUpsertGeneratedKey(t *testing.T) {
	rec, ctx := setup(t)
	u := usersTable()
	f := usersFake(u)
	inner := f.handler
	f.handler = func(q string, args []driver.Value) result {
		r := inner(q, args)
		if strings.HasPrefix(q, "INSERT") {
			r.lastID = 8
		}
		return r
	}
	db := open(t, f, "mysql")
	u.key = 0 // the read after the statement is by id
	if _, err := db.ExecContext(ctx, "INSERT INTO users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)", "b@example.com", "Bob"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if m := e.Mutations[0]; !reflect.DeepEqual(m.PK, map[string]any{"id": int64(8)}) || jsonMap(t, m.After)["name"] != "Bob" {
		t.Fatalf("mutation %+v", m)
	}
	if !hasFlag(e, "image_skipped") || e.Attrs["image_skip_reason"] != "key_not_in_values" {
		t.Fatalf("flags %v attrs %v", e.Flags, e.Attrs)
	}
}

// A multi-row upsert counts an updated row twice: the count says nothing
// about a single row, so nothing is read and nothing is claimed.
func TestMySQLUpsertMultiRow(t *testing.T) {
	rec, ctx := setup(t)
	f := myFake(stockTable(), 3, 0, 1)
	db := open(t, f, "mysql")
	if _, err := db.ExecContext(ctx, "INSERT INTO stock (sku, qty, name) VALUES (?, ?, ?), (?, ?, ?) ON DUPLICATE KEY UPDATE qty = VALUES(qty)", "widget", 7, "x", "gadget", 5, "Gadget"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if got := f.Calls(); len(got) != 1 || len(e.Mutations) != 1 || !hasFlag(e, "multi_row_insert") {
		t.Fatalf("mutations %+v flags %v calls:\n%s", e.Mutations, e.Flags, sqls(got))
	}
}

// The default key "id" is a guess where the catalog has none: when it
// matches more than the one row, it does not say which row was written.
func TestMySQLUpsertKeyNotUnique(t *testing.T) {
	rec, ctx := setup(t)
	u := &upTable{cols: []string{"id", "v"}, rows: [][]driver.Value{{int64(1), "a"}, {int64(1), "b"}}}
	f := myFake(u, 2, 0, 1)
	f.pk = nil
	db := open(t, f, "mysql")
	if _, err := db.ExecContext(ctx, "INSERT INTO log (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)", 1, "c"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if !hasFlag(e, "pk_not_unique") || !e.Mutations[0].After.IsZero() {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
}

// With the read before the statement off there is nothing on MySQL that says
// whether the row was there.
func TestMySQLUpsertBeforeOff(t *testing.T) {
	rec, ctx := setup(t)
	f := myFake(stockTable(), 2, 0, 1)
	db := open(t, f, "mysql", tsql.WithUpsertBefore(false))
	if _, err := db.ExecContext(ctx, myODKU, "widget", 5, "x"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if got := f.Calls(); len(got) != 2 || jsonMap(t, e.Mutations[0].After)["qty"] != float64(5) || !core.HasUncertainFlag(e.Flags) {
		t.Fatalf("mutations %+v calls:\n%s", e.Mutations, sqls(got))
	}
}
