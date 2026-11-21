package tsql_test

// What tsql sends around an upsert, and what it records when one of its two
// reads is missing or the two disagree. Fake driver.

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

const (
	upSelBefore = "SELECT * FROM stock WHERE sku IN ($1)"
	upSelAfter  = "SELECT *, xmax::text AS ts_capture_xmax FROM stock WHERE sku IN ($1)"
)

// An upsert that can update costs what an UPDATE costs: one fenced read
// before and one after. Inside a transaction the fence is a savepoint.
func TestUpsertSequence(t *testing.T) {
	_, ctx := setup(t)
	f := upFake(stockTable(), 1)
	db := open(t, f, "postgres", noPKLookup, tsql.WithPKColumns("stock", "sku"))
	if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7, "x"); err != nil {
		t.Fatal(err)
	}
	fenceWant(t, f,
		"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500", upSelBefore, "ROLLBACK",
		stockUpsert,
		"BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 500", upSelAfter, "ROLLBACK")

	f = upFake(stockTable(), 1)
	f.abortModel = true
	db = open(t, f, "postgres", noPKLookup, tsql.WithPKColumns("stock", "sku"))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the first fence of a connection opens in two statements
		if _, err := tx.ExecContext(ctx, stockUpsert, "widget", 7+i, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	raw := f.RawCalls()
	if len(raw) != 8+7 {
		t.Fatalf("driver saw:\n%s", sqls(raw))
	}
	if got, want := sqls(raw[8:]), strings.Join([]string{
		"SAVEPOINT ts_capture; SET LOCAL statement_timeout = 500", upSelBefore, "ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture",
		stockUpsert,
		"SAVEPOINT ts_capture; SET LOCAL statement_timeout = 500", upSelAfter, "ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture",
	}, "\n")+"\n"; got != want {
		t.Fatalf("driver saw:\n%s\nwant:\n%s", got, want)
	}
	if len(f.saves) != 0 {
		t.Fatalf("savepoints left: %v", f.saves)
	}
}

// The key is looked up once per table and handle, for upserts as for any
// other write.
func TestUpsertUsesKeyCache(t *testing.T) {
	_, ctx := setup(t)
	f := upFake(stockTable(), 1)
	db := open(t, f, "postgres")
	for i := 0; i < 3; i++ {
		if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7+i, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.PKLookups(); len(got) != 1 {
		t.Fatalf("%d key lookups, want 1", len(got))
	}
	if got := f.Calls(); len(got) != 9 {
		t.Fatalf("calls:\n%s", sqls(got))
	}
}

// WithUpsertBefore(false): one read, after the statement. A row the upsert
// inserted is recorded as ever; one it updated is an update whose earlier
// values are not known.
func TestUpsertBeforeOff(t *testing.T) {
	rec, ctx := setup(t)
	f := upFake(stockTable(), 1)
	db := open(t, f, "postgres", tsql.WithUpsertBefore(false))
	if _, err := db.ExecContext(ctx, stockUpsert, "gadget", 5, "Gadget"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		t.Fatalf("insert: mutations %+v flags %v", e.Mutations, e.Flags)
	}
	upCheck(t, e.Mutations[0], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")

	if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7, "x"); err != nil {
		t.Fatal(err)
	}
	e = lastWrite(t, rec)
	if len(e.Mutations) != 1 || !reflect.DeepEqual(e.Flags, []string{"image_skipped"}) || e.Attrs["image_skip_reason"] != "upsert_before_off" {
		t.Fatalf("update: mutations %+v flags %v attrs %v", e.Mutations, e.Flags, e.Attrs)
	}
	upCheck(t, e.Mutations[0], "update", false, "")
	if jsonMap(t, e.Mutations[0].After)["qty"] != float64(7) {
		t.Errorf("after %s", e.Mutations[0].After.Inline)
	}
	calls := f.Calls()
	if len(calls) != 4 || calls[1].SQL != `SELECT *, xmax::text AS ts_capture_xmax FROM stock WHERE sku IN ($1)` || calls[3].SQL != calls[1].SQL {
		t.Fatalf("calls, want each statement and one read after it:\n%s", sqls(calls))
	}
}

// The read before the statement and the row after it disagree: someone else
// wrote the row in between. Neither is taken for what the statement did.
func TestUpsertRace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sku   string
		other func(u *upTable)
	}{
		{"row inserted by another connection after the read", "gadget", func(u *upTable) { u.put([]driver.Value{"gadget", int64(1), "Theirs"}) }},
		{"row deleted by another connection after the read", "widget", func(u *upTable) { u.rows = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t)
			u := stockTable()
			f := upFake(u, 1)
			inner := f.handler
			f.handler = func(q string, args []driver.Value) result {
				if strings.HasPrefix(q, "INSERT") {
					tc.other(u)
				}
				return inner(q, args)
			}
			db := open(t, f, "postgres")
			if _, err := db.ExecContext(ctx, stockUpsert, tc.sku, 7, "Ours"); err != nil {
				t.Fatal(err)
			}
			e := lastWrite(t, rec)
			upUnknown(t, e)
			if len(e.Mutations) != 1 || jsonMap(t, e.Mutations[0].After)["qty"] != float64(7) || e.Mutations[0].PK["sku"] != tc.sku {
				t.Fatalf("mutations %+v", e.Mutations)
			}
		})
	}
}

// The read before the statement fails: the row after it still says whether
// it was inserted; an updated row has no before image and no diff.
func TestUpsertBeforeReadFails(t *testing.T) {
	for _, tc := range []struct {
		sku, op, diff string
	}{
		{"gadget", "upsert", "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget"},
		{"widget", "update", ""},
	} {
		rec, ctx := setup(t)
		f := upFake(stockTable(), 1)
		inner := f.handler
		f.handler = func(q string, args []driver.Value) result {
			if q == upSelBefore {
				return result{err: errors.New("permission denied for table stock")}
			}
			return inner(q, args)
		}
		db := open(t, f, "postgres")
		if _, err := db.ExecContext(ctx, stockUpsert, tc.sku, 5, "Gadget"); err != nil {
			t.Fatal(err)
		}
		e := lastWrite(t, rec)
		if len(e.Mutations) != 1 || !reflect.DeepEqual(e.Flags, []string{"image_failed"}) {
			t.Fatalf("%s: mutations %+v flags %v", tc.sku, e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], tc.op, false, tc.diff)
	}
}

// The read after the statement fails: nothing is known about the row.
func TestUpsertAfterReadFails(t *testing.T) {
	rec, ctx := setup(t)
	f := upFake(stockTable(), 1)
	inner := f.handler
	f.handler = func(q string, args []driver.Value) result {
		if q == upSelAfter {
			return result{err: context.DeadlineExceeded}
		}
		return inner(q, args)
	}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, stockUpsert, "widget", 5, "x"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	upUnknown(t, e)
	if m := e.Mutations[0]; !m.After.IsZero() || !reflect.DeepEqual(m.PK, map[string]any{"sku": "widget"}) || !hasFlag(e, "image_timeout") {
		t.Fatalf("mutation %+v flags %v", m, e.Flags)
	}
}

// A view has no xmax. The first upsert finds that out (its read fails, in
// the fence, and is made again without); later ones do not ask. The rows are
// then told apart by the read before the statement alone.
func TestUpsertRelationWithoutXmax(t *testing.T) {
	rec, ctx := setup(t)
	u := stockTable()
	u.noXmax = true
	f := upFake(u, 1)
	db := open(t, f, "postgres")
	for i, want := range []string{"qty:10->7", "qty:7->8"} {
		if _, err := db.ExecContext(ctx, stockUpsert, "widget", 7+i, "x"); err != nil {
			t.Fatal(err)
		}
		e := lastWrite(t, rec)
		if len(e.Mutations) != 1 || len(e.Flags) != 0 {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], "update", true, want)
	}
	if got := f.Calls(); len(got) != 4+3 || got[2].SQL != upSelAfter || got[6].SQL != upSelBefore {
		t.Fatalf("calls, want the first read after the statement made twice and the second once:\n%s", sqls(got))
	}
	if _, err := db.ExecContext(ctx, stockUpsert, "gadget", 5, "Gadget"); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	if len(e.Mutations) != 1 || len(e.Flags) != 0 {
		t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
	}
	upCheck(t, e.Mutations[0], "upsert", false, "name:<nil>->Gadget qty:<nil>->5 sku:<nil>->gadget")
}

// The statement's row count and the rows found under the keys do not add up.
func TestUpsertMultiRowCount(t *testing.T) {
	const stmt = "INSERT INTO stock (sku, qty, name) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty WHERE stock.qty > 5"
	run := func(t *testing.T, affected int64, apply func(u *upTable)) core.Event {
		rec, ctx := setup(t)
		u := stockTable()
		u.rows = append(u.rows, []driver.Value{"bolt", int64(3), "Bolt"})
		f := upFake(u)
		inner := f.handler
		f.handler = func(q string, args []driver.Value) result {
			if !strings.HasPrefix(q, "INSERT") {
				return inner(q, args)
			}
			apply(u)
			return result{affected: affected}
		}
		db := open(t, f, "postgres")
		if _, err := db.ExecContext(ctx, stmt, "widget", 7, "x", "bolt", 4, "y"); err != nil {
			t.Fatal(err)
		}
		return lastWrite(t, rec)
	}
	// one row was left as it was by the WHERE (and locked): it is no mutation
	t.Run("one row untouched", func(t *testing.T) {
		e := run(t, 1, func(u *upTable) {
			u.put([]driver.Value{"widget", int64(7), "x"}, 1)
			u.put([]driver.Value{"bolt", int64(4), "y"})
		})
		if len(e.Mutations) != 1 || core.HasUncertainFlag(e.Flags) {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], "update", true, "qty:10->7")
	})
	// one row written, both changed: which one the statement wrote is not known
	t.Run("more rows changed than written", func(t *testing.T) {
		e := run(t, 1, func(u *upTable) {
			u.put([]driver.Value{"widget", int64(7), "x"}, 1)
			u.put([]driver.Value{"bolt", int64(4), "y"}, 1)
		})
		upUnknown(t, e)
		if len(e.Mutations) != 2 {
			t.Fatalf("mutations %+v", e.Mutations)
		}
	})
	// a row was written that is not under either key
	t.Run("fewer rows found than written", func(t *testing.T) {
		e := run(t, 3, func(u *upTable) {
			u.put([]driver.Value{"widget", int64(7), "x"}, 1)
			u.put([]driver.Value{"bolt", int64(4), "y"}, 1)
		})
		if len(e.Mutations) != 3 || !hasFlag(e, "upsert_unknown") {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		upCheck(t, e.Mutations[0], "update", true, "qty:10->7")
		upCheck(t, e.Mutations[1], "update", true, "qty:3->4")
		upCheck(t, e.Mutations[2], "upsert", false, "")
	})
}

// An upsert with RETURNING goes through Query: the application receives its
// rows as they are, and the row is recorded as what happened to it.
func TestUpsertReturning(t *testing.T) {
	for _, tc := range []struct {
		name, returning string
		cols            []string
		opts            []tsql.Option
	}{
		{"key", " RETURNING sku", []string{"sku"}, nil},
		{"star", " RETURNING *", []string{"sku", "qty", "name"}, nil},
		{"appended", "", []string{"sku", "qty", "name"}, []tsql.Option{tsql.WithCaptureAfterImage(true)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t)
			u := stockTable()
			f := upFake(u, 1)
			inner := f.handler
			f.handler = func(q string, args []driver.Value) result {
				r := inner(q, args)
				if strings.HasPrefix(q, "INSERT") {
					row := u.rows[u.find(args[0])]
					r.cols = tc.cols
					r.rows = [][]driver.Value{row[:len(tc.cols)]}
				}
				return r
			}
			db := open(t, f, "postgres", tc.opts...)
			rs, err := db.QueryContext(ctx, stockUpsert+tc.returning, "widget", 7, "x")
			if err != nil {
				t.Fatal(err)
			}
			cols, _ := rs.Columns()
			n := 0
			for rs.Next() {
				n++
			}
			if err := rs.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cols, tc.cols) || n != 1 {
				t.Fatalf("the application received %d rows of %v", n, cols)
			}
			e := lastWrite(t, rec)
			if len(e.Mutations) != 1 || core.HasUncertainFlag(e.Flags) && tc.name != "key" {
				t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
			}
			upCheck(t, e.Mutations[0], "update", true, "qty:10->7")
		})
	}
}
