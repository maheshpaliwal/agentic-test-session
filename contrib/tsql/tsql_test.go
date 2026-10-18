package tsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"tsession/contrib/tsql"
	"tsession/core"
)

func orderRow(id int64, status string) []driver.Value {
	return []driver.Value{id, status, []byte("acme")}
}

var orderCols = []string{"id", "status", "tenant"}

func jsonMap(t *testing.T, p core.PayloadRef) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(p.Inline, &m); err != nil {
		t.Fatalf("payload %s: %v", p.Inline, err)
	}
	return m
}

func TestDisabledIsPassthrough(t *testing.T) {
	core.Configure(core.Config{Enabled: false})
	f := &fakeDB{}
	name := "disabled-fake"
	sql.Register(name, f)
	db, err := tsql.Open(name, "dsn")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Driver() != driver.Driver(f) {
		t.Fatalf("driver was wrapped while disabled: %T", db.Driver())
	}
	if tsql.WrapDriver(f) != driver.Driver(f) {
		t.Fatal("WrapDriver must return the original when disabled")
	}
}

func TestIdempotentWrapping(t *testing.T) {
	setup(t)
	f := &fakeDB{}
	w := tsql.WrapDriver(f)
	if w == driver.Driver(f) {
		t.Fatal("expected wrapper")
	}
	if tsql.WrapDriver(w) != w {
		t.Fatal("double WrapDriver must be a no-op")
	}
	name := "idem-fake"
	sql.Register(name, w)
	db, err := tsql.Open(name, "x")
	if err != nil {
		t.Fatal(err)
	}
	if db.Driver() != w {
		t.Fatal("Open on an already wrapped driver must not wrap again")
	}
	db.Close()
}

func TestNoSessionNoEvents(t *testing.T) {
	rec, _ := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "pg")
	if _, err := db.Exec("UPDATE t SET a = $1 WHERE id = $2", 1, 2); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("events without session: %d", n)
	}
	if n := len(f.Calls()); n != 1 {
		t.Fatalf("no capture queries expected without session, got %d", n)
	}
}

func TestSelectReadSummary(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		return result{cols: orderCols, rows: [][]driver.Value{orderRow(1, "new"), orderRow(2, "paid")}}
	}}
	db := open(t, f, "postgres")
	rows, err := db.QueryContext(ctx, "SELECT id, status, tenant FROM orders WHERE tenant = $1", "acme")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id int64
		var st, tn string
		if err := rows.Scan(&id, &st, &tn); err != nil {
			t.Fatal(err)
		}
		got = append(got, st+"/"+tn)
	}
	rows.Close()
	if !reflect.DeepEqual(got, []string{"new/acme", "paid/acme"}) {
		t.Fatalf("app saw %v", got)
	}
	evs := byKind(rec.Events(), core.KindDBRead)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(rec.Events()))
	}
	e := evs[0]
	if len(e.Reads) != 1 {
		t.Fatalf("reads=%+v", e.Reads)
	}
	r := e.Reads[0]
	if r.Store != "postgres" || r.Entity != "orders" || r.RowCount != 2 || !reflect.DeepEqual(r.PKs, []string{"1", "2"}) {
		t.Fatalf("read summary %+v", r)
	}
	if !strings.Contains(string(r.Sample.Inline), `"status":"new"`) {
		t.Fatalf("sample %s", r.Sample.Inline)
	}
	req := jsonMap(t, e.Req)
	if req["sql"] == nil || !strings.Contains(string(e.Req.Inline), "acme") {
		t.Fatalf("req %s", e.Req.Inline)
	}
	if e.SessionID != "s1" || e.Name == "" {
		t.Fatalf("event %+v", e)
	}
}

func TestPostgresUpdateImages(t *testing.T) {
	rec, ctx := setup(t)
	phase := 0
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		switch {
		case strings.HasPrefix(q, "SELECT * FROM"):
			phase++
			if phase == 1 {
				return result{cols: orderCols, rows: [][]driver.Value{orderRow(7, "new")}}
			}
			return result{cols: orderCols, rows: [][]driver.Value{orderRow(7, "shipped")}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "pgx")
	res, err := db.ExecContext(ctx, "UPDATE public.orders o SET status = $1, note = $2 WHERE o.id = $3", "shipped", "n", int64(7))
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("app result changed: %d", n)
	}
	calls := f.Calls()
	if len(calls) != 3 {
		t.Fatalf("calls:\n%s", sqls(calls))
	}
	if calls[0].SQL != "SELECT * FROM public.orders o WHERE o.id = $1 LIMIT 51" || !reflect.DeepEqual(calls[0].Args, []driver.Value{int64(7)}) {
		t.Fatalf("pre-image call %+v", calls[0])
	}
	if !strings.HasPrefix(calls[1].SQL, "UPDATE public.orders") || len(calls[1].Args) != 3 {
		t.Fatalf("app statement altered: %+v", calls[1])
	}
	if !strings.HasPrefix(calls[2].SQL, `SELECT * FROM public.orders WHERE id IN ($1)`) {
		t.Fatalf("after-image call %q", calls[2].SQL)
	}
	evs := byKind(rec.Events(), core.KindDBWrite)
	if len(evs) != 1 || len(evs[0].Mutations) != 1 {
		t.Fatalf("events %+v", rec.Events())
	}
	m := evs[0].Mutations[0]
	if m.Store != "postgres" || m.Entity != "orders" || m.Op != "update" || m.Source != "sdk" || m.PK["id"] != float64(7) && m.PK["id"] != int64(7) {
		t.Fatalf("mutation %+v", m)
	}
	if jsonMap(t, m.Before)["status"] != "new" || jsonMap(t, m.After)["status"] != "shipped" {
		t.Fatalf("images before=%s after=%s", m.Before.Inline, m.After.Inline)
	}
	if len(m.Diff) != 1 || m.Diff[0].Field != "status" || m.Diff[0].Old != "new" || m.Diff[0].New != "shipped" {
		t.Fatalf("diff %+v", m.Diff)
	}
	if m.Effective != nil {
		t.Fatal("Effective is resolved by the collector")
	}
}

func TestDeletePreImageAndSkips(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			return result{cols: orderCols, rows: [][]driver.Value{orderRow(3, "x"), orderRow(4, "y")}}
		}
		return result{affected: 2}
	}}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, "DELETE FROM orders WHERE tenant = $1", "acme"); err != nil {
		t.Fatal(err)
	}
	// a statement whose predicate cannot be isolated
	if _, err := db.ExecContext(ctx, "DELETE FROM orders USING other WHERE orders.id = other.id"); err != nil {
		t.Fatal(err)
	}
	evs := byKind(rec.Events(), core.KindDBWrite)
	if len(evs) != 2 {
		t.Fatalf("events=%d", len(evs))
	}
	if len(evs[0].Mutations) != 2 || evs[0].Mutations[0].Op != "delete" || evs[0].Mutations[0].After.Inline != nil {
		t.Fatalf("delete mutations %+v", evs[0].Mutations)
	}
	if jsonMap(t, evs[0].Mutations[1].Before)["status"] != "y" {
		t.Fatal("second before image wrong")
	}
	if !hasFlag(evs[1], "image_skipped") || evs[1].Attrs["image_skip_reason"] != "complex_statement" {
		t.Fatalf("expected image_skipped: %+v", evs[1])
	}
	if len(evs[1].Mutations) != 1 {
		t.Fatalf("still expect a mutation without images: %+v", evs[1].Mutations)
	}
}

func TestCaptureFailureNeverBreaksApp(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			return result{err: errors.New("boom: capture only")}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	res, err := db.ExecContext(ctx, "UPDATE t SET a = $1 WHERE id = $2", 1, 2)
	if err != nil {
		t.Fatalf("capture error leaked to the app: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatal("result altered")
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_failed") || e.Err != "" {
		t.Fatalf("flags=%v err=%q", e.Flags, e.Err)
	}
	if len(e.Mutations) != 1 || e.Mutations[0].PK["id"] == nil {
		t.Fatalf("PK should fall back to WHERE equality: %+v", e.Mutations)
	}
}

func TestAppErrorPropagatesAndNoMutation(t *testing.T) {
	rec, ctx := setup(t)
	want := errors.New("unique violation")
	f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{err: want} }}
	db := open(t, f, "postgres", tsql.WithImages(false))
	_, err := db.ExecContext(ctx, "INSERT INTO t (id) VALUES ($1)", 1)
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if e.Err == "" || len(e.Mutations) != 0 {
		t.Fatalf("%+v", e)
	}
}

func TestInsertAfterFromArgsAndMySQLReread(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.Contains(q, "* FROM") {
			return result{cols: orderCols, rows: [][]driver.Value{orderRow(42, "new")}}
		}
		return result{affected: 1, lastID: 42}
	}}
	db := open(t, f, "mysql")
	if _, err := db.ExecContext(ctx, "INSERT INTO orders (status, tenant) VALUES (?, ?)", "new", "acme"); err != nil {
		t.Fatal(err)
	}
	calls := f.Calls()
	if len(calls) != 2 || calls[1].SQL != "SELECT /*+ MAX_EXECUTION_TIME(500) */ * FROM orders WHERE `id` IN (?)" || calls[1].Args[0] != int64(42) {
		t.Fatalf("calls:\n%s", sqls(calls))
	}
	m := byKind(rec.Events(), core.KindDBWrite)[0].Mutations[0]
	if m.Store != "mysql" || m.Op != "insert" || m.Before.Inline != nil || jsonMap(t, m.After)["id"] == nil {
		t.Fatalf("%+v", m)
	}

	// Postgres, PK given in the args: after image comes from the re-read.
	rec2, ctx2 := setup(t)
	f2 := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
	db2 := open(t, f2, "postgres", tsql.WithImages(true))
	if _, err := db2.ExecContext(ctx2, "INSERT INTO orders (id, status) VALUES ($1, 'new')", int64(9)); err != nil {
		t.Fatal(err)
	}
	e := byKind(rec2.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "after_from_args") {
		t.Fatalf("expected args fallback when re-read finds nothing: %v", e.Flags)
	}
	am := jsonMap(t, e.Mutations[0].After)
	if am["id"] != float64(9) || am["status"] != "new" || e.Mutations[0].PK["id"] == nil {
		t.Fatalf("%+v %v", e.Mutations[0], am)
	}
	_ = rec
}

func TestReturningAppendOptIn(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		return result{cols: orderCols, rows: [][]driver.Value{orderRow(5, "new")}}
	}}
	db := open(t, f, "postgres", tsql.WithCaptureAfterImage(true))
	rows, err := db.QueryContext(ctx, "INSERT INTO orders (status) VALUES ($1);", "new")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close() // the app ignored the rows; the SDK still saw them
	calls := f.Calls()
	if calls[0].SQL != "INSERT INTO orders (status) VALUES ($1) RETURNING *" {
		t.Fatalf("rewritten SQL %q", calls[0].SQL)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "returning_appended") || len(e.Mutations) != 1 || jsonMap(t, e.Mutations[0].After)["id"] != float64(5) {
		t.Fatalf("%+v", e)
	}

	// default: no rewrite
	f2 := &fakeDB{handler: func(q string, a []driver.Value) result { return result{} }}
	db2 := open(t, f2, "postgres")
	r2, err := db2.QueryContext(ctx, "INSERT INTO orders (status) VALUES ($1)", "new")
	if err != nil {
		t.Fatal(err)
	}
	r2.Close()
	if strings.Contains(f2.Calls()[0].SQL, "RETURNING") {
		t.Fatal("RETURNING must only be appended when opted in")
	}
}

func TestTransactionEvents(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres", tsql.WithImages(false))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.ExecContext(ctx, "UPDATE a SET x = 1 WHERE id = 1")
	tx.ExecContext(ctx, "UPDATE b SET x = 1 WHERE id = 1")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx2, _ := db.BeginTx(ctx, nil)
	tx2.ExecContext(ctx, "UPDATE c SET x = 1 WHERE id = 1")
	tx2.Rollback()
	db.ExecContext(ctx, "UPDATE d SET x = 1 WHERE id = 1") // outside any tx

	evs := rec.Events()
	var seq []string
	ids := map[string]string{}
	for _, e := range evs {
		seq = append(seq, string(e.Kind))
		ids[e.Name] = e.TxID
	}
	want := []string{"db.tx.begin", "db.write", "db.write", "db.tx.commit", "db.tx.begin", "db.write", "db.tx.rollback", "db.write"}
	if !reflect.DeepEqual(seq, want) {
		t.Fatalf("sequence %v", seq)
	}
	if evs[0].TxID == "" || evs[1].TxID != evs[0].TxID || evs[2].TxID != evs[0].TxID || evs[3].TxID != evs[0].TxID {
		t.Fatalf("tx ids: %q %q %q %q", evs[0].TxID, evs[1].TxID, evs[2].TxID, evs[3].TxID)
	}
	if evs[4].TxID == evs[0].TxID || evs[5].TxID != evs[4].TxID || evs[6].TxID != evs[4].TxID {
		t.Fatal("second tx must have its own id")
	}
	if evs[7].TxID != "" {
		t.Fatalf("statement after tx end carries stale TxID %q", evs[7].TxID)
	}
	if f.commits != 1 || f.rbacks != 1 {
		t.Fatalf("commit/rollback not delegated: %d/%d", f.commits, f.rbacks)
	}
}

func TestSkipTables(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		return result{cols: []string{"id", "token"}, rows: [][]driver.Value{{int64(1), "secret-ish"}}, affected: 1}
	}}
	db := open(t, f, "postgres", tsql.WithSkipTables("Sessions"))
	db.ExecContext(ctx, "UPDATE sessions SET token = $1 WHERE id = $2", "tok", 1)
	rows, _ := db.QueryContext(ctx, "SELECT * FROM public.sessions")
	rows.Close()
	if n := len(f.Calls()); n != 2 {
		t.Fatalf("no image queries for skipped tables, calls:\n%s", sqls(f.Calls()))
	}
	for _, e := range rec.Events() {
		if !hasFlag(e, "table_skipped") || strings.Contains(string(e.Req.Inline), `"args"`) {
			t.Fatalf("skipped table leaked: %+v", e)
		}
		for _, m := range e.Mutations {
			if m.Before.Inline != nil || m.After.Inline != nil {
				t.Fatal("images captured for skipped table")
			}
		}
		for _, r := range e.Reads {
			if r.Sample.Inline != nil {
				t.Fatal("sample captured for skipped table")
			}
		}
	}
}

func TestWithImagesOff(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres", tsql.WithImages(false))
	db.ExecContext(ctx, "DELETE FROM t WHERE id = $1", 1)
	if len(f.Calls()) != 1 {
		t.Fatal("images disabled but extra queries ran")
	}
	if m := byKind(rec.Events(), core.KindDBWrite)[0].Mutations; len(m) != 1 || m[0].PK["id"] == nil {
		t.Fatalf("%+v", m)
	}
}

func TestPreparedAndLegacyPaths(t *testing.T) {
	for name, f := range map[string]*fakeDB{
		"plain-conn":  {plain: true},
		"errskip-ctx": {skipCtx: true},
	} {
		t.Run(name, func(t *testing.T) {
			rec, ctx := setup(t)
			f.handler = func(q string, a []driver.Value) result {
				if strings.HasPrefix(q, "SELECT") {
					return result{cols: orderCols, rows: [][]driver.Value{orderRow(1, "new")}}
				}
				return result{affected: 1}
			}
			db := open(t, f, "postgres")
			if _, err := db.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", "x", 1); err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(ctx, "SELECT id FROM orders WHERE id = $1", 1)
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				t.Fatal("rows lost")
			}
			rows.Close()
			stmt, err := db.PrepareContext(ctx, "DELETE FROM orders WHERE id = $1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stmt.ExecContext(ctx, 1); err != nil {
				t.Fatal(err)
			}
			stmt.Close()
			evs := rec.Events()
			var w, r int
			for _, e := range evs {
				switch e.Kind {
				case core.KindDBWrite:
					w++
				case core.KindDBRead:
					r++
				}
			}
			if w != 2 || r != 1 {
				t.Fatalf("each app statement must yield exactly one event (writes=%d reads=%d)", w, r)
			}
		})
	}
}

func TestOpenDBAndConnectorWrapping(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
	db := tsql.OpenDB(connFn{f}, tsql.WithStore("mysql"), tsql.WithImages(false))
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	db.ExecContext(ctx, "UPDATE t SET a = ? WHERE id = ?", 1, 2)
	e := byKind(rec.Events(), core.KindDBWrite)
	if len(e) != 1 || e[0].Mutations[0].Store != "mysql" {
		t.Fatalf("%+v", rec.Events())
	}
	db2 := tsql.OpenDB(connFn{f})
	db2.Close()
}

type connFn struct{ f *fakeDB }

func (c connFn) Connect(context.Context) (driver.Conn, error) { return c.f.Open("") }
func (c connFn) Driver() driver.Driver                        { return c.f }
