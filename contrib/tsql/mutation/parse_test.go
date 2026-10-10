package mutation

import (
	"reflect"
	"testing"
)

func TestParseKinds(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		d      Dialect
		kind   Kind
		schema string
		table  string
	}{
		{"select", "SELECT id, name FROM users WHERE id = $1", Postgres, Select, "", "users"},
		{"select-multiline-comments", "-- leading\nSELECT *\n  /* inline */ FROM\n\tpublic.orders o\nWHERE o.id = 1", Postgres, Select, "public", "orders"},
		{"select-join", "select a.x from a join b on a.id=b.id", Postgres, Select, "", "a"},
		{"select-extract-from", "SELECT EXTRACT(year FROM ts) FROM events", Postgres, Select, "", "events"},
		{"select-quoted", `SELECT 1 FROM "My Schema"."Order Items"`, Postgres, Select, "My Schema", "Order Items"},
		{"select-backtick", "SELECT 1 FROM `shop`.`order-items`", MySQL, Select, "shop", "order-items"},
		{"select-no-from", "SELECT 1", Postgres, Select, "", ""},
		{"insert", "INSERT INTO users (id, name) VALUES ($1, $2)", Postgres, Insert, "", "users"},
		{"insert-schema-quoted", `insert into "public"."Users"(id) values (1)`, Postgres, Insert, "public", "Users"},
		{"insert-on-conflict", "INSERT INTO t (id,v) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v RETURNING *", Postgres, Upsert, "", "t"},
		{"insert-on-duplicate", "INSERT INTO t (id,v) VALUES (?,?) ON DUPLICATE KEY UPDATE v = VALUES(v)", MySQL, Upsert, "", "t"},
		{"replace", "REPLACE INTO t (id) VALUES (?)", MySQL, Upsert, "", "t"},
		{"insert-select", "INSERT INTO t2 SELECT * FROM t1", Postgres, Insert, "", "t2"},
		{"insert-ignore", "INSERT IGNORE INTO t (a) VALUES (?)", MySQL, Insert, "", "t"},
		{"update", "UPDATE users SET name = $1 WHERE id = $2", Postgres, Update, "", "users"},
		{"update-only-alias", "UPDATE ONLY s.users AS u SET name = 'x' WHERE u.id = 1", Postgres, Update, "s", "users"},
		{"delete", "DELETE FROM sessions WHERE token = ?", MySQL, Delete, "", "sessions"},
		{"truncate", "TRUNCATE TABLE audit", Postgres, Truncate, "", "audit"},
		{"ddl", "CREATE TABLE IF NOT EXISTS foo (id int)", Postgres, DDL, "", "foo"},
		{"alter", "ALTER TABLE public.foo ADD COLUMN x int", Postgres, DDL, "public", "foo"},
		{"begin", "BEGIN", Postgres, Other, "", ""},
		{"cte-select", "WITH a AS (SELECT 1), b(x) AS MATERIALIZED (SELECT 2) SELECT * FROM a, b", Postgres, Select, "", "a"},
		{"cte-delete", "WITH d AS (DELETE FROM old WHERE id = $1 RETURNING *) SELECT * FROM d", Postgres, Delete, "", "old"},
		{"cte-main-insert", "WITH s AS (SELECT 1 AS x) INSERT INTO t (x) SELECT x FROM s", Postgres, Insert, "", "t"},
		{"paren-select", "(SELECT 1 FROM t) UNION (SELECT 2 FROM u)", Postgres, Select, "", "t"},
		{"trailing-semicolon", "UPDATE t SET a=1 WHERE id=2;", Postgres, Update, "", "t"},
		{"empty", "", Postgres, Other, "", ""},
		{"only-comment", "-- nothing", Postgres, Other, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := Parse(c.sql, c.d)
			if in.Kind != c.kind || in.Table != c.table || in.Schema != c.schema {
				t.Fatalf("got kind=%v schema=%q table=%q, want %v %q %q", in.Kind, in.Schema, in.Table, c.kind, c.schema, c.table)
			}
		})
	}
}

func TestStringsAndCommentsDoNotConfuse(t *testing.T) {
	in := Parse(`UPDATE t SET note = 'WHERE x = 1; -- not a comment' WHERE id = $1 -- trailing`, Postgres)
	if in.Where != "id = $1" {
		t.Fatalf("where=%q", in.Where)
	}
	in = Parse("UPDATE t SET a = $$ WHERE $$ WHERE id = $1", Postgres)
	if in.Where != "id = $1" {
		t.Fatalf("dollar-quote: where=%q", in.Where)
	}
	in = Parse(`SELECT 'it''s FROM x' AS s FROM real_t`, Postgres)
	if in.Table != "real_t" {
		t.Fatalf("table=%q", in.Table)
	}
	in = Parse(`SELECT 'a\' FROM x' FROM real_t`, MySQL)
	if in.Table != "real_t" {
		t.Fatalf("mysql backslash: table=%q", in.Table)
	}
	in = Parse("SELECT 1 /* a /* nested */ FROM x */ FROM real_t", Postgres)
	if in.Table != "real_t" {
		t.Fatalf("nested comment: table=%q", in.Table)
	}
	in = Parse("# c\nDELETE FROM t WHERE id = 1", MySQL)
	if in.Kind != Delete {
		t.Fatalf("mysql # comment: %v", in.Kind)
	}
	in = Parse("SELECT a::int FROM t WHERE b = $1::text", Postgres)
	if in.Table != "t" || in.Named {
		t.Fatalf("cast handled wrong: %+v", in)
	}
}

func TestInsertValues(t *testing.T) {
	in := Parse("INSERT INTO t (id, name, n, z, expr) VALUES ($1, 'bob', -4, NULL, now()+1)", Postgres)
	if !reflect.DeepEqual(in.Columns, []string{"id", "name", "n", "z", "expr"}) {
		t.Fatalf("cols=%v", in.Columns)
	}
	want := map[string]Value{
		"id":   {Arg: 0},
		"name": {Arg: -1, Literal: "bob"},
		"n":    {Arg: -1, Literal: int64(-4)},
		"z":    {Arg: -1},
	}
	if !reflect.DeepEqual(in.Values, want) {
		t.Fatalf("values=%v", in.Values)
	}
	in = Parse("INSERT INTO t (a) VALUES (?), (?)", MySQL)
	if !in.MultiRow || in.Values != nil {
		t.Fatalf("multirow: %+v", in)
	}
	in = Parse("INSERT INTO t (a, b) VALUES (?, ?)", MySQL)
	if in.Values["b"].Arg != 1 || !in.QMark || in.Dollar {
		t.Fatalf("mysql values: %+v", in)
	}
	in = Parse("INSERT INTO t (SELECT * FROM u)", Postgres)
	if len(in.Columns) != 0 {
		t.Fatalf("subselect treated as cols: %v", in.Columns)
	}
}

func TestReturning(t *testing.T) {
	in := Parse("UPDATE t SET a=1 WHERE id=1 RETURNING *", Postgres)
	if !in.HasReturning || !in.ReturningAll {
		t.Fatalf("%+v", in)
	}
	in = Parse("DELETE FROM t WHERE id=1 RETURNING id, name", Postgres)
	if !in.HasReturning || in.ReturningAll {
		t.Fatalf("%+v", in)
	}
	if in.Where != "id=1" {
		t.Fatalf("where=%q", in.Where)
	}
	in = Parse("INSERT INTO t (a) VALUES (1)", Postgres)
	if in.HasReturning {
		t.Fatal("unexpected returning")
	}
}

func TestComplexStatements(t *testing.T) {
	for _, sql := range []string{
		"UPDATE a SET x = b.x FROM b WHERE a.id = b.id",
		"UPDATE a JOIN b ON a.id=b.id SET a.x = b.x WHERE b.y = ?",
		"DELETE FROM a USING b WHERE a.id = b.id",
		"DELETE a FROM a JOIN b ON a.id = b.id WHERE b.z = ?",
		"WITH d AS (DELETE FROM old WHERE id = $1 RETURNING *) SELECT * FROM d",
	} {
		in := Parse(sql, Postgres)
		if !in.Complex {
			t.Errorf("%q: expected Complex", sql)
		}
		if _, _, reason := in.ImageSelect(10); reason == "" {
			t.Errorf("%q: expected image skip", sql)
		}
	}
	// subquery FROM inside SET is not "UPDATE ... FROM"
	in := Parse("UPDATE a SET x = (SELECT max(y) FROM b) WHERE id = $1", Postgres)
	if in.Complex {
		t.Fatal("subquery wrongly complex")
	}
}

func TestImageSelectRenumber(t *testing.T) {
	in := Parse("UPDATE public.orders o SET status = $1, note = $2 WHERE o.id = $3 AND o.tenant = $4 AND o.x <> $3", Postgres)
	q, idx, reason := in.ImageSelect(51)
	if reason != "" {
		t.Fatal(reason)
	}
	wantQ := "SELECT * FROM public.orders o WHERE o.id = $1 AND o.tenant = $2 AND o.x <> $1 LIMIT 51"
	if q != wantQ || !reflect.DeepEqual(idx, []int{2, 3}) {
		t.Fatalf("q=%q idx=%v", q, idx)
	}

	in = Parse("UPDATE t SET a = ?, b = ? WHERE id = ? AND k = ? ORDER BY id LIMIT 2", MySQL)
	q, idx, reason = in.ImageSelect(51)
	if reason != "" {
		t.Fatal(reason)
	}
	if q != "SELECT * FROM t WHERE id = ? AND k = ? ORDER BY id LIMIT 2" || !reflect.DeepEqual(idx, []int{2, 3}) {
		t.Fatalf("q=%q idx=%v", q, idx)
	}

	in = Parse("DELETE FROM t WHERE id = $1 RETURNING id", Postgres)
	q, _, _ = in.ImageSelect(0)
	if q != "SELECT * FROM t WHERE id = $1" {
		t.Fatalf("q=%q", q)
	}

	for sql, want := range map[string]string{
		"UPDATE t SET a = 1":                "no_predicate",
		"UPDATE t SET a = 1 WHERE id = :id": "named_placeholders",
		"SELECT 1":                          "not_update_or_delete",
	} {
		if _, _, r := Parse(sql, Postgres).ImageSelect(5); r != want {
			t.Errorf("%q: reason=%q want %q", sql, r, want)
		}
	}
}

func TestEquals(t *testing.T) {
	in := Parse(`UPDATE t SET a=1 WHERE t.ID = $2 AND "tenant" = 'acme' AND 7 = org AND (x > 3)`, Postgres)
	want := map[string]Value{
		"id":     {Arg: 1},
		"tenant": {Arg: -1, Literal: "acme"},
		"org":    {Arg: -1, Literal: int64(7)},
	}
	if !reflect.DeepEqual(in.Equals, want) {
		t.Fatalf("equals=%v", in.Equals)
	}
	if in := Parse("DELETE FROM t WHERE id = 1 OR id = 2", Postgres); in.Equals != nil {
		t.Fatalf("OR must disable equalities: %v", in.Equals)
	}
	if in := Parse("DELETE FROM t WHERE id IN ($1,$2)", Postgres); in.Equals != nil {
		t.Fatalf("IN must not yield equalities: %v", in.Equals)
	}
}

func TestPlaceholderStyles(t *testing.T) {
	if in := Parse("SELECT * FROM t WHERE a = $1", Postgres); !in.Dollar || in.QMark {
		t.Fatalf("%+v", in)
	}
	if in := Parse("SELECT * FROM t WHERE a = :a", Postgres); !in.Named {
		t.Fatalf("%+v", in)
	}
	if in := Parse("SELECT '?' FROM t", MySQL); in.QMark {
		t.Fatal("? inside string counted")
	}
}

func TestUnterminatedInputDoesNotPanic(t *testing.T) {
	for _, s := range []string{"SELECT 'abc", `SELECT "abc`, "SELECT /* x", "UPDATE", "DELETE FROM", "INSERT INTO", "WITH", "WITH a AS (", "UPDATE t SET a = $$x", "((((", "INSERT INTO t (a", "INSERT INTO t (a) VALUES (", "SELECT $", "WITH a AS"} {
		for _, d := range []Dialect{Postgres, MySQL} {
			Parse(s, d)
		}
	}
}

func TestEndAndMulti(t *testing.T) {
	q := "UPDATE t SET a = 1 WHERE id = 2 ; -- done"
	in := Parse(q, Postgres)
	if q[:in.End] != "UPDATE t SET a = 1 WHERE id = 2" || in.Multi {
		t.Fatalf("end=%q multi=%v", q[:in.End], in.Multi)
	}
	if !Parse("SELECT 1; SELECT 2", Postgres).Multi {
		t.Fatal("expected Multi")
	}
}
