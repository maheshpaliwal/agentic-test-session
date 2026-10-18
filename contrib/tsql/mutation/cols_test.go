package mutation

import (
	"reflect"
	"sort"
	"testing"
)

func argCols(in Info) map[int]string {
	out := map[int]string{}
	for a, cs := range in.ArgCols {
		sort.Strings(cs)
		s := ""
		for i, c := range cs {
			if i > 0 {
				s += "|"
			}
			s += c
		}
		out[a] = s
	}
	return out
}

func TestArgColumnMapping(t *testing.T) {
	cases := []struct {
		sql  string
		d    Dialect
		want map[int]string
	}{
		{"INSERT INTO users (name, password, email) VALUES ($1, $2, $3)", Postgres, map[int]string{0: "name", 1: "password", 2: "email"}},
		{"INSERT INTO users (name, password) VALUES (?, ?), (?, ?)", MySQL, map[int]string{0: "name", 1: "password", 2: "name", 3: "password"}},
		{"INSERT INTO users (id, token) VALUES ($1, lower($2))", Postgres, map[int]string{0: "id", 1: "token"}},
		{"UPDATE users SET name = $1, api_key = $2 WHERE id = $3", Postgres, map[int]string{0: "name", 1: "api_key", 2: "id"}},
		{"UPDATE users SET password = crypt($1, gen_salt('bf')) WHERE email = $2 AND tenant IN ($3, $4)", Postgres,
			map[int]string{0: "password", 1: "email", 2: "tenant", 3: "tenant"}},
		{"INSERT INTO t (id, secret) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET secret = $3, n = EXCLUDED.n + $4 WHERE t.id = $5", Postgres,
			map[int]string{0: "id", 1: "secret", 2: "secret", 3: "n", 4: "id"}},
		{"INSERT INTO t (a, token) VALUES (?, ?) ON DUPLICATE KEY UPDATE token = ?", MySQL, map[int]string{0: "a", 1: "token", 2: "token"}},
		{"SELECT * FROM u WHERE lower(email) = $1 AND token = ANY($2) AND age BETWEEN $3 AND $4 LIMIT $5", Postgres,
			map[int]string{0: "email", 1: "token", 2: "age", 3: "age", 4: ""}},
		{"SELECT * FROM u WHERE $1 = token AND password::text = $2", Postgres, map[int]string{0: "token", 1: "password"}},
		{"INSERT INTO t (a, password) SELECT $1, $2 FROM s WHERE x = $3", Postgres, map[int]string{0: "a", 1: "password", 2: "x"}},
		{"DELETE FROM t WHERE (a = $1 OR password = $2)", Postgres, map[int]string{0: "a", 1: "password"}},
		{"UPDATE t SET a = (SELECT x FROM y WHERE tok = $1) WHERE id = $2", Postgres, map[int]string{0: "tok", 1: "id"}},
		{"SELECT crypt(password, $1) FROM u", Postgres, map[int]string{}}, // unmapped
	}
	for _, c := range cases {
		in := Parse(c.sql, c.d)
		if got := argCols(in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s\n got %v\nwant %v", c.sql, got, c.want)
		}
	}
}

func TestLiteralMapping(t *testing.T) {
	q := "UPDATE u SET password = 'x', n = 5 WHERE token = 'y' AND id = 7 LIMIT 3"
	in := Parse(q, MySQL)
	got := map[string][]string{}
	for _, l := range in.Literals {
		got[q[l.Start:l.End]] = l.Cols
	}
	want := map[string][]string{"'x'": {"password"}, "5": {"n"}, "'y'": {"token"}, "7": {"id"}, "3": nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for _, l := range in.Literals {
		if q[l.Start:l.End] == "3" && !l.Benign {
			t.Fatal("LIMIT literal should be benign")
		}
	}
	in = Parse("INSERT INTO u (a, token) VALUES ('p', 'q'), ('r', 's')", Postgres)
	if len(in.Literals) != 4 || in.Literals[3].Cols[0] != "token" || in.Literals[2].Cols[0] != "a" {
		t.Fatalf("%+v", in.Literals)
	}
}

func TestEdgeCases(t *testing.T) {
	// dollar quoting hides everything inside, including ';' and fake keywords
	in := Parse("INSERT INTO t (a) VALUES ($tag$ x; DELETE FROM z WHERE id = $1 $tag$)", Postgres)
	if in.Kind != Insert || in.Multi || in.Table != "t" || len(in.Literals) != 1 {
		t.Fatalf("dollar quoting: %+v", in)
	}
	in = Parse("SELECT $$it's; DROP TABLE x$$, 1", Postgres)
	if in.Multi || in.Kind != Select {
		t.Fatalf("$$ quoting: %+v", in)
	}
	// nested comments (Postgres) vs MySQL (not nested)
	in = Parse("/* a /* b */ still comment */ DELETE FROM t WHERE id = 1", Postgres)
	if in.Kind != Delete || in.Table != "t" {
		t.Fatalf("nested comment: %+v", in)
	}
	in = Parse("/* a /* b */ DELETE FROM t WHERE id = 1", MySQL)
	if in.Kind != Delete {
		t.Fatalf("mysql comment: %+v", in)
	}
	// MySQL: "--x" is not a comment
	in = Parse("UPDATE t SET a = 5--1 WHERE id = 1", MySQL)
	if !in.HasWhere {
		t.Fatalf("mysql dashes: %+v", in)
	}
	// WHERE CURRENT OF
	in = Parse("UPDATE t SET a = 1 WHERE CURRENT OF c1", Postgres)
	if !in.CurrentOf || !in.Complex {
		t.Fatalf("current of: %+v", in)
	}
	if _, _, reason := in.ImageSelect(5); reason != "where_current_of" {
		t.Fatalf("reason %q", reason)
	}
	// UPDATE ... FROM with aliases: complex, never replayed
	in = Parse("UPDATE orders o SET status = s.v FROM src s WHERE o.id = s.id AND o.x = $1", Postgres)
	if in.Kind != Update || in.Table != "orders" || in.TableRef != "orders o" || !in.Complex {
		t.Fatalf("update from: %+v", in)
	}
	if got := argCols(in); got[0] != "x" {
		t.Fatalf("alias arg mapping %v", got)
	}
	// aliased target without FROM replays with alias
	in = Parse("UPDATE orders AS o SET status = 'x' WHERE o.id = $1", Postgres)
	q, _, reason := in.ImageSelect(5)
	if reason != "" || q != "SELECT * FROM orders AS o WHERE o.id = $1 LIMIT 5" {
		t.Fatalf("%q %q", q, reason)
	}
	// INSERT ... SELECT
	in = Parse("INSERT INTO a (x, y) SELECT x, y FROM b WHERE z = $1 RETURNING id", Postgres)
	if in.Kind != Insert || in.Table != "a" || in.Values != nil || !in.HasReturning || !reflect.DeepEqual(in.Columns, []string{"x", "y"}) {
		t.Fatalf("insert select: %+v", in)
	}
	// MERGE
	in = Parse("MERGE INTO tgt t USING src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)", Postgres)
	if in.Kind != Upsert || in.Table != "tgt" || !in.Complex || in.Verb != "MERGE" {
		t.Fatalf("merge: %+v", in)
	}
	if _, _, reason := in.ImageSelect(5); reason == "" {
		t.Fatal("merge must not replay")
	}
	// TRUNCATE
	in = Parse("TRUNCATE TABLE ONLY a.b, c RESTART IDENTITY", Postgres)
	if in.Kind != Truncate || in.Schema != "a" || in.Table != "b" || !reflect.DeepEqual(in.Tables, []string{"b", "c"}) {
		t.Fatalf("truncate: %+v", in)
	}
	in = Parse("TRUNCATE t", MySQL)
	if in.Kind != Truncate || in.Table != "t" {
		t.Fatalf("truncate: %+v", in)
	}
	// COPY
	in = Parse("COPY items (a, b) FROM STDIN WITH (FORMAT csv)", Postgres)
	if in.Kind != Insert || !in.Bulk || in.Table != "items" || !in.Kind.IsMutation() {
		t.Fatalf("copy from: %+v", in)
	}
	for _, q := range []string{"COPY items TO STDOUT", "COPY (SELECT 1) TO STDOUT"} {
		if in = Parse(q, Postgres); in.Kind.IsWrite() {
			t.Fatalf("%s must be a read: %+v", q, in)
		}
	}
	// multi-statement: the write is described, nothing is replayable, literals of both are listed
	in = Parse("SELECT 1; UPDATE t SET password = 'p' WHERE id = 1; DELETE FROM u WHERE id = $1", Postgres)
	if !in.Multi || !in.Complex || in.Kind != Update || in.Table != "t" || len(in.Literals) != 3 {
		t.Fatalf("multi: %+v", in)
	}
	if _, _, reason := in.ImageSelect(5); reason == "" {
		t.Fatal("multi-statement must not replay")
	}
	in = Parse("SELECT 1;", Postgres)
	if in.Multi {
		t.Fatal("trailing semicolon is not multi")
	}
	// ';' inside quotes/comments does not split
	in = Parse("UPDATE t SET a = 'x;y' /* ; */ WHERE id = 1 -- ;\n", Postgres)
	if in.Multi || !in.HasWhere {
		t.Fatalf("%+v", in)
	}
}
