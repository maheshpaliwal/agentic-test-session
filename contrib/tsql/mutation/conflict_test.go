package mutation

import (
	"fmt"
	"reflect"
	"testing"
)

// The conflict clause of an upsert: the columns a row is found by are known
// only for a plain column list.
func TestConflictClause(t *testing.T) {
	for _, c := range []struct {
		sql  string
		d    Dialect
		want Conflict
	}{
		{"INSERT INTO t (id, v) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v", Postgres, Conflict{Target: []string{"id"}}},
		{`INSERT INTO t (a, b) VALUES ($1, $2) ON CONFLICT (a, "B") DO UPDATE SET v = 1 RETURNING *`, Postgres, Conflict{Target: []string{"a", "B"}}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT (ID) DO NOTHING", Postgres, Conflict{Target: []string{"id"}, DoNothing: true}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT DO NOTHING", Postgres, Conflict{DoNothing: true}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET v = 1", Postgres, Conflict{}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT (id) WHERE v > 0 DO UPDATE SET v = 1", Postgres, Conflict{}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT (lower(id)) DO UPDATE SET v = 1", Postgres, Conflict{}},
		{`INSERT INTO t (id) VALUES ($1) ON CONFLICT (id COLLATE "C") DO UPDATE SET v = 1`, Postgres, Conflict{}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT (id text_pattern_ops, v) DO UPDATE SET v = 1", Postgres, Conflict{}},
		{"INSERT INTO t (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET v = (SELECT 1) WHERE t.v IS DISTINCT FROM 1", Postgres, Conflict{Target: []string{"id"}}},
		{"INSERT INTO t (id, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)", MySQL, Conflict{AnyKey: true}},
		{"REPLACE INTO t (id, v) VALUES (?, ?)", MySQL, Conflict{AnyKey: true, Replace: true}},
	} {
		if got := Parse(c.sql, c.d); got.Kind != Upsert || !reflect.DeepEqual(got.Conflict, c.want) {
			t.Errorf("%s:\n kind %v conflict %+v, want %+v", c.sql, got.Kind, got.Conflict, c.want)
		}
	}
	if got := Parse("INSERT INTO t (id, v) VALUES ($1, $2)", Postgres); got.Kind != Insert || !reflect.DeepEqual(got.Conflict, Conflict{}) {
		t.Errorf("plain insert: kind %v conflict %+v", got.Kind, got.Conflict)
	}
}

// Every row of a multi-row VALUES list is kept, up to a bound.
func TestMultiRowValues(t *testing.T) {
	in := Parse("INSERT INTO t (id, v) VALUES ($1, $2), (3, lower($3)), ($4, 'x') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v", Postgres)
	want := []map[string]Value{
		{"id": {Arg: 0}, "v": {Arg: 1}},
		{"id": {Arg: -1, Literal: int64(3)}},
		{"id": {Arg: 3}, "v": {Arg: -1, Literal: "x"}},
	}
	if !in.MultiRow || in.Values != nil || !reflect.DeepEqual(in.Rows, want) {
		panic(fmt.Sprintf("rows %+v values %+v", in.Rows, in.Values))
	}
	if in := Parse("INSERT INTO t (id, v) VALUES ($1, $2)", Postgres); in.Rows != nil || len(in.Values) != 2 {
		panic(fmt.Sprintf("single row: rows %+v values %+v", in.Rows, in.Values))
	}
	if in := Parse("INSERT INTO t VALUES ($1, $2), ($3, $4)", Postgres); in.Rows != nil || !in.MultiRow {
		panic(fmt.Sprintf("no column list: rows %+v", in.Rows))
	}
	sql := "INSERT INTO t (id) VALUES (1)"
	for i := 0; i < maxValueRows; i++ {
		sql += ", (2)"
	}
	if in := Parse(sql, Postgres); in.Rows != nil || !in.MultiRow {
		panic(fmt.Sprintf("%d rows kept, want none beyond %d", len(in.Rows), maxValueRows))
	}
}
