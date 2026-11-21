package tsql_test

// An UPDATE whose row was read back but could not be read before the write:
// what changed is not known, so no diff is recorded (the mirror of
// "after_unavailable"). Fake driver; nobefore_pg_test.go has a real Postgres.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// noBeforeCheck asserts that e is an update recorded with its after image
// only: no before image, no diff, and a flag that says the image is
// incomplete.
func noBeforeCheck(t *testing.T, e core.Event, flag string, rows int) {
	t.Helper()
	if len(e.Mutations) != rows {
		t.Fatalf("%d mutations, want %d (flags %v)", len(e.Mutations), rows, e.Flags)
	}
	for _, m := range e.Mutations {
		if m.Op != "update" || !m.Before.IsZero() || m.After.IsZero() {
			t.Fatalf("mutation %+v: want an update with an after image only", m)
		}
		if len(m.Diff) != 0 {
			t.Errorf("a diff was recorded although the values before the update are not known: %+v", m.Diff)
		}
		if m.PK == nil {
			t.Errorf("no key recorded: %+v", m)
		}
	}
	if !hasFlag(e, flag) || !core.HasUncertainFlag(e.Flags) || hasFlag(e, "after_unavailable") {
		t.Errorf("flags %v, want %q", e.Flags, flag)
	}
}

func TestUpdateWithoutBeforeImageRecordsNoDiff(t *testing.T) {
	row := result{cols: []string{"id", "a", "b"}, rows: [][]driver.Value{{int64(2), int64(1), "x"}}}
	for _, tc := range []struct {
		name string
		flag string
		opts []tsql.Option
		prep func(f *fakeDB)
		stmt string
		rows bool // the statement returns rows
	}{
		{name: "pre-image read fails", flag: "image_failed", stmt: "UPDATE t SET a = 1 WHERE id = 2",
			prep: func(f *fakeDB) {
				inner := f.handler
				f.handler = func(q string, a []driver.Value) result {
					if strings.HasPrefix(q, "SELECT * FROM t WHERE id = 2") {
						return result{err: errors.New("permission denied for table t")}
					}
					return inner(q, a)
				}
			}},
		{name: "pre-image read times out", flag: "image_timeout", stmt: "UPDATE t SET a = 1 WHERE id = 2",
			opts: []tsql.Option{tsql.WithCaptureTimeout(20 * time.Millisecond)},
			prep: func(f *fakeDB) {
				f.slow = func(q string) time.Duration {
					if strings.HasPrefix(q, "SELECT * FROM t WHERE id = 2") {
						return time.Second
					}
					return 0
				}
			}},
		{name: "predicate cannot be replayed, RETURNING *", flag: "image_skipped", rows: true,
			stmt: "UPDATE t SET a = 1 FROM u WHERE t.id = u.id RETURNING *"},
		{name: "no predicate, RETURNING *", flag: "image_skipped", rows: true, stmt: "UPDATE t SET a = 1 RETURNING *"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, ctx := setup(t)
			f := &fakeDB{handler: func(q string, _ []driver.Value) result {
				if strings.HasPrefix(q, "SELECT") || strings.Contains(q, "RETURNING") {
					return row
				}
				return result{affected: 1}
			}}
			if tc.prep != nil {
				tc.prep(f)
			}
			db := open(t, f, "postgres", append([]tsql.Option{noPKLookup}, tc.opts...)...)
			if tc.rows {
				rs, err := db.QueryContext(ctx, tc.stmt)
				if err != nil {
					t.Fatal(err)
				}
				for rs.Next() {
				}
				if err := rs.Close(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, tc.stmt); err != nil {
				t.Fatal(err)
			}
			noBeforeCheck(t, lastWrite(t, rec), tc.flag, 1)
		})
	}
}

// With both images the diff is what it was; with neither there is none.
func TestUpdateDiffNeedsBothImages(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, fenceFake(&fakeDB{}), "postgres", noPKLookup)
	if _, err := db.ExecContext(ctx, fenceUpdate); err != nil {
		t.Fatal(err)
	}
	m := lastWrite(t, rec).Mutations[0]
	if len(m.Diff) != 1 || m.Diff[0].Field != "a" || m.Before.IsZero() || m.After.IsZero() {
		t.Fatalf("update with both images: %+v", m)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(context.WithoutCancel(ctx), fenceUpdate); err != nil {
		t.Fatal(err)
	}
	e := lastWrite(t, rec)
	if m := e.Mutations[0]; len(m.Diff) != 0 || !m.Before.IsZero() || !m.After.IsZero() || !hasFlag(e, "image_skipped") {
		t.Fatalf("update with no image: %+v flags %v", m, e.Flags)
	}
}
