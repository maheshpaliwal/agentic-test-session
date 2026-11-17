package tsql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func drain(t *testing.T, rows interface {
	Next() bool
	Close() error
}, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	rows.Close()
}

// A slow capture may only use a fraction of the caller's remaining time, so
// the application's own statement still has most of its deadline.
func TestCaptureLeavesCallerDeadline(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{
		handler: func(string, []driver.Value) result { return result{affected: 1} },
		slow: func(q string) time.Duration {
			if strings.HasPrefix(q, "SELECT * FROM") {
				return 5 * time.Second
			}
			return 0
		},
	}
	db := open(t, f, "postgres")
	dctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := db.ExecContext(dctx, "DELETE FROM t WHERE id = $1", 1); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 600*time.Millisecond {
		t.Fatalf("capture used %v of an 800ms deadline", d)
	}
	if dctx.Err() != nil {
		t.Fatal("caller deadline exhausted by capture")
	}
	if e := byKind(rec.Events(), core.KindDBWrite)[0]; !hasFlag(e, "image_timeout") || len(e.Mutations) != 1 {
		t.Fatalf("flags %v mutations %d", e.Flags, len(e.Mutations))
	}
}

// With almost no time left nothing is captured at all.
func TestCaptureSkippedWhenDeadlineIsTight(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres")
	dctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := db.ExecContext(dctx, "DELETE FROM t WHERE id = $1", 1); err != nil {
		t.Fatal(err)
	}
	if s := sqls(f.RawCalls()); s != "DELETE FROM t WHERE id = $1\n" {
		t.Fatalf("capture ran despite a tight deadline:\n%s", s)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_skipped:deadline") || e.Attrs["image_skip_reason"] != "deadline" || len(e.Mutations) != 1 {
		t.Fatalf("flags %v attrs %v", e.Flags, e.Attrs)
	}
}

func TestAliasedSensitiveColumnIsMasked(t *testing.T) {
	t.Run("alias in read sample", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(string, []driver.Value) result {
			return result{cols: []string{"id", "p"}, rows: [][]driver.Value{{int64(1), "hunter2"}}}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "SELECT id, password AS p FROM users WHERE id = $1", 1)
		drain(t, rows, err)
		e := byKind(rec.Events(), core.KindDBRead)[0]
		mustNotLeak(t, e, "hunter2")
		if s := string(e.Reads[0].Sample.Inline); !strings.Contains(s, `"id":1`) || !strings.Contains(s, `"p":"[REDACTED]"`) {
			t.Fatalf("sample %s", s)
		}
		if hasFlag(e, "rows_redacted_conservatively") {
			t.Fatalf("only the aliased column needs masking: %v", e.Flags)
		}
	})
	t.Run("alias through subquery", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(string, []driver.Value) result {
			return result{cols: []string{"id", "q"}, rows: [][]driver.Value{{int64(1), "hunter2"}}}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "SELECT id, p AS q FROM (SELECT id, password AS p FROM users) s")
		drain(t, rows, err)
		mustNotLeak(t, byKind(rec.Events(), core.KindDBRead)[0], "hunter2")
	})
	t.Run("unnamed expression masks the whole sample", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(string, []driver.Value) result {
			return result{cols: []string{"id", "upper"}, rows: [][]driver.Value{{int64(1), "HUNTER2"}}}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "SELECT id, upper(password) FROM users")
		drain(t, rows, err)
		e := byKind(rec.Events(), core.KindDBRead)[0]
		mustNotLeak(t, e, "HUNTER2")
		if !hasFlag(e, "rows_redacted_conservatively") {
			t.Fatalf("flags %v", e.Flags)
		}
	})
	t.Run("sensitive column only in WHERE keeps the sample", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(string, []driver.Value) result {
			return result{cols: []string{"id", "n"}, rows: [][]driver.Value{{int64(1), int64(3)}}}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "SELECT id, count(*) AS n FROM users WHERE token = $1 GROUP BY id", "tok")
		drain(t, rows, err)
		e := byKind(rec.Events(), core.KindDBRead)[0]
		if s := string(e.Reads[0].Sample.Inline); s != `[{"id":1,"n":3}]` {
			t.Fatalf("sample %s", s)
		}
	})
	t.Run("alias in RETURNING", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(q string, _ []driver.Value) result {
			if strings.HasPrefix(q, "INSERT") {
				return result{cols: []string{"id"}, rows: [][]driver.Value{{"SECRETKEY"}}}
			}
			return result{}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "INSERT INTO t (name) VALUES ($1) RETURNING api_key AS id", "bob")
		drain(t, rows, err)
		e := byKind(rec.Events(), core.KindDBWrite)[0]
		mustNotLeak(t, e, "SECRETKEY")
		if len(e.Mutations) != 1 {
			t.Fatalf("mutations %d", len(e.Mutations))
		}
	})
}

func TestErrorTextIsRedacted(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, _ []driver.Value) result {
		switch {
		case strings.HasPrefix(q, "INSERT INTO users"):
			return result{err: errors.New("Error 1062 (23000): Duplicate entry 'tok_live_abc123' for key 'users.api_token'")}
		case strings.HasPrefix(q, "UPDATE users"):
			return result{err: errors.New(`ERROR: duplicate key value violates unique constraint "users_token_key" DETAIL: Key (token)=(tok_live_abc123) already exists.`)}
		case strings.HasPrefix(q, "SELECT * FROM missing"):
			return result{err: errors.New("Error 1146 (42S02): Table 'db.missing' doesn't exist")}
		}
		return result{}
	}}
	db := open(t, f, "mysql", tsql.WithImages(false))
	_, err := db.ExecContext(ctx, "INSERT INTO users (id, api_token) VALUES (?, ?)", 1, "tok_live_abc123")
	if err == nil || !strings.Contains(err.Error(), "tok_live_abc123") {
		t.Fatalf("the application must still get the original error, got %v", err)
	}
	db.ExecContext(ctx, "UPDATE users SET token = ? WHERE id = ?", "tok_live_abc123", 1)
	if _, err := db.QueryContext(ctx, "SELECT * FROM missing"); err == nil {
		t.Fatal("expected error")
	}
	ws := byKind(rec.Events(), core.KindDBWrite)
	for _, e := range ws {
		mustNotLeak(t, e, "tok_live_abc123")
		if !strings.Contains(e.Err, core.RedactedValue) {
			t.Fatalf("err %q", e.Err)
		}
	}
	if !strings.Contains(ws[0].Err, "Error 1062 (23000): Duplicate entry") {
		t.Fatalf("error lost its shape: %q", ws[0].Err)
	}
	// nothing sensitive in the statement: the message is kept verbatim
	if e := byKind(rec.Events(), core.KindDBRead)[0]; e.Err != "Error 1146 (42S02): Table 'db.missing' doesn't exist" {
		t.Fatalf("err %q", e.Err)
	}
}

// The rows of "WITH w AS (<write> RETURNING *) SELECT ..." belong to the outer
// query and must not become after images.
func TestWriteCTEOuterRowsAreNotImages(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result {
		return result{cols: []string{"count"}, rows: [][]driver.Value{{int64(3)}}}
	}}
	db := open(t, f, "postgres")
	rows, err := db.QueryContext(ctx, "WITH u AS (UPDATE t SET a=1 WHERE b=2 RETURNING *) SELECT count(*) FROM u")
	drain(t, rows, err)
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if len(e.Mutations) != 1 {
		t.Fatalf("mutations %d", len(e.Mutations))
	}
	if m := e.Mutations[0]; m.Op != "update" || !m.After.IsZero() || len(m.Diff) != 0 {
		t.Fatalf("outer query row recorded as an image: after=%s diff=%v", m.After.Inline, m.Diff)
	}
}

func TestReturningBeyondMaxRowsIsFlagged(t *testing.T) {
	rec, ctx := setup(t)
	var rs [][]driver.Value
	for i := 0; i < 200; i++ {
		rs = append(rs, []driver.Value{int64(i), "x"})
	}
	f := &fakeDB{handler: func(string, []driver.Value) result {
		return result{cols: []string{"id", "name"}, rows: rs}
	}}
	db := open(t, f, "postgres")
	rows, err := db.QueryContext(ctx, "INSERT INTO t (name) SELECT name FROM src RETURNING *")
	drain(t, rows, err)
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if len(e.Mutations) != 50 || !hasFlag(e, "mutations_truncated") {
		t.Fatalf("mutations=%d flags=%v", len(e.Mutations), e.Flags)
	}
}

func TestTruncateEveryListedTable(t *testing.T) {
	rec, ctx := setup(t)
	db := open(t, &fakeDB{}, "postgres")
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE a, public.b"); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range byKind(rec.Events(), core.KindDBWrite)[0].Mutations {
		got = append(got, m.Op+":"+m.Entity)
	}
	if strings.Join(got, ",") != "truncate:a,truncate:b" {
		t.Fatalf("mutations %v", got)
	}
}

// Values are captured up to a length cap; a diff that found two capped values
// equal must say that it could not see beyond the cap.
func TestDiffOfTruncatedValuesIsFlagged(t *testing.T) {
	rec, ctx := setup(t)
	long := strings.Repeat("a", 2000)
	selects := 0
	f := &fakeDB{handler: func(q string, _ []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM docs") {
			selects++
			body := long + "OLD"
			if selects > 1 {
				body = long + "NEW"
			}
			return result{cols: []string{"id", "body"}, rows: [][]driver.Value{{int64(1), body}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, "UPDATE docs SET body = $1 WHERE id = $2", long+"NEW", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", "x", 1); err != nil {
		t.Fatal(err)
	}
	ws := byKind(rec.Events(), core.KindDBWrite)
	if !hasFlag(ws[0], "diff_truncated") {
		t.Fatalf("flags %v", ws[0].Flags)
	}
	if hasFlag(ws[1], "diff_truncated") {
		t.Fatalf("short values flagged: %v", ws[1].Flags)
	}
}

// Hand-written transaction control is tracked across every statement of the
// text, so capture never runs BEGIN ... ROLLBACK inside an open transaction
// and never uses a savepoint outside one.
func TestRawTxControlVariants(t *testing.T) {
	const update = "UPDATE t SET a = 1 WHERE id = 2"
	cases := []struct {
		name  string
		stmts []string
		inTx  bool
	}{
		{"commit and chain keeps the tx open", []string{"BEGIN", "COMMIT AND CHAIN"}, true},
		{"rollback and chain keeps the tx open", []string{"BEGIN", "ROLLBACK AND CHAIN"}, true},
		{"rollback work to savepoint keeps the tx open", []string{"BEGIN", "SAVEPOINT s", "ROLLBACK WORK TO SAVEPOINT s"}, true},
		{"commit then begin in one text", []string{"BEGIN", "COMMIT; BEGIN"}, true},
		{"begin without commit in one text", []string{"BEGIN; INSERT INTO x (id) VALUES (1)"}, true},
		{"begin and commit in one text", []string{"BEGIN; INSERT INTO x (id) VALUES (1); COMMIT"}, false},
		{"commit and no chain", []string{"BEGIN", "COMMIT AND NO CHAIN"}, false},
		{"prepare transaction", []string{"BEGIN", "PREPARE TRANSACTION 'x'"}, false},
		{"commit prepared outside a tx", []string{"COMMIT PREPARED 'x'"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ctx := setup(t)
			f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
			db := open(t, f, "postgres")
			db.SetMaxOpenConns(1)
			for _, s := range c.stmts {
				if _, err := db.ExecContext(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			f.mu.Lock()
			f.calls = nil
			f.mu.Unlock()
			if _, err := db.ExecContext(ctx, update); err != nil {
				t.Fatal(err)
			}
			s := sqls(f.RawCalls())
			savepoint, own := strings.Contains(s, "SAVEPOINT ts_capture"), strings.Contains(s, "BEGIN READ ONLY")
			if savepoint != c.inTx || own == c.inTx {
				t.Fatalf("inTx=%v but capture ran:\n%s", c.inTx, s)
			}
		})
	}
}

func TestUnwrap(t *testing.T) {
	_, ctx := setup(t)
	f := &fakeDB{}
	db := open(t, f, "postgres")
	if got := tsql.UnwrapDriver(db.Driver()); got != driver.Driver(f) {
		t.Fatalf("UnwrapDriver = %T", got)
	}
	if got := tsql.UnwrapDriver(f); got != driver.Driver(f) {
		t.Fatalf("unwrapped driver changed: %T", got)
	}
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.Raw(func(dc any) error {
		if _, ok := dc.(*fakeConn); ok {
			t.Fatal("expected a wrapped connection")
		}
		inner, ok := tsql.UnwrapConn(dc).(*fakeConn)
		if !ok {
			t.Fatalf("UnwrapConn = %T", tsql.UnwrapConn(dc))
		}
		if tsql.UnwrapConn(inner) != driver.Conn(inner) {
			t.Fatal("an unwrapped connection must be returned as is")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if tsql.UnwrapConn("not a conn") != nil {
		t.Fatal("non-conn must yield nil")
	}
}
