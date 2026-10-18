package tsql_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"tsession/contrib/tsql"
	"tsession/core"
)

func eventJSON(t *testing.T, e core.Event) string {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustNotLeak(t *testing.T, e core.Event, secrets ...string) {
	t.Helper()
	s := eventJSON(t, e)
	for _, x := range secrets {
		if strings.Contains(s, x) {
			t.Fatalf("secret %q leaked in event: %s", x, s)
		}
	}
}

var userCols = []string{"id", "name", "password", "api_key"}

func TestDiffBeforeAfterRedactedByFieldName(t *testing.T) {
	rec, ctx := setup(t)
	n := 0
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			n++
			if n == 1 {
				return result{cols: userCols, rows: [][]driver.Value{{int64(1), "bob", "OLDPW-hash", "OLDKEY"}}}
			}
			return result{cols: userCols, rows: [][]driver.Value{{int64(1), "bobby", "NEWPW-hash", "NEWKEY"}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, "UPDATE users SET name = $1, password = $2, api_key = $3 WHERE id = $4",
		"bobby", "NEWPW-plain", "NEWKEY", int64(1)); err != nil {
		t.Fatal(err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	mustNotLeak(t, e, "OLDPW", "NEWPW", "OLDKEY", "NEWKEY")
	m := e.Mutations[0]
	got := map[string][2]any{}
	for _, d := range m.Diff {
		got[d.Field] = [2]any{d.Old, d.New}
	}
	if got["password"] != [2]any{core.RedactedValue, core.RedactedValue} || got["api_key"] != [2]any{core.RedactedValue, core.RedactedValue} {
		t.Fatalf("diff %+v", m.Diff)
	}
	if got["name"] != [2]any{"bob", "bobby"} {
		t.Fatalf("non-sensitive diff must stay: %+v", m.Diff)
	}
	if jsonMap(t, m.Before)["password"] != core.RedactedValue || jsonMap(t, m.After)["api_key"] != core.RedactedValue {
		t.Fatalf("images not redacted: %s %s", m.Before.Inline, m.After.Inline)
	}
}

func TestInsertAfterFromArgsRedactsNestedAndDiff(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, name, password) VALUES ($1, $2, $3)", int64(5), "al", "PLAINPW"); err != nil {
		t.Fatal(err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	mustNotLeak(t, e, "PLAINPW")
}

func TestArgAndLiteralRedaction(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		args   []any
		masked []int // arg positions that must be masked
		clean  []int // positions that must keep their value
		secret []string
		cons   bool
	}{
		{"insert", "INSERT INTO users (name, password, email) VALUES ($1, $2, $3)",
			[]any{"n1", "S3CRET1", "e1"}, []int{1}, []int{0, 2}, []string{"S3CRET1"}, false},
		{"multi-row", "INSERT INTO users (name, token) VALUES ($1, $2), ($3, $4)",
			[]any{"n1", "S3CRET2", "n2", "S3CRET3"}, []int{1, 3}, []int{0, 2}, []string{"S3CRET2", "S3CRET3"}, false},
		{"update-where", "UPDATE users SET name = $1, api_key = $2 WHERE token = $3 AND id = $4",
			[]any{"n1", "S3CRET4", "S3CRET5", int64(9)}, []int{1, 2}, []int{0, 3}, []string{"S3CRET4", "S3CRET5"}, false},
		{"on-conflict", "INSERT INTO users (id, secret) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET secret = $3, name = $4",
			[]any{int64(1), "S3CRET6", "S3CRET7", "nm"}, []int{1, 2}, []int{0, 3}, []string{"S3CRET6", "S3CRET7"}, false},
		{"literals", "UPDATE users SET password = 'LIT-PW', name = 'plainname' WHERE token = 'LIT-TOK' AND id = $1",
			[]any{int64(3)}, nil, []int{0}, []string{"LIT-PW", "LIT-TOK"}, false},
		{"conservative", "SELECT * FROM users WHERE crypt(password, $1) = $2",
			[]any{"S3CRET8", "S3CRET9"}, []int{0, 1}, nil, []string{"S3CRET8", "S3CRET9"}, true},
		{"unmapped-but-no-sensitive-column", "SELECT * FROM users WHERE lower(trim(name)) || $1 = $2",
			[]any{"visible1", "visible2"}, nil, []int{0, 1}, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, ctx := setup(t)
			f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
			db := open(t, f, "postgres", tsql.WithImages(false))
			rows, err := db.QueryContext(ctx, c.sql, c.args...)
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
			e := rec.Events()[0]
			mustNotLeak(t, e, c.secret...)
			var req struct {
				SQL  string `json:"sql"`
				Args []any  `json:"args"`
			}
			if err := json.Unmarshal(e.Req.Inline, &req); err != nil {
				t.Fatal(err)
			}
			for _, i := range c.masked {
				if req.Args[i] != core.RedactedValue {
					t.Fatalf("arg %d not masked: %v", i, req.Args)
				}
			}
			for _, i := range c.clean {
				if req.Args[i] == core.RedactedValue {
					t.Fatalf("arg %d needlessly masked: %v", i, req.Args)
				}
			}
			if hasFlag(e, "args_redacted_conservatively") != c.cons {
				t.Fatalf("conservative flag=%v want %v (%v)", hasFlag(e, "args_redacted_conservatively"), c.cons, e.Flags)
			}
			if strings.Contains(c.sql, "'") {
				if !strings.Contains(req.SQL, "password = '[REDACTED]'") || !strings.Contains(req.SQL, "name = 'plainname'") ||
					!strings.Contains(e.Name, "token = '[REDACTED]'") {
					t.Fatalf("sql=%q name=%q", req.SQL, e.Name)
				}
			}
			// the app's statement itself is never altered
			if f.Calls()[0].SQL != c.sql {
				t.Fatalf("app SQL altered: %q", f.Calls()[0].SQL)
			}
		})
	}
}

func TestReadSummaryRedaction(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result {
		return result{cols: userCols, rows: [][]driver.Value{{int64(1), "bob", "ROWPW", "ROWKEY"}}}
	}}
	db := open(t, f, "postgres")
	rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE token = 'LIT-T' AND password = $1", "ARGPW")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	rows.Close()
	e := byKind(rec.Events(), core.KindDBRead)[0]
	mustNotLeak(t, e, "ROWPW", "ROWKEY", "LIT-T", "ARGPW")
	if len(e.Reads) != 1 || !strings.Contains(e.Reads[0].Statement, "'[REDACTED]'") {
		t.Fatalf("%+v", e.Reads)
	}
	if !strings.Contains(string(e.Reads[0].Sample.Inline), `"name":"bob"`) {
		t.Fatalf("sample %s", e.Reads[0].Sample.Inline)
	}
}

// ---- in-transaction capture safety ----

func TestCaptureInPGTxUsesSavepointAndNeverPoisonsTx(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{abortModel: true, handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			return result{err: errors.New("invalid input syntax for type bytea")}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE t SET a = $1 WHERE id = $2", 1, 2); err != nil {
		t.Fatalf("app statement failed after capture error: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE t SET a = $1 WHERE id = $2", 3, 4); err != nil {
		t.Fatalf("second app statement failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT must succeed: %v", err)
	}
	calls := f.RawCalls()
	want := []string{"SAVEPOINT ts_capture", "SELECT current_setting('statement_timeout'),", "SELECT *", "ROLLBACK TO", "RELEASE SAVEPOINT", "UPDATE t"}
	for i, w := range want {
		if !strings.HasPrefix(calls[i].SQL, strings.Fields(w)[0]) {
			t.Fatalf("call %d = %q, want prefix %q\n%s", i, calls[i].SQL, w, sqls(calls))
		}
	}
	for _, e := range byKind(rec.Events(), core.KindDBWrite) {
		if !hasFlag(e, "image_failed") || e.Err != "" {
			t.Fatalf("flags %v err %q", e.Flags, e.Err)
		}
	}
}

func TestCaptureInPGTxSuccessReleasesAndRestoresTimeout(t *testing.T) {
	_, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		switch {
		case strings.HasPrefix(q, "SELECT current_setting"):
			return result{cols: []string{"current_setting", "set_config"}, rows: [][]driver.Value{{"0", "500"}}}
		case strings.HasPrefix(q, "SELECT * FROM"):
			return result{cols: []string{"id", "v"}, rows: [][]driver.Value{{int64(2), "x"}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	tx, _ := db.BeginTx(ctx, nil)
	if _, err := tx.ExecContext(ctx, "DELETE FROM t WHERE id = $1", 2); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	var got []string
	for _, c := range f.RawCalls() {
		got = append(got, c.SQL)
	}
	if !strings.HasPrefix(got[0], "SAVEPOINT ts_capture") || !strings.Contains(got[1], "set_config('statement_timeout', '500', true)") ||
		!strings.HasPrefix(got[2], "SELECT * FROM") || got[3] != "SELECT set_config('statement_timeout', '0', true)" ||
		got[4] != "RELEASE SAVEPOINT ts_capture" || !strings.HasPrefix(got[5], "DELETE") {
		t.Fatalf("sequence:\n%s", strings.Join(got, "\n"))
	}
}

func TestSerializableTxSkipsCapture(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres")
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE t SET a = $1 WHERE id = $2", 1, 2); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	if n := len(f.RawCalls()); n != 1 {
		t.Fatalf("no capture queries expected in SERIALIZABLE, calls:\n%s", sqls(f.RawCalls()))
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_skipped:serializable") || !hasFlag(e, "image_skipped") {
		t.Fatalf("flags %v", e.Flags)
	}
	if len(e.Mutations) != 1 {
		t.Fatalf("mutation still recorded: %+v", e.Mutations)
	}
}

func TestMySQLCaptureInTxHasNoSavepoint(t *testing.T) {
	_, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "mysql")
	tx, _ := db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, "UPDATE t SET a = ? WHERE id = ?", 1, 2)
	tx.Commit()
	if s := sqls(f.RawCalls()); strings.Contains(s, "SAVEPOINT") || strings.Contains(s, "BEGIN READ ONLY") {
		t.Fatalf("mysql must not use savepoints:\n%s", s)
	}
}

// ---- lossy values never become bind parameters ----

func TestBinaryPKBoundAsBytes(t *testing.T) {
	rec, ctx := setup(t)
	pk1, pk2 := []byte{0xff, 0xfe, 0x00, 0x01}, []byte{0xff, 0xfe, 0x00, 0x02}
	n := 0
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			n++
			if n == 1 {
				return result{cols: []string{"id", "v"}, rows: [][]driver.Value{{pk1, "a"}, {pk2, "b"}}}
			}
			return result{cols: []string{"id", "v"}, rows: [][]driver.Value{{pk2, "B"}, {pk1, "A"}}}
		}
		return result{affected: 2}
	}}
	db := open(t, f, "postgres", tsql.WithImages(true))
	if _, err := db.ExecContext(ctx, "UPDATE blobs SET v = upper(v) WHERE v < $1", "z"); err != nil {
		t.Fatal(err)
	}
	calls := f.Calls()
	if len(calls) != 3 {
		t.Fatalf("calls:\n%s", sqls(calls))
	}
	if !reflect.DeepEqual(calls[2].Args, []driver.Value{pk1, pk2}) {
		t.Fatalf("PKs must be bound as []byte, got %#v", calls[2].Args)
	}
	for _, a := range calls[2].Args {
		if _, ok := a.([]byte); !ok {
			t.Fatalf("lossy PK %T", a)
		}
	}
	// before/after are paired by raw PK (display forms collide: "<4 bytes>")
	ms := byKind(rec.Events(), core.KindDBWrite)[0].Mutations
	if len(ms) != 2 || jsonMap(t, ms[0].Before)["v"] != "a" || jsonMap(t, ms[0].After)["v"] != "A" ||
		jsonMap(t, ms[1].Before)["v"] != "b" || jsonMap(t, ms[1].After)["v"] != "B" {
		t.Fatalf("mispaired: %+v", ms)
	}
}

func TestNullPKSkipsReread(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			return result{cols: []string{"id", "v"}, rows: [][]driver.Value{{nil, "a"}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	if _, err := db.ExecContext(ctx, "UPDATE blobs SET v = 'x' WHERE v = $1", "a"); err != nil {
		t.Fatal(err)
	}
	if n := len(f.Calls()); n != 2 {
		t.Fatalf("no re-read expected, calls:\n%s", sqls(f.Calls()))
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_skipped:pk_unbindable") {
		t.Fatalf("flags %v", e.Flags)
	}
}

func TestLongPKNotTruncatedWhenRebound(t *testing.T) {
	_, ctx := setup(t)
	long := strings.Repeat("k", 3000)
	n := 0
	f := &fakeDB{handler: func(q string, a []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			n++
			return result{cols: []string{"id", "v"}, rows: [][]driver.Value{{long, "a"}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	db.ExecContext(ctx, "UPDATE t SET v = 'x' WHERE v = $1", "a")
	calls := f.Calls()
	if got, _ := calls[2].Args[0].(string); got != long {
		t.Fatalf("PK truncated when re-bound (%d bytes)", len(got))
	}
}

// ---- capture timeout ----

func TestCaptureTimeoutConfigurableAndBoundedByCtx(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{
		handler: func(string, []driver.Value) result { return result{affected: 1} },
		slow: func(q string) time.Duration {
			if strings.HasPrefix(q, "SELECT * FROM") {
				return 3 * time.Second
			}
			return 0
		},
	}
	db := open(t, f, "postgres", tsql.WithCaptureTimeout(60*time.Millisecond))
	start := time.Now()
	if _, err := db.ExecContext(ctx, "DELETE FROM t WHERE id = $1", 1); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("capture took %v", d)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_timeout") || e.Err != "" {
		t.Fatalf("flags %v err %q", e.Flags, e.Err)
	}

	// default 500ms, but never beyond the caller's own deadline
	f2 := &fakeDB{handler: f.handler, slow: f.slow}
	db2 := open(t, f2, "postgres")
	dctx, cancel := contextWithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	db2.ExecContext(dctx, "DELETE FROM t WHERE id = $1", 1)
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("capture exceeded caller deadline: %v", d)
	}
}

// ---- INSERT ... RETURNING ----

func TestInsertReturningVariants(t *testing.T) {
	t.Run("returning id single row", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(q string, a []driver.Value) result {
			if strings.HasPrefix(q, "SELECT * FROM") {
				return result{cols: []string{"id", "name"}, rows: [][]driver.Value{{int64(11), "ann"}}}
			}
			return result{cols: []string{"id"}, rows: [][]driver.Value{{int64(11)}}}
		}}
		db := open(t, f, "postgres")
		var id int64
		if err := db.QueryRowContext(ctx, "INSERT INTO people (name) VALUES ($1) RETURNING id", "ann").Scan(&id); err != nil {
			t.Fatal(err)
		}
		if id != 11 {
			t.Fatalf("app saw id=%d", id)
		}
		e := byKind(rec.Events(), core.KindDBWrite)[0]
		if len(e.Mutations) != 1 {
			t.Fatalf("mutations %+v flags %v", e.Mutations, e.Flags)
		}
		m := e.Mutations[0]
		if m.Op != "insert" || m.PK["id"] != int64(11) || jsonMap(t, m.After)["name"] != "ann" || !hasFlag(e, "returning_partial") {
			t.Fatalf("%+v flags=%v", m, e.Flags)
		}
		if hasFlag(e, "pk_unknown") {
			t.Fatal("PK known from RETURNING")
		}
		last := f.Calls()[len(f.Calls())-1]
		if !strings.HasPrefix(last.SQL, "SELECT * FROM people WHERE id IN ($1)") || last.Args[0] != int64(11) {
			t.Fatalf("re-read %+v", last)
		}
	})
	t.Run("returning star multi-row", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(q string, a []driver.Value) result {
			return result{cols: []string{"id", "name"}, rows: [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}}
		}}
		db := open(t, f, "postgres")
		rows, err := db.QueryContext(ctx, "INSERT INTO people (name) VALUES ($1), ($2), ($3) RETURNING *", "a", "b", "c")
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if n != 3 {
			t.Fatalf("app saw %d rows", n)
		}
		e := byKind(rec.Events(), core.KindDBWrite)[0]
		if len(e.Mutations) != 3 || len(f.Calls()) != 1 {
			t.Fatalf("mutations=%d calls:\n%s", len(e.Mutations), sqls(f.Calls()))
		}
		for i, m := range e.Mutations {
			if m.PK["id"] != int64(i+1) || m.Op != "insert" {
				t.Fatalf("%d: %+v", i, m)
			}
		}
		if hasFlag(e, "returning_partial") || !hasFlag(e, "multi_row_insert") {
			t.Fatalf("flags %v", e.Flags)
		}
	})
	t.Run("returning id multi-row rereads", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(q string, a []driver.Value) result {
			if strings.HasPrefix(q, "SELECT * FROM") {
				return result{cols: []string{"id", "name"}, rows: [][]driver.Value{{int64(1), "a"}, {int64(2), "b"}}}
			}
			return result{cols: []string{"id"}, rows: [][]driver.Value{{int64(1)}, {int64(2)}}}
		}}
		db := open(t, f, "postgres")
		rows, _ := db.QueryContext(ctx, "INSERT INTO people (name) VALUES ($1), ($2) RETURNING id", "a", "b")
		for rows.Next() {
		}
		rows.Close()
		e := byKind(rec.Events(), core.KindDBWrite)[0]
		if len(e.Mutations) != 2 || jsonMap(t, e.Mutations[1].After)["name"] != "b" {
			t.Fatalf("%+v flags=%v", e.Mutations, e.Flags)
		}
	})
	t.Run("without returning PK unknown", func(t *testing.T) {
		rec, ctx := setup(t)
		f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
		db := open(t, f, "postgres")
		if _, err := db.ExecContext(ctx, "INSERT INTO people (name) VALUES ($1)", "zed"); err != nil {
			t.Fatal(err)
		}
		e := byKind(rec.Events(), core.KindDBWrite)[0]
		if len(e.Mutations) != 1 || !hasFlag(e, "pk_unknown") || !hasFlag(e, "after_from_args") {
			t.Fatalf("flags %v muts %+v", e.Flags, e.Mutations)
		}
		if e.Mutations[0].PK != nil || jsonMap(t, e.Mutations[0].After)["name"] != "zed" {
			t.Fatalf("%+v", e.Mutations[0])
		}
	})
}

func TestRawBeginIsTreatedAsOpenTx(t *testing.T) {
	_, ctx := setup(t)
	f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
	db := open(t, f, "postgres")
	db.SetMaxOpenConns(1)
	db.ExecContext(ctx, "BEGIN")
	db.ExecContext(ctx, "UPDATE t SET a = 1 WHERE id = 2")
	s := sqls(f.RawCalls())
	if strings.Contains(s, "BEGIN READ ONLY") || !strings.Contains(s, "SAVEPOINT ts_capture") {
		t.Fatalf("capture inside a hand-written BEGIN must use a savepoint:\n%s", s)
	}
	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
	db.ExecContext(ctx, "COMMIT")
	db.ExecContext(ctx, "UPDATE t SET a = 1 WHERE id = 2")
	if s := sqls(f.RawCalls()); !strings.Contains(s, "BEGIN READ ONLY") {
		t.Fatalf("after COMMIT capture runs in its own read-only tx:\n%s", s)
	}
}
