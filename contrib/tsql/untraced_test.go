package tsql_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Transaction control executed outside a session still decides how a later
// statement of a session on the same connection captures its image: a raw
// BEGIN that no span recorded must make the capture use a savepoint, never its
// own BEGIN READ ONLY ... ROLLBACK (which would end the application's
// transaction). The assertion is the one TestRawTxControlVariants makes.
func TestTxControlOutsideSessionIsTracked(t *testing.T) {
	const update = "UPDATE t SET a = 1 WHERE id = 2"
	big := literalInsert(2000, true)
	cases := []struct {
		name  string
		stmts []string
		inTx  bool
	}{
		{"begin", []string{"BEGIN"}, true},
		{"lower case begin after comments", []string{"-- app\n/* tx */  begin isolation level read committed;"}, true},
		{"start transaction", []string{"START TRANSACTION"}, true},
		{"begin then commit", []string{"BEGIN", "COMMIT"}, false},
		{"begin then end", []string{"BEGIN", "END"}, false},
		{"begin then rollback", []string{"BEGIN", "ROLLBACK"}, false},
		{"begin then abort", []string{"BEGIN", "abort"}, false},
		{"commit and chain keeps the tx open", []string{"BEGIN", "COMMIT AND CHAIN"}, true},
		{"rollback and chain keeps the tx open", []string{"BEGIN", "ROLLBACK AND CHAIN"}, true},
		{"rollback to savepoint keeps the tx open", []string{"BEGIN", "SAVEPOINT s", "ROLLBACK WORK TO SAVEPOINT s"}, true},
		{"release savepoint keeps the tx open", []string{"BEGIN", "SAVEPOINT s", "RELEASE SAVEPOINT s"}, true},
		{"commit then begin in one text", []string{"BEGIN", "COMMIT; BEGIN"}, true},
		{"begin without commit in one text", []string{"BEGIN; INSERT INTO x (id) VALUES (1)"}, true},
		{"begin and commit in one text", []string{"BEGIN; INSERT INTO x (id) VALUES (1); COMMIT"}, false},
		{"begin after a statement in one text", []string{"SET search_path = app; BEGIN"}, true},
		{"commit after a bulk insert in one text", []string{"BEGIN", big + "; COMMIT"}, false},
		{"begin before a bulk insert in one text", []string{"BEGIN; " + big}, true},
		{"commit and no chain", []string{"BEGIN", "COMMIT AND NO CHAIN"}, false},
		{"prepare transaction", []string{"BEGIN", "PREPARE TRANSACTION 'x'"}, false},
		{"commit prepared outside a tx", []string{"COMMIT PREPARED 'x'"}, false},
		{"commit prepared inside a tx", []string{"BEGIN", "COMMIT PREPARED 'x'"}, true},
		{"begin inside a literal", []string{"INSERT INTO x (note) VALUES ('a; BEGIN')"}, false},
		{"begin inside a comment", []string{"SELECT 1 /* ; BEGIN */"}, false},
		{"commit inside a literal", []string{"BEGIN", "INSERT INTO x (note) VALUES ('a; COMMIT; b')"}, true},
	}
	for _, via := range []string{"exec", "query", "prepared exec", "prepared query"} {
		for _, c := range cases {
			t.Run(via+"/"+c.name, func(t *testing.T) {
				rec, sessCtx := setup(t)
				// In no session at all, and in a session that is not active.
				outside := []context.Context{
					context.Background(),
					core.ContextWith(context.Background(), core.SpanContext{SessionID: "other", TraceID: "t9"}),
				}
				f := &fakeDB{handler: func(string, []driver.Value) result { return result{affected: 1} }}
				db := open(t, f, "postgres")
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				for i, s := range c.stmts {
					ctx := outside[i%len(outside)]
					switch via {
					case "exec":
						_, err = conn.ExecContext(ctx, s)
					case "query":
						rows, qerr := conn.QueryContext(ctx, s)
						if err = qerr; err == nil {
							err = rows.Close()
						}
					case "prepared exec":
						st, perr := conn.PrepareContext(ctx, s)
						if err = perr; err == nil {
							_, err = st.ExecContext(ctx)
							st.Close()
						}
					case "prepared query":
						st, perr := conn.PrepareContext(ctx, s)
						if err = perr; err == nil {
							rows, qerr := st.QueryContext(ctx)
							if err = qerr; err == nil {
								err = rows.Close()
							}
							st.Close()
						}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if n := len(rec.Events()); n != 0 {
					t.Fatalf("statements outside a session recorded %d events", n)
				}
				f.mu.Lock()
				f.calls = nil
				f.mu.Unlock()
				if _, err := conn.ExecContext(sessCtx, update); err != nil {
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
}

// The scenario end to end, with what is recorded: BEGIN outside a session, an
// UPDATE inside one on the same connection, COMMIT outside, another UPDATE
// inside.
func TestRawBeginOutsideSessionThenUpdateInside(t *testing.T) {
	rec, ctx := setup(t)
	f := &fakeDB{handler: func(q string, _ []driver.Value) result {
		if strings.HasPrefix(q, "SELECT * FROM") {
			return result{cols: []string{"id", "a"}, rows: [][]driver.Value{{int64(2), int64(0)}}}
		}
		return result{affected: 1}
	}}
	db := open(t, f, "postgres")
	db.SetMaxOpenConns(1)
	bg := context.Background()

	if _, err := db.ExecContext(bg, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("BEGIN outside a session recorded %d events", n)
	}
	if _, err := db.ExecContext(ctx, "UPDATE t SET a = 1 WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	// Every statement the driver saw, in order: both captures (before and
	// after image) are fenced with a savepoint inside the application's
	// transaction, as if the BEGIN had been traced.
	want := []string{
		"BEGIN",
		"SAVEPOINT ts_capture",
		"SET LOCAL statement_timeout = 500",
		"SELECT * FROM t WHERE id = 2 LIMIT 51",
		"ROLLBACK TO SAVEPOINT ts_capture",
		"RELEASE SAVEPOINT ts_capture",
		"UPDATE t SET a = 1 WHERE id = 2",
		"SAVEPOINT ts_capture",
		"SET LOCAL statement_timeout = 500",
		"SELECT * FROM t WHERE id IN ($1)",
		"ROLLBACK TO SAVEPOINT ts_capture",
		"RELEASE SAVEPOINT ts_capture",
	}
	if got := sqls(f.RawCalls()); got != strings.Join(want, "\n")+"\n" {
		t.Fatalf("driver saw:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	ws := byKind(rec.Events(), core.KindDBWrite)
	if len(ws) != 1 || len(ws[0].Mutations) != 1 || hasFlag(ws[0], "image_skipped") || hasFlag(ws[0], "image_failed") {
		t.Fatalf("write events: %+v", ws)
	}
	if m := ws[0].Mutations[0]; m.Op != "update" || jsonMap(t, m.Before)["a"] != float64(0) {
		t.Fatalf("mutation %+v", m)
	}

	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
	if _, err := db.ExecContext(bg, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE t SET a = 1 WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if s := sqls(f.RawCalls()); !strings.Contains(s, "BEGIN READ ONLY") || strings.Contains(s, "SAVEPOINT ts_capture") {
		t.Fatalf("after COMMIT capture runs in its own read-only tx:\n%s", s)
	}
	if n := len(rec.Events()); n != 2 {
		t.Fatalf("events = %d, want the two UPDATE spans only", n)
	}
}
