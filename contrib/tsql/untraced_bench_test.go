package tsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
)

// Statement shapes of traffic that belongs to no session in a shared test
// environment: bulk statements with inlined literals are longer than the parse
// cache keeps, short ones with inlined literals never repeat.

// literalInsert is a multi-row INSERT with inlined values (about 20 bytes a
// row, 27 when wide).
func literalInsert(rows int, wide bool) string {
	var b strings.Builder
	b.WriteString("INSERT INTO public.orders (id, status, amount) VALUES ")
	for i := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		if wide {
			fmt.Fprintf(&b, "(%d, 'status-%04d', %d.50)", 100000+i, i, 1000+i)
		} else {
			fmt.Fprintf(&b, "(%d, 'new', %d.5)", 100000+i, 10+i%90)
		}
	}
	return b.String()
}

// inList is a SELECT with n inlined ids.
func inList(n int) string {
	var b strings.Builder
	b.WriteString("SELECT id, status, note FROM public.orders WHERE id IN (")
	for i := range n {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%d", 100000+i)
	}
	b.WriteString(")")
	return b.String()
}

// literalSelects returns n distinct ~150-byte SELECTs with inlined literals;
// with more of them than the parse cache holds, cycling through them never
// hits it.
func literalSelects(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("SELECT id, status, note, amount FROM public.orders WHERE tenant = 'acme-%06d' AND status = 'shipped' AND amount > %d ORDER BY id DESC LIMIT 20", i, 100+i)
	}
	return out
}

type untracedShape struct {
	name  string
	query bool
	stmts []string
	args  []any
}

func untracedShapes() []untracedShape {
	return []untracedShape{
		{"param_select_cached", true, []string{"SELECT id, status, note FROM public.orders WHERE id = $1"}, []any{int64(7)}},
		{"literal_select_150B", true, literalSelects(4096), nil},
		{"insert_400rows_8KB", false, []string{literalInsert(400, false)}, nil},
		{"in_list_2000_14KB", true, []string{inList(2000)}, nil},
		{"insert_2000rows_54KB", false, []string{literalInsert(2000, true)}, nil},
	}
}

// BenchmarkUntraced measures statements whose context belongs to no session,
// through tsql and through the bare driver. The SDK is enabled and a session
// is active elsewhere, as in a shared environment.
func BenchmarkUntraced(b *testing.B) {
	for _, s := range untracedShapes() {
		for _, mode := range []string{"tsql", "unwrapped"} {
			b.Run(s.name+"/"+mode, func(b *testing.B) {
				benchSetup()
				defer benchTeardown()
				ctx := context.Background()
				f := &fakeDB{handler: func(q string, _ []driver.Value) result {
					if strings.HasPrefix(q, "SELECT") {
						return result{cols: benchCols}
					}
					return result{affected: 1}
				}}
				var db *sql.DB
				if mode == "tsql" {
					db = benchOpen(b, f)
					if tsql.UnwrapDriver(db.Driver()) == db.Driver() {
						panic("driver is not wrapped")
					}
				} else {
					name := uniqueName("bare-bench")
					sql.Register(name, f)
					var err error
					if db, err = sql.Open(name, "dsn"); err != nil {
						panic(err)
					}
					db.SetMaxOpenConns(1)
					b.Cleanup(func() { db.Close() })
				}
				run := func(q string) {
					if s.query {
						rows, err := db.QueryContext(ctx, q, s.args...)
						if err != nil {
							panic(err)
						}
						rows.Close()
					} else if _, err := db.ExecContext(ctx, q, s.args...); err != nil {
						panic(err)
					}
				}
				reset := func() { // the fake's call log, without scanning the statements
					f.mu.Lock()
					f.calls = f.calls[:0]
					f.mu.Unlock()
				}
				run(s.stmts[0]) // warm up the connection
				reset()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run(s.stmts[i%len(s.stmts)])
					reset()
				}
			})
		}
	}
}
