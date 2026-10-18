package tsql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

type benchNopRec struct{}

func (benchNopRec) Record(core.Event) {}

func benchSetup() context.Context {
	core.Configure(core.Config{Service: "svc", Instance: "i1", Enabled: true})
	core.SetRecorder(benchNopRec{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	return core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: "t1"})
}

func benchTeardown() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }

func benchOpen(b *testing.B, f *fakeDB, opts ...tsql.Option) *sql.DB {
	name := fmt.Sprintf("pgx-bench-%d", driverSeq.Add(1))
	sql.Register(name, f)
	db, err := tsql.Open(name, "dsn", opts...)
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	return db
}

var benchCols = []string{"id", "status", "note"}

func benchHandler(q string, a []driver.Value) result {
	if strings.HasPrefix(q, "SELECT") {
		return result{cols: benchCols, rows: [][]driver.Value{{int64(7), "new", "n"}}}
	}
	return result{affected: 1}
}

// drain resets the fake's call log and returns the number of calls it held.
func (f *fakeDB) benchDrain() (raw, app int) {
	f.mu.Lock()
	cs := f.calls
	f.calls = nil
	f.mu.Unlock()
	raw = len(cs)
	for _, c := range cs {
		switch {
		case c.SQL == "BEGIN READ ONLY", c.SQL == "ROLLBACK", strings.Contains(c.SQL, "SAVEPOINT ts_capture"),
			strings.Contains(c.SQL, "set_config('statement_timeout'"):
			continue
		}
		app++
	}
	return
}

func BenchmarkTSQL(b *testing.B) {
	cases := []struct {
		name  string
		opts  []tsql.Option
		query bool
		stmt  string
		args  []any
	}{
		{"select", nil, true, "SELECT id, status, note FROM public.orders WHERE id = $1", []any{int64(7)}},
		{"insert", nil, false, "INSERT INTO public.orders (status, note) VALUES ($1, $2)", []any{"new", "n"}},
		{"update_images_on", nil, false, "UPDATE public.orders o SET status = $1, note = $2 WHERE o.id = $3", []any{"shipped", "n", int64(7)}},
		{"update_images_off", []tsql.Option{tsql.WithImages(false)}, false, "UPDATE public.orders o SET status = $1, note = $2 WHERE o.id = $3", []any{"shipped", "n", int64(7)}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ctx := benchSetup()
			defer benchTeardown()
			f := &fakeDB{handler: benchHandler}
			db := benchOpen(b, f, c.opts...)
			run := func() {
				if c.query {
					rows, err := db.QueryContext(ctx, c.stmt, c.args...)
					if err != nil {
						b.Fatal(err)
					}
					for rows.Next() {
					}
					rows.Close()
				} else if _, err := db.ExecContext(ctx, c.stmt, c.args...); err != nil {
					b.Fatal(err)
				}
			}
			run() // warm up connection
			f.benchDrain()
			var rawN, appN int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
				r, a := f.benchDrain()
				rawN += r
				appN += a
			}
			b.ReportMetric(float64(rawN)/float64(b.N), "rt/op")
			b.ReportMetric(float64(appN)/float64(b.N), "rt_nofence/op")
		})
	}
}
