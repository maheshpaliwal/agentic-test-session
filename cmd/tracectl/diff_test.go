package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestNormalizeName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"ship", "ship"},
		// URL paths
		{"GET /orders/42", "GET /orders/?"},
		{"GET /orders/42/items/7?expand=1", "GET /orders/?/items/??expand=?"},
		{"GET /users/550e8400-e29b-41d4-a716-446655440000/cart", "GET /users/?/cart"},
		{"GET /users/550E8400-E29B-41D4-A716-446655440000", "GET /users/?"},
		{"GET /sessions/ts_0193a5b7c2d4f6a8b0c1d2e", "GET /sessions/?"},
		{"GET /blobs/9f86d081884c7d659a2feaa0c55ad015", "GET /blobs/?"},
		{"GET /v1/orders", "GET /v1/orders"},
		{"GET /orders/42abc", "GET /orders/42abc"},
		// SQL
		{"SELECT * FROM users WHERE id = 42", "SELECT * FROM users WHERE id = ?"},
		{"SELECT * FROM users WHERE id = 123456789012345", "SELECT * FROM users WHERE id = ?"},
		{"SELECT * FROM users WHERE name = 'O''Brien' AND age > 30", "SELECT * FROM users WHERE name = ? AND age > ?"},
		{"SELECT * FROM users WHERE token = 'abc 123 ts_0193a5b7c2d4f6a8b0c1d2e'", "SELECT * FROM users WHERE token = ?"},
		{"SELECT * FROM users WHERE name = ''", "SELECT * FROM users WHERE name = ?"},
		{"SELECT * FROM users WHERE a = $1 AND b = $2", "SELECT * FROM users WHERE a = ? AND b = ?"},
		{"SELECT * FROM items WHERE price > 9.99 LIMIT 10 OFFSET 20", "SELECT * FROM items WHERE price > ? LIMIT ? OFFSET ?"},
		{"SELECT * FROM t WHERE id IN (1, 2, 3)", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE id IN (1)", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE id in ($1,$2)", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE id IN ( ?, ?,? )", "SELECT * FROM t WHERE id IN (?)"},
		{"SELECT * FROM t WHERE c IN ('a','b') AND d IN (5, 6)", "SELECT * FROM t WHERE c IN (?) AND d IN (?)"},
		{"INSERT INTO orders (id, status) VALUES (42, 'new')", "INSERT INTO orders (id, status) VALUES (?, ?)"},
		{"  SELECT  *\n\tFROM   orders ", "SELECT * FROM orders"},
		// digits that belong to an identifier stay
		{"INSERT INTO t2 (c1) VALUES (5)", "INSERT INTO t2 (c1) VALUES (?)"},
		{"SELECT * FROM orders_2024", "SELECT * FROM orders_2024"},
		{`SELECT "user id" FROM "Orders"`, `SELECT "user id" FROM "Orders"`},
		// Redis keys and job names
		{"GET user:42:cart", "GET user:?:cart"},
		{"GET session:9f86d081884c7d65", "GET session:?"},
		{"ship-order-42", "ship-order-?"},
		// error text
		{"can't reach db-1: don't retry", "can't reach db-?: don't retry"},
		{"dial tcp 10.0.0.5:5432: timeout", "dial tcp ?.?:?: timeout"},
		{"tx tx_9f86d081884c7d65 aborted", "tx ? aborted"},
		// codes that name an outcome are kept; the values around them are not
		{"HTTP 500", "HTTP 500"},
		{"GET /orders/42: HTTP 503", "GET /orders/?: HTTP 503"},
		{"HTTP/1.1 502 Bad Gateway after 3 tries", "HTTP/1.1 502 Bad Gateway after ? tries"},
		{"unexpected status code: 429 (attempt 2)", "unexpected status code: 429 (attempt ?)"},
		{"upstream 17 returned status 404", "upstream ? returned status 404"},
		{"HTTP 500 then HTTP 503", "HTTP 500 then HTTP 503"},
		{`ERROR: duplicate key value violates unique constraint "orders_pkey" (SQLSTATE 23505) at row 7`,
			`ERROR: duplicate key value violates unique constraint "orders_pkey" (SQLSTATE 23505) at row ?`},
		{"Error 1062 (23000): Duplicate entry '42' for key 'PRIMARY'", "Error 1062 (23000): Duplicate entry ? for key ?"},
		{"rpc error: code = Code(17) desc = shard 4", "rpc error: code = Code(17) desc = shard ?"},
		{"rpc error: code = Unavailable desc = conn 12 closed", "rpc error: code = Unavailable desc = conn ? closed"},
		// not such codes: an HTTP status is three digits from 1xx to 5xx, and a SQL literal stays a literal
		{"HTTP 7", "HTTP ?"},
		{"HTTP 5000", "HTTP ?"},
		{"HTTP 999", "HTTP ?"},
		{"UPDATE jobs SET status = 500 WHERE id = 3", "UPDATE jobs SET status = ? WHERE id = ?"},
		{"INSERT INTO log (msg) VALUES ('HTTP 500')", "INSERT INTO log (msg) VALUES (?)"},
		{"HTTP 500 \uE000 9", "HTTP ? \uE000 ?"},
		// not ids: a hex-looking word, a short hex string
		{"GET /tags/deadbeefcafe", "GET /tags/deadbeefcafe"},
		{"GET /tags/deadbeefcafefeed", "GET /tags/?"},
		{"GET /blobs/123456789012", "GET /blobs/?"},
		{"GET /commits/9f86d08", "GET /commits/9f86d08"},
	} {
		if got := normalizeName(tc.in); got != tc.want {
			t.Errorf("normalizeName(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}

	long := normalizeName("SELECT " + strings.Repeat("é", 300))
	if utf8.RuneCountInString(long) != maxNormalized || !strings.HasSuffix(long, "…") || !utf8.ValidString(long) {
		t.Errorf("long name: %d runes: %q", utf8.RuneCountInString(long), long)
	}
}

func TestNormalizeNameMergesAndKeepsApart(t *testing.T) {
	same := [][2]string{
		{"GET /orders/1", "GET /orders/987654"},
		{"GET /orders/1/items", "GET /orders/550e8400-e29b-41d4-a716-446655440000/items"},
		{"SELECT * FROM users WHERE id = 1", "SELECT  * FROM users\nWHERE id = 99"},
		{"SELECT * FROM users WHERE id = 1", "SELECT * FROM users WHERE id = 123456789012345"},
		{"SELECT * FROM users WHERE email = 'a@x.io'", "SELECT * FROM users WHERE email = 'b''s@y.io'"},
		{"DELETE FROM t WHERE id IN (1)", "DELETE FROM t WHERE id IN (1, 2, 3, 4)"},
		{"DELETE FROM t WHERE id IN ($1, $2)", "DELETE FROM t WHERE id IN (?)"},
		{"SET cart:17", "SET cart:18"},
		{"job ts_0193a5b7c2d4f6a8b0c1d2e", "job ts_0193a5b7c2d4f6a8b0c1d2f"},
		{"GET /orders/1: HTTP 500", "GET /orders/22: HTTP 500"},
		{"HTTP 503 after 120 ms", "HTTP 503 after 95 ms"},
	}
	for _, p := range same {
		if a, b := normalizeName(p[0]), normalizeName(p[1]); a != b {
			t.Errorf("must merge:\n %q -> %q\n %q -> %q", p[0], a, p[1], b)
		}
	}
	apart := [][2]string{
		{"SELECT * FROM orders WHERE id = 1", "SELECT * FROM order_items WHERE id = 1"},
		{"SELECT * FROM orders_2023", "SELECT * FROM orders_2024"},
		{"SELECT id FROM users WHERE id = 1", "SELECT name FROM users WHERE id = 1"},
		{"SELECT * FROM t WHERE a = 1", "SELECT * FROM t WHERE b = 1"},
		{"SELECT * FROM t WHERE id = 1", "SELECT * FROM t WHERE id IN (1)"},
		{"SELECT * FROM t WHERE id IN (1)", "SELECT * FROM t WHERE id NOT IN (1)"},
		{"UPDATE t SET a = 1", "DELETE FROM t"},
		{"GET /orders/1", "GET /users/1"},
		{"GET /orders/1", "POST /orders/1"},
		{"GET /orders/1", "GET /orders/1/items"},
		{"GET /orders", "GET /orders/1"},
		{"GET /v1/orders", "GET /v2/orders"},
		{"GET /orders/pending", "GET /orders/shipped"},
		{"GET user:1", "GET cart:1"},
		{"GET user:1", "SET user:1"},
		{"ship-order", "bill-order"},
		{"it's broken", "it's fixed"},
		{"HTTP 500", "HTTP 503"},
		{"HTTP 404", "HTTP 500"},
		{"unexpected status 502", "unexpected status 504"},
		{"insert failed (SQLSTATE 23505)", "insert failed (SQLSTATE 40001)"},
		{"Error 1062 (23000): Duplicate entry", "Error 1213 (40001): Duplicate entry"},
		{"rpc error: code = Code(17)", "rpc error: code = Code(18)"},
		{"rpc error: code = Unavailable", "rpc error: code = Internal"},
	}
	for _, p := range apart {
		if a, b := normalizeName(p[0]), normalizeName(p[1]); a == b {
			t.Errorf("must stay distinct: %q and %q both became %q", p[0], p[1], a)
		}
	}
}

// diffRun is one recording of the same scenario: an order is created by the
// api and shipped by a worker. id is the order id of that run; apiFirst
// records all api events before the worker's instead of interleaving them.
func diffRun(id string, apiFirst bool) []core.Event {
	yes := true
	api := []core.Event{
		{Service: "api", Kind: core.KindAPIIn, Name: "POST /orders/" + id},
		{Service: "api", Kind: core.KindDBRead, Name: "SELECT * FROM users WHERE id = " + id},
		{Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders (id, status) VALUES (" + id + ", 'new')",
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", Effective: &yes}}},
		{Service: "api", Kind: core.KindCacheWrite, Name: "SET order:" + id},
		{Service: "api", Kind: core.KindQueuePub, Name: "ship"},
	}
	worker := []core.Event{
		{Service: "worker", Kind: core.KindWorkerStart, Name: "ship"},
		{Service: "worker", Kind: core.KindDBWrite, Name: "UPDATE orders SET status = 'shipped' WHERE id = " + id,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "UPDATE", Effective: &yes}}},
		{Service: "worker", Kind: core.KindWorkerEnd, Name: "ship"},
	}
	var evs []core.Event
	if apiFirst {
		evs = append(append(evs, api...), worker...)
	} else {
		evs = append(append(append(evs, api[:2]...), worker...), api[2:]...)
	}
	return reseq(evs)
}

// reseq numbers evs from 1.
func reseq(evs []core.Event) []core.Event {
	for i := range evs {
		evs[i].Seq = uint64(i + 1)
	}
	return evs
}

// insertAt returns a renumbered copy of evs with e as its i-th element.
func insertAt(evs []core.Event, i int, e core.Event) []core.Event {
	out := append([]core.Event{}, evs[:i]...)
	out = append(out, e)
	return reseq(append(out, evs[i:]...))
}

func TestDiffEvents(t *testing.T) {
	base := diffRun("42", true)

	t.Run("identical apart from ids and interleaving", func(t *testing.T) {
		rep := diffEvents(base, diffRun("977", false), false)
		if rep.Differs {
			t.Fatalf("unexpected differences: %+v", rep)
		}
	})

	t.Run("extra failure", func(t *testing.T) {
		b := diffRun("43", true)
		b[7].Err = "carrier timeout for order 43"
		rep := diffEvents(base, b, false)
		if !rep.Differs || len(rep.Failures.OnlyA) != 0 || len(rep.Failures.OnlyB) != 1 ||
			len(rep.Mutations)+len(rep.Events)+len(rep.Divergence) != 0 {
			t.Fatalf("report: %+v", rep)
		}
		f := rep.Failures.OnlyB[0]
		if f.Service != "worker" || f.Kind != core.KindWorkerEnd || f.Err != "carrier timeout for order ?" || f.Count != 1 || len(f.Seqs) != 1 || f.Seqs[0] != 8 {
			t.Fatalf("failure: %+v", f)
		}
		// The same failure in both runs is not a difference, whatever the id in its text.
		a := diffRun("42", true)
		a[7].Err = "carrier timeout for order 42"
		if rep := diffEvents(a, b, false); rep.Differs {
			t.Fatalf("same failure on both sides: %+v", rep)
		}
		// Failures are a multiset: a second occurrence is reported once.
		b2 := insertAt(b, 8, b[7])
		rep = diffEvents(a, b2, true)
		if len(rep.Failures.OnlyB) != 1 || rep.Failures.OnlyB[0].Count != 1 || len(rep.Failures.OnlyB[0].Seqs) != 2 {
			t.Fatalf("repeated failure: %+v", rep.Failures)
		}
	})

	t.Run("missing mutation", func(t *testing.T) {
		b := diffRun("43", true)
		b[6].Mutations = nil
		rep := diffEvents(base, b, false)
		if len(rep.Mutations) != 1 || len(rep.Events)+len(rep.Divergence)+len(rep.Failures.OnlyA)+len(rep.Failures.OnlyB) != 0 {
			t.Fatalf("report: %+v", rep)
		}
		if m := rep.Mutations[0]; m.Store != "postgres" || m.Entity != "orders" || m.Op != "update" || m.A != 1 || m.B != 0 {
			t.Fatalf("mutation: %+v", m)
		}
	})

	t.Run("rolled back mutation", func(t *testing.T) {
		no := false
		b := diffRun("43", true)
		b[2].Mutations[0].Effective = &no
		rep := diffEvents(base, b, false)
		if len(rep.Mutations) != 1 {
			t.Fatalf("report: %+v", rep)
		}
		if m := rep.Mutations[0]; m.Op != "insert" || m.A != 1 || m.B != 0 || m.RolledBackA != 0 || m.RolledBackB != 1 {
			t.Fatalf("mutation: %+v", m)
		}
	})

	t.Run("extra event", func(t *testing.T) {
		b := insertAt(diffRun("43", true), 2, core.Event{Service: "api", Kind: core.KindDBRead, Name: "SELECT * FROM coupons WHERE code = 'X1'"})
		rep := diffEvents(base, b, false)
		if len(rep.Events) != 1 || len(rep.Mutations)+len(rep.Failures.OnlyA)+len(rep.Failures.OnlyB) != 0 {
			t.Fatalf("report: %+v", rep)
		}
		if e := rep.Events[0]; e.Service != "api" || e.Kind != core.KindDBRead || e.Name != "SELECT * FROM coupons WHERE code = ?" || e.A != 0 || e.B != 1 {
			t.Fatalf("event: %+v", e)
		}
		if len(rep.Divergence) != 1 || rep.Divergence[0].Service != "api" || rep.Divergence[0].Position != 3 {
			t.Fatalf("divergence: %+v", rep.Divergence)
		}
	})

	t.Run("reordered", func(t *testing.T) {
		b := diffRun("43", true)
		b[3].Kind, b[3].Name, b[4].Kind, b[4].Name = b[4].Kind, b[4].Name, b[3].Kind, b[3].Name
		rep := diffEvents(base, b, false)
		if !rep.Differs || len(rep.Events)+len(rep.Mutations)+len(rep.Failures.OnlyA)+len(rep.Failures.OnlyB) != 0 || len(rep.Divergence) != 1 {
			t.Fatalf("a reorder must only show as a divergence: %+v", rep)
		}
		d := rep.Divergence[0]
		if d.Service != "api" || d.Position != 4 || d.LenA != 5 || d.LenB != 5 || len(d.A) != 4 || len(d.B) != 4 {
			t.Fatalf("divergence: %+v", d)
		}
		if a, b := d.A[2], d.B[2]; !a.Diverges || a.Seq != 4 || a.Kind != core.KindCacheWrite || a.Name != "SET order:42" ||
			!b.Diverges || b.Seq != 4 || b.Kind != core.KindQueuePub {
			t.Fatalf("diverging events: %+v %+v", a, b)
		}
	})

	t.Run("one side ends early, a service only in b", func(t *testing.T) {
		b := append(diffRun("43", true)[:7], core.Event{Service: "mailer", Kind: core.KindAPIOut, Name: "POST /send"})
		rep := diffEvents(base, reseq(b), false)
		if len(rep.Divergence) != 2 {
			t.Fatalf("divergence: %+v", rep.Divergence)
		}
		// Ordered by where it happens in the baseline; a service the baseline never ran comes last.
		w, m := rep.Divergence[0], rep.Divergence[1]
		if w.Service != "worker" || w.Position != 3 || w.LenB != 2 || len(w.B) != 2 || w.B[0].Diverges || w.B[1].Diverges {
			t.Fatalf("worker: %+v", w)
		}
		if m.Service != "mailer" || m.Position != 1 || m.LenA != 0 || len(m.A) != 0 {
			t.Fatalf("mailer: %+v", m)
		}
	})

	t.Run("ignore counts", func(t *testing.T) {
		// The api retries its read twice and inserts twice.
		b := diffRun("43", true)
		b = insertAt(insertAt(b, 2, b[1]), 2, b[1])
		b = insertAt(b, 5, b[4])
		rep := diffEvents(base, b, false)
		if len(rep.Events) != 2 || rep.Events[0].B != 3 || len(rep.Mutations) != 1 || rep.Mutations[0].B != 2 || len(rep.Divergence) != 1 {
			t.Fatalf("without --ignore-counts: %+v", rep)
		}
		if rep := diffEvents(base, b, true); rep.Differs {
			t.Fatalf("with --ignore-counts: %+v", rep)
		}
		// Something present on one side only still differs.
		b = insertAt(b, 0, core.Event{Service: "api", Kind: core.KindCacheRead, Name: "GET flags"})
		if rep := diffEvents(base, b, true); len(rep.Events) != 1 || rep.Events[0].Name != "GET flags" || len(rep.Divergence) != 1 {
			t.Fatalf("new key with --ignore-counts: %+v", rep)
		}
	})

	t.Run("sorted", func(t *testing.T) {
		b := diffRun("43", true)
		for _, n := range []string{"GET z", "GET a", "GET m"} {
			b = insertAt(b, 0, core.Event{Service: "api", Kind: core.KindCacheRead, Name: n, Err: "miss " + n})
		}
		rep := diffEvents(base, b, false)
		if len(rep.Events) != 3 || rep.Events[0].Name != "GET a" || rep.Events[2].Name != "GET z" ||
			len(rep.Failures.OnlyB) != 3 || rep.Failures.OnlyB[0].Name != "GET a" || rep.Failures.OnlyB[2].Name != "GET z" {
			t.Fatalf("report: %+v", rep)
		}
	})
}

func TestCLIDiff(t *testing.T) {
	env := newResultsEnv(t)
	broken := diffRun("44", true)
	// An extra failure, a missing mutation, two reordered events and an extra event.
	broken[7].Err = "carrier timeout"
	broken[6].Mutations = nil
	broken[3].Kind, broken[3].Name, broken[4].Kind, broken[4].Name = broken[4].Kind, broken[4].Name, broken[3].Kind, broken[3].Name
	broken = insertAt(broken, 5, core.Event{Service: "api", Kind: core.KindAPIOut, Name: "POST /audit/44"})
	env.events = map[string][]core.Event{
		"pass":   diffRun("42", true),
		"again":  diffRun("550e8400-e29b-41d4-a716-446655440000", false),
		"broken": broken,
		"short":  diffRun("45", true)[:7],
	}
	ids := map[string]string{}
	for _, name := range []string{"pass", "again", "broken", "short", "default"} {
		code, out, e := env.tc("start", "--name", name)
		var info struct{ ID string }
		if err := json.Unmarshal([]byte(out), &info); code != 0 || err != nil || info.ID == "" {
			t.Fatalf("start %s: %d %s %v", name, code, e, err)
		}
		ids[name] = info.ID
	}

	// Identical runs: no differences, and the gate passes.
	code, out, e := env.tc("diff", "pass", "again", "--fail-on-diff")
	want := "a  " + ids["pass"] + "  pass\nb  " + ids["again"] + "  again\nno differences\n"
	if code != 0 || out != want || e != "" {
		t.Fatalf("identical: %d %q\n%s", code, e, out)
	}
	// A session is identical to itself, by id as well as by name.
	if code, out, _ := env.tc("diff", ids["default"], "default", "--fail-on-diff"); code != 0 || !strings.Contains(out, "no differences") {
		t.Fatalf("self: %d\n%s", code, out)
	}

	code, out, e = env.tc("diff", "pass", "broken")
	want = strings.Join([]string{
		"a  " + ids["pass"] + "  pass",
		"b  " + ids["broken"] + "  broken",
		"",
		"failures only in b:",
		"  1x worker worker.end ship: carrier timeout  (#9)",
		"",
		"mutations that took effect (a -> b):",
		"  postgres/orders update  1 -> 0",
		"",
		"events (a -> b):",
		"  api api.out POST /audit/?  0 -> 1",
		"",
		"first divergence: service api, position 4 (a has 5 events, b has 6)",
		"  a  #2 db.read SELECT * FROM users WHERE id = 42",
		"  a  #3 db.write INSERT INTO orders (id, status) VALUES (42, 'new')",
		"  a> #4 cache.write SET order:42",
		"  a  #5 queue.publish ship",
		"  b  #2 db.read SELECT * FROM users WHERE id = 44",
		"  b  #3 db.write INSERT INTO orders (id, status) VALUES (44, 'new')",
		"  b> #4 queue.publish ship",
		"  b  #5 cache.write SET order:44",
		"  b  #6 api.out POST /audit/44",
		"",
	}, "\n")
	if code != 0 || out != want || e != "" {
		t.Fatalf("broken: %d %q\n%s", code, e, out)
	}
	if again := func() string { _, out, _ := env.tc("diff", "pass", "broken"); return out }(); again != out {
		t.Fatalf("output is not stable:\n%s\n%s", out, again)
	}

	// Differences fail the gate only when asked to, and still print.
	code, out, e = env.tc("diff", "--fail-on-diff", "pass", "broken")
	if code != exitCheckFailed || !strings.Contains(e, "sessions differ") || !strings.Contains(out, "first divergence") {
		t.Fatalf("--fail-on-diff: %d %q\n%s", code, e, out)
	}
	// The other direction, against a run that stopped early.
	if code, out, _ := env.tc("diff", "broken", "short"); code != 0 || !strings.Contains(out, "failures only in a:") ||
		strings.Contains(out, "failures only in b:") || !strings.Contains(out, "  worker worker.end ship  1 -> 0") ||
		!strings.Contains(out, "  a> #9 worker.end ship\n  b  #6 worker.start ship\n  b  #7 db.write UPDATE orders SET status = 'shipped' WHERE id = 45\n  b> (no more events)\n") {
		t.Fatalf("broken vs short: %d\n%s", code, out)
	}
	// Sessions without an override get the default events.
	if code, out, _ := env.tc("diff", "pass", "default"); code != 0 || !strings.Contains(out, "  api db.write INSERT INTO orders  0 -> 1") {
		t.Fatalf("pass vs default: %d\n%s", code, out)
	}

	if code, _, e := env.tc("diff", "pass", "nope"); code != 1 || !strings.Contains(e, `no session named "nope"`) {
		t.Fatalf("unknown session: %d %s", code, e)
	}
}

// Two runs that differ only in the status a request failed with are
// different: 500 is a bug in the service, 503 an unavailable dependency.
func TestCLIDiffHTTPStatus(t *testing.T) {
	env := newResultsEnv(t)
	run := func(order, status string) []core.Event {
		return reseq([]core.Event{
			{Service: "api", Kind: core.KindAPIIn, Name: "POST /orders/" + order + "/pay", Err: "HTTP " + status},
			{Service: "api", Kind: core.KindAPIOut, Name: "POST http://psp/charges/" + order, Err: "HTTP " + status},
		})
	}
	env.events = map[string][]core.Event{"r500": run("42", "500"), "r503": run("43", "503"), "r500b": run("44", "500")}
	ids := map[string]string{}
	for _, name := range []string{"r500", "r503", "r500b"} {
		code, out, e := env.tc("start", "--name", name)
		var info struct{ ID string }
		if err := json.Unmarshal([]byte(out), &info); code != 0 || err != nil {
			t.Fatalf("start %s: %d %s %v", name, code, e, err)
		}
		ids[name] = info.ID
	}
	code, out, e := env.tc("diff", "r500", "r503", "--fail-on-diff")
	t.Logf("500 vs 503: exit %d\n%s%s", code, out, e)
	want := "a  " + ids["r500"] + "  r500\nb  " + ids["r503"] + "  r503\n" + `
failures only in a:
  1x api api.in POST /orders/?/pay: HTTP 500  (#1)
  1x api api.out POST http://psp/charges/?: HTTP 500  (#2)

failures only in b:
  1x api api.in POST /orders/?/pay: HTTP 503  (#1)
  1x api api.out POST http://psp/charges/?: HTTP 503  (#2)
`
	if code != exitCheckFailed || out != want {
		t.Fatalf("500 vs 503: %d\n got %q\nwant %q", code, out, want)
	}
	// The same status for another order is still the same failure.
	if code, out, _ := env.tc("diff", "r500", "r500b", "--fail-on-diff"); code != 0 || !strings.Contains(out, "no differences") {
		t.Fatalf("500 vs 500: %d\n%s", code, out)
	}
}

func TestCLIDiffJSON(t *testing.T) {
	env := newResultsEnv(t)
	b := diffRun("43", true)
	b[7].Err = "carrier timeout"
	b[6].Mutations = nil
	env.events = map[string][]core.Event{"pass": diffRun("42", true), "again": diffRun("7", false), "broken": insertAt(b, 0, core.Event{Service: "api", Kind: core.KindCacheRead, Name: "GET flags"})}
	for _, name := range []string{"pass", "again", "broken"} {
		env.tc("start", "--name", name)
	}

	// No differences: every section is present and empty, never null.
	code, out, _ := env.tc("diff", "pass", "again", "--json")
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(out)); code != 0 || err != nil {
		t.Fatalf("--json: %d %v\n%s", code, err, out)
	}
	if got := compact.String(); !strings.HasSuffix(got, `"differs":false,"failures":{"only_a":[],"only_b":[]},"mutations":[],"events":[],"divergence":[]}`) ||
		!strings.Contains(got, `"name":"pass"},"b":{"id":"ts_`) {
		t.Fatalf("--json without differences: %s", got)
	}

	code, out, e := env.tc("diff", "--json", "--fail-on-diff", "pass", "broken")
	var rep struct {
		A, B     struct{ ID, Name string }
		Differs  bool
		Failures struct {
			OnlyA []map[string]any `json:"only_a"`
			OnlyB []map[string]any `json:"only_b"`
		}
		Mutations  []map[string]any
		Events     []map[string]any
		Divergence []struct {
			Service  string
			Position int
			LenA     int `json:"len_a"`
			LenB     int `json:"len_b"`
			A, B     []map[string]any
		}
	}
	if err := json.Unmarshal([]byte(out), &rep); code != exitCheckFailed || err != nil {
		t.Fatalf("--json: %d %s %v\n%s", code, e, err, out)
	}
	if !rep.Differs || rep.A.Name != "pass" || rep.B.Name != "broken" || !strings.HasPrefix(rep.B.ID, "ts_") {
		t.Fatalf("header: %+v", rep)
	}
	wantObj := func(what string, got, want map[string]any) {
		t.Helper()
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		if string(g) != string(w) {
			t.Errorf("%s:\n got %s\nwant %s", what, g, w)
		}
	}
	if len(rep.Failures.OnlyA) != 0 || len(rep.Failures.OnlyB) != 1 || len(rep.Mutations) != 1 || len(rep.Events) != 1 || len(rep.Divergence) != 1 {
		t.Fatalf("sections: %s", out)
	}
	wantObj("failure", rep.Failures.OnlyB[0], map[string]any{"service": "worker", "kind": "worker.end", "name": "ship", "err": "carrier timeout", "count": 1, "seqs": []int{9}})
	wantObj("mutation", rep.Mutations[0], map[string]any{"store": "postgres", "entity": "orders", "op": "update", "a": 1, "b": 0, "rolled_back_a": 0, "rolled_back_b": 0})
	wantObj("event", rep.Events[0], map[string]any{"service": "api", "kind": "cache.read", "name": "GET flags", "a": 0, "b": 1})
	d := rep.Divergence[0]
	if d.Service != "api" || d.Position != 1 || d.LenA != 5 || d.LenB != 6 || len(d.A) != 3 || len(d.B) != 3 {
		t.Fatalf("divergence: %+v", d)
	}
	wantObj("diverging event", d.B[0], map[string]any{"seq": 1, "kind": "cache.read", "name": "GET flags", "diverges": true})
	wantObj("context event", d.B[1], map[string]any{"seq": 2, "kind": "api.in", "name": "POST /orders/43"})
}

func TestCLIDiffIgnoreCounts(t *testing.T) {
	env := newResultsEnv(t)
	retried := diffRun("43", true)
	retried = insertAt(retried, 2, retried[1])
	env.events = map[string][]core.Event{"pass": diffRun("42", true), "retried": retried}
	env.tc("start", "--name", "pass")
	env.tc("start", "--name", "retried")

	code, out, _ := env.tc("diff", "pass", "retried", "--fail-on-diff")
	if code != exitCheckFailed || !strings.Contains(out, "  api db.read SELECT * FROM users WHERE id = ?  1 -> 2") {
		t.Fatalf("retry without --ignore-counts: %d\n%s", code, out)
	}
	if code, out, e := env.tc("diff", "pass", "retried", "--fail-on-diff", "--ignore-counts"); code != 0 || !strings.Contains(out, "no differences") {
		t.Fatalf("retry with --ignore-counts: %d %s\n%s", code, e, out)
	}
}

func TestCLIDiffUsage(t *testing.T) {
	for _, args := range [][]string{{"diff"}, {"diff", "a"}, {"diff", "a", "b", "c"}, {"diff", "--json", "a"}} {
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code == 0 || !strings.Contains(errb.String(), "usage: tracectl diff <id|name a> <id|name b>") || out.Len() != 0 {
			t.Errorf("%v: %d %q %q", args, code, out.String(), errb.String())
		}
	}
	var out, errb bytes.Buffer
	if code := run([]string{"diff", "--help"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "-ignore-counts") {
		t.Errorf("diff --help: %d %s", code, errb.String())
	}
	if code := run([]string{"diff", "--bogus", "a", "b"}, &out, &errb); code == 0 {
		t.Errorf("unknown flag: %d", code)
	}
}
