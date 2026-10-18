package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tsession/collector"
	"tsession/collector/store"
	"tsession/core"
	"tsession/dashboard"
)

func TestE2E_MemStore(t *testing.T) { runSuite(t, "mem") }
func TestE2E_PGStore(t *testing.T)  { runSuite(t, "pg") }

func runSuite(t *testing.T, kind string) {
	pgDSN, redisAddr := itEnv(t)
	appSchema := "app_it_" + kind
	schemas := []string{appSchema}
	var st store.Store
	if kind == "pg" {
		schemas = append(schemas, "col_it")
	}
	schemaSetup(t, pgDSN, schemas...)
	if kind == "pg" {
		p, err := store.OpenPGConfig(context.Background(), store.PGConfig{DSN: pgDSN, Schema: "col_it"})
		if err != nil {
			t.Fatal(err)
		}
		st = p
		t.Cleanup(func() { p.Close() })
	} else {
		st = store.NewMem()
	}
	in := newInfra(t, st)
	in.startRuntime(t, "orders")
	ns := "it" + kind + ":"
	delNS(t, redisAddr, ns)
	ae := newApp(t, withParam(pgDSN, "search_path", appSchema), redisAddr, ns)
	col := colAPI{in.colSrv.URL}

	var (
		s1                                  = in.start(t, "header")
		okOrder, failID                     int64
		evs                                 []core.Event
		pubIDs                              = map[string]string{} // job -> publish span id
		authSecret, pwSecret                = "Bearer synthetic-token-abc123", "hunter2-synthetic"
	)
	waitActive(t, s1.ID)

	t.Run("drive_and_seal", func(t *testing.T) {
		h := s1.Header()
		h.Set("Authorization", authSecret)
		code, o := ae.createOrder(t, h, orderBody{SKU: "widget", Qty: 2, Customer: "alice-it", Password: pwSecret,
			Jobs: []string{"flaky_job", "poison_job"}})
		if code != 201 || o.ID == 0 {
			t.Fatalf("POST /orders: %d %+v", code, o)
		}
		okOrder = o.ID
		code, raw := doReq(t, "POST", ae.base+"/orders/fail", nil, s1.Header())
		if code != 422 {
			t.Fatalf("POST /orders/fail: %d %s", code, raw)
		}
		if i := strings.Index(string(raw), `"id":`); i >= 0 {
			failID, _ = strconv.ParseInt(strings.TrimRight(strings.SplitN(string(raw[i+5:]), "}", 2)[0], " \n"), 10, 64)
		}
		if code, _ := doReq(t, "GET", ae.base+"/orders/"+strconv.FormatInt(okOrder, 10), nil, s1.Header()); code != 200 {
			t.Fatalf("GET /orders/{id}: %d", code)
		}
		info := in.seal(t, s1)
		// ledger drained -> not incomplete
		if info.Incomplete {
			t.Errorf("session incomplete after seal: %+v", info)
		}
		evs = col.events(t, s1.ID)
		t.Logf("%d events:\n%s", len(evs), fmtSeq(evs))
		if n := ae.a.DLQCount(); n != 1 {
			t.Errorf("redis DLQ len = %d, want 1", n)
		}
	})

	t.Run("kinds_present", func(t *testing.T) {
		for _, k := range []core.Kind{core.KindAPIIn, core.KindDBRead, core.KindDBWrite, core.KindCacheWrite, core.KindCacheRead,
			core.KindQueuePub, core.KindWorkerStart, core.KindWorkerEnd, core.KindAPIOut, core.KindDBTxCommit, core.KindDBTxRollbck} {
			if len(ofKind(evs, k)) == 0 {
				t.Errorf("no %s events", k)
			}
		}
		if n := len(ofKind(evs, core.KindQueuePub)); n < 3 {
			t.Errorf("queue.publish = %d, want >=3 (receipt, flaky, poison)", n)
		}
		for _, e := range ofKind(evs, core.KindQueuePub) {
			pubIDs[e.Name] = e.SpanID
		}
	})

	t.Run("seq_gapless_and_from_seq", func(t *testing.T) {
		for i, e := range evs {
			if e.Seq != uint64(i+1) {
				t.Fatalf("seq gap at index %d: got %d want %d", i, e.Seq, i+1)
			}
			if e.SessionID != s1.ID {
				t.Errorf("event from other session: %+v", e.SessionID)
			}
		}
		var r eventsResp
		col.get(t, "/v1/sessions/"+s1.ID+"/events?from_seq=3&limit=2", &r)
		if len(r.Events) != 2 || r.Events[0].Seq != 3 || r.NextSeq != 5 || !r.Sealed {
			t.Errorf("from_seq=3 limit=2 => first=%d n=%d next=%d sealed=%v (want inclusive 3, next 5, sealed)",
				firstSeq(r.Events), len(r.Events), r.NextSeq, r.Sealed)
		}
		col.get(t, "/v1/sessions/"+s1.ID+"/events?from_seq="+strconv.Itoa(len(evs)+1), &r)
		if len(r.Events) != 0 || r.NextSeq != uint64(len(evs)+1) {
			t.Errorf("past-end poll: n=%d next=%d want 0,%d", len(r.Events), r.NextSeq, len(evs)+1)
		}
	})

	t.Run("worker_link_params_trace", func(t *testing.T) {
		ws := named(evs, core.KindWorkerStart, "send_receipt")
		if len(ws) != 1 {
			t.Fatalf("worker.start send_receipt = %d", len(ws))
		}
		w := ws[0]
		pub := pubIDs["send_receipt"]
		if !contains(w.LinkIDs, pub) {
			t.Errorf("worker.start LinkIDs %v lacks publish span %s", w.LinkIDs, pub)
		}
		p := inline(w.Req)
		if p["customer"] != "alice-it" || p["order_id"] != float64(okOrder) {
			t.Errorf("worker.start params = %v", p)
		}
		var apiIn core.Event
		for _, e := range named(evs, core.KindAPIIn, "POST /orders") {
			if !strings.Contains(e.Name, "fail") {
				apiIn = e
			}
		}
		if apiIn.SpanID == "" {
			t.Fatal("no api.in POST /orders")
		}
		if w.TraceID != apiIn.TraceID {
			t.Errorf("worker.start trace %s != api.in trace %s", w.TraceID, apiIn.TraceID)
		}
		// nested spans inside worker: children of worker.start share the trace
		var nestedDB, nestedOut int
		for _, e := range evs {
			if e.ParentID == w.SpanID {
				if e.TraceID != w.TraceID {
					t.Errorf("worker child %s %q trace %s != %s", e.Kind, e.Name, e.TraceID, w.TraceID)
				}
				switch e.Kind {
				case core.KindDBWrite:
					nestedDB++
				case core.KindAPIOut:
					nestedOut++
				}
			}
		}
		if nestedDB < 1 || nestedOut != 1 {
			t.Errorf("worker.start children: db.write=%d api.out=%d (want >=1, exactly 1: double-recording check)", nestedDB, nestedOut)
		}
		if n := len(named(evs, core.KindAPIOut, "/receipts")); n != 1 {
			t.Errorf("api.out /receipts recorded %d times, want 1 (thttp + patched DefaultTransport)", n)
		}
		if n := len(named(evs, core.KindAPIIn, "/receipts")); n != 1 {
			t.Errorf("downstream api.in /receipts = %d, want 1", n)
		}
		if len(ofKind(evs, core.KindQueueCons)) > 0 {
			t.Logf("note: queue.consume events present: %d", len(ofKind(evs, core.KindQueueCons)))
		}
	})

	t.Run("trace_tree_placement", func(t *testing.T) {
		tr := col.trace(t, s1.ID)
		var walk func(n *collector.Node, depth int, f func(n *collector.Node, linked bool))
		walk = func(n *collector.Node, depth int, f func(*collector.Node, bool)) {
			f(n, false)
			for _, c := range n.Children {
				walk(c, depth+1, f)
			}
			for _, l := range n.Linked {
				f(l, true)
				for _, c := range l.Children {
					walk(c, depth+1, f)
				}
			}
		}
		total := 0
		var pubLinked bool
		for _, r := range tr.Roots {
			walk(r, 0, func(n *collector.Node, linked bool) {
				total++
				if n.Event.Kind == core.KindQueuePub {
					for _, l := range n.Linked {
						if l.Event.Kind == core.KindWorkerStart {
							pubLinked = true
						}
					}
				}
			})
		}
		if !pubLinked {
			t.Error("no queue.publish node has a linked worker.start")
		}
		if total != len(evs) {
			t.Errorf("trace tree has %d nodes (reachable via children/linked), events=%d", total, len(evs))
		}
		var apiRoots int
		for _, r := range tr.Roots {
			if r.Event.Kind == core.KindAPIIn {
				apiRoots++
			}
		}
		if apiRoots != 3 {
			t.Errorf("api.in roots = %d, want 3 (orders, orders/fail, GET order); roots=%d", apiRoots, len(tr.Roots))
		}
		if len(tr.Roots) != apiRoots {
			for _, r := range tr.Roots {
				t.Logf("root: %s %q", r.Event.Kind, r.Event.Name)
			}
		}
	})

	t.Run("retry_and_dlq", func(t *testing.T) {
		flakyEnds := named(evs, core.KindWorkerEnd, "flaky_job")
		retries := named(evs, core.KindWorkerRetry, "flaky_job")
		if len(flakyEnds) != 3 || len(retries) != 2 {
			t.Fatalf("flaky: worker.end=%d worker.retry=%d (want 3,2)", len(flakyEnds), len(retries))
		}
		last := flakyEnds[len(flakyEnds)-1]
		if last.Attempt != 3 || last.Err != "" {
			t.Errorf("flaky final worker.end = attempt %d err %q", last.Attempt, last.Err)
		}
		if !contains(retries[0].LinkIDs, pubIDs["flaky_job"]) {
			t.Errorf("worker.retry link missing: %v", retries[0].LinkIDs)
		}
		if n := len(named(evs, core.KindWorkerDLQ, "poison_job")); n != 1 {
			t.Errorf("poison worker.dlq = %d, want 1", n)
		}
		if n := len(named(evs, core.KindWorkerRetry, "poison_job")); n != 1 {
			t.Errorf("poison worker.retry = %d, want 1", n)
		}
		if n := len(named(evs, core.KindWorkerStart, "flaky_job")); n != 3 {
			t.Errorf("flaky worker.start = %d, want 3", n)
		}
	})

	t.Run("mutations", func(t *testing.T) {
		var insert, invUpdate, rolled *core.Mutation
		for _, e := range ofKind(evs, core.KindDBWrite) {
			for i := range e.Mutations {
				m := &e.Mutations[i]
				switch {
				case m.Entity == "orders" && m.Op == "insert" && e.TxID != "":
					if idv, ok := m.PK["id"]; ok && int64(toF(idv)) == failID && failID != 0 {
						rolled = m
					} else if ok && int64(toF(idv)) == okOrder {
						insert = m
					}
				case m.Entity == "inventory" && m.Op == "update":
					invUpdate = m
				}
			}
		}
		if insert == nil {
			for _, e := range ofKind(evs, core.KindDBWrite) {
				t.Logf("db.write seq=%d tx=%q %q flags=%v muts=%d", e.Seq, e.TxID, e.Name, e.Flags, len(e.Mutations))
				for _, m := range e.Mutations {
					t.Logf("   mut %s %s %s pk=%v", m.Store, m.Entity, m.Op, m.PK)
				}
			}
			for _, e := range ofKind(evs, core.KindDBRead) {
				t.Logf("db.read seq=%d tx=%q %q flags=%v", e.Seq, e.TxID, e.Name, e.Flags)
			}
			t.Fatal("no orders insert mutation for the committed order")
		}
		if insert.Effective == nil || !*insert.Effective {
			t.Errorf("committed insert effective=%v", insert.Effective)
		}
		if insert.Store != "postgres" || insert.After.IsZero() {
			t.Errorf("insert mutation: store=%s after.zero=%v", insert.Store, insert.After.IsZero())
		}
		if invUpdate == nil {
			t.Fatal("no inventory update mutation")
		}
		b, a := inline(invUpdate.Before), inline(invUpdate.After)
		if b["stock"] == nil || a["stock"] == nil || toF(b["stock"])-toF(a["stock"]) != 2 {
			t.Errorf("inventory before/after: %v -> %v (diff=%v)", b, a, invUpdate.Diff)
		}
		if rolled == nil {
			t.Fatalf("no mutation for rolled-back insert (failID=%d)", failID)
		}
		if rolled.Effective == nil || *rolled.Effective {
			t.Errorf("rolled back mutation effective=%v want false", rolled.Effective)
		}
		// worker update has before/after
		var workerUpd bool
		for _, e := range ofKind(evs, core.KindDBWrite) {
			for _, m := range e.Mutations {
				if m.Entity == "orders" && m.Op == "update" {
					bb, aa := inline(m.Before), inline(m.After)
					if bb["status"] != nil && aa["status"] != nil && bb["status"] != aa["status"] {
						workerUpd = true
					}
				}
			}
		}
		if !workerUpd {
			t.Error("no orders update mutation with before/after status change")
		}
		// redis
		var redisIns, redisUpd bool
		for _, e := range ofKind(evs, core.KindCacheWrite) {
			for _, m := range e.Mutations {
				if m.Store == "redis" && strings.Contains(m.Entity, "order:") {
					redisIns = true
					if !m.Before.IsZero() && !m.After.IsZero() {
						redisUpd = true
					}
				}
			}
		}
		if !redisIns {
			t.Error("no redis order:* write mutation")
		}
		if !redisUpd {
			t.Error("no redis mutation with before and after image (WithBeforeImage)")
		}
	})

	t.Run("entity_history", func(t *testing.T) {
		h := col.history(t, s1.ID, "postgres", "orders", strconv.FormatInt(okOrder, 10))
		if len(h.Mutations) < 3 {
			t.Fatalf("history = %d mutations, want >=3 (insert + 2 updates): %+v", len(h.Mutations), h)
		}
		if h.Mutations[0].Op != "insert" {
			t.Errorf("first op %s", h.Mutations[0].Op)
		}
		for i := 1; i < len(h.Mutations); i++ {
			if h.Mutations[i].Seq <= h.Mutations[i-1].Seq {
				t.Errorf("history not ordered by seq: %d then %d", h.Mutations[i-1].Seq, h.Mutations[i].Seq)
			}
		}
		var ents []collector.Entity
		col.get(t, "/v1/sessions/"+s1.ID+"/entities", &ents)
		found := false
		for _, e := range ents {
			if e.Store == "postgres" && e.Entity == "orders" {
				found = true
			}
		}
		if !found {
			t.Errorf("entities list lacks postgres/orders: %+v", ents)
		}
	})

	t.Run("redaction", func(t *testing.T) {
		if anyContains(evs, "hunter2-synthetic") {
			t.Error("password value leaked into events")
		}
		if anyContains(evs, "synthetic-token-abc123") {
			t.Error("Authorization value leaked into events")
		}
		var seen bool
		for _, e := range named(evs, core.KindAPIIn, "POST /orders") {
			if strings.Contains(string(e.Req.Inline), core.RedactedValue) {
				seen = true
			}
		}
		if !seen {
			t.Error("api.in request has no [REDACTED] markers")
		}
	})

	t.Run("summary_ledger", func(t *testing.T) {
		s := col.summary(t, s1.ID)
		if s.PendingAsync != 0 || s.Incomplete || !s.Sealed {
			t.Errorf("summary pending=%d incomplete=%v sealed=%v ids=%v", s.PendingAsync, s.Incomplete, s.Sealed, s.PendingIDs)
		}
		if s.DroppedEvents != 0 {
			t.Errorf("dropped=%d", s.DroppedEvents)
		}
	})

	t.Run("dashboard_proxy", func(t *testing.T) {
		dh, err := dashboard.New(dashboard.Config{ControlPlaneURL: in.cpSrv.URL, CollectorURL: in.colSrv.URL})
		if err != nil {
			t.Fatal(err)
		}
		ds := httptest.NewServer(dh)
		defer ds.Close()
		var tr collector.Trace
		colAPI{ds.URL + "/api/col"}.get(t, "/v1/sessions/"+s1.ID+"/trace", &tr)
		if len(tr.Roots) == 0 || tr.SessionID != s1.ID {
			t.Errorf("dashboard trace proxy: %+v", tr.SessionID)
		}
		var info map[string]any
		colAPI{ds.URL + "/api/cp"}.get(t, "/v1/sessions/"+s1.ID, &info)
		if info["state"] != "SEALED" {
			t.Errorf("dashboard cp proxy state=%v", info["state"])
		}
	})

	if kind == "pg" {
		t.Run("pg_store_really_used", func(t *testing.T) {
			p, err := pgxpool.New(context.Background(), pgDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			var n, m int
			if err := p.QueryRow(context.Background(), "SELECT count(*) FROM col_it.session_events WHERE session_id=$1", s1.ID).Scan(&n); err != nil {
				t.Fatal(err)
			}
			_ = p.QueryRow(context.Background(), "SELECT count(*) FROM col_it.entity_mutations WHERE session_id=$1", s1.ID).Scan(&m)
			if n != len(evs) || m == 0 {
				t.Errorf("col_it.session_events=%d (want %d) entity_mutations=%d", n, len(evs), m)
			}
		})
	}

	t.Run("two_concurrent_sessions_isolated", func(t *testing.T) {
		a, b := in.start(t, "header"), in.start(t, "header")
		waitActive(t, a.ID)
		waitActive(t, b.ID)
		var wg sync.WaitGroup
		res := map[string]app2{}
		var mu sync.Mutex
		for _, s := range []struct {
			sess     string
			customer string
			hdr      http.Header
		}{{"A", "cust-AAA", a.Header()}, {"B", "cust-BBB", b.Header()}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 3; i++ {
					code, o := ae.createOrder(t, s.hdr, orderBody{SKU: "gadget", Qty: 1, Customer: s.customer})
					if code != 201 {
						t.Errorf("session %s order: %d", s.sess, code)
						return
					}
					mu.Lock()
					r := res[s.sess]
					r.ids = append(r.ids, o.ID)
					res[s.sess] = r
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		in.seal(t, a)
		in.seal(t, b)
		ea, eb := col.events(t, a.ID), col.events(t, b.ID)
		for name, c := range map[string]struct {
			mine, other []core.Event
			own, foreign string
		}{"A": {ea, eb, "cust-AAA", "cust-BBB"}, "B": {eb, ea, "cust-BBB", "cust-AAA"}} {
			if len(c.mine) == 0 {
				t.Fatalf("session %s: no events", name)
			}
			if anyContains(c.mine, c.foreign) {
				t.Errorf("session %s contains foreign customer %s", name, c.foreign)
			}
			if !anyContains(c.mine, c.own) {
				t.Errorf("session %s lacks own customer", name)
			}
			if n := len(named(c.mine, core.KindAPIIn, "POST /orders")); n != 3 {
				t.Errorf("session %s api.in = %d want 3", name, n)
			}
			if n := len(ofKind(c.mine, core.KindWorkerEnd)); n != 3 {
				t.Errorf("session %s worker.end = %d want 3", name, n)
			}
			ids := map[string]bool{}
			for _, e := range c.mine {
				ids[e.TraceID] = true
			}
			for _, e := range c.other {
				if ids[e.TraceID] {
					t.Errorf("trace %s appears in both sessions", e.TraceID)
				}
			}
		}
	})

	t.Run("exclusive_mode_stamps_headerless", func(t *testing.T) {
		x := in.start(t, "exclusive")
		waitActive(t, x.ID)
		waitFor(t, 3*time.Second, "exclusive visible", func() bool {
			_, sp := core.BeginIngress(context.Background(), core.KindAPIIn, "probe", core.MapCarrier{})
			return sp != nil
		})
		code, o := ae.createOrder(t, nil, orderBody{SKU: "widget", Qty: 1, Customer: "cust-XXX"})
		if code != 201 {
			t.Fatalf("exclusive order %d", code)
		}
		if code, _ := doReq(t, "GET", ae.base+"/orders/"+strconv.FormatInt(o.ID, 10), nil, nil); code != 200 {
			t.Errorf("GET %d", code)
		}
		in.seal(t, x)
		ex := col.events(t, x.ID)
		if len(named(ex, core.KindAPIIn, "POST /orders")) != 1 || len(ofKind(ex, core.KindWorkerEnd)) != 1 ||
			len(ofKind(ex, core.KindDBWrite)) == 0 {
			t.Errorf("exclusive events:\n%s", fmtSeq(ex))
		}
		if len(named(ex, core.KindAPIIn, "GET /orders")) != 1 {
			t.Errorf("exclusive GET not stamped:\n%s", fmtSeq(ex))
		}
	})

	t.Run("no_session_no_events", func(t *testing.T) {
		before := col.sessions(t)
		code, o := ae.createOrder(t, nil, orderBody{SKU: "widget", Qty: 1, Customer: "cust-NONE"})
		if code != 201 {
			t.Fatalf("order %d", code)
		}
		// inactive/unknown session header
		bogus := http.Header{}
		bogus.Set(core.KeySession, "ts_bogus_inactive")
		bogus.Set(core.KeyTraceparent, "00-"+core.NewTraceID()+"-"+core.NewSpanID()+"-01")
		ae.createOrder(t, bogus, orderBody{SKU: "widget", Qty: 1, Customer: "cust-BOGUS"})
		// also a request on a session that is already SEALED
		stale := s1.Header()
		ae.createOrder(t, stale, orderBody{SKU: "widget", Qty: 1, Customer: "cust-STALE"})
		waitFor(t, 5*time.Second, "worker finished", func() bool {
			c, _ := doReq(t, "GET", ae.base+"/orders/"+strconv.FormatInt(o.ID, 10), nil, nil)
			return c == 200
		})
		time.Sleep(1200 * time.Millisecond) // > exporter flush + collector window
		after := col.sessions(t)
		if len(after) != len(before) {
			t.Errorf("sessions before=%d after=%d: %+v", len(before), len(after), after)
		}
		if got := col.events(t, s1.ID); len(got) != len(evs) {
			t.Errorf("sealed session grew from %d to %d events", len(evs), len(got))
		}
	})
}

type app2 struct{ ids []int64 }

func firstSeq(e []core.Event) uint64 {
	if len(e) == 0 {
		return 0
	}
	return e[0].Seq
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func toF(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return -1
}
