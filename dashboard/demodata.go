package dashboard

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"tsession/core"
)

// All demo data is synthetic. Hostnames use the reserved .invalid TLD.

type builder struct {
	sid   string
	base  time.Time
	evs   []core.Event
	local map[string]uint64
}

func pj(v any) core.PayloadRef {
	b, _ := json.Marshal(v)
	return core.PayloadRef{Inline: b, Size: len(b)}
}

func pjRedacted(v any) core.PayloadRef {
	p := pj(v)
	p.Redacted = true
	return p
}

type m = map[string]any

// add records a span; offsets are milliseconds from the session base.
func (b *builder) add(key, trace, svc string, kind core.Kind, name, parent string, s, e int, f func(*core.Event)) {
	inst := svc + "-1"
	b.local[inst]++
	ev := core.Event{
		SessionID: b.sid, TraceID: trace, SpanID: "sp_" + key, Service: svc, Instance: inst,
		Kind: kind, Name: name, LocalSeq: b.local[inst],
		StartNs: b.base.Add(time.Duration(s) * time.Millisecond).UnixNano(),
		EndNs:   b.base.Add(time.Duration(e) * time.Millisecond).UnixNano(),
	}
	if parent != "" {
		ev.ParentID = "sp_" + parent
	}
	ev.HLC, ev.EndHLC = uint64(ev.StartNs), uint64(ev.EndNs)
	if f != nil {
		f(&ev)
	}
	b.evs = append(b.evs, ev)
}

func link(keys ...string) func(*core.Event) {
	return func(e *core.Event) {
		for _, k := range keys {
			e.LinkIDs = append(e.LinkIDs, "sp_"+k)
		}
	}
}

func chain(fs ...func(*core.Event)) func(*core.Event) {
	return func(e *core.Event) {
		for _, f := range fs {
			f(e)
		}
	}
}

func req(v any) func(*core.Event)  { return func(e *core.Event) { e.Req = pj(v) } }
func resp(v any) func(*core.Event) { return func(e *core.Event) { e.Resp = pj(v) } }
func errs(s string) func(*core.Event) {
	return func(e *core.Event) { e.Err = s }
}
func tx(id string) func(*core.Event) { return func(e *core.Event) { e.TxID = id } }
func attempt(n int) func(*core.Event) {
	return func(e *core.Event) { e.Attempt = n }
}
func attrs(kv ...string) func(*core.Event) {
	return func(e *core.Event) {
		e.Attrs = map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			e.Attrs[kv[i]] = kv[i+1]
		}
	}
}
func mut(mu core.Mutation) func(*core.Event) {
	return func(e *core.Event) { e.Mutations = append(e.Mutations, mu) }
}
func read(r core.ReadSummary) func(*core.Event) {
	return func(e *core.Event) { e.Reads = append(e.Reads, r) }
}
func flags(f ...string) func(*core.Event) {
	return func(e *core.Event) { e.Flags = append(e.Flags, f...) }
}

// demoEvents builds the rich synthetic session: POST /orders -> db/cache ->
// queue publishes -> workers (retry, DLQ), plus a rolled-back transaction.
// Events are returned in Seq (completion) order with Seq assigned 1..N.
func demoEvents(sid string, base time.Time) []core.Event {
	b := &builder{sid: sid, base: base, local: map[string]uint64{}}
	const tr = "tr_demo01"
	const api, wk1, wk2 = "orders-api", "fulfil-worker", "notify-worker"
	order := m{"id": "ord_1001", "customer_id": "cus_synth_001", "status": "pending", "total_cents": 1895}

	b.add("api", tr, api, core.KindAPIIn, "POST /orders", "", 0, 180, chain(
		func(e *core.Event) {
			e.Req = pjRedacted(m{"method": "POST", "path": "/orders",
				"headers": m{"authorization": "[REDACTED]", "content-type": "application/json"},
				"body": m{"customer_id": "cus_synth_001", "coupon": "WELCOME10",
					"items":   []m{{"sku": "SKU-APPLE-1", "qty": 2}, {"sku": "SKU-MILK-2", "qty": 1}},
					"payment": m{"card_number": "[REDACTED]", "token": "[REDACTED]"}}})
		},
		resp(m{"status": 201, "body": m{"order_id": "ord_1001", "status": "pending", "total_cents": 1895}}),
		attrs("http.route", "/orders", "http.status", "201"), flags("redacted")))
	b.add("cart", tr, api, core.KindCacheRead, "GET cart:cus_synth_001", "api", 5, 8, chain(
		req(m{"cmd": "GET", "key": "cart:cus_synth_001"}),
		resp(m{"hit": true, "value": m{"items": 2}}),
		read(core.ReadSummary{Store: "redis", Entity: "cart:*", Statement: "GET", RowCount: 1, PKs: []string{"cart:cus_synth_001"}})))
	b.add("inv", tr, api, core.KindDBRead, "SELECT sku, stock FROM inventory WHERE sku = ANY($1)", "api", 10, 18, chain(
		func(e *core.Event) {
			e.Reads = []core.ReadSummary{{Store: "postgres", Entity: "inventory",
				Statement: "SELECT sku, stock FROM inventory WHERE sku = ANY($1)",
				Args:      pj([]any{[]string{"SKU-APPLE-1", "SKU-MILK-2"}}), RowCount: 2,
				PKs: []string{"SKU-APPLE-1", "SKU-MILK-2"},
				Sample: core.PayloadRef{Inline: pj("[{\"sku\":\"SKU-APPLE-1\",\"stock\":40},{\"sku\":\"SKU-MILK-2\",\"stock\"").Inline,
					Size: 4096, Truncated: true}}}
		}, flags("truncated")))
	b.add("tx1b", tr, api, core.KindDBTxBegin, "BEGIN", "api", 20, 21, tx("tx_01"))
	b.add("insord", tr, api, core.KindDBWrite, "INSERT INTO orders (id, customer_id, status, total_cents) VALUES ($1,$2,$3,$4)", "api", 22, 30, chain(
		tx("tx_01"), req(m{"args": []any{"ord_1001", "cus_synth_001", "pending", 1895}}),
		mut(core.Mutation{Store: "postgres", Entity: "orders", PK: m{"id": "ord_1001"}, Op: "insert", Source: "sdk",
			After: pj(order),
			Diff: []core.FieldChange{{Field: "id", New: "ord_1001"}, {Field: "customer_id", New: "cus_synth_001"},
				{Field: "status", New: "pending"}, {Field: "total_cents", New: 1895}}})))
	b.add("updinv", tr, api, core.KindDBWrite, "UPDATE inventory SET stock = stock - $1 WHERE sku = $2", "api", 31, 35, chain(
		tx("tx_01"), req(m{"args": []any{2, "SKU-APPLE-1"}}),
		mut(core.Mutation{Store: "postgres", Entity: "inventory", PK: m{"sku": "SKU-APPLE-1"}, Op: "update", Source: "sdk",
			Before: pj(m{"sku": "SKU-APPLE-1", "stock": 40, "reserved": 3}),
			After:  pj(m{"sku": "SKU-APPLE-1", "stock": 38, "reserved": 3}),
			Diff:   []core.FieldChange{{Field: "stock", Old: 40, New: 38}}})))
	b.add("updinv2", tr, api, core.KindDBWrite, "UPDATE inventory SET stock = stock - $1 WHERE sku = $2", "api", 35, 39, chain(
		tx("tx_01"), req(m{"args": []any{1, "SKU-MILK-2"}}),
		mut(core.Mutation{Store: "postgres", Entity: "inventory", PK: m{"sku": "SKU-MILK-2"}, Op: "update", Source: "sdk",
			Before: pj(m{"sku": "SKU-MILK-2", "stock": 12, "reserved": 0}),
			After:  pj(m{"sku": "SKU-MILK-2", "stock": 11, "reserved": 0}),
			Diff:   []core.FieldChange{{Field: "stock", Old: 12, New: 11}}})))
	b.add("tx1c", tr, api, core.KindDBTxCommit, "COMMIT", "api", 40, 43, tx("tx_01"))
	b.add("cachew", tr, api, core.KindCacheWrite, "SET order:ord_1001", "api", 45, 47, chain(
		req(m{"cmd": "SET", "key": "order:ord_1001", "ttl_s": 300}),
		mut(core.Mutation{Store: "redis", Entity: "order:*", PK: m{"key": "order:ord_1001"}, Op: "upsert", Source: "sdk",
			After: pj(order), Diff: []core.FieldChange{{Field: "value", New: "{order}"}}})))
	b.add("cachedel", tr, api, core.KindCacheWrite, "DEL cart:cus_synth_001", "api", 47, 49, chain(
		req(m{"cmd": "DEL", "key": "cart:cus_synth_001"}),
		mut(core.Mutation{Store: "redis", Entity: "cart:*", PK: m{"key": "cart:cus_synth_001"}, Op: "delete", Source: "sdk",
			Before: pj(m{"items": 2}), Diff: []core.FieldChange{{Field: "items", Old: 2}}})))
	// failed transaction, rolled back
	b.add("tx2b", tr, api, core.KindDBTxBegin, "BEGIN", "api", 50, 51, tx("tx_02"))
	b.add("loy", tr, api, core.KindDBWrite, "INSERT INTO loyalty_ledger (customer_id, order_id, points) VALUES ($1,$2,$3)", "api", 52, 56, chain(
		tx("tx_02"), req(m{"args": []any{"cus_synth_001", "ord_1001", 19}}),
		mut(core.Mutation{Store: "postgres", Entity: "loyalty_ledger", PK: m{"customer_id": "cus_synth_001", "order_id": "ord_1001"},
			Op: "insert", Source: "sdk", After: pj(m{"customer_id": "cus_synth_001", "order_id": "ord_1001", "points": 19}),
			Diff: []core.FieldChange{{Field: "points", New: 19}}})))
	b.add("loy2", tr, api, core.KindDBWrite, "UPDATE customers SET points = points + $1 WHERE id = $2", "api", 56, 59, chain(
		tx("tx_02"), req(m{"args": []any{19, "cus_synth_001"}}),
		errs(`ERROR: new row for relation "customers" violates check constraint "points_cap" (SQLSTATE 23514)`)))
	b.add("tx2r", tr, api, core.KindDBTxRollbck, "ROLLBACK", "api", 60, 64, tx("tx_02"))
	// async publishes
	b.add("pub1", tr, api, core.KindQueuePub, "orders.fulfil", "api", 125, 130, chain(
		req(m{"order_id": "ord_1001", "items": []m{{"sku": "SKU-APPLE-1", "qty": 2}, {"sku": "SKU-MILK-2", "qty": 1}}, "priority": "standard", "max_attempts": 3}),
		resp(m{"message_id": "msg_synth_0001"}), attrs("queue", "orders.fulfil", "broker", "sqs")))
	b.add("pub2", tr, api, core.KindQueuePub, "orders.notify", "api", 131, 135, chain(
		func(e *core.Event) {
			e.Req = pjRedacted(m{"order_id": "ord_1001", "channel": "email", "template": "order_confirmation", "to": "[REDACTED]"})
		},
		resp(m{"message_id": "msg_synth_0002"}), attrs("queue", "orders.notify", "broker", "sqs"), flags("redacted")))

	// fulfil worker: attempt 1 fails (retry), attempt 2 succeeds
	b.add("w1a", tr, wk1, core.KindWorkerStart, "orders.fulfil", "", 200, 262, chain(link("pub1"), attempt(1), errs("inventory lock timeout")))
	b.add("w1a_db", tr, wk1, core.KindDBRead, "SELECT * FROM inventory WHERE sku = $1 FOR UPDATE NOWAIT", "w1a", 205, 255, chain(
		errs("ERROR: could not obtain lock on row in relation \"inventory\" (SQLSTATE 55P03)")))
	b.add("w1a_end", tr, wk1, core.KindWorkerEnd, "orders.fulfil", "w1a", 260, 261, chain(attempt(1), errs("inventory lock timeout")))
	b.add("w1a_retry", tr, wk1, core.KindWorkerRetry, "orders.fulfil", "w1a", 261, 262, chain(attempt(1), attrs("backoff_ms", "500", "next_attempt", "2")))
	b.add("w1b", tr, wk1, core.KindWorkerStart, "orders.fulfil", "", 800, 930, chain(link("pub1"), attempt(2)))
	b.add("w1b_db", tr, wk1, core.KindDBWrite, "UPDATE orders SET status = $1 WHERE id = $2", "w1b", 810, 825, chain(
		req(m{"args": []any{"fulfilling", "ord_1001"}}),
		mut(core.Mutation{Store: "postgres", Entity: "orders", PK: m{"id": "ord_1001"}, Op: "update", Source: "sdk",
			Before: pj(order), After: pj(m{"id": "ord_1001", "customer_id": "cus_synth_001", "status": "fulfilling", "total_cents": 1895}),
			Diff: []core.FieldChange{{Field: "status", Old: "pending", New: "fulfilling"}}})))
	b.add("w1b_api", tr, wk1, core.KindAPIOut, "POST https://shipping.example.invalid/v1/labels", "w1b", 830, 910, chain(
		req(m{"order_id": "ord_1001", "service": "ground"}), resp(m{"status": 200, "body": m{"label_id": "lbl_synth_77"}}),
		attrs("http.status", "200")))
	b.add("w1b_end", tr, wk1, core.KindWorkerEnd, "orders.fulfil", "w1b", 925, 928, chain(link("pub1"), attempt(2), resp(m{"result": "ok"})))

	// notify worker: both attempts fail -> DLQ
	b.add("w2a", tr, wk2, core.KindWorkerStart, "orders.notify", "", 220, 301, chain(link("pub2"), attempt(1), errs("mail provider unavailable")))
	b.add("w2a_api", tr, wk2, core.KindAPIOut, "POST https://mail.example.invalid/v1/send", "w2a", 225, 295, chain(
		req(m{"template": "order_confirmation", "to": "[REDACTED]"}), resp(m{"status": 503}), errs("503 Service Unavailable"), attrs("http.status", "503")))
	b.add("w2a_end", tr, wk2, core.KindWorkerEnd, "orders.notify", "w2a", 298, 299, chain(attempt(1), errs("mail provider unavailable")))
	b.add("w2a_retry", tr, wk2, core.KindWorkerRetry, "orders.notify", "w2a", 299, 300, chain(attempt(1), attrs("backoff_ms", "700", "next_attempt", "2")))
	b.add("w2b", tr, wk2, core.KindWorkerStart, "orders.notify", "", 1000, 1090, chain(link("pub2"), attempt(2), errs("mail provider unavailable")))
	b.add("w2b_api", tr, wk2, core.KindAPIOut, "POST https://mail.example.invalid/v1/send", "w2b", 1005, 1080, chain(
		req(m{"template": "order_confirmation", "to": "[REDACTED]"}), resp(m{"status": 503}), errs("503 Service Unavailable"), attrs("http.status", "503")))
	b.add("w2b_end", tr, wk2, core.KindWorkerEnd, "orders.notify", "w2b", 1085, 1086, chain(link("pub2"), attempt(2), errs("mail provider unavailable")))
	b.add("w2b_dlq", tr, wk2, core.KindWorkerDLQ, "orders.notify", "w2b", 1086, 1087, chain(link("pub2"),
		attempt(2), attrs("dlq", "orders.notify.dlq", "reason", "max attempts exceeded")))

	evs := b.evs
	sort.SliceStable(evs, func(i, j int) bool {
		a, c := evs[i], evs[j]
		if a.EndHLC != c.EndHLC {
			return a.EndHLC < c.EndHLC
		}
		if a.Service != c.Service {
			return a.Service < c.Service
		}
		return a.LocalSeq < c.LocalSeq
	})
	for i := range evs {
		evs[i].Seq = uint64(i + 1)
	}
	return evs
}

// resolveTx returns a deep-enough copy of evs with Mutation.Effective set from
// commit/rollback events (no TxID => effective; unresolved tx => nil).
func resolveTx(evs []core.Event) []core.Event {
	out := make([]core.Event, len(evs))
	res := map[string]bool{}
	for _, e := range evs {
		switch e.Kind {
		case core.KindDBTxCommit:
			res[e.TxID] = true
		case core.KindDBTxRollbck:
			res[e.TxID] = false
		}
	}
	for i, e := range evs {
		e.Mutations = append([]core.Mutation(nil), e.Mutations...)
		for j := range e.Mutations {
			var eff *bool
			if e.TxID == "" {
				t := true
				eff = &t
			} else if v, ok := res[e.TxID]; ok {
				eff = &v
			}
			e.Mutations[j].Effective = eff
		}
		out[i] = e
	}
	return out
}

// Node is one entry of the /trace tree.
type Node struct {
	Event    core.Event `json:"event"`
	Children []*Node    `json:"children"`
	Linked   []*Node    `json:"linked"`
}

// BuildTrace builds the span tree: ParentID nesting, LinkIDs for async links.
// Events whose parent is absent become roots (or linked, if a link resolves).
func BuildTrace(evs []core.Event) []*Node {
	nodes := make(map[string]*Node, len(evs))
	for _, e := range evs {
		nodes[e.SpanID] = &Node{Event: e, Children: []*Node{}, Linked: []*Node{}}
	}
	roots := []*Node{}
	for _, e := range evs {
		n := nodes[e.SpanID]
		if p, ok := nodes[e.ParentID]; ok && e.ParentID != "" && p != n {
			p.Children = append(p.Children, n)
			continue
		}
		placed := false
		for _, l := range e.LinkIDs {
			if p, ok := nodes[l]; ok && p != n {
				p.Linked = append(p.Linked, n)
				placed = true
				break
			}
		}
		if !placed {
			roots = append(roots, n)
		}
	}
	less := func(s []*Node) {
		sort.SliceStable(s, func(i, j int) bool {
			a, b := s[i].Event, s[j].Event
			if a.StartNs != b.StartNs {
				return a.StartNs < b.StartNs
			}
			return a.Seq < b.Seq
		})
	}
	less(roots)
	for _, n := range nodes {
		less(n.Children)
		less(n.Linked)
	}
	return roots
}

func pkString(pk map[string]any) string {
	if len(pk) == 1 {
		for _, v := range pk {
			return fmt.Sprint(v)
		}
	}
	keys := make([]string, 0, len(pk))
	for k := range pk {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	s := ""
	for i, k := range keys {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%s=%v", k, pk[k])
	}
	return s
}

// EntityRef identifies a mutated entity instance.
type EntityRef struct {
	Store         string `json:"store"`
	Entity        string `json:"entity"`
	PK            string `json:"pk"`
	MutationCount int    `json:"mutation_count"`
}

// EntityMutation is one history row for an entity.
type EntityMutation struct {
	Seq      uint64             `json:"seq"`
	SpanID   string             `json:"span_id"`
	Service  string             `json:"service"`
	TimeNs   int64              `json:"time_ns"`
	TxID     string             `json:"tx_id,omitempty"`
	Mutation core.Mutation      `json:"mutation"`
	Op       string             `json:"op"`
	Effect   *bool              `json:"effective,omitempty"`
	Before   core.PayloadRef    `json:"before,omitempty"`
	After    core.PayloadRef    `json:"after,omitempty"`
	Diff     []core.FieldChange `json:"diff,omitempty"`
}

func entityList(evs []core.Event) []EntityRef {
	idx := map[[3]string]*EntityRef{}
	var order [][3]string
	for _, e := range evs {
		for _, mu := range e.Mutations {
			k := [3]string{mu.Store, mu.Entity, pkString(mu.PK)}
			if idx[k] == nil {
				idx[k] = &EntityRef{Store: k[0], Entity: k[1], PK: k[2]}
				order = append(order, k)
			}
			idx[k].MutationCount++
		}
	}
	out := make([]EntityRef, 0, len(order))
	for _, k := range order {
		out = append(out, *idx[k])
	}
	return out
}

func entityHistory(evs []core.Event, store, entity, pk string) []EntityMutation {
	out := []EntityMutation{}
	for _, e := range evs {
		for _, mu := range e.Mutations {
			if mu.Store == store && mu.Entity == entity && pkString(mu.PK) == pk {
				out = append(out, EntityMutation{Seq: e.Seq, SpanID: e.SpanID, Service: e.Service, TimeNs: e.EndNs,
					TxID: e.TxID, Mutation: mu, Op: mu.Op, Effect: mu.Effective, Before: mu.Before, After: mu.After, Diff: mu.Diff})
			}
		}
	}
	return out
}

func summarize(evs []core.Event, sess Session) map[string]any {
	byKind, bySvc := map[string]int{}, map[string]int{}
	type ek struct{ Store, Entity string }
	muts := map[ek]int{}
	failures := []uint64{}
	closed := map[string]bool{}
	flagc := map[string]int{}
	var minS, maxE int64
	dropped := sess.DroppedEvents
	for i, e := range evs {
		byKind[string(e.Kind)]++
		bySvc[e.Service]++
		if e.Err != "" {
			failures = append(failures, e.Seq)
		}
		for _, mu := range e.Mutations {
			muts[ek{mu.Store, mu.Entity}]++
		}
		for _, f := range e.Flags {
			flagc[f]++
		}
		if e.Kind == core.KindWorkerEnd {
			for _, l := range e.LinkIDs {
				closed[l] = true
			}
		}
		if i == 0 || e.StartNs < minS {
			minS = e.StartNs
		}
		if e.EndNs > maxE {
			maxE = e.EndNs
		}
	}
	pending := []string{}
	for _, e := range evs {
		if e.Kind == core.KindQueuePub && !closed[e.SpanID] {
			pending = append(pending, e.SpanID)
		}
	}
	ml := []m{}
	for k, c := range muts {
		ml = append(ml, m{"store": k.Store, "entity": k.Entity, "count": c})
	}
	sort.Slice(ml, func(i, j int) bool {
		return fmt.Sprint(ml[i]["store"], ml[i]["entity"]) < fmt.Sprint(ml[j]["store"], ml[j]["entity"])
	})
	return m{"session_id": sess.ID, "event_count": len(evs), "duration_ns": maxE - minS,
		"by_kind": byKind, "by_service": bySvc, "mutations": ml, "failures": failures,
		"pending_async": pending, "dropped_events": dropped, "flags": flagc}
}
