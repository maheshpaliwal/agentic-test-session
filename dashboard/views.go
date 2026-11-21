package dashboard

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The collector's read views, for the sessions the dashboard serves itself
// (demo mode and exported files).
//
// The collector is a module of its own that depends on this one (and on the
// store drivers), so the dashboard cannot import it. What follows is a port of
// collector/views.go, of resolve in collector/collector.go and of the ledger
// in collector/actor.go. The response types are those of sdk/client, which the
// integration module already pins to the collector's
// (TestClientTypesMatchCollector); the logic is pinned by
// integration/parity_test.go, which serves one session from the collector,
// from an export of it and from the demo, and compares every answer. Change
// the collector's views and that test says what to change here.

// buildTrace nests events by ParentID; a span whose LinkIDs name a present
// span hangs under that span's Linked. evs must be in Seq order.
func buildTrace(sid string, evs []core.Event) client.Trace {
	nodes := make(map[string]*client.TraceNode, len(evs))
	order := make([]*client.TraceNode, 0, len(evs))
	for i := range evs {
		n := &client.TraceNode{Event: evs[i], Children: []*client.TraceNode{}, Linked: []*client.TraceNode{}}
		order = append(order, n)
		if evs[i].SpanID != "" {
			if _, dup := nodes[evs[i].SpanID]; !dup {
				nodes[evs[i].SpanID] = n
			}
		}
	}
	t := client.Trace{SessionID: sid, Roots: []*client.TraceNode{}}
	for _, n := range order {
		e := &n.Event
		parentIsLink := slices.Contains(e.LinkIDs, e.ParentID)
		if p := nodes[e.ParentID]; p != nil && p != n && !parentIsLink {
			p.Children = append(p.Children, n)
			continue
		}
		attached := false
		for _, l := range e.LinkIDs {
			if p := nodes[l]; p != nil && p != n {
				p.Linked = append(p.Linked, n)
				attached = true
				break
			}
		}
		if !attached {
			t.Roots = append(t.Roots, n)
		}
	}
	return t
}

// pkJSON is the canonical form of a primary key: JSON with sorted keys, "{}"
// when the key is not known.
func pkJSON(pk map[string]any) string {
	if len(pk) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(pk) // map keys are emitted sorted
	return string(b)
}

// pkMatches accepts the canonical JSON form or, for a single-column key, the
// bare value ("42" for {"id":42}).
func pkMatches(pk map[string]any, want string) bool {
	if pkJSON(pk) == want {
		return true
	}
	if len(pk) == 1 {
		for _, v := range pk {
			if fmt.Sprint(v) == want {
				return true
			}
			// A number decoded from JSON is a float64, which prints as 1e+06
			// from a million on: accept the plain digits as well.
			f, ok := v.(float64)
			return ok && strconv.FormatFloat(f, 'f', -1, 64) == want
		}
	}
	return false
}

// listEntities returns every entity mutated in evs with its mutation count,
// sorted by store, entity and primary key.
func listEntities(evs []core.Event) []client.Entity {
	type k struct{ s, e, p string }
	type n struct{ all, uncertain int }
	cnt := map[k]n{}
	for i := range evs {
		uncertain := 0
		if core.HasUncertainFlag(evs[i].Flags) {
			uncertain = 1
		}
		for _, m := range evs[i].Mutations {
			key := k{m.Store, m.Entity, pkJSON(m.PK)}
			c := cnt[key]
			cnt[key] = n{c.all + 1, c.uncertain + uncertain}
		}
	}
	out := make([]client.Entity, 0, len(cnt))
	for key, c := range cnt {
		out = append(out, client.Entity{Store: key.s, Entity: key.e, PK: key.p, MutationCount: c.all, Uncertain: c.uncertain})
	}
	slices.SortFunc(out, func(a, b client.Entity) int {
		return cmp.Or(cmp.Compare(a.Store, b.Store), cmp.Compare(a.Entity, b.Entity), cmp.Compare(a.PK, b.PK))
	})
	return out
}

// entityHistory returns the mutations of one entity in evs order. pk is the
// canonical JSON key or, for a single-column key, the bare value.
func entityHistory(evs []core.Event, store, entity, pk string) client.EntityHistory {
	h := client.EntityHistory{Store: store, Entity: entity, PK: pk, Mutations: []client.EntityMutation{}}
	for i := range evs {
		e := &evs[i]
		for _, m := range e.Mutations {
			if m.Store != store || m.Entity != entity || !pkMatches(m.PK, pk) {
				continue
			}
			h.PK = pkJSON(m.PK)
			h.Mutations = append(h.Mutations, client.EntityMutation{
				Seq: e.Seq, SpanID: e.SpanID, Service: e.Service, Kind: e.Kind, TxID: e.TxID, EndNs: e.EndNs,
				Op: m.Op, Before: m.Before, After: m.After, Diff: m.Diff, Source: m.Source, Effective: m.Effective,
				Flags: e.Flags,
			})
		}
	}
	return h
}

// buildSummary computes the part of the summary that the events alone give.
func buildSummary(id string, evs []core.Event) client.Summary {
	s := client.Summary{SessionID: id, EventCount: len(evs), ByKind: map[string]int{}, ByService: map[string]int{},
		Mutations: map[string]map[string]int{}, Failures: []client.Failure{}, PendingIDs: []string{}, Flags: map[string]int{}}
	var minS, maxE int64
	for i := range evs {
		e := &evs[i]
		s.ByKind[string(e.Kind)]++
		s.ByService[e.Service]++
		for _, m := range e.Mutations {
			if s.Mutations[m.Store] == nil {
				s.Mutations[m.Store] = map[string]int{}
			}
			s.Mutations[m.Store][m.Entity]++
		}
		if e.Err != "" {
			s.Failures = append(s.Failures, client.Failure{Seq: e.Seq, SpanID: e.SpanID, Service: e.Service, Kind: e.Kind, Name: e.Name, Err: e.Err})
		}
		for _, f := range e.Flags {
			s.Flags[f]++
		}
		if e.StartNs > 0 && (minS == 0 || e.StartNs < minS) {
			minS = e.StartNs
		}
		maxE = max(maxE, e.EndNs)
	}
	if maxE > minS && minS > 0 {
		s.DurationNs = maxE - minS
	}
	return s
}

// ledger is what the collector keeps per session besides the events: the
// published jobs no worker has finished, and how many events the SDKs
// reported as dropped. evs must be in Seq order.
func ledger(evs []core.Event) (open []string, dropped int64) {
	pending, closed := map[string]struct{}{}, map[string]struct{}{}
	for i := range evs {
		e := &evs[i]
		switch e.Kind {
		case core.KindQueuePub:
			if _, ok := closed[e.SpanID]; ok {
				delete(closed, e.SpanID)
			} else {
				pending[e.SpanID] = struct{}{}
			}
		case core.KindWorkerEnd, core.KindWorkerDLQ:
			// A failed attempt with a retry scheduled only records the attempt.
			if e.Kind == core.KindWorkerEnd && e.Attrs["retry_pending"] == "true" {
				break
			}
			for _, l := range e.LinkIDs {
				if _, ok := pending[l]; ok {
					delete(pending, l)
				} else {
					closed[l] = struct{}{}
				}
			}
		case core.KindSDKDropped:
			var n int64
			for _, c := range e.Attrs["count"] {
				if c < '0' || c > '9' {
					n = 0
					break
				}
				n = n*10 + int64(c-'0')
			}
			dropped += n
		}
	}
	return mapx.SortedKeys(pending), dropped
}

// retryPending returns the orphaned publisher ids that saw a worker attempt
// end with retry_pending (or a worker.retry event) but never a final outcome.
func retryPending(evs []core.Event, orphaned map[string]bool) map[string]bool {
	out := map[string]bool{}
	if len(orphaned) == 0 {
		return out
	}
	for i := range evs {
		e := &evs[i]
		if e.Kind == core.KindWorkerRetry || (e.Kind == core.KindWorkerEnd && e.Attrs["retry_pending"] == "true") {
			for _, l := range e.LinkIDs {
				if orphaned[l] {
					out[l] = true
				}
			}
		}
	}
	return out
}

// txOutcomes reads what each transaction of evs came to. A negative outcome
// is sticky: a transaction that was ever rolled back or failed to commit
// stays ineffective whatever the order of arrival.
func txOutcomes(evs []core.Event) map[string]bool {
	out := map[string]bool{}
	for i := range evs {
		e := &evs[i]
		if e.TxID == "" {
			continue
		}
		var effective bool
		switch e.Kind {
		case core.KindDBTxCommit:
			effective = e.Err == "" && e.Attrs["commit_failed"] != "true"
		case core.KindDBTxRollbck:
		default:
			continue
		}
		if prev, ok := out[e.TxID]; ok && !prev {
			continue
		}
		out[e.TxID] = effective
	}
	return out
}

// resolve returns copies of evs as the collector serves stored events:
// Mutation.Effective filled in from the transaction outcomes and orphaned
// publishers flagged. With final set (a sealed session), transactions that
// never finished count as not effective.
func resolve(evs []core.Event, final bool, orphans map[string]bool) []core.Event {
	outcomes := txOutcomes(evs)
	retry := retryPending(evs, orphans)
	out := make([]core.Event, len(evs))
	copy(out, evs)
	for i := range out {
		e := &out[i]
		if orphans[e.SpanID] && e.Kind == core.KindQueuePub {
			e.Flags = append(append([]string(nil), e.Flags...), "orphaned")
			if retry[e.SpanID] {
				e.Flags = append(e.Flags, "retry_pending")
			}
		}
		if len(e.Mutations) == 0 {
			continue
		}
		ms := make([]core.Mutation, len(e.Mutations))
		copy(ms, e.Mutations)
		unresolved := false
		for j := range ms {
			if ms[j].Effective != nil {
				continue
			}
			yes, no := true, false
			if e.TxID == "" {
				ms[j].Effective = &yes
			} else if ok, known := outcomes[e.TxID]; known {
				ms[j].Effective = &ok
			} else if final {
				ms[j].Effective = &no
				unresolved = true
			}
		}
		e.Mutations = ms
		if unresolved {
			e.Flags = append(append([]string(nil), e.Flags...), "tx_unresolved")
		}
	}
	return out
}

// colView is what the collector knows of one session: its events as served,
// and its seal.
type colView struct {
	id     string
	evs    []core.Event // resolved, in Seq order
	sealed bool
	// incomplete is the seal's verdict: jobs left open, events dropped by an
	// SDK or (for a file) a recording that was cut short.
	incomplete bool
	pending    []string // published jobs no worker finished; orphaned once sealed
	dropped    int64
}

// newColView resolves raw events the way the collector does on a read: when
// sealed, the jobs still open are orphaned and open transactions are final.
func newColView(id string, raw []core.Event, sealed bool) colView {
	open, dropped := ledger(raw)
	v := colView{id: id, sealed: sealed, pending: open, dropped: dropped}
	if sealed {
		orph := make(map[string]bool, len(open))
		for _, o := range open {
			orph[o] = true
		}
		v.evs = resolve(raw, true, orph)
		v.incomplete = len(open) > 0 || dropped > 0
	} else {
		v.evs = resolve(raw, false, nil)
	}
	return v
}

func (v colView) summary() client.Summary {
	s := buildSummary(v.id, v.evs)
	s.Sealed, s.Incomplete = v.sealed, v.sealed && v.incomplete
	if v.pending != nil {
		s.PendingIDs = v.pending
	}
	s.PendingAsync = len(s.PendingIDs)
	s.DroppedEvents = v.dropped
	return s
}

// The limits of GET .../events, as the collector has them.
const (
	defaultEventPage = 1000
	maxEventPage     = 10000
)

// page is GET .../events?from_seq=&limit=: up to limit events with
// Seq >= from (inclusive, at least 1); next_seq is the from_seq of the page
// after it.
func (v colView) page(fromSeq string, limitParam string) client.EventPage {
	from, _ := strconv.ParseUint(fromSeq, 10, 64)
	from = max(from, 1)
	limit := defaultEventPage
	if n, err := strconv.Atoi(limitParam); err == nil && n > 0 {
		limit = min(n, maxEventPage)
	}
	p := client.EventPage{Events: []core.Event{}, NextSeq: from, Sealed: v.sealed}
	for i := range v.evs {
		if v.evs[i].Seq < from {
			continue
		}
		if len(p.Events) == limit {
			break
		}
		p.Events = append(p.Events, v.evs[i])
		p.NextSeq = v.evs[i].Seq + 1
	}
	return p
}

// sessionInfo is one element of the collector's GET /v1/sessions.
type sessionInfo struct {
	ID         string `json:"id"`
	EventCount int    `json:"event_count"`
	FirstNs    int64  `json:"first_ns"`
	LastNs     int64  `json:"last_ns"`
	Sealed     bool   `json:"sealed"`
}

// sessionCount is one element of GET /v1/sessions?ids=, which leaves the
// times out (docs/PROTOCOL.md).
type sessionCount struct {
	ID         string `json:"id"`
	EventCount int    `json:"event_count"`
	Sealed     bool   `json:"sealed"`
}

func (v colView) info() sessionInfo {
	si := sessionInfo{ID: v.id, EventCount: len(v.evs), Sealed: v.sealed}
	for i := range v.evs {
		if i == 0 || v.evs[i].StartNs < si.FirstNs {
			si.FirstNs = v.evs[i].StartNs
		}
		si.LastNs = max(si.LastNs, v.evs[i].EndNs)
	}
	return si
}
