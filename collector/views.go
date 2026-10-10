package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"tsession/core"
)

// Node is one span in the trace tree.
type Node struct {
	Event    core.Event `json:"event"`
	Children []*Node    `json:"children"`
	Linked   []*Node    `json:"linked"`
}

// Trace is the GET .../trace response.
type Trace struct {
	SessionID string  `json:"session_id"`
	Roots     []*Node `json:"roots"`
}

// BuildTrace nests events by ParentID; spans whose LinkIDs name a present
// span (and whose parent is not that span's link target chain) hang under
// that span's `linked`. evs must be in Seq order.
func BuildTrace(sid string, evs []core.Event) Trace {
	nodes := make(map[string]*Node, len(evs))
	order := make([]*Node, 0, len(evs))
	for i := range evs {
		n := &Node{Event: evs[i], Children: []*Node{}, Linked: []*Node{}}
		order = append(order, n)
		if evs[i].SpanID != "" {
			if _, dup := nodes[evs[i].SpanID]; !dup {
				nodes[evs[i].SpanID] = n
			}
		}
	}
	t := Trace{SessionID: sid, Roots: []*Node{}}
	for _, n := range order {
		e := &n.Event
		parentIsLink := false
		for _, l := range e.LinkIDs {
			if l == e.ParentID {
				parentIsLink = true
			}
		}
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

// Entity is one row of the entities list.
type Entity struct {
	Store         string `json:"store"`
	Entity        string `json:"entity"`
	PK            string `json:"pk"`
	MutationCount int    `json:"mutation_count"`
}

// EntityMutation is one entry of an entity's history.
type EntityMutation struct {
	Seq       uint64             `json:"seq"`
	SpanID    string             `json:"span_id"`
	Service   string             `json:"service"`
	Kind      core.Kind          `json:"kind"`
	TxID      string             `json:"tx_id,omitempty"`
	EndNs     int64              `json:"end_ns"`
	Op        string             `json:"op"`
	Before    core.PayloadRef    `json:"before,omitempty"`
	After     core.PayloadRef    `json:"after,omitempty"`
	Diff      []core.FieldChange `json:"diff,omitempty"`
	Source    string             `json:"source,omitempty"`
	Effective *bool              `json:"effective,omitempty"`
}

// EntityHistory is the history response.
type EntityHistory struct {
	Store     string           `json:"store"`
	Entity    string           `json:"entity"`
	PK        string           `json:"pk"`
	Mutations []EntityMutation `json:"mutations"`
}

func pkString(pk map[string]any) string {
	if len(pk) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(pk) // map keys are emitted sorted
	return string(b)
}

// pkMatches accepts the canonical JSON form or, for single-column keys,
// the bare value (e.g. "42" for {"id":42}).
func pkMatches(pk map[string]any, want string) bool {
	if pkString(pk) == want {
		return true
	}
	if len(pk) == 1 {
		for _, v := range pk {
			return fmt.Sprint(v) == want
		}
	}
	return false
}

func ListEntities(evs []core.Event) []Entity {
	type k struct{ s, e, p string }
	cnt := map[k]int{}
	for i := range evs {
		for _, m := range evs[i].Mutations {
			cnt[k{m.Store, m.Entity, pkString(m.PK)}]++
		}
	}
	out := make([]Entity, 0, len(cnt))
	for key, n := range cnt {
		out = append(out, Entity{key.s, key.e, key.p, n})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Store != b.Store {
			return a.Store < b.Store
		}
		if a.Entity != b.Entity {
			return a.Entity < b.Entity
		}
		return a.PK < b.PK
	})
	return out
}

func History(evs []core.Event, store, entity, pk string) EntityHistory {
	h := EntityHistory{Store: store, Entity: entity, PK: pk, Mutations: []EntityMutation{}}
	for i := range evs {
		e := &evs[i]
		for _, m := range e.Mutations {
			if m.Store != store || m.Entity != entity || !pkMatches(m.PK, pk) {
				continue
			}
			h.PK = pkString(m.PK)
			h.Mutations = append(h.Mutations, EntityMutation{
				Seq: e.Seq, SpanID: e.SpanID, Service: e.Service, Kind: e.Kind, TxID: e.TxID, EndNs: e.EndNs,
				Op: m.Op, Before: m.Before, After: m.After, Diff: m.Diff, Source: m.Source, Effective: m.Effective,
			})
		}
	}
	return h
}

// Failure is one failed event in the summary.
type Failure struct {
	Seq     uint64    `json:"seq"`
	SpanID  string    `json:"span_id"`
	Service string    `json:"service"`
	Kind    core.Kind `json:"kind"`
	Name    string    `json:"name,omitempty"`
	Err     string    `json:"err"`
}

// Summary is the GET .../summary response.
type Summary struct {
	SessionID     string                    `json:"session_id"`
	EventCount    int                       `json:"event_count"`
	ByKind        map[string]int            `json:"by_kind"`
	ByService     map[string]int            `json:"by_service"`
	Mutations     map[string]map[string]int `json:"mutations"` // store -> entity -> count
	Failures      []Failure                 `json:"failures"`
	PendingAsync  int                       `json:"pending_async"`
	PendingIDs    []string                  `json:"pending_async_ids"`
	DroppedEvents int64                     `json:"dropped_events"`
	Flags         map[string]int            `json:"flags"`
	DurationNs    int64                     `json:"duration_ns"`
	Sealed        bool                      `json:"sealed"`
	Incomplete    bool                      `json:"incomplete"`
}

func (c *Collector) summary(ctx context.Context, id string, evs []core.Event) (Summary, error) {
	s := Summary{SessionID: id, EventCount: len(evs), ByKind: map[string]int{}, ByService: map[string]int{},
		Mutations: map[string]map[string]int{}, Failures: []Failure{}, PendingIDs: []string{}, Flags: map[string]int{}}
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
			s.Failures = append(s.Failures, Failure{e.Seq, e.SpanID, e.Service, e.Kind, e.Name, e.Err})
		}
		for _, f := range e.Flags {
			s.Flags[f]++
		}
		if e.StartNs > 0 && (minS == 0 || e.StartNs < minS) {
			minS = e.StartNs
		}
		if e.EndNs > maxE {
			maxE = e.EndNs
		}
	}
	if maxE > minS && minS > 0 {
		s.DurationNs = maxE - minS
	}
	si, err := c.st.GetSeal(ctx, id)
	if err != nil {
		return s, err
	}
	if si != nil {
		s.Sealed, s.Incomplete = true, si.Incomplete
		s.PendingIDs = nonNil(si.Orphaned)
	}
	st, err := c.seq.Status(ctx, id)
	if err != nil {
		return s, err
	}
	s.DroppedEvents = st.Dropped
	if si == nil {
		s.PendingIDs = nonNil(st.Open)
	}
	s.PendingAsync = len(s.PendingIDs)
	return s, nil
}
