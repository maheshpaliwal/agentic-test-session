package client

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// The types below mirror the collector's read API (docs/PROTOCOL.md). They
// let a test assert on what a session did without parsing JSON by hand:
//
//	info, _ := sess.StopAndWait(ctx)
//	sum, _ := sess.Summary(ctx)
//	if len(sum.Failures) > 0 { t.Fatalf("session recorded failures: %+v", sum.Failures) }
//	h, _ := sess.History(ctx, "postgres", "orders", "42")

// Failure is one failed event of a session.
type Failure struct {
	Seq     uint64    `json:"seq"`
	SpanID  string    `json:"span_id"`
	Service string    `json:"service"`
	Kind    core.Kind `json:"kind"`
	Name    string    `json:"name,omitempty"`
	Err     string    `json:"err"`
}

// Summary is the collector's roll-up of a session.
type Summary struct {
	SessionID  string         `json:"session_id"`
	EventCount int            `json:"event_count"`
	ByKind     map[string]int `json:"by_kind"`
	ByService  map[string]int `json:"by_service"`
	// Mutations counts recorded mutations as store -> entity -> count.
	Mutations map[string]map[string]int `json:"mutations"`
	Failures  []Failure                 `json:"failures"`
	// PendingAsync is the number of published jobs no worker has finished
	// (PendingIDs are their publish span ids); after the seal these are the
	// orphaned ones.
	PendingAsync  int            `json:"pending_async"`
	PendingIDs    []string       `json:"pending_async_ids"`
	DroppedEvents int64          `json:"dropped_events"`
	Flags         map[string]int `json:"flags"`
	DurationNs    int64          `json:"duration_ns"`
	Sealed        bool           `json:"sealed"`
	Incomplete    bool           `json:"incomplete"`
	Quarantined   int            `json:"quarantined"`
}

// EventPage is one page of a session's events in Seq order.
type EventPage struct {
	Events []core.Event `json:"events"`
	// NextSeq is the from_seq of the following page.
	NextSeq uint64 `json:"next_seq"`
	Sealed  bool   `json:"sealed"`
}

// Entity identifies one row, document or key the session mutated.
type Entity struct {
	Store         string `json:"store"`
	Entity        string `json:"entity"`
	PK            string `json:"pk"`
	MutationCount int    `json:"mutation_count"`
}

// EntityMutation is one change to an entity.
type EntityMutation struct {
	Seq     uint64             `json:"seq"`
	SpanID  string             `json:"span_id"`
	Service string             `json:"service"`
	Kind    core.Kind          `json:"kind"`
	TxID    string             `json:"tx_id,omitempty"`
	EndNs   int64              `json:"end_ns"`
	Op      string             `json:"op"`
	Before  core.PayloadRef    `json:"before"`
	After   core.PayloadRef    `json:"after"`
	Diff    []core.FieldChange `json:"diff,omitempty"`
	Source  string             `json:"source,omitempty"`
	// Effective is false for a write whose transaction rolled back, true for
	// one that took effect, and nil while the transaction is still open.
	Effective *bool `json:"effective,omitempty"`
}

// AfterUnavailable reports an update whose new values were not captured; see
// core.Mutation.AfterUnavailable. Its Diff is not to be read.
func (m EntityMutation) AfterUnavailable() bool {
	return core.AfterUnavailable(m.Op, m.Before, m.After)
}

// EntityHistory is every recorded change to one entity, in Seq order.
type EntityHistory struct {
	Store     string           `json:"store"`
	Entity    string           `json:"entity"`
	PK        string           `json:"pk"`
	Mutations []EntityMutation `json:"mutations"`
}

// TraceNode is one span of the call tree. Linked holds asynchronous work the
// span triggered (worker runs linked to a publish).
type TraceNode struct {
	Event    core.Event   `json:"event"`
	Children []*TraceNode `json:"children"`
	Linked   []*TraceNode `json:"linked"`
}

// Trace is the session's call tree.
type Trace struct {
	SessionID string       `json:"session_id"`
	Roots     []*TraceNode `json:"roots"`
}

// maxEventPage is the collector's page-size ceiling for /events.
const maxEventPage = 10000

func (s *Session) read(ctx context.Context, suffix string, out any) error {
	return s.call(ctx, s.col, http.MethodGet, "/v1/sessions/"+url.PathEscape(s.ID)+suffix, nil, out)
}

// Summary returns the collector's roll-up of the session.
func (s *Session) Summary(ctx context.Context) (Summary, error) {
	var out Summary
	return out, s.read(ctx, "/summary", &out)
}

// EventsPage returns up to limit events with Seq >= fromSeq (limit <= 0 uses
// the server default of 1000).
func (s *Session) EventsPage(ctx context.Context, fromSeq uint64, limit int) (EventPage, error) {
	q := url.Values{"from_seq": {strconv.FormatUint(fromSeq, 10)}}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out EventPage
	return out, s.read(ctx, "/events?"+q.Encode(), &out)
}

// Events returns every event recorded so far, in Seq order. Transaction
// outcomes are already applied to the mutations (see EntityMutation.Effective).
func (s *Session) Events(ctx context.Context) ([]core.Event, error) {
	var all []core.Event
	from := uint64(1)
	for {
		p, err := s.EventsPage(ctx, from, maxEventPage)
		if err != nil {
			return nil, err
		}
		all = append(all, p.Events...)
		if len(p.Events) < maxEventPage || p.NextSeq <= from {
			return all, nil
		}
		from = p.NextSeq
	}
}

// MutationFilter selects mutations. Empty fields match everything.
type MutationFilter struct {
	Store  string // "postgres", "redis", ...
	Entity string // table, collection or key pattern
	Op     string // "insert", "update", "delete", ...; compared case-insensitively
	// ExcludeRolledBack drops writes whose transaction rolled back.
	ExcludeRolledBack bool
}

func (f MutationFilter) match(m *core.Mutation) bool {
	return (f.Store == "" || f.Store == m.Store) && (f.Entity == "" || f.Entity == m.Entity) &&
		(f.Op == "" || strings.EqualFold(f.Op, m.Op)) &&
		!(f.ExcludeRolledBack && m.Effective != nil && !*m.Effective)
}

// Mutation is one recorded mutation together with the event that made it.
// Before reading Diff or After of an update, check AfterUnavailable: an update
// whose row could not be read back has neither.
type Mutation struct {
	Seq     uint64 // of the event
	Service string
	Kind    core.Kind
	TxID    string
	core.Mutation
}

// Mutations returns the session's mutations matching f, in Seq order. It is the flat form of
// Entities + History for assertions such as "exactly one orders row was inserted". Stop the session
// first (StopAndWait): transaction outcomes and asynchronous work are only complete after the seal.
func (s *Session) Mutations(ctx context.Context, f MutationFilter) ([]Mutation, error) {
	evs, err := s.Events(ctx)
	if err != nil {
		return nil, err
	}
	var out []Mutation
	for i := range evs {
		e := &evs[i]
		for j := range e.Mutations {
			if f.match(&e.Mutations[j]) {
				out = append(out, Mutation{Seq: e.Seq, Service: e.Service, Kind: e.Kind, TxID: e.TxID, Mutation: e.Mutations[j]})
			}
		}
	}
	return out, nil
}

// Export writes every event recorded so far to w, one JSON object per line
// in Seq order, and returns how many it wrote. This is the format
// `tracectl export` produces and `dashboard -open` reads. Events hold recorded
// payloads (redacted, but possibly still sensitive): treat the output accordingly.
func (s *Session) Export(ctx context.Context, w io.Writer) (int, error) {
	evs, err := s.Events(ctx)
	if err != nil {
		return 0, err
	}
	out := bufio.NewWriter(w)
	enc := json.NewEncoder(out)
	for i := range evs {
		if err := enc.Encode(&evs[i]); err != nil {
			return i, err
		}
	}
	return len(evs), out.Flush()
}

// Entities lists the entities the session mutated.
func (s *Session) Entities(ctx context.Context) ([]Entity, error) {
	var out []Entity
	return out, s.read(ctx, "/entities", &out)
}

// History returns the changes to one entity. pk is the canonical JSON form
// from Entities (`{"id":42}`) or, for a single-column key, the bare value
// ("42").
func (s *Session) History(ctx context.Context, store, entity, pk string) (EntityHistory, error) {
	var out EntityHistory
	suffix := "/entities/" + url.PathEscape(store) + "/" + url.PathEscape(entity) + "/" + url.PathEscape(pk)
	return out, s.read(ctx, suffix, &out)
}

// Trace returns the session's call tree.
func (s *Session) Trace(ctx context.Context) (Trace, error) {
	var out Trace
	return out, s.read(ctx, "/trace", &out)
}
