// Package collector ingests SDK events, sequences them per session, and
// serves the read API described in docs/PROTOCOL.md.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"tsession/collector/store"
	"tsession/core"
)

// Config tunes a Collector. Window 0 means "sequence immediately".
type Config struct {
	Window       time.Duration
	BundleDir    string // empty disables bundle writing
	MaxBodyBytes int64  // per ingest request
	MaxLineBytes int    // per NDJSON line
	Logger       *slog.Logger
}

// DefaultConfig returns the protocol defaults.
func DefaultConfig() Config {
	return Config{Window: 200 * time.Millisecond, MaxBodyBytes: 16 << 20, MaxLineBytes: 1 << 20}
}

// Collector wires the store, sequencer and HTTP handlers together.
type Collector struct {
	cfg Config
	st  store.Store
	seq *Sequencer
	log *slog.Logger
}

// New builds a Collector over st.
func New(st store.Store, cfg Config) *Collector {
	d := DefaultConfig()
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = d.MaxBodyBytes
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = d.MaxLineBytes
	}
	if cfg.Window < 0 {
		cfg.Window = 0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Collector{cfg: cfg, st: st, seq: newSequencer(st, cfg.Window), log: cfg.Logger}
}

// Close stops the sequencer goroutines (buffered events are not flushed;
// seal first if they matter).
func (c *Collector) Close() { c.seq.Close() }

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$`)

// IngestResult is the ingest response body.
type IngestResult struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
	Rejected   int `json:"rejected,omitempty"`
}

// Ingest groups events by session and hands them to the sequencer.
func (c *Collector) Ingest(ctx context.Context, instance string, evs []core.Event) (IngestResult, error) {
	var res IngestResult
	by := map[string][]core.Event{}
	for _, e := range evs {
		if !validID.MatchString(e.SessionID) {
			res.Rejected++
			continue
		}
		if e.Instance == "" {
			e.Instance = instance
		}
		by[e.SessionID] = append(by[e.SessionID], e)
	}
	ids := make([]string, 0, len(by))
	for id := range by {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var firstErr error
	for _, id := range ids {
		a, d, err := c.seq.Add(ctx, id, by[id])
		res.Accepted += a
		res.Duplicates += d
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return res, firstErr
}

// DrainStatus is the control plane's view of a draining session.
type DrainStatus struct {
	Pending         int   `json:"pending"`
	LastEventUnixNs int64 `json:"last_event_unix_ns"`
	EventCount      int   `json:"event_count"`
	DroppedEvents   int64 `json:"dropped_events"`
}

func (c *Collector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	st, err := c.seq.Status(ctx, id)
	if err != nil {
		return DrainStatus{}, err
	}
	return DrainStatus{Pending: st.Pending, LastEventUnixNs: st.LastEventNs, EventCount: st.EventCount, DroppedEvents: st.Dropped}, nil
}

// SealResult is the seal response body.
type SealResult struct {
	Incomplete bool     `json:"incomplete"`
	Orphaned   []string `json:"orphaned"`
	BundleURI  string   `json:"bundle_uri"`
}

var errBadID = errors.New("invalid session id")

// Seal flushes the reorder window, resolves transactions, marks orphaned
// async work and writes the bundle. It is idempotent.
func (c *Collector) Seal(ctx context.Context, id string) (SealResult, error) {
	if !validID.MatchString(id) {
		return SealResult{}, errBadID
	}
	if si, err := c.st.GetSeal(ctx, id); err != nil {
		return SealResult{}, err
	} else if si != nil {
		return SealResult{Incomplete: si.Incomplete, Orphaned: nonNil(si.Orphaned), BundleURI: si.BundleURI}, nil
	}
	st, err := c.seq.Flush(ctx, id)
	if err != nil {
		return SealResult{}, err
	}
	evs, err := c.st.Events(ctx, id, 0, 0)
	if err != nil {
		return SealResult{}, err
	}
	si := store.SealInfo{
		Orphaned:   nonNil(st.Open),
		Incomplete: len(st.Open) > 0 || st.Dropped > 0,
		SealedAt:   time.Now().UTC(),
	}
	res := resolve(evs, nil, true, setOf(si.Orphaned))
	if c.cfg.BundleDir != "" {
		uri, err := writeBundle(c.cfg.BundleDir, id, res, si)
		if err != nil {
			return SealResult{}, err
		}
		si.BundleURI = uri
	}
	if err := c.st.PutSeal(ctx, id, si); err != nil {
		return SealResult{}, err
	}
	return SealResult{Incomplete: si.Incomplete, Orphaned: si.Orphaned, BundleURI: si.BundleURI}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func setOf(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}

func ptr(b bool) *bool { return &b }

// resolve returns copies of evs with Mutation.Effective filled in from tx
// outcomes (derived from evs when outcomes is nil) and orphaned publishers
// flagged. With final set, transactions that never finished count as not
// effective.
func resolve(evs []core.Event, outcomes map[string]bool, final bool, orphans map[string]bool) []core.Event {
	if outcomes == nil {
		outcomes = map[string]bool{}
		for i := range evs {
			switch evs[i].Kind {
			case core.KindDBTxCommit:
				outcomes[evs[i].TxID] = true
			case core.KindDBTxRollbck:
				outcomes[evs[i].TxID] = false
			}
		}
	}
	out := make([]core.Event, len(evs))
	copy(out, evs)
	for i := range out {
		e := &out[i]
		if orphans[e.SpanID] && e.Kind == core.KindQueuePub {
			e.Flags = append(append([]string(nil), e.Flags...), "orphaned")
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
			if e.TxID == "" {
				ms[j].Effective = ptr(true)
			} else if ok, known := outcomes[e.TxID]; known {
				ms[j].Effective = ptr(ok)
			} else if final {
				ms[j].Effective = ptr(false)
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
