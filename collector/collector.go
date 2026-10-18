// Package collector ingests SDK events, sequences them per session, and
// serves the read API described in docs/PROTOCOL.md.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Config tunes a Collector. Window 0 means "sequence immediately".
type Config struct {
	Window       time.Duration
	BundleDir    string // empty disables bundle writing
	MaxBodyBytes int64  // per ingest request
	MaxLineBytes int    // per NDJSON line; longer lines are rejected individually
	// IdleEvict is how long a sealed, idle session's actor (goroutine plus
	// dedupe state) is kept before being evicted; negative disables eviction.
	IdleEvict time.Duration
	// MaxBufferedEvents caps events buffered per session while the store is
	// failing (ingest then answers 503).
	MaxBufferedEvents int
	// AllowedHosts are extra Host header values accepted besides loopback
	// (e.g. docker service names).
	AllowedHosts []string
	// AllowedOrigins are browser origins allowed to call the API.
	AllowedOrigins []string
	// InternalAllow lists CIDRs (or bare IPs) besides loopback that may call
	// /v1/internal/*.
	InternalAllow []string
	Logger        *slog.Logger
}

// DefaultConfig returns the protocol defaults.
//
// Window is 500ms: the SDK exporter flushes every 100ms and the batch then
// crosses the network, and a cross-service child span can reach the collector
// before its parent's batch (the parent ends later and so is exported
// later). Anything shorter regularly inverts parent/child Seq order.
func DefaultConfig() Config {
	return Config{Window: 500 * time.Millisecond, MaxBodyBytes: 32 << 20, MaxLineBytes: 4 << 20,
		IdleEvict: 5 * time.Minute, MaxBufferedEvents: 100000}
}

// Collector wires the store, sequencer and HTTP handlers together.
type Collector struct {
	cfg      Config
	st       store.Store
	seq      *Sequencer
	log      *slog.Logger
	internal []*net.IPNet

	vmu   sync.Mutex
	views map[string]*viewEntry
	vtick uint64
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
	if cfg.IdleEvict == 0 {
		cfg.IdleEvict = d.IdleEvict
	}
	if cfg.MaxBufferedEvents <= 0 {
		cfg.MaxBufferedEvents = d.MaxBufferedEvents
	}
	if cfg.Window < 0 {
		cfg.Window = 0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Collector{cfg: cfg, st: st, log: cfg.Logger, views: map[string]*viewEntry{}}
	c.seq = newSequencer(st, cfg.Window, cfg.IdleEvict, cfg.MaxBufferedEvents)
	for _, s := range cfg.InternalAllow {
		n, err := ParseCIDR(s)
		if err != nil {
			c.log.Error("ignoring invalid internal-allow entry", "entry", s, "err", err)
			continue
		}
		c.internal = append(c.internal, n)
	}
	return c
}

// ParseCIDR parses a CIDR or a bare IP (treated as /32 or /128).
func ParseCIDR(s string) (*net.IPNet, error) {
	s = strings.TrimSpace(s)
	if !strings.Contains(s, "/") {
		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP %q", s)
		}
		if ip4 := ip.To4(); ip4 != nil {
			return &net.IPNet{IP: ip4, Mask: net.CIDRMask(32, 32)}, nil
		}
		return &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}, nil
	}
	_, n, err := net.ParseCIDR(s)
	return n, err
}

// Close stops the sequencer goroutines (buffered events are not flushed;
// seal first if they matter).
func (c *Collector) Close() { c.seq.Close() }

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$`)

// IngestResult is the ingest response body.
type IngestResult struct {
	Accepted        int            `json:"accepted"`
	Duplicates      int            `json:"duplicates"`
	Rejected        int            `json:"rejected,omitempty"`
	RejectedReasons map[string]int `json:"rejected_reasons,omitempty"`
}

func (r *IngestResult) reject(reason string, n int) {
	if n <= 0 {
		return
	}
	r.Rejected += n
	if r.RejectedReasons == nil {
		r.RejectedReasons = map[string]int{}
	}
	r.RejectedReasons[reason] += n
}

// Ingest groups events by session and hands them to the sequencer.
func (c *Collector) Ingest(ctx context.Context, instance string, evs []core.Event) (IngestResult, error) {
	var res IngestResult
	by := map[string][]core.Event{}
	for _, e := range evs {
		if !validID.MatchString(e.SessionID) {
			res.reject("invalid_session_id", 1)
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
	// Pending counts buffered (not yet sequenced) events plus open async work.
	Pending int `json:"pending"`
	// LastEventUnixNs is the collector's RECEIVE time of the latest ingest for
	// the session (not the SDK's EndNs: client clocks may be skewed). It
	// advances on arrival, before the reorder window flushes.
	LastEventUnixNs int64 `json:"last_event_unix_ns"`
	ReceivedUnixNs  int64 `json:"received_unix_ns"` // same value, explicit name
	EventCount      int   `json:"event_count"`
	DroppedEvents   int64 `json:"dropped_events"`
	// Quarantined counts poison events isolated and dropped for the session.
	Quarantined int `json:"quarantined"`
}

func (c *Collector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	st, err := c.seq.Status(ctx, id)
	if err != nil {
		return DrainStatus{}, err
	}
	return DrainStatus{Pending: st.Pending, LastEventUnixNs: st.LastEventNs, ReceivedUnixNs: st.LastEventNs,
		EventCount: st.EventCount, DroppedEvents: st.Dropped, Quarantined: st.Quarantined}, nil
}

// SealResult is the seal response body.
type SealResult struct {
	Incomplete bool     `json:"incomplete"`
	Orphaned   []string `json:"orphaned"`
	// RetryPending is the subset of Orphaned whose worker attempts ended with
	// a retry pending (the retry never completed).
	RetryPending []string `json:"retry_pending"`
	BundleURI    string   `json:"bundle_uri"`
	Quarantined  int      `json:"quarantined"`
}

var errBadID = errors.New("invalid session id")

// Seal flushes the reorder window, resolves transactions, marks orphaned
// async work and writes the bundle. It is idempotent, and concurrent calls
// for one session are serialised on the session's actor so the bundle is
// written exactly once.
func (c *Collector) Seal(ctx context.Context, id string) (res SealResult, err error) {
	if !validID.MatchString(id) {
		return SealResult{}, errBadID
	}
	if !c.seq.with(id, true, true, func(a *sessionActor) { res, err = c.sealLocked(ctx, a) }) {
		return SealResult{}, errSeqClosed
	}
	return
}

func (c *Collector) sealLocked(ctx context.Context, a *sessionActor) (SealResult, error) {
	id := a.id
	if si, err := c.st.GetSeal(ctx, id); err != nil {
		return SealResult{}, err
	} else if si != nil {
		a.sealed.Store(true)
		return sealResult(si, c.retryOf(ctx, id, si.Orphaned)), nil
	}
	if err := a.load(ctx); err != nil {
		return SealResult{}, err
	}
	if err := a.flushNow(); err != nil {
		return SealResult{}, err
	}
	st := a.status()
	evs, err := c.st.Events(ctx, id, 0, 0)
	if err != nil {
		return SealResult{}, err
	}
	q := st.Quarantined
	si := store.SealInfo{
		Orphaned:    nonNil(st.Open),
		Incomplete:  len(st.Open) > 0 || st.Dropped > 0 || q > 0,
		SealedAt:    time.Now().UTC(),
		Quarantined: q,
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
	a.sealed.Store(true)
	return sealResult(&si, retryPending(evs, si.Orphaned)), nil
}

func sealResult(si *store.SealInfo, retry []string) SealResult {
	return SealResult{Incomplete: si.Incomplete, Orphaned: nonNil(si.Orphaned), RetryPending: nonNil(retry),
		BundleURI: si.BundleURI, Quarantined: si.Quarantined}
}

func (c *Collector) retryOf(ctx context.Context, id string, orphaned []string) []string {
	if len(orphaned) == 0 {
		return nil
	}
	evs, err := c.st.Events(ctx, id, 0, 0)
	if err != nil {
		return nil
	}
	return retryPending(evs, orphaned)
}

// retryPending returns the orphaned publisher ids that saw a worker attempt
// end with retry_pending (or a worker.retry event) but never a final outcome.
func retryPending(evs []core.Event, orphaned []string) []string {
	if len(orphaned) == 0 {
		return nil
	}
	retrying := map[string]bool{}
	for i := range evs {
		e := &evs[i]
		if e.Kind == core.KindWorkerRetry || (e.Kind == core.KindWorkerEnd && e.Attrs["retry_pending"] == "true") {
			for _, l := range e.LinkIDs {
				retrying[l] = true
			}
		}
	}
	var out []string
	for _, id := range orphaned {
		if retrying[id] {
			out = append(out, id)
		}
	}
	return out
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

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
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
			if evs[i].TxID == "" {
				continue
			}
			if eff, ok := store.TxResult(&evs[i]); ok {
				store.MergeTxOutcome(outcomes, evs[i].TxID, eff)
			}
		}
	}
	retry := map[string]bool{}
	if len(orphans) > 0 {
		for _, id := range retryPending(evs, keys(orphans)) {
			retry[id] = true
		}
	}
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
