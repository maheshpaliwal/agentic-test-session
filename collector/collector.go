// Package collector ingests SDK events, sequences them per session, and
// serves the read API described in docs/PROTOCOL.md.
package collector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// Config tunes a Collector. Window 0 means "sequence immediately".
type Config struct {
	Window          time.Duration
	BundleDir       string // empty disables bundle writing
	MaxBodyBytes    int64  // per ingest request
	MaxMessageBytes int    // per protobuf message; longer ones are rejected individually
	// IdleEvict is how long a sealed, idle session's actor (goroutine plus
	// dedupe state) is kept before being evicted; negative disables eviction.
	IdleEvict time.Duration
	// MaxBufferedEvents caps events buffered per session while the store is
	// failing (ingest then answers 503).
	MaxBufferedEvents int
	// Retention deletes a session this long after it was sealed (0 keeps
	// sessions forever). A session that was never sealed is deleted once its
	// newest event is this old and its actor has been evicted for idleness.
	Retention time.Duration
	// RetentionInterval is how often the retention sweep runs; default
	// Retention/10 clamped to [1s, 10m].
	RetentionInterval time.Duration
	// StoreTimeout bounds each store call the sequencer makes outside a
	// request (window flushes, catch-up, poison-event probes); default 30s.
	StoreTimeout time.Duration
	// ShutdownTimeout bounds, in total, the flushing of reorder windows that
	// Close does: store calls still running that long after Close began are
	// cancelled and the events they carried are lost (and logged). Sessions
	// flush concurrently, so a session whose store calls hang does not use up
	// the time of the others. Default 15s.
	ShutdownTimeout time.Duration
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
	return Config{Window: 500 * time.Millisecond, MaxBodyBytes: 32 << 20, MaxMessageBytes: 4 << 20,
		IdleEvict: 5 * time.Minute, MaxBufferedEvents: 100000, StoreTimeout: 30 * time.Second,
		ShutdownTimeout: 15 * time.Second}
}

// Collector wires the store, sequencer and HTTP handlers together.
type Collector struct {
	cfg      Config
	st       store.Store
	seq      *Sequencer
	tel      *telemetry
	log      *slog.Logger
	internal []*net.IPNet

	vmu   sync.Mutex
	views map[string]*viewEntry
	vtick uint64
	vgen  uint64 // bumped by every delete; a view built across one is not cached

	stopOnce  sync.Once
	stop      chan struct{} // closed by Close
	retention chan struct{} // closed when the retention loop has exited

	sweepMu    sync.Mutex
	sweepAfter string // where the next retention sweep resumes; "" is the start
}

// New builds a Collector over st.
func New(st store.Store, cfg Config) *Collector {
	d := DefaultConfig()
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = d.MaxBodyBytes
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = d.MaxMessageBytes
	}
	cfg.IdleEvict = cmp.Or(cfg.IdleEvict, d.IdleEvict)
	if cfg.MaxBufferedEvents <= 0 {
		cfg.MaxBufferedEvents = d.MaxBufferedEvents
	}
	if cfg.StoreTimeout <= 0 {
		cfg.StoreTimeout = d.StoreTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = d.ShutdownTimeout
	}
	if cfg.Window < 0 {
		cfg.Window = 0
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Retention > 0 && cfg.RetentionInterval <= 0 {
		cfg.RetentionInterval = min(max(cfg.Retention/10, time.Second), 10*time.Minute)
	}
	c := &Collector{cfg: cfg, st: st, log: cfg.Logger, views: map[string]*viewEntry{},
		tel: newTelemetry(), stop: make(chan struct{}), retention: make(chan struct{})}
	c.seq = newSequencer(st, cfg, c.tel)
	c.tel.observe(c.seq)
	if cfg.Retention > 0 {
		go c.retentionLoop()
	} else {
		close(c.retention)
	}
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

// Close stops the sequencer goroutines after flushing every session's reorder
// window, for at most Config.ShutdownTimeout in total. Stop the HTTP server
// first: an ingest that races with Close fails with a storage error (which
// the SDK retries).
func (c *Collector) Close() {
	c.stopOnce.Do(func() { close(c.stop) })
	<-c.retention
	c.seq.Close()
}

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
		e.Instance = cmp.Or(e.Instance, instance)
		by[e.SessionID] = append(by[e.SessionID], e)
	}
	var firstErr error
	for _, id := range mapx.SortedKeys(by) {
		a, d, err := c.seq.Add(ctx, id, by[id])
		res.Accepted += a
		res.Duplicates += d
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.tel.ingested.With("accepted").Add(res.Accepted)
	c.tel.ingested.With("duplicate").Add(res.Duplicates)
	for reason, n := range res.RejectedReasons {
		c.tel.rejected.With(reason).Add(n)
	}
	if firstErr != nil {
		c.tel.ingestErrors.With(ingestErrorCause(firstErr)).Inc()
	}
	return res, firstErr
}

func ingestErrorCause(err error) string {
	switch {
	case errors.Is(err, errBackpressure):
		return "backpressure"
	case errors.Is(err, errSeqClosed):
		return "shutdown"
	}
	return "storage"
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

// DrainStatus reports how much work is still outstanding for a session. It
// never creates in-memory state: an unknown id yields a zero status.
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

// Delete removes everything the collector holds for a session: events still
// in the reorder window, stored events, the seal, cached views and the bundle
// directory. It is idempotent; an unknown id is not an error. Events that
// arrive for the id afterwards start a fresh session at Seq 1.
func (c *Collector) Delete(ctx context.Context, id string) error {
	return c.delete(ctx, id, "api")
}

func (c *Collector) delete(ctx context.Context, id, cause string) error {
	if !validID.MatchString(id) {
		return errBadID
	}
	ctx, cancel := c.seq.bound(ctx)
	defer cancel()
	var err error
	if !c.seq.with(id, true, false, func(a *sessionActor) { err = a.purge(ctx) }) {
		return errSeqClosed
	}
	if err != nil {
		return err
	}
	c.seq.drop(id)
	c.seq.forgetQuarantine(id)
	c.seq.forgetStatus(id)
	c.vmu.Lock()
	delete(c.views, id)
	c.vgen++
	c.vmu.Unlock()
	if c.cfg.BundleDir != "" {
		// validID admits no path separators and no leading dot, so the join
		// cannot leave BundleDir.
		if err := os.RemoveAll(filepath.Join(c.cfg.BundleDir, id)); err != nil {
			c.log.Warn("session deleted but its bundle could not be removed", "session", id, "err", err)
		}
	}
	c.tel.deleted.With(cause).Inc()
	return nil
}

func (c *Collector) retentionLoop() {
	defer close(c.retention)
	t := time.NewTicker(c.cfg.RetentionInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			// A sweep in flight ends when the collector is closed.
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.StoreTimeout)
			swept := make(chan struct{})
			go func() {
				select {
				case <-c.stop:
					cancel()
				case <-swept:
				}
			}()
			n, err := c.PurgeExpired(ctx, now)
			close(swept)
			cancel()
			if err != nil {
				c.log.Warn("retention sweep", "err", err)
			}
			if n > 0 {
				c.log.Info("retention sweep deleted expired sessions", "sessions", n, "retention", c.cfg.Retention)
			}
		}
	}
}

// retentionPage is how many sessions a retention sweep asks a
// store.ExpiredLister for at a time.
const retentionPage = 256

// PurgeExpired deletes the sessions whose retention has run out as of now and
// returns how many were deleted. It is what the retention loop runs; with
// Config.Retention unset it does nothing.
//
// A store.ExpiredLister is asked only for the sessions that may be due, a
// page at a time until there are no more, so one call still deletes every
// due session. A call that ends early (ctx, or the store failing to list)
// leaves the rest to the next call, which carries on after the last session
// this one looked at instead of starting over: sessions that cannot be
// deleted do not keep the ones behind them waiting. Having reached the end,
// such a call goes round once more from the start for the sessions before
// the point it began at. Any other store is listed whole.
func (c *Collector) PurgeExpired(ctx context.Context, now time.Time) (int, error) {
	if c.cfg.Retention <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-c.cfg.Retention)
	el, ok := c.st.(store.ExpiredLister)
	if !ok {
		sessions, err := c.st.ListSessions(ctx)
		if err != nil {
			return 0, err
		}
		n, _, err := c.purge(ctx, sessions, cutoff)
		return n, err
	}
	c.sweepMu.Lock()
	defer c.sweepMu.Unlock()
	n := 0
	var firstErr error
	wrap := c.sweepAfter != "" // began mid-way: the sessions before that point are still owed
	for ctx.Err() == nil {
		page, err := el.ListExpired(ctx, cutoff, c.sweepAfter, retentionPage)
		if err == nil && len(page) > 0 && page[len(page)-1].ID <= c.sweepAfter {
			err = fmt.Errorf("store listed session %s after %s", page[len(page)-1].ID, c.sweepAfter)
		}
		if err != nil {
			firstErr = cmp.Or(firstErr, err)
			break
		}
		d, last, err := c.purge(ctx, page, cutoff)
		n += d
		firstErr = cmp.Or(firstErr, err)
		if last < len(page) {
			// Cut short: the next sweep resumes behind the last session tried.
			if last > 0 {
				c.sweepAfter = page[last-1].ID
			}
			break
		}
		if len(page) < retentionPage {
			c.sweepAfter = ""
			if wrap {
				wrap = false
				continue
			}
			break
		}
		c.sweepAfter = page[len(page)-1].ID
	}
	return n, firstErr
}

// purge deletes those of sessions that are expired at cutoff, stopping when
// ctx ends. It returns how many it deleted, how many it got through and the
// first error.
func (c *Collector) purge(ctx context.Context, sessions []store.SessionInfo, cutoff time.Time) (n, tried int, firstErr error) {
	for _, s := range sessions {
		expired, err := c.expired(ctx, s, cutoff)
		if err == nil && expired {
			if err = c.delete(ctx, s.ID, "retention"); err == nil {
				n++
			}
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("session %s: %w", s.ID, err)
		}
		tried++
		if ctx.Err() != nil {
			break
		}
	}
	return n, tried, firstErr
}

func (c *Collector) expired(ctx context.Context, s store.SessionInfo, cutoff time.Time) (bool, error) {
	if s.Sealed {
		si, err := c.st.GetSeal(ctx, s.ID)
		if err != nil || si == nil {
			return false, err
		}
		return si.SealedAt.Before(cutoff), nil
	}
	// Never sealed (aborted, or abandoned by its control plane). LastNs is an
	// SDK clock, so it only counts once the session has also been idle here
	// long enough for its actor to be evicted.
	return s.LastNs > 0 && time.Unix(0, s.LastNs).Before(cutoff) && !c.seq.has(s.ID), nil
}

// Seal flushes the reorder window, resolves transactions, marks orphaned
// async work and writes the bundle. It is idempotent, and concurrent calls
// for one session are serialised on the session's actor so the bundle is
// written exactly once.
func (c *Collector) Seal(ctx context.Context, id string) (res SealResult, err error) {
	if !validID.MatchString(id) {
		return SealResult{}, errBadID
	}
	ctx, cancel := c.seq.bound(ctx)
	defer cancel()
	if !c.seq.with(id, true, true, func(a *sessionActor) { res, err = c.sealLocked(ctx, a) }) {
		err = errSeqClosed
	}
	switch {
	case err != nil:
		c.tel.seals.With("error").Inc()
		return SealResult{}, err
	case res.Incomplete:
		c.tel.seals.With("incomplete").Inc()
	default:
		c.tel.seals.With("complete").Inc()
	}
	return res, nil
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
	return slicex.Filter(orphaned, func(id string) bool { return retrying[id] })
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
		for _, id := range retryPending(evs, slices.Collect(maps.Keys(orphans))) {
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
