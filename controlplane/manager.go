package controlplane

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// Config tunes a Manager. Zero values pick sensible defaults.
type Config struct {
	Store     Store
	Collector Collector
	Logger    *slog.Logger

	DefaultTTL      int // seconds; default 3600
	DefaultQuiesce  int // seconds; default 3
	DefaultMaxDrain int // seconds; default 60

	PollInterval  time.Duration // drain-status polling; default 500ms
	SweepInterval time.Duration // TTL janitor; default 1s
	// SealRetryBudget bounds how long a failing collector seal call is retried
	// (exponential backoff from PollInterval, capped at 2s); default 30s.
	SealRetryBudget time.Duration
	// SealRetryMaxInterval caps the backoff between background retries of a
	// seal that failed past SealRetryBudget (they start at PollInterval and
	// double after every failure); default 1m.
	SealRetryMaxInterval time.Duration
	// SealPendingBudget bounds how long such a seal keeps being retried,
	// counted from the session's EndedAt; default 24h. After it the session
	// keeps its SealError and is no longer retried.
	SealPendingBudget time.Duration
	HeartbeatPeriod   time.Duration // SSE heartbeat; default 15s
	// DeliveryGrace is how long a drain waits for an SDK instance that still
	// owes the collector events and gets nowhere, and for the report of a
	// connected instance whose events the collector has (see delivery.go);
	// after it the session is sealed and marked incomplete for them. It must
	// be longer than an SDK needs to notice that its collector is back (an
	// exporter pass and its cooldown). A connected instance of which no report
	// has arrived a tenth of it after it connected is named in
	// delivery_unconfirmed. Default 10 TimeUnits; negative turns the check
	// off: reports are stored, nothing is waited for and no session is marked
	// incomplete because of them.
	DeliveryGrace time.Duration
	// Retention is how long terminal sessions stay in the hot in-memory set
	// before being evicted from it (still readable if the Store archives them,
	// e.g. FileStore); default 24h.
	Retention time.Duration
	// AllowedHosts / AllowedOrigins extend the loopback-only Host/Origin guard
	// (e.g. docker service names such as "controlplane:7070").
	AllowedHosts   []string
	AllowedOrigins []string
	// TimeUnit is the duration of one "second" in ttl/quiesce/max_drain
	// fields. Tests shrink it; default time.Second.
	TimeUnit time.Duration
	// OnPanicBudgetSpent is called once when the sweep has panicked
	// gosafe.PanicBudget times within gosafe.PanicBudgetWindow: the sweep is
	// not started again, and the service is to end (cmd/controlplane shuts
	// down and exits 1). Nil leaves the Manager running and degraded (see
	// Health), which only suits a Manager that is part of another process.
	OnPanicBudgetSpent func(reason string)
}

// Manager owns the session FSM.
type Manager struct {
	cfg Config
	log *slog.Logger
	tel *telemetry
	// watch is the health of the Manager's goroutines (see Health).
	watch *gosafe.Watch

	mu      sync.Mutex
	kill    bool
	gen     uint64 // bumped on every broadcast; identifies a version of the active set
	subs    map[chan ActiveSet]Subscriber
	cancels map[string]context.CancelFunc // in-flight drains
	pending map[string]*sealRetry         // finished sessions whose seal is still being retried
	// delivery holds what is not stored of the delivery reports of live sessions.
	delivery map[string]*deliveryState
	// progress is, per reporting instance, when it was last seen to deliver
	// events of any session (see noteProgressLocked).
	progress map[string]instanceProgress
	// busySince is when the control plane last went from no session that is
	// ACTIVE or DRAINING to one; zero while there is none. The SDKs send
	// heartbeats from then on (see unheard).
	busySince time.Time

	// live indexes non-terminal sessions so Sweep/active-set are O(live), not
	// O(all sessions ever). terminal queues finished sessions for eviction.
	live     map[string]Session
	terminal []termEntry

	rootCtx context.Context
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

// NewManager builds a Manager and resumes the drains and the pending seal
// retries that were in flight when a persistent Store was last written. Call
// Run for TTL expiry and further seal retries, and Close when done.
func NewManager(cfg Config) *Manager {
	if cfg.Store == nil {
		cfg.Store = NewMemStore()
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 3600
	}
	if cfg.DefaultQuiesce <= 0 {
		cfg.DefaultQuiesce = 3
	}
	if cfg.DefaultMaxDrain <= 0 {
		cfg.DefaultMaxDrain = 60
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = time.Second
	}
	if cfg.SealRetryBudget <= 0 {
		cfg.SealRetryBudget = 30 * time.Second
	}
	if cfg.SealRetryMaxInterval <= 0 {
		cfg.SealRetryMaxInterval = time.Minute
	}
	if cfg.SealPendingBudget <= 0 {
		cfg.SealPendingBudget = 24 * time.Hour
	}
	if cfg.HeartbeatPeriod <= 0 {
		cfg.HeartbeatPeriod = 15 * time.Second
	}
	cfg.DefaultTTL = min(cfg.DefaultTTL, MaxDurationSeconds)
	cfg.DefaultQuiesce = min(cfg.DefaultQuiesce, MaxDurationSeconds)
	cfg.DefaultMaxDrain = min(cfg.DefaultMaxDrain, MaxDurationSeconds)
	if cfg.Retention <= 0 {
		cfg.Retention = 24 * time.Hour
	}
	if cfg.TimeUnit <= 0 {
		cfg.TimeUnit = time.Second
	}
	if cfg.DeliveryGrace == 0 {
		cfg.DeliveryGrace = 10 * cfg.TimeUnit
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg: cfg, log: log,
		subs:    map[chan ActiveSet]Subscriber{},
		cancels: map[string]context.CancelFunc{},
		pending: map[string]*sealRetry{},
		live:    map[string]Session{}, delivery: map[string]*deliveryState{}, progress: map[string]instanceProgress{},
		rootCtx: ctx, stop: cancel,
	}
	m.tel = newTelemetry(m)
	m.watch = gosafe.NewWatch(m.budgetSpent)
	if pc, ok := cfg.Store.(panicCounter); ok {
		pc.countPanics(m.countPanic)
	}
	// Resume drains and pending seals that were in flight when a persistent
	// store was last written. Archived sessions can have a seal pending; the
	// finished ones need nothing, so a store that can leave them out does.
	var stored []Session
	cs, checked := cfg.Store.(CheckedStore)
	failedBefore := int64(0)
	if checked {
		failedBefore = cs.ReadErrors()
	}
	if sl, ok := cfg.Store.(SelectiveLister); ok {
		stored = sl.ListPending()
	} else {
		stored = cfg.Store.List()
	}
	if checked {
		if n := cs.ReadErrors() - failedBefore; n > 0 {
			log.Error("stored sessions could not be read at startup; a drain or a pending seal among them is not resumed until the next start",
				"sessions", n)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range stored {
		m.indexLocked(s)
		switch {
		case s.State == StateDraining:
			m.startDrainLocked(s.ID)
		case s.SealPending && s.State.Terminal():
			m.armSealRetryLocked(s.ID, time.Time{})
		}
	}
	m.retrySealsLocked(time.Now())
	return m
}

// panicCounter is a Store with goroutines of its own (FileStore's
// compaction): it is given what counts their panics.
type panicCounter interface {
	countPanics(count func(goroutine string))
}

// MaxDurationSeconds bounds ttl/quiesce/max_drain (7 days); it also keeps
// seconds*time.Second far from Duration overflow.
const MaxDurationSeconds = 7 * 24 * 3600

type termEntry struct {
	id    string
	ended time.Time
}

// indexLocked updates the live/terminal indexes for s (caller holds m.mu or
// is single-threaded in the constructor).
func (m *Manager) indexLocked(s Session) {
	if !s.State.Terminal() {
		if len(m.live) == 0 {
			m.busySince = time.Now()
		}
		m.live[s.ID] = s
		return
	}
	delete(m.live, s.ID)
	if len(m.live) == 0 {
		m.busySince = time.Time{}
	}
	ended := s.EndedAt
	if ended.IsZero() {
		ended = s.CreatedAt
	}
	m.terminal = append(m.terminal, termEntry{s.ID, ended})
}

// putLocked persists s and keeps the indexes in step.
func (m *Manager) putLocked(s Session) error {
	err := m.cfg.Store.Put(s)
	m.indexLocked(s)
	return err
}

// pruneLocked evicts terminal sessions older than Retention from the hot set,
// all of them in one Store call.
func (m *Manager) pruneLocked(now time.Time) {
	n := 0
	for n < len(m.terminal) && now.Sub(m.terminal[n].ended) > m.cfg.Retention {
		n++
	}
	if n == 0 {
		return
	}
	if ev, ok := m.cfg.Store.(Evictor); ok {
		ids := make([]string, n)
		for i, e := range m.terminal[:n] {
			ids[i] = e.id
		}
		ev.Evict(ids...)
	}
	m.terminal = append(m.terminal[:0:0], m.terminal[n:]...)
}

// Run executes the janitor (TTL expiry, retention, pending seal retries)
// until ctx is done.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.Sweep(now)
		}
	}
}

// Close cancels in-flight drains and seal retries and waits for them.
func (m *Manager) Close() {
	m.stop()
	m.wg.Wait()
}

// StartRequest is the body of POST /v1/sessions.
type StartRequest struct {
	Name            string    `json:"name"`
	Mode            Mode      `json:"mode"`
	TTLSeconds      int       `json:"ttl_seconds"`
	QuiesceSeconds  int       `json:"quiesce_seconds"`
	MaxDrainSeconds int       `json:"max_drain_seconds"`
	InitiatedBy     string    `json:"initiated_by"`
	Bindings        []Binding `json:"bindings"`
}

// Start validates req and creates an ACTIVE session. It fails with
// ErrInvalid, ErrConflict (an exclusive session is running) or a
// *BindingConflictError.
func (m *Manager) Start(req StartRequest) (Session, error) {
	req.Mode = cmp.Or(req.Mode, ModeHeader)
	if req.Mode != ModeHeader && req.Mode != ModeExclusive {
		return Session{}, ErrInvalid
	}
	if req.Mode == ModeExclusive && len(req.Bindings) > 0 {
		return Session{}, fmt.Errorf("%w: bindings cannot be combined with mode=exclusive", ErrInvalid)
	}
	bindings, err := NormalizeBindings(req.Bindings)
	if err != nil {
		return Session{}, err
	}
	for _, v := range []int{req.TTLSeconds, req.QuiesceSeconds, req.MaxDrainSeconds} {
		if v < 0 || v > MaxDurationSeconds {
			return Session{}, fmt.Errorf("%w: ttl/quiesce/max_drain seconds must be 0 (default) or 1..%d", ErrInvalid, MaxDurationSeconds)
		}
	}
	def := func(v, d int) int {
		if v == 0 {
			return d
		}
		return v
	}
	req.InitiatedBy = cmp.Or(req.InitiatedBy, "api")
	now := time.Now().UTC()
	name, auto := resolveName(req.Name, bindings, req.InitiatedBy, now)
	s := Session{
		ID: newID(now), Name: name, NameAuto: auto, Mode: req.Mode, State: StateCreated,
		InitiatedBy: req.InitiatedBy, CreatedAt: now,
		TTLSeconds:      def(req.TTLSeconds, m.cfg.DefaultTTL),
		QuiesceSeconds:  def(req.QuiesceSeconds, m.cfg.DefaultQuiesce),
		MaxDrainSeconds: def(req.MaxDrainSeconds, m.cfg.DefaultMaxDrain),
		Bindings:        bindings,
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if s.Mode == ModeExclusive && m.exclusiveLocked() != "" {
		return Session{}, ErrConflict
	}
	if c := m.bindingConflictLocked(bindings); c != nil {
		return Session{}, c
	}
	s.State, s.StartedAt = StateActive, now
	if err := m.putLocked(s); err != nil {
		return Session{}, err
	}
	m.tel.started.Inc()
	m.broadcastLocked()
	return s, nil
}

// Stop moves ACTIVE -> DRAINING and kicks off the drain. Stopping a session
// that is already DRAINING or SEALED is a no-op returning its current state
// (a SEALED session with SealPending is sealed by the background retry, not
// by Stop).
func (m *Manager) Stop(id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.stored(id)
	if err != nil {
		return Session{}, err
	}
	if s.State == StateDraining || s.State == StateSealed {
		return s, nil
	}
	if !canTransition(s.State, StateDraining) {
		return s, ErrBadState
	}
	s.State = StateDraining
	if err := m.putLocked(s); err != nil {
		return s, err
	}
	m.startDrainLocked(id)
	m.broadcastLocked()
	return s, nil
}

// Abort ends a session immediately without draining or sealing it; an
// in-flight drain is cancelled. Aborting an ABORTED session is a no-op.
func (m *Manager) Abort(id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.stored(id)
	if err != nil {
		return Session{}, err
	}
	if s.State == StateAborted {
		return s, nil
	}
	if !canTransition(s.State, StateAborted) {
		return s, ErrBadState
	}
	if c := m.cancels[id]; c != nil {
		c()
		delete(m.cancels, id)
	}
	delete(m.delivery, id)
	s.State, s.EndedAt = StateAborted, time.Now().UTC()
	if err := m.putLocked(s); err != nil {
		return s, err
	}
	m.tel.finished.With(string(StateAborted)).Inc()
	m.broadcastLocked()
	return s, nil
}

// Delete removes a finished session for good: the collector drops its events,
// seal and bundle, then the control plane forgets it. Only terminal sessions
// can be deleted (ErrBadState otherwise): stop or abort a running one first.
// A collector failure leaves the session in place so the call can be retried.
//
// A session whose seal is still pending (SealPending) can be deleted too: its
// seal retries are stopped first, and an attempt already in flight is
// cancelled and waited for, so that it cannot reach the collector after the
// delete. If the collector delete then fails, the retries resume.
func (m *Manager) Delete(ctx context.Context, id string) error {
	inFlight, err := m.beginDelete(id)
	if err != nil {
		return err
	}
	if inFlight != nil {
		select {
		case <-inFlight:
		case <-ctx.Done():
			m.rearmSealRetry(id)
			return ctx.Err()
		}
	}
	if m.cfg.Collector != nil {
		if err := m.cfg.Collector.Delete(ctx, id); err != nil {
			m.rearmSealRetry(id)
			return fmt.Errorf("%w: %v", ErrCollector, err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disarmSealRetryLocked(id) // a concurrent Delete that failed may have re-armed it
	if err := m.cfg.Store.Delete(id); err != nil {
		return err
	}
	m.tel.deleted.Inc()
	return nil
}

// beginDelete checks that id can be deleted and stops its seal retries; it
// returns the attempt in flight, if any (see disarmSealRetryLocked). The
// store is read under the lock, so the unlock is deferred.
func (m *Manager) beginDelete(id string) (inFlight <-chan struct{}, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.stored(id)
	// A session the store has but cannot read (ErrStore) is an archived one,
	// and only finished sessions are archived: it can still be deleted, here
	// and in the collector. Refusing would leave it undeletable until a
	// restart, which then forgets it while the collector keeps its events.
	unreadable := errors.Is(err, ErrStore)
	if err != nil && !unreadable {
		return nil, err
	}
	if !unreadable && !s.State.Terminal() { // terminal states never change, so no lock is needed across the collector call
		return nil, fmt.Errorf("%w: session is %s; only finished sessions can be deleted", ErrBadState, s.State)
	}
	return m.disarmSealRetryLocked(id), nil
}

// Sweep ends ACTIVE sessions whose TTL has elapsed as of now. They go through
// DRAINING and the normal seal path (reason=ttl); drain() records the final
// state (EXPIRED on a clean seal, SEALED+incomplete+seal_error otherwise).
// Sweep also evicts finished sessions past Retention and starts the pending
// seal retries that are due.
func (m *Manager) Sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	faultpoint.Hit("sweep.top")
	changed := false
	for _, s := range m.live {
		if s.State == StateDraining && m.cancels[s.ID] == nil && m.rootCtx.Err() == nil {
			// Nobody drains it: whatever moved it to DRAINING panicked before
			// it had started the drain (a sweep, say, which is started again).
			m.startDrainLocked(s.ID)
		}
		if s.State != StateActive {
			continue
		}
		if now.After(s.StartedAt.Add(time.Duration(s.TTLSeconds) * m.cfg.TimeUnit)) {
			s.State, s.Reason = StateDraining, "ttl"
			if err := m.putLocked(s); err != nil {
				m.log.Error("persist expiring session", "id", s.ID, "err", err)
			}
			faultpoint.Hit("sweep.expired")
			m.startDrainLocked(s.ID)
			changed = true
		}
	}
	m.pruneLocked(now)
	m.pruneProgressLocked(time.Now()) // on the clock the progress was noted with
	m.retrySealsLocked(now)
	if changed {
		m.broadcastLocked()
	}
}

// Get returns the session with the given id.
func (m *Manager) Get(id string) (Session, bool) { return m.cfg.Store.Get(id) }

// stored returns the session with the given id from the store: ErrNotFound if
// the store has none, and an error wrapping ErrStore if it has one and could
// not read it (only a CheckedStore can tell).
func (m *Manager) stored(id string) (Session, error) {
	cs, ok := m.cfg.Store.(CheckedStore)
	if !ok {
		if s, ok := m.cfg.Store.Get(id); ok {
			return s, nil
		}
		return Session{}, ErrNotFound
	}
	s, ok, err := cs.GetErr(id)
	switch {
	case err != nil:
		return Session{}, fmt.Errorf("%w: %v", ErrStore, err)
	case !ok:
		return Session{}, ErrNotFound
	}
	return s, nil
}

// List returns sessions, optionally filtered by state ("" = all), oldest
// first. It produces every one of them: see ListPage for a part.
func (m *Manager) List(state State) []Session {
	if state == "" {
		return m.cfg.Store.List()
	}
	if sl, ok := m.cfg.Store.(SelectiveLister); ok {
		return sl.ListState(state)
	}
	return slicex.Filter(m.cfg.Store.List(), func(s Session) bool { return s.State == state })
}

// SetKill flips the global kill switch: while set, SDKs record nothing.
func (m *Manager) SetKill(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kill = v
	m.broadcastLocked()
}

// Active returns the current active set (what SDKs subscribe to).
func (m *Manager) Active() ActiveSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeLocked()
}

func (m *Manager) exclusiveLocked() string {
	for _, s := range m.live {
		if s.Mode == ModeExclusive {
			return s.ID
		}
	}
	return ""
}

func (m *Manager) activeLocked() ActiveSet {
	a := ActiveSet{Active: make([]string, 0, len(m.live)), Kill: m.kill, Generation: m.gen, DeliveryReports: true,
		DeliveryHeartbeatMs: m.heartbeatPeriod().Milliseconds()}
	for _, s := range m.live {
		a.Active = append(a.Active, s.ID)
		if s.Mode == ModeExclusive {
			a.Exclusive = s.ID
		}
	}
	slices.Sort(a.Active) // IDs are time-prefixed, so this is creation order
	for _, id := range a.Active {
		switch s := m.live[id]; s.State {
		case StateActive:
			for _, b := range s.Bindings {
				a.Bindings = append(a.Bindings, ActiveBinding{SessionID: id, Binding: b})
			}
		case StateDraining:
			a.Draining = append(a.Draining, id)
		}
	}
	return a
}

// Subscriber describes one connection following the active set, normally the
// SDK inside an instrumented service.
type Subscriber struct {
	// Service and Instance are what the SDK reported about itself; both are
	// empty for SDKs that do not send them and for other clients.
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance,omitempty"`
	// DeliveryInstance is the instance name the follower sends its delivery
	// reports under (the one its events carry, unique to its process); empty
	// for an SDK that sends no reports. A follower that names it reports the
	// sessions it recorded events for, and says once, when it connects, that
	// its reports arrive (see LastDeliveryReport).
	DeliveryInstance string    `json:"delivery_instance,omitempty"`
	ConnectedAt      time.Time `json:"connected_at"`
	// LastDeliveryReport is when a delivery report of the follower last
	// arrived, for any session or for none (it sends one when it connects);
	// zero when none has since it connected. A follower that names a
	// DeliveryInstance and stays at zero cannot get its reports through.
	LastDeliveryReport time.Time `json:"last_delivery_report,omitzero"`
	// Generation is the newest ActiveSet.Generation written to this follower's
	// connection. A follower at or past the generation in which a session
	// became active has been sent that session (sent, not necessarily applied).
	Generation uint64 `json:"generation"`
	// stream marks a follower served over HTTP, whose deliveries are recorded
	// as they are written. An in-process follower (Subscribe) receives every
	// version through its channel and is reported as up to date.
	stream bool
	// DeliveryUnheard: the follower names a DeliveryInstance and nothing of
	// it has arrived for too long, no report and no heartbeat (see
	// Manager.unheard): its reports do not get through. It is what the verdict
	// of a session that ended now would go by.
	DeliveryUnheard bool `json:"delivery_unheard,omitempty"`
	// Pending is what the follower last said of its events of the sessions
	// that run now, when some had not reached the collector or its last
	// request to it failed; nil otherwise. It is how an operator sees, during
	// a run, a service that follows and reports and cannot deliver.
	Pending *PendingDelivery `json:"delivery_pending,omitempty"`
	// since and heard are ConnectedAt and LastDeliveryReport as the drain
	// counts from them: with the monotonic clock reading, which a step of the
	// wall clock does not move. reported is when a request that carried a
	// report last arrived (heard: also one without, a heartbeat).
	since, heard, reported time.Time
}

// Subscribers lists the current followers of the active set, by service and
// instance. It answers "is my service connected?" when a session stays empty.
func (m *Manager) Subscribers() []Subscriber {
	out := m.subscribers()
	slices.SortFunc(out, func(a, b Subscriber) int {
		return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.Instance, b.Instance), a.ConnectedAt.Compare(b.ConnectedAt))
	})
	return out
}

func (m *Manager) subscribers() []Subscriber {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Subscriber, 0, len(m.subs))
	now := time.Now()
	pending := m.pendingLocked()
	for _, s := range m.subs {
		if !s.stream {
			s.Generation = m.gen
		}
		s.DeliveryUnheard = m.unheard(s, now)
		if s.DeliveryInstance != "" {
			s.Pending = pending[deliveryKey(s.Service, s.DeliveryInstance)]
		}
		out = append(out, s)
	}
	return out
}

// Subscribe returns a channel that receives the current active set
// immediately and again on every change (slow readers see only the latest).
func (m *Manager) Subscribe() (<-chan ActiveSet, func()) {
	return m.SubscribeAs("", "")
}

// SubscribeAs is Subscribe for a follower that identified itself.
func (m *Manager) SubscribeAs(service, instance string) (<-chan ActiveSet, func()) {
	return m.subscribe(Subscriber{Service: service, Instance: instance})
}

func (m *Manager) subscribe(sub Subscriber) (chan ActiveSet, func()) {
	ch := make(chan ActiveSet, 1)
	sub.since = time.Now()
	sub.ConnectedAt = sub.since.UTC()
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.subs[ch] = sub
		ch <- m.activeLocked()
	}()
	return ch, func() {
		m.mu.Lock()
		delete(m.subs, ch) // a delete: cannot panic
		m.mu.Unlock()
	}
}

// delivered records that the follower behind ch was sent generation gen.
func (m *Manager) delivered(ch chan ActiveSet, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.subs[ch]; ok && gen > s.Generation {
		s.Generation = gen
		m.subs[ch] = s
	}
}

func (m *Manager) broadcastLocked() {
	m.gen++
	a := m.activeLocked()
	for ch := range m.subs {
		select {
		case <-ch: // drop stale value
		default:
		}
		ch <- a // buffer 1 and we hold the lock, so this cannot block
	}
}
