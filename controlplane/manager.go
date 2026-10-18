package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
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
	HeartbeatPeriod time.Duration // SSE heartbeat; default 15s
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
}

// Manager owns the session FSM.
type Manager struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	kill    bool
	subs    map[chan ActiveSet]struct{}
	cancels map[string]context.CancelFunc // in-flight drains

	// live indexes non-terminal sessions so Sweep/active-set are O(live), not
	// O(all sessions ever). terminal queues finished sessions for eviction.
	live     map[string]Session
	terminal []termEntry

	rootCtx context.Context
	stop    context.CancelFunc
	wg      sync.WaitGroup
}

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
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg: cfg, log: log,
		subs:    map[chan ActiveSet]struct{}{},
		cancels: map[string]context.CancelFunc{},
		live:    map[string]Session{},
		rootCtx: ctx, stop: cancel,
	}
	// Resume drains that were in flight when a persistent store was last written.
	for _, s := range cfg.Store.List() {
		m.indexLocked(s)
		if s.State == StateDraining {
			m.startDrain(s.ID)
		}
	}
	return m
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
		m.live[s.ID] = s
		return
	}
	if _, was := m.live[s.ID]; was {
		delete(m.live, s.ID)
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

// pruneLocked evicts terminal sessions older than Retention from the hot set.
func (m *Manager) pruneLocked(now time.Time) {
	ev, _ := m.cfg.Store.(Evictor)
	n := 0
	for n < len(m.terminal) && now.Sub(m.terminal[n].ended) > m.cfg.Retention {
		if ev != nil {
			ev.Evict(m.terminal[n].id)
		}
		n++
	}
	if n > 0 {
		m.terminal = append(m.terminal[:0:0], m.terminal[n:]...)
	}
}

// Run executes the TTL janitor until ctx is done.
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

// Close cancels in-flight drains and waits for them.
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

func (m *Manager) Start(req StartRequest) (Session, error) {
	if req.Mode == "" {
		req.Mode = ModeHeader
	}
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
	if req.InitiatedBy == "" {
		req.InitiatedBy = "api"
	}
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
	m.broadcastLocked()
	return s, nil
}

// Stop moves ACTIVE -> DRAINING and kicks off the drain. Stopping a session
// that is already DRAINING or SEALED is a no-op returning its current state.
func (m *Manager) Stop(id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.cfg.Store.Get(id)
	if !ok {
		return Session{}, ErrNotFound
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

func (m *Manager) Abort(id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.cfg.Store.Get(id)
	if !ok {
		return Session{}, ErrNotFound
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
	s.State, s.EndedAt = StateAborted, time.Now().UTC()
	if err := m.putLocked(s); err != nil {
		return s, err
	}
	m.broadcastLocked()
	return s, nil
}

// Sweep ends ACTIVE sessions whose TTL has elapsed as of now. They go through
// DRAINING and the normal seal path (reason=ttl); drain() records the final
// state (EXPIRED on a clean seal, SEALED+incomplete+seal_error otherwise).
func (m *Manager) Sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for _, s := range m.live {
		if s.State != StateActive {
			continue
		}
		if now.After(s.StartedAt.Add(time.Duration(s.TTLSeconds) * m.cfg.TimeUnit)) {
			s.State, s.Reason = StateDraining, "ttl"
			if err := m.putLocked(s); err != nil {
				m.log.Error("persist expiring session", "id", s.ID, "err", err)
			}
			m.startDrainLocked(s.ID)
			changed = true
		}
	}
	m.pruneLocked(now)
	if changed {
		m.broadcastLocked()
	}
}

func (m *Manager) Get(id string) (Session, bool) { return m.cfg.Store.Get(id) }

// List returns sessions, optionally filtered by state ("" = all).
func (m *Manager) List(state State) []Session {
	all := m.cfg.Store.List()
	if state == "" {
		return all
	}
	out := all[:0:0]
	for _, s := range all {
		if s.State == state {
			out = append(out, s)
		}
	}
	return out
}

func (m *Manager) SetKill(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kill = v
	m.broadcastLocked()
}

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
	a := ActiveSet{Active: make([]string, 0, len(m.live)), Kill: m.kill}
	for _, s := range m.live {
		a.Active = append(a.Active, s.ID)
		if s.Mode == ModeExclusive {
			a.Exclusive = s.ID
		}
	}
	sort.Strings(a.Active) // IDs are time-prefixed, so this is creation order
	for _, id := range a.Active {
		if s := m.live[id]; s.State == StateActive {
			for _, b := range s.Bindings {
				a.Bindings = append(a.Bindings, ActiveBinding{SessionID: id, Binding: b})
			}
		}
	}
	return a
}

// Subscribe returns a channel that receives the current active set
// immediately and again on every change (slow readers see only the latest).
func (m *Manager) Subscribe() (<-chan ActiveSet, func()) {
	ch := make(chan ActiveSet, 1)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	ch <- m.activeLocked()
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		delete(m.subs, ch)
		m.mu.Unlock()
	}
}

func (m *Manager) broadcastLocked() {
	a := m.activeLocked()
	for ch := range m.subs {
		select {
		case <-ch: // drop stale value
		default:
		}
		ch <- a // buffer 1 and we hold the lock, so this cannot block
	}
}
