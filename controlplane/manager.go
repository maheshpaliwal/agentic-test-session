package controlplane

import (
	"context"
	"log/slog"
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

	PollInterval    time.Duration // drain-status polling; default 500ms
	SweepInterval   time.Duration // TTL janitor; default 1s
	HeartbeatPeriod time.Duration // SSE heartbeat; default 15s
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
	if cfg.HeartbeatPeriod <= 0 {
		cfg.HeartbeatPeriod = 15 * time.Second
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
		rootCtx: ctx, stop: cancel,
	}
	// Resume drains that were in flight when a persistent store was last written.
	for _, s := range cfg.Store.List() {
		if s.State == StateDraining {
			m.startDrain(s.ID)
		}
	}
	return m
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
	Name            string `json:"name"`
	Mode            Mode   `json:"mode"`
	TTLSeconds      int    `json:"ttl_seconds"`
	QuiesceSeconds  int    `json:"quiesce_seconds"`
	MaxDrainSeconds int    `json:"max_drain_seconds"`
	InitiatedBy     string `json:"initiated_by"`
}

func (m *Manager) Start(req StartRequest) (Session, error) {
	if req.Mode == "" {
		req.Mode = ModeHeader
	}
	if req.Mode != ModeHeader && req.Mode != ModeExclusive {
		return Session{}, ErrInvalid
	}
	if req.TTLSeconds < 0 || req.QuiesceSeconds < 0 || req.MaxDrainSeconds < 0 {
		return Session{}, ErrInvalid
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
	s := Session{
		ID: newID(now), Name: req.Name, Mode: req.Mode, State: StateCreated,
		InitiatedBy: req.InitiatedBy, CreatedAt: now,
		TTLSeconds:      def(req.TTLSeconds, m.cfg.DefaultTTL),
		QuiesceSeconds:  def(req.QuiesceSeconds, m.cfg.DefaultQuiesce),
		MaxDrainSeconds: def(req.MaxDrainSeconds, m.cfg.DefaultMaxDrain),
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if s.Mode == ModeExclusive && m.exclusiveLocked() != "" {
		return Session{}, ErrConflict
	}
	if err := m.cfg.Store.Put(s); err != nil {
		return Session{}, err
	}
	s.State, s.StartedAt = StateActive, now
	if err := m.cfg.Store.Put(s); err != nil {
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
	if err := m.cfg.Store.Put(s); err != nil {
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
	if err := m.cfg.Store.Put(s); err != nil {
		return s, err
	}
	m.broadcastLocked()
	return s, nil
}

// Sweep expires ACTIVE sessions whose TTL has elapsed as of now.
func (m *Manager) Sweep(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for _, s := range m.cfg.Store.List() {
		if s.State != StateActive {
			continue
		}
		if now.After(s.StartedAt.Add(time.Duration(s.TTLSeconds) * m.cfg.TimeUnit)) {
			s.State, s.EndedAt = StateExpired, now.UTC()
			if err := m.cfg.Store.Put(s); err != nil {
				m.log.Error("persist expired session", "id", s.ID, "err", err)
			}
			changed = true
		}
	}
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
	for _, s := range m.cfg.Store.List() {
		if s.Mode == ModeExclusive && (s.State == StateActive || s.State == StateDraining) {
			return s.ID
		}
	}
	return ""
}

func (m *Manager) activeLocked() ActiveSet {
	a := ActiveSet{Active: []string{}, Kill: m.kill, Exclusive: m.exclusiveLocked()}
	for _, s := range m.cfg.Store.List() {
		if s.State == StateActive || s.State == StateDraining {
			a.Active = append(a.Active, s.ID)
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
