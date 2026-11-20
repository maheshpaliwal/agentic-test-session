// Package controlplane implements the tsession session FSM, its REST/SSE API
// and the drain/seal orchestration against the collector.
package controlplane

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
)

// State is a session's position in the lifecycle FSM.
type State string

const (
	StateCreated  State = "CREATED"
	StateActive   State = "ACTIVE"
	StateDraining State = "DRAINING"
	StateSealed   State = "SEALED"
	StateAborted  State = "ABORTED"
	StateExpired  State = "EXPIRED"
)

// Terminal reports whether no further transitions are possible.
func (s State) Terminal() bool {
	return s == StateSealed || s == StateAborted || s == StateExpired
}

// validTransitions is the FSM edge set.
var validTransitions = map[State][]State{
	StateCreated:  {StateActive, StateAborted},
	StateActive:   {StateDraining, StateAborted},
	StateDraining: {StateSealed, StateExpired, StateAborted},
}

func canTransition(from, to State) bool {
	for _, t := range validTransitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// Mode selects how requests join a session: by header/binding (ModeHeader)
// or all traffic while the session runs (ModeExclusive).
type Mode string

const (
	ModeHeader    Mode = "header"
	ModeExclusive Mode = "exclusive"
)

// Session is the control plane's record of one session; it is also the wire
// form returned by the API.
type Session struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	NameAuto        bool      `json:"name_auto"`
	Mode            Mode      `json:"mode"`
	State           State     `json:"state"`
	InitiatedBy     string    `json:"initiated_by"`
	CreatedAt       time.Time `json:"created_at"`
	StartedAt       time.Time `json:"started_at,omitzero"`
	EndedAt         time.Time `json:"ended_at,omitzero"`
	TTLSeconds      int       `json:"ttl_seconds"`
	QuiesceSeconds  int       `json:"quiesce_seconds"`
	MaxDrainSeconds int       `json:"max_drain_seconds"`
	Incomplete      bool      `json:"incomplete"`
	DroppedEvents   int64     `json:"dropped_events"`
	BundleURI       string    `json:"bundle_uri"`
	// Reason is "ttl" when the session was ended by TTL expiry (empty otherwise).
	Reason string `json:"reason,omitempty"`
	// SealError is set when the collector seal could not be completed after
	// bounded retries; the session is then SEALED with Incomplete=true. It is
	// cleared if a background retry (see SealPending) seals the session later.
	SealError string `json:"seal_error,omitempty"`
	// SealPending marks a finished session whose seal failed past
	// Config.SealRetryBudget and is still being retried in the background.
	// The session is terminal meanwhile (SDKs have stopped recording). The
	// marker is cleared when a retry succeeds (SealError is cleared and
	// BundleURI/Incomplete are filled in), or when Config.SealPendingBudget
	// runs out (SealError stays).
	SealPending bool `json:"seal_pending,omitempty"`
	// DrainTimedOut is true when the drain ended at max_drain_seconds instead
	// of on a quiet collector. It is kept so that a seal that succeeds late
	// still reports the session Incomplete.
	DrainTimedOut bool `json:"drain_timed_out,omitempty"`
	// Bindings attribute requests carrying a matching attribute to this
	// session (see docs/BINDING.md). Live only while the session is ACTIVE.
	Bindings []Binding `json:"bindings,omitempty"`
}

// ActiveSet is the payload of GET /v1/active and the SSE stream.
type ActiveSet struct {
	Active    []string `json:"active"`
	Exclusive string   `json:"exclusive"`
	Kill      bool     `json:"kill"`
	// Bindings lists the bindings of ACTIVE sessions only (not DRAINING).
	Bindings []ActiveBinding `json:"bindings,omitempty"`
	// Generation counts the changes to the active set since the control plane
	// started. Compare it with Subscriber.Generation to see which followers
	// have been sent this version.
	Generation uint64 `json:"generation"`
}

// Errors returned by Manager; the HTTP layer maps them to status codes.
var (
	ErrNotFound = errors.New("session not found")
	ErrConflict = errors.New("exclusive session already running")
	ErrBadState = errors.New("invalid state transition")
	ErrInvalid  = errors.New("invalid request")
	// ErrCollector wraps a failed call to the collector made on behalf of an
	// API request (HTTP 502).
	ErrCollector = errors.New("collector request failed")
)

// Store persists sessions. Implementations must be goroutine-safe.
type Store interface {
	Put(Session) error
	Get(id string) (Session, bool)
	List() []Session // ordered by CreatedAt ascending
	// Delete forgets a session for good (archive included). Deleting an
	// unknown id is not an error.
	Delete(id string) error
}

// Evictor is an optional Store capability: drop terminal sessions from the
// hot in-memory set, all ids of one call as one batch (a retention sweep
// makes one call). Ids the store does not hold are ignored. Stores with
// durable backing (FileStore) keep the sessions readable via Get/List;
// MemStore simply forgets them.
type Evictor interface {
	Evict(ids ...string)
}

// MemStore is the default in-memory Store.
type MemStore struct {
	mu sync.RWMutex
	m  map[string]Session
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{m: map[string]Session{}} }

// Put implements Store.
func (s *MemStore) Put(x Session) error {
	s.mu.Lock()
	s.m[x.ID] = x
	s.mu.Unlock()
	return nil
}

// Delete implements Store.
func (s *MemStore) Delete(id string) error {
	s.Evict(id)
	return nil
}

// Evict implements Evictor by forgetting the sessions (in-memory stores have
// no archive).
func (s *MemStore) Evict(ids ...string) {
	s.mu.Lock()
	for _, id := range ids {
		delete(s.m, id)
	}
	s.mu.Unlock()
}

// Get implements Store.
func (s *MemStore) Get(id string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x, ok := s.m[id]
	return x, ok
}

// sortSessions orders sessions by creation time, then ID.
func sortSessions(s []Session) {
	slices.SortFunc(s, func(a, b Session) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
}

// List implements Store.
func (s *MemStore) List() []Session {
	s.mu.RLock()
	out := mapx.Values(s.m)
	s.mu.RUnlock()
	sortSessions(out)
	return out
}

func newID(now time.Time) string {
	var r [6]byte
	_, _ = rand.Read(r[:])
	return fmt.Sprintf("ts_%011x%s", now.UnixMilli(), hex.EncodeToString(r[:]))
}
