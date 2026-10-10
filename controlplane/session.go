// Package controlplane implements the tsession session FSM, its REST/SSE API
// and the drain/seal orchestration against the collector.
package controlplane

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

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
	StateActive:   {StateDraining, StateAborted, StateExpired},
	StateDraining: {StateSealed, StateAborted},
}

func canTransition(from, to State) bool {
	for _, t := range validTransitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

type Mode string

const (
	ModeHeader    Mode = "header"
	ModeExclusive Mode = "exclusive"
)

type Session struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
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
}

// ActiveSet is the payload of GET /v1/active and the SSE stream.
type ActiveSet struct {
	Active    []string `json:"active"`
	Exclusive string   `json:"exclusive"`
	Kill      bool     `json:"kill"`
}

var (
	ErrNotFound = errors.New("session not found")
	ErrConflict = errors.New("exclusive session already running")
	ErrBadState = errors.New("invalid state transition")
	ErrInvalid  = errors.New("invalid request")
)

// Store persists sessions. Implementations must be goroutine-safe.
type Store interface {
	Put(Session) error
	Get(id string) (Session, bool)
	List() []Session // ordered by CreatedAt ascending
}

// MemStore is the default in-memory Store.
type MemStore struct {
	mu sync.RWMutex
	m  map[string]Session
}

func NewMemStore() *MemStore { return &MemStore{m: map[string]Session{}} }

func (s *MemStore) Put(x Session) error {
	s.mu.Lock()
	s.m[x.ID] = x
	s.mu.Unlock()
	return nil
}

func (s *MemStore) Get(id string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x, ok := s.m[id]
	return x, ok
}

func (s *MemStore) List() []Session {
	s.mu.RLock()
	out := make([]Session, 0, len(s.m))
	for _, x := range s.m {
		out = append(out, x)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func newID(now time.Time) string {
	var r [6]byte
	_, _ = rand.Read(r[:])
	return fmt.Sprintf("ts_%011x%s", now.UnixMilli(), hex.EncodeToString(r[:]))
}
