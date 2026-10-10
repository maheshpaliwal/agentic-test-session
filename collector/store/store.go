// Package store defines the collector's persistence interface and its
// in-memory and Postgres implementations.
package store

import (
	"context"
	"errors"
	"time"

	"tsession/core"
)

// ErrSeqConflict is returned when appended events do not continue the
// session's gapless Seq run.
var ErrSeqConflict = errors.New("store: seq conflict")

// SessionInfo summarises the events held for one session.
type SessionInfo struct {
	ID         string `json:"id"`
	EventCount int    `json:"event_count"`
	FirstNs    int64  `json:"first_ns"`
	LastNs     int64  `json:"last_ns"`
	Sealed     bool   `json:"sealed"`
}

// SealInfo is the persisted outcome of sealing a session.
type SealInfo struct {
	Incomplete bool      `json:"incomplete"`
	Orphaned   []string  `json:"orphaned"`
	BundleURI  string    `json:"bundle_uri"`
	SealedAt   time.Time `json:"sealed_at"`
}

// Store persists sequenced events. Implementations must be safe for
// concurrent use. Events passed to AppendEvents already carry their Seq,
// which must continue the session's run (last Seq + 1, ...).
type Store interface {
	AppendEvents(ctx context.Context, sessionID string, evs []core.Event) error
	// Events returns events with Seq >= fromSeq in ascending order; limit <= 0 means all.
	Events(ctx context.Context, sessionID string, fromSeq uint64, limit int) ([]core.Event, error)
	// TxOutcomes maps TxID -> committed (true) / rolled back (false).
	TxOutcomes(ctx context.Context, sessionID string) (map[string]bool, error)
	ListSessions(ctx context.Context) ([]SessionInfo, error)
	// GetSeal returns nil when the session is not sealed.
	GetSeal(ctx context.Context, sessionID string) (*SealInfo, error)
	PutSeal(ctx context.Context, sessionID string, si SealInfo) error
	Close() error
}

func txOutcome(e *core.Event, out map[string]bool) {
	if e.TxID == "" {
		return
	}
	switch e.Kind {
	case core.KindDBTxCommit:
		out[e.TxID] = true
	case core.KindDBTxRollbck:
		out[e.TxID] = false
	}
}
