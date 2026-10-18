// Package store defines the collector's persistence interface and its
// in-memory and Postgres implementations.
package store

import (
	"bytes"
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
	// Quarantined counts poison events dropped from the session (see
	// docs/PROTOCOL.md, Collector).
	Quarantined int `json:"quarantined,omitempty"`
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
	// Head returns the highest stored Seq (0 when the session has no events).
	Head(ctx context.Context, sessionID string) (uint64, error)
	ListSessions(ctx context.Context) ([]SessionInfo, error)
	// GetSeal returns nil when the session is not sealed.
	GetSeal(ctx context.Context, sessionID string) (*SealInfo, error)
	PutSeal(ctx context.Context, sessionID string, si SealInfo) error
	Close() error
}

// TxResult reports whether e decides a transaction and, if so, whether the
// transaction's writes took effect. A commit event that carries Err or
// Attrs["commit_failed"]=="true" did NOT commit; a rollback never commits
// (tsql reports a failed commit as db.tx.rollback with commit_failed=true).
func TxResult(e *core.Event) (effective, decides bool) {
	switch e.Kind {
	case core.KindDBTxCommit:
		if e.Err != "" || e.Attrs["commit_failed"] == "true" {
			return false, true
		}
		return true, true
	case core.KindDBTxRollbck:
		return false, true
	}
	return false, false
}

// MergeTxOutcome folds one outcome into out. A negative outcome is sticky: a
// transaction that was ever rolled back or failed to commit stays ineffective
// regardless of arrival order.
func MergeTxOutcome(out map[string]bool, txID string, effective bool) {
	if prev, ok := out[txID]; ok && !prev {
		return
	}
	out[txID] = effective
}

func txOutcome(e *core.Event, out map[string]bool) {
	if e.TxID == "" {
		return
	}
	if eff, ok := TxResult(e); ok {
		MergeTxOutcome(out, e.TxID, eff)
	}
}

// StripNUL replaces JSON "\u0000" escapes (which Postgres jsonb/text reject)
// with "\ufffd". An escaped backslash followed by the literal text u0000 is
// left alone. It returns line itself when nothing needed replacing.
func StripNUL(line []byte) []byte {
	nul := []byte(`\u0000`)
	if !bytes.Contains(line, nul) {
		return line
	}
	out := make([]byte, 0, len(line))
	i := 0
	for i < len(line) {
		j := bytes.Index(line[i:], nul)
		if j < 0 {
			out = append(out, line[i:]...)
			break
		}
		j += i
		run := 1
		for k := j - 1; k >= 0 && line[k] == '\\'; k-- {
			run++
		}
		if run%2 == 1 {
			out = append(out, line[i:j]...)
			out = append(out, `\ufffd`...)
			i = j + len(nul)
		} else {
			out = append(out, line[i:j+1]...)
			i = j + 1
		}
	}
	return out
}
