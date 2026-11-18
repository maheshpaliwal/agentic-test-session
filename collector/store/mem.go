package store

import (
	"context"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// Mem is the in-memory Store (default; used in tests).
type Mem struct {
	mu   sync.RWMutex
	ev   map[string][]core.Event
	seal map[string]SealInfo
}

// NewMem returns an empty in-memory store.
func NewMem() *Mem {
	return &Mem{ev: map[string][]core.Event{}, seal: map[string]SealInfo{}}
}

// AppendEvents implements Store.
func (m *Mem) AppendEvents(_ context.Context, sid string, evs []core.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.ev[sid]
	for i := range evs {
		if evs[i].Seq != uint64(len(cur)+i+1) {
			return ErrSeqConflict
		}
	}
	m.ev[sid] = append(cur, evs...)
	return nil
}

// Events implements Store.
func (m *Mem) Events(_ context.Context, sid string, from uint64, limit int) ([]core.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := m.ev[sid]
	start := 0
	if from > 1 {
		start = int(from - 1)
	}
	if start >= len(all) {
		return nil, nil
	}
	end := len(all)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return append([]core.Event(nil), all[start:end]...), nil
}

// Head implements Store.
func (m *Mem) Head(_ context.Context, sid string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.ev[sid])), nil
}

// TxOutcomes implements Store.
func (m *Mem) TxOutcomes(_ context.Context, sid string) (map[string]bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]bool{}
	for i := range m.ev[sid] {
		txOutcome(&m.ev[sid][i], out)
	}
	return out, nil
}

// ListSessions implements Store.
func (m *Mem) ListSessions(context.Context) ([]SessionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []SessionInfo
	for id, evs := range m.ev {
		si := SessionInfo{ID: id, EventCount: len(evs)}
		for i, e := range evs {
			if i == 0 || e.StartNs < si.FirstNs {
				si.FirstNs = e.StartNs
			}
			if e.EndNs > si.LastNs {
				si.LastNs = e.EndNs
			}
		}
		_, si.Sealed = m.seal[id]
		out = append(out, si)
	}
	slicex.SortBy(out, func(s SessionInfo) string { return s.ID })
	return out, nil
}

// GetSeal implements Store.
func (m *Mem) GetSeal(_ context.Context, sid string) (*SealInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.seal[sid]; ok {
		return &s, nil
	}
	return nil, nil
}

// PutSeal implements Store.
func (m *Mem) PutSeal(_ context.Context, sid string, si SealInfo) error {
	m.mu.Lock()
	m.seal[sid] = si
	m.mu.Unlock()
	return nil
}

// DeleteSession implements Store.
func (m *Mem) DeleteSession(_ context.Context, sid string) error {
	m.mu.Lock()
	delete(m.ev, sid)
	delete(m.seal, sid)
	m.mu.Unlock()
	return nil
}

// Close implements Store.
func (m *Mem) Close() error { return nil }
