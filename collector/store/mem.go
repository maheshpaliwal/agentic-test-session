package store

import (
	"context"
	"sort"
	"sync"

	"tsession/core"
)

// Mem is the in-memory Store (default; used in tests).
type Mem struct {
	mu   sync.RWMutex
	ev   map[string][]core.Event
	seal map[string]SealInfo
}

func NewMem() *Mem {
	return &Mem{ev: map[string][]core.Event{}, seal: map[string]SealInfo{}}
}

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

func (m *Mem) TxOutcomes(_ context.Context, sid string) (map[string]bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]bool{}
	for i := range m.ev[sid] {
		txOutcome(&m.ev[sid][i], out)
	}
	return out, nil
}

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
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Mem) GetSeal(_ context.Context, sid string) (*SealInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.seal[sid]; ok {
		return &s, nil
	}
	return nil, nil
}

func (m *Mem) PutSeal(_ context.Context, sid string, si SealInfo) error {
	m.mu.Lock()
	m.seal[sid] = si
	m.mu.Unlock()
	return nil
}

func (m *Mem) Close() error { return nil }
