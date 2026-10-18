package collector

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Sequencer routes events to per-session actors and evicts idle ones.
type Sequencer struct {
	st     store.Store
	window time.Duration
	idle   time.Duration
	maxBuf int

	mu     sync.Mutex
	actors map[string]*sessionActor
	closed bool

	qmu  sync.Mutex
	quar map[string]int

	stop chan struct{}
	jd   chan struct{}
}

func newSequencer(st store.Store, window, idle time.Duration, maxBuf int) *Sequencer {
	if maxBuf <= 0 {
		maxBuf = 100000
	}
	s := &Sequencer{st: st, window: window, idle: idle, maxBuf: maxBuf,
		actors: map[string]*sessionActor{}, quar: map[string]int{},
		stop: make(chan struct{}), jd: make(chan struct{})}
	if idle > 0 {
		go s.janitor()
	} else {
		close(s.jd)
	}
	return s
}

func (s *Sequencer) janitor() {
	defer close(s.jd)
	iv := min(max(s.idle/4, 5*time.Millisecond), 30*time.Second)
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.sweep(time.Now())
		case <-s.stop:
			return
		}
	}
}

// sweep evicts actors that are idle with nothing buffered. Their state is
// reloaded from the store on demand.
func (s *Sequencer) sweep(now time.Time) {
	var gone []*sessionActor
	s.mu.Lock()
	for id, a := range s.actors {
		if a.inflight > 0 || a.bufLen.Load() > 0 {
			continue
		}
		limit := s.idle * unsealedIdleFactor
		if a.sealed.Load() {
			limit = s.idle
		}
		if now.Sub(time.Unix(0, a.lastActive.Load())) >= limit {
			delete(s.actors, id)
			close(a.ch)
			gone = append(gone, a)
		}
	}
	s.mu.Unlock()
	for _, a := range gone {
		<-a.done
	}
}

// with runs f on the session's actor. With create unset an unknown session
// is not given an actor (false is returned); reads must not spawn goroutines.
// touch marks the session as recently used (keeps it from being evicted).
func (s *Sequencer) with(id string, create, touch bool, f func(*sessionActor)) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	a := s.actors[id]
	if a == nil {
		if !create {
			s.mu.Unlock()
			return false
		}
		a = newActor(id, s)
		s.actors[id] = a
	}
	a.inflight++
	if touch {
		a.lastActive.Store(time.Now().UnixNano())
	}
	s.mu.Unlock()
	a.run(f)
	s.mu.Lock()
	a.inflight--
	s.mu.Unlock()
	return true
}

func (s *Sequencer) actorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.actors)
}

func (s *Sequencer) addQuarantine(id string, n int) {
	s.qmu.Lock()
	s.quar[id] += n
	s.qmu.Unlock()
}

func (s *Sequencer) quarantined(id string) int {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.quar[id]
}

// Add submits events of one session.
func (s *Sequencer) Add(ctx context.Context, id string, evs []core.Event) (acc, dup int, err error) {
	if !s.with(id, true, true, func(a *sessionActor) { acc, dup, err = a.add(ctx, evs) }) {
		return 0, 0, errSeqClosed
	}
	return
}

// Status is a live snapshot of a session's sequencer state.
type Status struct {
	Pending     int
	Buffered    int
	Open        []string
	LastEventNs int64 // collector receive time of the latest ingest, unix ns
	EventCount  int
	Dropped     int64
	Quarantined int
}

func (a *sessionActor) status() Status {
	st := Status{Buffered: len(a.buf), LastEventNs: a.lastRecv, EventCount: a.count, Dropped: a.dropped,
		Quarantined: a.seqr.quarantined(a.id)}
	for k := range a.open {
		st.Open = append(st.Open, k)
	}
	sort.Strings(st.Open)
	st.Pending = len(a.open) + len(a.buf)
	return st
}

// Status reports a session's live state without creating an actor: unknown
// or evicted sessions are answered from the store.
func (s *Sequencer) Status(ctx context.Context, id string) (st Status, err error) {
	ok := s.with(id, false, false, func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		st = a.status()
	})
	if ok {
		return
	}
	t := &sessionActor{id: id, seqr: s, st: s.st}
	if err = t.load(ctx); err != nil {
		return Status{}, err
	}
	return t.status(), nil
}

// Close stops the janitor and every actor; buffered events are not flushed
// (seal first if they matter).
func (s *Sequencer) Close() {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if !already {
		close(s.stop)
		<-s.jd
	}
	s.evictAll()
}

// evictAll stops all actors; state reloads from the store on next use.
func (s *Sequencer) evictAll() {
	s.mu.Lock()
	gone := make([]*sessionActor, 0, len(s.actors))
	for id, a := range s.actors {
		close(a.ch)
		gone = append(gone, a)
		delete(s.actors, id)
	}
	s.mu.Unlock()
	for _, a := range gone {
		<-a.done
	}
}

// Flush forces the reorder window closed for an existing session and returns
// its status afterwards.
func (s *Sequencer) Flush(ctx context.Context, id string) (st Status, err error) {
	s.with(id, true, true, func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		if err = a.flushNow(); err != nil {
			return
		}
		st = a.status()
	})
	return
}
