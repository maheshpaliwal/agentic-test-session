package collector

import (
	"context"
	"sort"
	"sync"
	"time"

	"tsession/collector/store"
	"tsession/core"
)

type evKey struct {
	instance string
	local    uint64
}

type buffered struct {
	ev core.Event
	at time.Time
}

// sessionActor owns all mutable state of one session. Every operation runs
// on its single goroutine, which makes Seq assignment gapless by construction.
type sessionActor struct {
	id     string
	st     store.Store
	window time.Duration
	ch     chan func(*sessionActor)
	done   chan struct{}

	loaded    bool
	next      uint64 // next Seq to assign
	seen      map[evKey]struct{}
	buf       []buffered
	timer     *time.Timer
	open      map[string]struct{} // queue.publish span ids awaiting worker.end
	closed    map[string]struct{} // ids closed before their publish was seen
	dropped   int64
	count     int
	lastEvent int64
}

func newActor(id string, st store.Store, window time.Duration) *sessionActor {
	a := &sessionActor{id: id, st: st, window: window,
		ch: make(chan func(*sessionActor)), done: make(chan struct{})}
	go a.loop()
	return a
}

func (a *sessionActor) loop() {
	defer close(a.done)
	var tc <-chan time.Time
	for {
		if a.timer != nil {
			tc = a.timer.C
		} else {
			tc = nil
		}
		select {
		case f, ok := <-a.ch:
			if !ok {
				if a.timer != nil {
					a.timer.Stop()
				}
				return
			}
			f(a)
		case <-tc:
			a.timer = nil
			if err := a.flush(); err != nil {
				a.arm(50 * time.Millisecond) // store trouble: retry shortly
			}
		}
	}
}

// run executes f on the actor goroutine and waits for it.
func (a *sessionActor) run(f func(*sessionActor)) {
	fin := make(chan struct{})
	a.ch <- func(a *sessionActor) { f(a); close(fin) }
	<-fin
}

func (a *sessionActor) arm(d time.Duration) {
	if a.timer == nil {
		a.timer = time.NewTimer(d)
	}
}

func (a *sessionActor) load(ctx context.Context) error {
	if a.loaded {
		return nil
	}
	evs, err := a.st.Events(ctx, a.id, 0, 0)
	if err != nil {
		return err
	}
	a.seen = map[evKey]struct{}{}
	a.open = map[string]struct{}{}
	a.closed = map[string]struct{}{}
	a.next = 1
	for i := range evs {
		a.apply(&evs[i])
		a.next = evs[i].Seq + 1
	}
	a.loaded = true
	return nil
}

// apply updates in-memory indexes for an event that has been persisted.
func (a *sessionActor) apply(e *core.Event) {
	a.seen[evKey{e.Instance, e.LocalSeq}] = struct{}{}
	a.count++
	if e.EndNs > a.lastEvent {
		a.lastEvent = e.EndNs
	}
	switch e.Kind {
	case core.KindQueuePub:
		if _, ok := a.closed[e.SpanID]; ok {
			delete(a.closed, e.SpanID)
		} else {
			a.open[e.SpanID] = struct{}{}
		}
	case core.KindWorkerEnd:
		for _, l := range e.LinkIDs {
			if _, ok := a.open[l]; ok {
				delete(a.open, l)
			} else {
				a.closed[l] = struct{}{}
			}
		}
	case core.KindSDKDropped:
		var n int64
		for _, c := range e.Attrs["count"] {
			if c < '0' || c > '9' {
				n = 0
				break
			}
			n = n*10 + int64(c-'0')
		}
		a.dropped += n
	}
}

func less(x, y *core.Event) bool {
	if x.EndHLC != y.EndHLC {
		return x.EndHLC < y.EndHLC
	}
	if x.Service != y.Service {
		return x.Service < y.Service
	}
	if x.Instance != y.Instance {
		return x.Instance < y.Instance
	}
	return x.LocalSeq < y.LocalSeq
}

// add buffers events (dropping duplicates) and flushes immediately when the
// window is zero. Returns accepted and duplicate counts.
func (a *sessionActor) add(ctx context.Context, evs []core.Event) (acc, dup int, err error) {
	if err = a.load(ctx); err != nil {
		return 0, 0, err
	}
	now := time.Now()
	for i := range evs {
		k := evKey{evs[i].Instance, evs[i].LocalSeq}
		if _, ok := a.seen[k]; ok {
			dup++
			continue
		}
		a.seen[k] = struct{}{}
		evs[i].Seq = 0
		a.buf = append(a.buf, buffered{evs[i], now})
		acc++
	}
	if len(a.buf) == 0 {
		return acc, dup, nil
	}
	if a.window <= 0 {
		if err = a.flush(); err != nil {
			a.arm(50 * time.Millisecond)
		}
		return acc, dup, err
	}
	a.arm(a.window)
	return acc, dup, nil
}

// flush releases every buffered event in (EndHLC, Service, LocalSeq) order.
func (a *sessionActor) flush() error {
	if len(a.buf) == 0 {
		return nil
	}
	if !a.loaded {
		if err := a.load(context.Background()); err != nil {
			return err
		}
	}
	batch := make([]core.Event, len(a.buf))
	for i := range a.buf {
		batch[i] = a.buf[i].ev
	}
	sort.SliceStable(batch, func(i, j int) bool { return less(&batch[i], &batch[j]) })
	for i := range batch {
		batch[i].Seq = a.next + uint64(i)
	}
	if err := a.st.AppendEvents(context.Background(), a.id, batch); err != nil {
		return err
	}
	a.next += uint64(len(batch))
	a.buf = nil
	for i := range batch {
		a.apply(&batch[i])
	}
	return nil
}

// Sequencer routes events to per-session actors.
type Sequencer struct {
	st     store.Store
	window time.Duration
	mu     sync.Mutex
	actors map[string]*sessionActor
}

func newSequencer(st store.Store, window time.Duration) *Sequencer {
	return &Sequencer{st: st, window: window, actors: map[string]*sessionActor{}}
}

func (s *Sequencer) actor(id string) *sessionActor {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.actors[id]
	if a == nil {
		a = newActor(id, s.st, s.window)
		s.actors[id] = a
	}
	return a
}

// Add submits events of one session.
func (s *Sequencer) Add(ctx context.Context, id string, evs []core.Event) (acc, dup int, err error) {
	s.actor(id).run(func(a *sessionActor) { acc, dup, err = a.add(ctx, evs) })
	return
}

// Status is a live snapshot of a session's sequencer state.
type Status struct {
	Pending     int
	Buffered    int
	Open        []string
	LastEventNs int64
	EventCount  int
	Dropped     int64
}

func (s *Sequencer) Status(ctx context.Context, id string) (st Status, err error) {
	s.actor(id).run(func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		st = Status{Buffered: len(a.buf), LastEventNs: a.lastEvent, EventCount: a.count, Dropped: a.dropped}
		for k := range a.open {
			st.Open = append(st.Open, k)
		}
		sort.Strings(st.Open)
		st.Pending = len(a.open) + len(a.buf)
	})
	return
}

// Flush forces the reorder window closed and returns the status afterwards.
func (s *Sequencer) Flush(ctx context.Context, id string) (st Status, err error) {
	s.actor(id).run(func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		if err = a.flush(); err != nil {
			return
		}
		st = Status{LastEventNs: a.lastEvent, EventCount: a.count, Dropped: a.dropped}
		for k := range a.open {
			st.Open = append(st.Open, k)
		}
		sort.Strings(st.Open)
		st.Pending = len(st.Open)
	})
	return
}

// Close stops all actors, flushing nothing further.
func (s *Sequencer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.actors {
		close(a.ch)
		<-a.done
	}
	s.actors = map[string]*sessionActor{}
}
