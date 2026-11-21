package collector

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
	"github.com/maheshpaliwal/agentic-test-session/internal/ident"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
)

// Sequencer routes events to per-session actors and evicts idle ones.
type Sequencer struct {
	st     store.Store
	window time.Duration
	idle   time.Duration
	maxBuf int
	// storeTimeout bounds each background store call made by an actor.
	storeTimeout time.Duration
	// shutdownTimeout is how long Close lets the actors flush before bg is
	// cancelled.
	shutdownTimeout time.Duration
	// bg is the parent of every background store call; cancelling it ends
	// them all at once.
	bg     context.Context
	cancel context.CancelFunc
	log    *slog.Logger
	tel    *telemetry

	mu     sync.Mutex
	actors map[string]*sessionActor
	closed bool

	qmu  sync.Mutex
	quar map[string]int

	// memo holds the status of sessions without a live actor, valid while the
	// store's head Seq is unchanged, so polling an evicted session does not
	// read all of its events every time.
	memoMu  sync.Mutex
	memo    map[string]statusMemo
	memoGen uint64 // bumped by forgetStatus; a status computed across one is not kept

	stop chan struct{}
	jd   chan struct{}
}

// newSequencer expects cfg to have been normalised by New.
func newSequencer(st store.Store, cfg Config, tel *telemetry) *Sequencer {
	s := &Sequencer{st: st, window: cfg.Window, idle: cfg.IdleEvict, maxBuf: cfg.MaxBufferedEvents,
		storeTimeout: cfg.StoreTimeout, shutdownTimeout: cfg.ShutdownTimeout, log: cfg.Logger, tel: tel,
		actors: map[string]*sessionActor{}, quar: map[string]int{},
		stop: make(chan struct{}), jd: make(chan struct{})}
	s.bg, s.cancel = context.WithCancel(context.Background())
	if s.idle > 0 {
		// Started again after a panic, without limit: without it no idle
		// actor is ever evicted, and the collector's memory only grows.
		gosafe.Go(gosafe.Options{Name: "janitor", Restarts: -1, Stop: s.stop, Done: func() { close(s.jd) }, OnPanic: tel.onPanic(s.log)}, s.janitor)
	} else {
		close(s.jd)
	}
	return s
}

// janitor may be started again after a panic: it closes nothing.
func (s *Sequencer) janitor() {
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
			a.stop()
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
	ok := a.run(f)
	s.mu.Lock()
	a.inflight--
	s.mu.Unlock()
	return ok
}

// drop stops and forgets a session's actor right away when nothing is using
// it. Deleting a session must not leave a goroutine behind until (or, with
// eviction disabled, forever instead of) the next idle sweep.
func (s *Sequencer) drop(id string) {
	s.mu.Lock()
	a := s.actors[id]
	if a == nil || a.inflight > 0 || a.bufLen.Load() > 0 {
		s.mu.Unlock()
		return
	}
	delete(s.actors, id)
	a.stop()
	s.mu.Unlock()
	<-a.done
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
	s.tel.quarantined.Add(n)
}

// buffered is the number of events sitting in reorder windows.
func (s *Sequencer) buffered() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, a := range s.actors {
		n += a.bufLen.Load()
	}
	return n
}

func (s *Sequencer) forgetQuarantine(id string) {
	s.qmu.Lock()
	delete(s.quar, id)
	s.qmu.Unlock()
}

// maxStatusMemo bounds the status memo; it is simply emptied when full.
const maxStatusMemo = 256

type statusMemo struct {
	head uint64
	st   Status
}

func (s *Sequencer) forgetStatus(id string) {
	s.memoMu.Lock()
	delete(s.memo, id)
	s.memoGen++
	s.memoMu.Unlock()
}

// has reports whether the session currently has a live actor.
func (s *Sequencer) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.actors[id] != nil
}

func (s *Sequencer) quarantined(id string) int {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.quar[id]
}

// bound returns ctx cancelled as well when the shutdown budget runs out, so
// a request still inside the store cannot hold Close past it.
func (s *Sequencer) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.bg, cancel)
	return ctx, func() { stop(); cancel() }
}

// Add submits events of one session.
func (s *Sequencer) Add(ctx context.Context, id string, evs []core.Event) (acc, dup int, err error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
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
	// Instances counts the session's events by the service and instance that
	// recorded them, sorted by both; InstancesTruncated says that there were
	// more than MaxStatusInstances and the rest is left out.
	Instances          []InstanceCount
	InstancesTruncated bool
	// all holds every instance's count when Instances was cut short; never
	// changed once the status is built.
	all map[instKey]int
}

// InstanceCount is how many events of a session the collector has from one
// SDK instance: those it stored and those still in the reorder window. An
// event is counted once however often it was sent (duplicates are dropped on
// instance and local_seq). The sdk.dropped events an SDK sends to report its
// drop counts are not events of the session and are not counted.
type InstanceCount struct {
	Service string `json:"service,omitempty"`
	// Instance is the instance of the events: the id the SDK made unique for
	// its process, which is also the one its delivery reports carry.
	Instance string `json:"instance"`
	Events   int    `json:"events"`
}

// MaxStatusInstances bounds Status.Instances, and with it the size of a
// drain-status response.
const MaxStatusInstances = 512

// instKey identifies the SDK instance an event came from.
type instKey struct{ service, instance string }

// countsForInstance reports whether e is an event its instance recorded for
// the session, as opposed to the instance's report of a drop count.
func countsForInstance(e *core.Event) bool { return e.Kind != core.KindSDKDropped }

func (a *sessionActor) status() Status {
	st := Status{Buffered: len(a.buf), LastEventNs: a.lastRecv, EventCount: a.count, Dropped: a.dropped,
		Quarantined: a.seqr.quarantined(a.id)}
	st.Open = mapx.SortedKeys(a.open)
	st.Pending = len(a.open) + len(a.buf)
	st.Instances, st.all = a.instances()
	st.InstancesTruncated = st.all != nil
	return st
}

// instances lists what the collector has of the session per instance: the
// stored events and the ones accepted into the reorder window. When there
// are more than MaxStatusInstances the list holds the first of them and all
// is a copy of every count.
func (a *sessionActor) instances() (list []InstanceCount, all map[instKey]int) {
	counts := a.inst
	if len(a.buf) > 0 {
		if counts = maps.Clone(a.inst); counts == nil {
			counts = map[instKey]int{}
		}
		for i := range a.buf {
			if e := &a.buf[i].ev; countsForInstance(e) {
				counts[instKey{e.Service, e.Instance}]++
			}
		}
	}
	out := make([]InstanceCount, 0, len(counts))
	for k, n := range counts {
		out = append(out, InstanceCount{Service: k.service, Instance: k.instance, Events: n})
	}
	slices.SortFunc(out, func(x, y InstanceCount) int {
		return cmp.Or(cmp.Compare(x.Service, y.Service), cmp.Compare(x.Instance, y.Instance))
	})
	if len(out) > MaxStatusInstances {
		if len(a.buf) == 0 {
			counts = maps.Clone(counts) // a.inst itself: the actor goes on changing it
		}
		return out[:MaxStatusInstances:MaxStatusInstances], counts
	}
	return out, nil
}

// countsOf answers how many events the status holds from each instance asked
// about, in order. Names are compared the way the control plane compares
// them (ident.Clean): it knows an instance by its cleaned name, and two
// instances whose names clean to the same are one to it.
func (st Status) countsOf(ask []InstanceCount) []InstanceCount {
	clean := func(service, instance string) instKey { return instKey{ident.Clean(service), ident.Clean(instance)} }
	sum := make(map[instKey]int, len(st.Instances))
	if st.all != nil {
		for k, n := range st.all {
			sum[clean(k.service, k.instance)] += n
		}
	} else {
		for _, i := range st.Instances {
			sum[clean(i.Service, i.Instance)] += i.Events
		}
	}
	out := make([]InstanceCount, len(ask))
	for i, q := range ask {
		out[i] = InstanceCount{Service: q.Service, Instance: q.Instance, Events: sum[clean(q.Service, q.Instance)]}
	}
	return out
}

// Status reports a session's live state without creating an actor: unknown
// or evicted sessions are answered from the store.
func (s *Sequencer) Status(ctx context.Context, id string) (st Status, err error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()
	ok := s.with(id, false, false, func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		st = a.status()
	})
	if ok {
		return
	}
	head, err := s.st.Head(ctx, id)
	if err != nil {
		return Status{}, err
	}
	s.memoMu.Lock()
	m, hit := s.memo[id]
	gen := s.memoGen
	s.memoMu.Unlock()
	if hit && m.head == head {
		m.st.Quarantined = s.quarantined(id)
		return m.st, nil
	}
	t := &sessionActor{id: id, seqr: s, st: s.st}
	if err = t.load(ctx); err != nil {
		return Status{}, err
	}
	st = t.status()
	if t.next == head+1 { // the load saw exactly the head it is filed under
		s.memoMu.Lock()
		if s.memoGen == gen {
			if s.memo == nil || len(s.memo) >= maxStatusMemo {
				s.memo = map[string]statusMemo{}
			}
			s.memo[id] = statusMemo{head: head, st: st}
		}
		s.memoMu.Unlock()
	}
	return st, nil
}

// Close stops the janitor and every actor. Each actor first flushes what is
// still in its reorder window, so a graceful stop loses nothing the store
// accepts within the shutdown timeout. The actors flush concurrently and
// share that one deadline, so Close returns shortly after it at the latest
// and a session whose store calls hang takes no time from the others.
func (s *Sequencer) Close() {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if !already {
		close(s.stop)
		// The deadline also ends a window flush that was already hanging in
		// the store when Close was called.
		t := time.AfterFunc(s.shutdownTimeout, s.cancel)
		defer s.cancel()
		defer t.Stop()
		<-s.jd
	}
	s.evictAll()
}

// evictAll stops all actors; state reloads from the store on next use.
func (s *Sequencer) evictAll() {
	s.mu.Lock()
	gone := make([]*sessionActor, 0, len(s.actors))
	for id, a := range s.actors {
		a.stop()
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
	ctx, cancel := s.bound(ctx)
	defer cancel()
	ok := s.with(id, true, true, func(a *sessionActor) {
		if err = a.load(ctx); err != nil {
			return
		}
		if err = a.flushNow(); err != nil {
			return
		}
		st = a.status()
	})
	if !ok {
		return Status{}, errSeqClosed
	}
	return
}
