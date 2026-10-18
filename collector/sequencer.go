package collector

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

type evKey struct {
	instance string
	local    uint64
}

type buffered struct {
	ev core.Event
	at time.Time
}

const (
	// isolateAfter is the number of consecutive failed append attempts after
	// which a batch is bisected to find (and quarantine) a poison event.
	isolateAfter = 3
	// unsealedIdleFactor: an idle actor of a session that was never sealed is
	// evicted after idle*unsealedIdleFactor (it reloads from the store).
	unsealedIdleFactor = 12
)

var (
	errBackpressure = errors.New("collector: session buffer full")
	errSeqClosed    = errors.New("collector: shutting down")
)

// sessionActor owns all mutable state of one session. Every operation runs
// on its single goroutine, which makes Seq assignment gapless by construction.
type sessionActor struct {
	id     string
	seqr   *Sequencer
	st     store.Store
	window time.Duration
	ch     chan func(*sessionActor)
	done   chan struct{}

	// guarded by Sequencer.mu
	inflight int
	// atomics read by the janitor
	lastActive atomic.Int64 // unix ns of the last write-ish use
	sealed     atomic.Bool
	bufLen     atomic.Int64

	loaded   bool
	next     uint64 // next Seq to assign
	seen     map[evKey]struct{}
	buf      []buffered
	timer    *time.Timer
	open     map[string]struct{} // queue.publish span ids awaiting a closing worker event
	closed   map[string]struct{} // ids closed before their publish was seen
	dropped  int64
	count    int
	lastRecv int64 // collector receive time (unix ns) of the latest ingest
	failures int   // consecutive failed append attempts
}

func newActor(id string, s *Sequencer) *sessionActor {
	a := &sessionActor{id: id, seqr: s, st: s.st, window: s.window,
		ch: make(chan func(*sessionActor)), done: make(chan struct{})}
	a.lastActive.Store(time.Now().UnixNano())
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
				a.arm(a.backoff()) // store trouble: retry shortly
			}
		}
	}
}

func (a *sessionActor) backoff() time.Duration {
	d := 50 * time.Millisecond
	for i := 1; i < a.failures && d < 2*time.Second; i++ {
		d *= 2
	}
	return d
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

func (a *sessionActor) setBuf(b []buffered) {
	a.buf = b
	a.bufLen.Store(int64(len(b)))
}

func (a *sessionActor) load(ctx context.Context) error {
	if a.loaded {
		return nil
	}
	si, err := a.st.GetSeal(ctx, a.id)
	if err != nil {
		return err
	}
	evs, err := a.st.Events(ctx, a.id, 0, 0)
	if err != nil {
		return err
	}
	a.seen = map[evKey]struct{}{}
	a.open = map[string]struct{}{}
	a.closed = map[string]struct{}{}
	a.next, a.count, a.dropped = 1, 0, 0
	for i := range evs {
		a.apply(&evs[i])
		a.next = evs[i].Seq + 1
	}
	a.sealed.Store(si != nil)
	a.loaded = true
	return nil
}

func closesEntry(e *core.Event) bool {
	switch e.Kind {
	case core.KindWorkerDLQ:
		return true
	case core.KindWorkerEnd:
		// A failed attempt with a retry scheduled only records the attempt.
		return e.Attrs["retry_pending"] != "true"
	}
	return false // worker.retry and everything else never close
}

// apply updates in-memory indexes for an event that has been persisted.
func (a *sessionActor) apply(e *core.Event) {
	a.seen[evKey{e.Instance, e.LocalSeq}] = struct{}{}
	a.count++
	switch e.Kind {
	case core.KindQueuePub:
		if _, ok := a.closed[e.SpanID]; ok {
			delete(a.closed, e.SpanID)
		} else {
			a.open[e.SpanID] = struct{}{}
		}
	case core.KindWorkerEnd, core.KindWorkerDLQ:
		if !closesEntry(e) {
			break
		}
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
	a.lastActive.Store(time.Now().UnixNano())
	if err = a.load(ctx); err != nil {
		return 0, 0, err
	}
	if len(a.buf) >= a.seqr.maxBuf {
		return 0, 0, errBackpressure
	}
	now := time.Now()
	a.lastRecv = now.UnixNano()
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
	a.bufLen.Store(int64(len(a.buf)))
	if len(a.buf) == 0 {
		return acc, dup, nil
	}
	if a.window <= 0 {
		if err = a.flushNow(); err != nil {
			a.arm(a.backoff())
		}
		return acc, dup, err
	}
	a.arm(a.window)
	return acc, dup, nil
}

// flushNow flushes, retrying a few times back-to-back so that a poison event
// is isolated within one call (used by seal and zero-window ingest).
func (a *sessionActor) flushNow() (err error) {
	for i := 0; i <= isolateAfter; i++ {
		if err = a.flush(); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

// flush releases every buffered event in (EndHLC, Service, LocalSeq) order.
// On failure the unpersisted events stay buffered; a persistently failing
// batch is bisected and the offending event quarantined.
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
	rest, err := a.persist(batch)
	if len(rest) == 0 {
		a.setBuf(nil)
		return nil
	}
	now := time.Now()
	nb := make([]buffered, len(rest))
	for i := range rest {
		nb[i] = buffered{rest[i], now}
	}
	a.setBuf(nb)
	return err
}

// tryAppend assigns consecutive Seqs and appends evs in one store call.
func (a *sessionActor) tryAppend(evs []core.Event) error {
	for i := range evs {
		evs[i].Seq = a.next + uint64(i)
	}
	if err := a.st.AppendEvents(context.Background(), a.id, evs); err != nil {
		return err
	}
	a.next += uint64(len(evs))
	for i := range evs {
		a.apply(&evs[i])
	}
	return nil
}

// persist appends batch. It returns the events that are neither stored nor
// quarantined (to be retried) and the error that stopped them.
func (a *sessionActor) persist(batch []core.Event) ([]core.Event, error) {
	var err error
	for try := 0; try < 4; try++ {
		if err = a.tryAppend(batch); err == nil {
			a.failures = 0
			return nil, nil
		}
		if !errors.Is(err, store.ErrSeqConflict) {
			break
		}
		// Another writer (e.g. a second collector) appended to this session:
		// catch up from the store, drop what it already stored, and retry.
		var cerr error
		if batch, cerr = a.catchUp(batch); cerr != nil {
			return batch, cerr
		}
		if len(batch) == 0 {
			a.failures = 0
			return nil, nil
		}
	}
	a.failures++
	if a.failures < isolateAfter {
		return batch, err
	}
	rest, err := a.isolate(batch, err)
	if len(rest) == 0 {
		a.failures = 0
	}
	return rest, err
}

// catchUp applies events another writer stored past a.next and returns batch
// without the events that are now already stored.
func (a *sessionActor) catchUp(batch []core.Event) ([]core.Event, error) {
	tail, err := a.st.Events(context.Background(), a.id, a.next, 0)
	if err != nil {
		return batch, err
	}
	stored := make(map[evKey]struct{}, len(tail))
	for i := range tail {
		stored[evKey{tail[i].Instance, tail[i].LocalSeq}] = struct{}{}
		a.apply(&tail[i])
		a.next = tail[i].Seq + 1
	}
	out := batch[:0:0]
	for i := range batch {
		if _, dup := stored[evKey{batch[i].Instance, batch[i].LocalSeq}]; !dup {
			out = append(out, batch[i])
		}
	}
	return out, nil
}

// isolate bisects a batch that keeps failing. A single event that still
// fails while the store answers reads is quarantined. If the store itself is
// unhealthy nothing is quarantined and the events stay buffered.
func (a *sessionActor) isolate(batch []core.Event, cause error) ([]core.Event, error) {
	err := a.tryAppend(batch)
	if err == nil {
		return nil, nil
	}
	if errors.Is(err, store.ErrSeqConflict) {
		return batch, err
	}
	if len(batch) == 1 {
		if _, perr := a.st.Events(context.Background(), a.id, a.next, 1); perr != nil {
			return batch, perr // store down, not a poison event
		}
		a.quarantine(&batch[0], err)
		return nil, nil
	}
	mid := len(batch) / 2
	r1, e1 := a.isolate(batch[:mid], cause)
	if len(r1) > 0 {
		return append(append([]core.Event(nil), r1...), batch[mid:]...), e1
	}
	return a.isolate(batch[mid:], cause)
}

func (a *sessionActor) quarantine(e *core.Event, err error) {
	a.seqr.addQuarantine(a.id, 1)
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	slog.Warn("quarantined poison event", "session", a.id, "span", e.SpanID, "kind", e.Kind,
		"instance", e.Instance, "local_seq", e.LocalSeq, "err", msg)
}

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
