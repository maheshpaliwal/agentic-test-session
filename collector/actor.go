package collector

import (
	"context"
	"errors"
	"sort"
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
	quit   chan struct{} // closed to stop the loop (eviction, shutdown)
	done   chan struct{} // closed when the loop has exited

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
		ch: make(chan func(*sessionActor)), quit: make(chan struct{}), done: make(chan struct{})}
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
		case f := <-a.ch:
			f(a)
		case <-a.quit:
			a.shutdown()
			return
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

// shutdown runs on the actor goroutine when it is told to stop. Whatever is
// still inside the reorder window is flushed so that a graceful collector
// stop (or an eviction) never discards accepted events.
func (a *sessionActor) shutdown() {
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	if len(a.buf) == 0 {
		return
	}
	if err := a.flushNow(); err != nil {
		a.seqr.log.Error("events lost at shutdown: store rejected the final flush",
			"session", a.id, "events", len(a.buf), "err", err)
	}
}

// stop asks the loop to exit; wait on a.done for it to finish.
func (a *sessionActor) stop() { close(a.quit) }

// run executes f on the actor goroutine and waits for it. It reports false,
// without running f, when the actor has already stopped.
func (a *sessionActor) run(f func(*sessionActor)) bool {
	fin := make(chan struct{})
	select {
	case a.ch <- func(a *sessionActor) { f(a); close(fin) }:
		<-fin
		return true
	case <-a.done:
		return false
	}
}

// opCtx bounds one background store operation, so a hung store cannot wedge
// the actor (and with it every request for the session) forever.
func (a *sessionActor) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), a.seqr.storeTimeout)
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
		ctx, cancel := a.opCtx()
		err := a.load(ctx)
		cancel()
		if err != nil {
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
	ctx, cancel := a.opCtx()
	defer cancel()
	if err := a.st.AppendEvents(ctx, a.id, evs); err != nil {
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
	ctx, cancel := a.opCtx()
	defer cancel()
	tail, err := a.st.Events(ctx, a.id, a.next, 0)
	if err != nil {
		return batch, err
	}
	if len(tail) == 0 {
		return a.resync(ctx, batch)
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

// resync handles a conflict that no newer event explains: the store holds
// less than this actor believes (the session was deleted or restored behind
// its back). The in-memory state is rebuilt from the store and batch is
// returned without the events the store already has.
func (a *sessionActor) resync(ctx context.Context, batch []core.Event) ([]core.Event, error) {
	head, err := a.st.Head(ctx, a.id)
	if err != nil || head+1 == a.next {
		return batch, err // in step after all; the caller retries or gives up
	}
	a.loaded = false
	if err := a.load(ctx); err != nil {
		return batch, err
	}
	out := batch[:0:0]
	for i := range batch {
		k := evKey{batch[i].Instance, batch[i].LocalSeq}
		if _, dup := a.seen[k]; !dup {
			a.seen[k] = struct{}{} // still buffered: keep rejecting replays
			out = append(out, batch[i])
		}
	}
	return out, nil
}

// purge deletes the session from the store and forgets everything the actor
// holds for it. Running on the actor serialises it against appends.
func (a *sessionActor) purge(ctx context.Context) error {
	if err := a.st.DeleteSession(ctx, a.id); err != nil {
		return err
	}
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	a.setBuf(nil)
	a.loaded, a.failures, a.lastRecv = false, 0, 0
	a.sealed.Store(false)
	a.lastActive.Store(0) // nothing left to keep warm: evict at the next sweep
	return nil
}

// isolate bisects a batch that keeps failing. A single event that still
// fails while the store answers reads is quarantined. If the store itself is
// unhealthy nothing is quarantined and the events stay buffered.
func (a *sessionActor) isolate(batch []core.Event, cause error) ([]core.Event, error) {
	err := a.tryAppend(batch)
	if err == nil {
		return nil, nil
	}
	if errors.Is(err, store.ErrSeqConflict) || isTimeout(err) {
		// A conflict is resolved by catching up, and a timeout says the store
		// is slow, not that the event is bad: neither may cost an event.
		return batch, err
	}
	if len(batch) == 1 {
		ctx, cancel := a.opCtx()
		_, perr := a.st.Events(ctx, a.id, a.next, 1)
		cancel()
		if perr != nil {
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

func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (a *sessionActor) quarantine(e *core.Event, err error) {
	a.seqr.addQuarantine(a.id, 1)
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	a.seqr.log.Warn("quarantined poison event", "session", a.id, "span", e.SpanID, "kind", e.Kind,
		"instance", e.Instance, "local_seq", e.LocalSeq, "err", msg)
}
