package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/scrub"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// ExporterConfig tunes batching and retry. Zero values get defaults.
type ExporterConfig struct {
	CollectorURL  string
	BatchSize     int           // events per POST (default 256)
	FlushInterval time.Duration // default 100ms
	MaxRetries    int           // extra attempts per batch in one pass (default 2)
	RetryBackoff  time.Duration // first backoff, doubles (default 100ms)
	MaxCooldown   time.Duration // cap on the pause between passes while the collector is failing (default 5s)
	HTTPTimeout   time.Duration // per request (default 2s)
	Encoder       BatchEncoder  // wire format (default ProtobufEncoder)
}

// Exporter ships batches from a Recorder to the collector from a single
// background goroutine. An event leaves the ring only once the collector
// accepted it or refused it for good; while the collector is unreachable or
// answers 5xx/429 it stays buffered and is retried with backoff, so events
// are lost only when the ring overflows.
//
// A collector that fails single sessions says which (failed_sessions in the
// body of a failed request). Those sessions are then left out of the batches
// and retried on their own backoff, so the others keep flowing; their events
// stay in the ring like everything else. See drain.
type Exporter struct {
	cfg    ExporterConfig
	rec    *Recorder
	client *http.Client

	flushReq chan chan bool
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	// ctx is cancelled by Close so a request in flight to an unresponsive
	// collector is abandoned instead of holding up shutdown.
	ctx    context.Context
	cancel context.CancelFunc
	// lastErr is why the last request to the collector failed, nil once one
	// succeeded. The delivery reporter reads it.
	lastErr atomic.Pointer[string]
	// urgent are the sessions whose events go first (see setUrgent).
	urgent atomic.Pointer[map[string]struct{}]

	// Everything below is touched by the export goroutine only.

	failures int
	nextTry  time.Time
	// down and refusalLogged make the exporter say once when delivery stops,
	// once when it resumes, and once when the collector refuses events.
	down          bool
	outageLogged  bool      // the current outage was logged, so its recovery is too
	lastOutageLog time.Time // when an outage was last logged
	refusalLogged bool
	heldLogged    bool      // sessions being held were logged, so their release is too
	lastHeldLog   time.Time // when held sessions were last logged

	// failing are the sessions the collector failed by name and has not
	// accepted since.
	failing map[string]*failingSession
	// drops are the sdk.dropped events taken from the recorder and not
	// delivered yet, one per session. Each is resent unchanged (same
	// local_seq, so the collector deduplicates) until it is resolved.
	drops map[string]core.Event
	// freshFrom is a ring id below which every buffered event belongs to a
	// failing session, so that a pass need not walk over them again.
	freshFrom uint64
	pass      uint64 // number of the current drain pass
	left      int    // events the last pass had to leave in the ring
}

// failingSession is the state of a session the collector failed by name.
type failingSession struct {
	failures int
	nextTry  time.Time
	pass     uint64 // the last pass that sent its events
	// okFirst and okLast are the first and the last time the collector
	// accepted events of other sessions since this one began to fail, with no
	// outage in between.
	okFirst, okLast time.Time
	// held is set once the others were accepted for MaxCooldown while this
	// session kept failing. It lasts until the session is accepted.
	held bool
}

// NewExporter starts the export goroutine.
func NewExporter(rec *Recorder, cfg ExporterConfig) *Exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	} else if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.MaxCooldown <= 0 {
		cfg.MaxCooldown = 5 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 2 * time.Second
	}
	if cfg.Encoder == nil {
		cfg.Encoder = ProtobufEncoder{}
	}
	cfg.CollectorURL = strings.TrimRight(cfg.CollectorURL, "/")
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exporter{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		rec:      rec,
		client:   &http.Client{Timeout: cfg.HTTPTimeout, Transport: newPlainTransport()},
		flushReq: make(chan chan bool),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		failing:  map[string]*failingSession{},
		drops:    map[string]core.Event{},
	}
	startLoop("exporter", "recorded events are no longer sent to the collector (they are dropped as the buffer fills)",
		e.stop, e.loop, func() { close(e.done) }, func(final bool) {
			if final {
				// The reporter goes on: the sessions' delivery reports say why
				// their events did not arrive.
				msg := "the SDK's exporter stopped after an internal error (it panicked repeatedly); restart the service"
				e.lastErr.Store(&msg)
			}
		})
	return e
}

func newPlainTransport() *http.Transport {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return &http.Transport{}
}

// loop may be started again after a panic (startLoop): it closes nothing.
// Every pass builds its requests from the ring anew, so a pass that panicked
// half way loses no event.
func (e *Exporter) loop() {
	t := time.NewTicker(e.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-t.C:
			if time.Now().After(e.nextTry) {
				e.drain(false)
			}
		case <-e.rec.wake:
			// The ring is half full. When part of that is events of failing
			// sessions, which a pass cannot send, every new event wakes the
			// exporter: wait for a batch worth of others, or for the ticker.
			if time.Now().After(e.nextTry) && (e.left == 0 || e.rec.Len() >= e.left+e.cfg.BatchSize) {
				e.drain(false)
			}
		case ack := <-e.flushReq:
			ack <- e.drain(true) // buffered: never blocks on a caller that gave up
		}
	}
}

const (
	// maxBarren is how many requests in a row of one pass may fail for
	// every session they hold before the pass treats it as an outage.
	maxBarren = 2
	// maxFreshRun is how many batches of sessions that are not failing a
	// pass sends before it looks at the failing ones that are due.
	maxFreshRun = 16
)

// drain is one export pass. It sends batches until nothing is left to send
// (true) or the collector fails (false). An event stays in the ring until it
// is resolved, and every request is built from the ring again, so a resend
// carries the same events with the same local_seq.
//
// What a failed request means depends on what the collector says:
//
//   - It names no session of the request (an older collector, a 429, no
//     answer at all): an outage. The pass ends and the next one is delayed
//     by a growing cooldown, as it always was.
//   - It names sessions: those are failing. They are left out of the
//     following requests and retried when their own backoff has passed (the
//     same schedule as the cooldown), oldest events first, so one session's
//     events are always sent in the order they were recorded. If the answer
//     also counts accepted or duplicate events, the events of the sessions
//     it does not name are resolved; otherwise they are sent again.
//
// Sessions that are not failing go first, so a failing one cannot delay them
// by more than its own request. A failing session costs nothing but ring
// space; only one that kept failing while others were accepted for
// MaxCooldown is held, which makes its newest unsent events the ones a full
// ring drops (see Recorder.evictLocked). force retries every failing session
// regardless of its backoff (Flush).
func (e *Exporter) drain(force bool) bool {
	e.pass++
	accepted, barren, freshRun := false, 0, 0
	for {
		select {
		case <-e.stop:
			return false
		default:
		}
		// Sessions that are not failing go first, but may not keep the
		// failing ones from being retried for a whole pass under load.
		var b *batch
		if freshRun >= maxFreshRun {
			b, freshRun = e.retryBatch(force), 0
		}
		if b == nil {
			if b = e.freshBatch(); b != nil {
				freshRun++
			} else if b = e.retryBatch(force); b == nil {
				break
			}
		}
		e.send(b, b.all())
		lost := e.resolve(b)
		if !b.total && len(lost) > 0 && !e.refusalLogged {
			e.refusalLogged = true
			log.Printf("tsession: the collector at %s refused events and they were dropped; check that TSESSION_COLLECTOR_URL points at a tsession collector of a matching version", DisplayURL(e.cfg.CollectorURL))
		}
		switch {
		case b.total:
			e.cooldown(true)
			return false
		case len(b.ids) == 0 && len(lost) > 0:
			// A refused batch that held only sdk.dropped events had its counts
			// returned to the recorder, which would hand them straight back:
			// pause like for an outage instead of spinning.
			e.cooldown(false)
			e.left = e.rec.Len()
			return e.left == 0 && len(e.drops) == 0
		case b.accepted:
			accepted, barren = true, 0
			e.up()
		case !b.retry:
			// Every session of the request failed. One such request can be a
			// batch that happened to hold only failing sessions; several in a
			// row are an outage in which the collector names what it fails.
			if barren++; barren >= maxBarren {
				e.cooldown(true)
				return false
			}
		}
	}
	if barren > 0 && !accepted {
		e.noteDown()
	}
	e.left = e.rec.Len()
	return e.left == 0 && len(e.drops) == 0 && !e.rec.dropsPending()
}

// batch is one request's worth of events and what became of them.
type batch struct {
	evs   []core.Event
	ids   []uint64 // ring ids of evs[:len(ids)]; the others are sdk.dropped events
	retry bool     // holds failing sessions that were due, not fresh ones
	state []eventState
	// failed are the sessions the collector named as failed.
	failed set.Set[string]
	// accepted says that the collector took events; total that a request
	// failed without the collector blaming a session.
	accepted, total bool
	// rejected is how many of the events it answered for the collector
	// rejected (see countRejected).
	rejected int
}

type eventState uint8

const (
	eventPending   eventState = iota // still to be delivered
	eventDelivered                   // accepted by the collector
	eventLost                        // refused for good
)

func (b *batch) all() []int {
	idx := make([]int, len(b.evs))
	for i := range idx {
		idx[i] = i
	}
	return idx
}

func (b *batch) mark(idx []int, st eventState) {
	for _, i := range idx {
		b.state[i] = st
	}
}

// freshBatch takes the oldest events of the sessions that are not failing,
// those of the urgent sessions before the others. Nil means there are none.
func (e *Exporter) freshBatch() *batch {
	fresh := func(sid string) bool { return e.failing[sid] == nil }
	b := &batch{}
	if u := e.urgent.Load(); u != nil && len(*u) > 0 {
		first := func(sid string) bool { _, ok := (*u)[sid]; return ok && fresh(sid) }
		// The events before the first urgent one stay: freshFrom is not moved.
		if b.evs, b.ids, _ = e.rec.take(e.freshFrom, e.cfg.BatchSize, first); len(b.evs) > 0 {
			return e.ready(b, first)
		}
	}
	b.evs, b.ids, e.freshFrom = e.rec.take(e.freshFrom, e.cfg.BatchSize, fresh)
	return e.ready(b, fresh)
}

// setUrgent names the sessions whose events are sent before those of the
// others: the draining ones, which the control plane is waiting for, so that
// they do not queue behind the backlog of sessions that are still running. A
// session's own events keep their order. The map must not change afterwards.
func (e *Exporter) setUrgent(sessions map[string]struct{}) { e.urgent.Store(&sessions) }

// retryBatch takes the oldest events of the failing sessions that are due
// (all of them with force) and were not tried in this pass. Nil means there
// are none.
func (e *Exporter) retryBatch(force bool) *batch {
	now := time.Now()
	due := func(sid string) bool {
		f := e.failing[sid]
		return f != nil && f.pass != e.pass && (force || !now.Before(f.nextTry))
	}
	anyDue := false
	for sid := range e.failing {
		anyDue = anyDue || due(sid)
	}
	if !anyDue {
		return nil // leave the ring alone
	}
	// Sent events cannot be dropped to make room (see Recorder.evictLocked):
	// a retry marks at most a quarter of the ring.
	b := &batch{retry: true}
	b.evs, b.ids, _ = e.rec.take(0, min(e.cfg.BatchSize, max(1, len(e.rec.buf)/4)), due)
	if e.ready(b, due) != nil {
		return b
	}
	// A due session with nothing buffered is not failing any more. An event
	// of it that is recorded from now on is found like any new one.
	held := false
	for sid, f := range e.failing {
		if due(sid) {
			delete(e.failing, sid)
			held = held || f.held
		}
	}
	if held {
		e.syncHeld()
	}
	return nil
}

// ready completes a batch with the sdk.dropped events of the sessions want
// accepts: those taken earlier and not delivered yet, and new ones for the
// counts the recorder holds. It returns nil for a batch without events.
func (e *Exporter) ready(b *batch, want func(sid string) bool) *batch {
	for _, d := range e.rec.takeDrops(func(sid string) bool {
		_, taken := e.drops[sid]
		return !taken && want(sid)
	}) {
		e.drops[d.SessionID] = d
	}
	for sid, d := range e.drops {
		if want(sid) {
			b.evs = append(b.evs, d)
		}
	}
	if len(b.evs) == 0 {
		return nil
	}
	b.state, b.failed = make([]eventState, len(b.evs)), set.Set[string]{}
	return b
}

// resolve gives the batch back to the recorder (what was delivered or
// refused leaves the ring, the rest stays) and updates the failing sessions.
// It returns the events that were refused for good, already counted as drops.
func (e *Exporter) resolve(b *batch) (lost []core.Event) {
	var gone []uint64
	// sent are the sessions of the batch; open those with an unresolved
	// event, took those with a delivered one.
	sent, open, took := set.Set[string]{}, set.Set[string]{}, set.Set[string]{}
	// o counts the ring events the collector answered for, for the delivery
	// accounts. An sdk.dropped event is not an event of the session.
	o := outcome{delivered: map[string]uint64{}, rejected: uint64(b.rejected)}
	for i := range b.evs {
		sid := b.evs[i].SessionID
		sent.Add(sid)
		if b.state[i] == eventPending {
			open.Add(sid)
			continue
		}
		if b.state[i] == eventDelivered {
			took.Add(sid)
			if i < len(b.ids) {
				if o.delivered[sid] == 0 {
					o.order = append(o.order, sid)
				}
				o.delivered[sid]++
			}
		} else {
			lost = append(lost, b.evs[i])
		}
		if i < len(b.ids) {
			gone = append(gone, b.ids[i])
		} else {
			delete(e.drops, sid)
		}
	}
	o.lost = lost
	e.rec.settle(b.ids, gone, &o)

	now := time.Now()
	heldChanged := false
	for sid := range sent {
		f := e.failing[sid]
		switch {
		case b.failed.Has(sid):
			if f == nil {
				f = &failingSession{}
				e.failing[sid] = f
			}
			f.failures++
			f.nextTry = now.Add(e.backoff(f.failures))
			// Held only now that it failed again: the others were accepted
			// for long enough before that.
			if !f.held && !f.okFirst.IsZero() && f.okLast.Sub(f.okFirst) >= e.cfg.MaxCooldown {
				f.held, heldChanged = true, true
			}
		case f != nil && took.Has(sid) && !open.Has(sid):
			// Accepted again. Its other events may lie anywhere in the ring.
			delete(e.failing, sid)
			e.freshFrom = 0
			heldChanged = heldChanged || f.held
			continue
		}
		if f != nil {
			f.pass = e.pass
		}
	}
	// Whether the collector accepts other sessions while these fail.
	for _, f := range e.failing {
		switch {
		case b.accepted:
			if f.okFirst.IsZero() {
				f.okFirst = now
			}
			f.okLast = now
		case !f.held && (b.total || !b.retry):
			f.okFirst, f.okLast = time.Time{}, time.Time{}
		}
	}
	if heldChanged {
		e.syncHeld()
	}
	return lost
}

// backoff is the pause after the n-th failure in a row, for the collector as
// a whole and for a single failing session alike.
func (e *Exporter) backoff(n int) time.Duration {
	return min(e.cfg.FlushInterval<<min(n, 6), e.cfg.MaxCooldown)
}

// cooldown delays the next pass. outage says that the collector failed (and
// not merely refused drop counts), which is logged.
func (e *Exporter) cooldown(outage bool) {
	if outage {
		e.noteDown()
	}
	e.failures++
	e.nextTry = time.Now().Add(e.backoff(e.failures))
}

// noteDown records that nothing is being delivered. A wrong collector URL
// otherwise shows up only as an empty session. At most one outage (and its
// recovery) is logged per outageLogGap, so a flapping collector cannot flood
// the host's log.
func (e *Exporter) noteDown() {
	if e.down {
		return
	}
	e.down = true
	if now := time.Now(); e.lastOutageLog.IsZero() || now.Sub(e.lastOutageLog) >= outageLogGap {
		e.lastOutageLog, e.outageLogged = now, true
		log.Printf("tsession: cannot deliver events to the collector at %s; they stay buffered and are retried, and are dropped when the buffer fills", DisplayURL(e.cfg.CollectorURL))
	}
}

// up records that the collector accepted events.
func (e *Exporter) up() {
	if e.down {
		e.down = false
		if e.outageLogged {
			e.outageLogged = false
			log.Printf("tsession: delivering events to the collector at %s again", DisplayURL(e.cfg.CollectorURL))
		}
	}
	e.failures = 0
	e.nextTry = time.Time{}
}

// syncHeld tells the recorder which sessions are held, and logs when the
// first becomes held and when none is left, at most once per outageLogGap.
// Session ids are not logged.
func (e *Exporter) syncHeld() {
	held := map[string]struct{}{}
	for sid, f := range e.failing {
		if f.held {
			held[sid] = struct{}{}
		}
	}
	e.rec.setHeld(held)
	switch now := time.Now(); {
	case len(held) > 0 && !e.heldLogged && (e.lastHeldLog.IsZero() || now.Sub(e.lastHeldLog) >= outageLogGap):
		e.lastHeldLog, e.heldLogged = now, true
		log.Printf("tsession: the collector at %s keeps failing some sessions while it accepts others; their events stay buffered and are retried, and are the first to be dropped when the buffer fills", DisplayURL(e.cfg.CollectorURL))
	case len(held) == 0 && e.heldLogged:
		e.heldLogged = false
		log.Printf("tsession: the collector at %s accepts all sessions again", DisplayURL(e.cfg.CollectorURL))
	}
}

// outageLogGap is the least time between two logged outages.
const outageLogGap = time.Minute

// DisplayURL returns raw in a form that is safe to log: without userinfo,
// query or fragment, any of which can carry a credential.
func DisplayURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparseable URL>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

type postResult int

const (
	postOK        postResult = iota
	postTooLarge             // 413: bisect
	postPermanent            // refused for good (4xx, encode error): drop
	postTransient            // unreachable, 5xx, 429: keep and retry later
	postFailed               // 5xx or 429 that names the sessions it failed
)

// answer is what one request came to.
type answer struct {
	result postResult
	// With postFailed: the sessions of the request the collector named,
	// whether it took events of others, and whether that resolves the events
	// of every session it did not name.
	failed          set.Set[string]
	took, tookOther bool
	// rejected is the number of events the answer rejected, when it was
	// counted as dropped (see countRejected).
	rejected int
}

// send ships the events of b at idx in order and records what became of
// each: delivered, refused for good, or still pending. It reports false when
// a request failed without the collector blaming a session; the rest is then
// not sent. A 413 bisects, so one oversize event only drops itself.
func (e *Exporter) send(b *batch, idx []int) bool {
	// A session that failed earlier in this batch is not sent again in it:
	// its later events must not overtake the ones that failed.
	if len(b.failed) > 0 {
		idx = slicex.Filter(idx, func(i int) bool { return !b.failed.Has(b.evs[i].SessionID) })
	}
	if len(idx) == 0 {
		return true
	}
	evs := b.evs // the usual case: the whole batch
	if len(idx) < len(evs) {
		evs = slicex.Map(idx, func(i int) core.Event { return b.evs[i] })
	}
	a := e.post(evs)
	if a.result == postTooLarge {
		if len(idx) > 1 {
			mid := len(idx) / 2
			return e.send(b, idx[:mid]) && e.send(b, idx[mid:])
		}
		if a = e.post([]core.Event{oversizeStub(evs[0])}); a.result == postTooLarge {
			a.result = postPermanent
		}
	}
	b.rejected += a.rejected
	switch a.result {
	case postOK:
		b.accepted = true
		b.mark(idx, eventDelivered)
	case postFailed:
		for sid := range a.failed {
			b.failed.Add(sid)
		}
		b.accepted = b.accepted || a.took
		if a.tookOther {
			b.mark(slicex.Filter(idx, func(i int) bool { return !a.failed.Has(b.evs[i].SessionID) }), eventDelivered)
		}
	case postPermanent:
		b.mark(idx, eventLost)
	default:
		b.total = true
		return false
	}
	return true
}

// oversizeStub replaces an event the collector refuses as too large with a
// minimal one that keeps its identity (so ordering/idempotency still hold).
func oversizeStub(ev core.Event) core.Event {
	return core.Event{
		SessionID: ev.SessionID, TraceID: ev.TraceID, SpanID: ev.SpanID, ParentID: ev.ParentID,
		Service: ev.Service, Instance: ev.Instance, Kind: ev.Kind, Name: ev.Name,
		HLC: ev.HLC, EndHLC: ev.EndHLC, LocalSeq: ev.LocalSeq,
		StartNs: ev.StartNs, EndNs: ev.EndNs, TxID: ev.TxID, Attempt: ev.Attempt,
		Err:   "event exceeded collector size limit; payload dropped",
		Flags: []string{"oversize_dropped"},
	}
}

// ingestAnswer is what the exporter reads of an ingest response body.
type ingestAnswer struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
	Rejected   int `json:"rejected"`
	// On a failed request: the sessions it failed, and whether more failed
	// than are listed.
	FailedSessions          map[string]string `json:"failed_sessions"`
	FailedSessionsTruncated bool              `json:"failed_sessions_truncated"`
}

func (e *Exporter) post(batch []core.Event) answer {
	body, err := e.cfg.Encoder.Encode(batch)
	if err != nil {
		return answer{result: postPermanent} // encoding is deterministic; retrying will not help
	}
	inst := core.GetConfig().Instance
	backoff := e.cfg.RetryBackoff
	for attempt := 0; attempt <= e.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
			case <-e.stop:
				return answer{result: postTransient}
			}
			backoff *= 2
		}
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, e.cfg.CollectorURL+"/v1/ingest", bytes.NewReader(body))
		if err != nil {
			e.noteErr("the collector URL is malformed")
			return answer{result: postPermanent} // retrying will not help
		}
		req.Header.Set("Content-Type", e.cfg.Encoder.ContentType())
		req.Header.Set("X-TSession-Instance", inst)
		resp, err := e.client.Do(req)
		if err != nil {
			e.noteErr(scrubError(err))
			continue
		}
		rb, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		var ir ingestAnswer
		parsed := rerr == nil && json.Unmarshal(rb, &ir) == nil
		switch {
		case resp.StatusCode/100 == 2:
			e.lastErr.Store(nil)
			a := answer{result: postOK}
			if parsed {
				a.rejected = e.countRejected(batch, ir.Rejected, nil)
			}
			return a
		case resp.StatusCode == http.StatusRequestEntityTooLarge:
			return answer{result: postTooLarge}
		case resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests:
			e.noteErr(fmt.Sprintf("the collector refused the events (HTTP %d)", resp.StatusCode))
			return answer{result: postPermanent}
		}
		e.noteErr(fmt.Sprintf("the collector answered HTTP %d", resp.StatusCode))
		if !parsed {
			continue
		}
		if a, ok := failedSessions(batch, ir); ok {
			if a.tookOther {
				a.rejected = e.countRejected(batch, ir.Rejected, a.failed)
			}
			return a // the sessions' own backoff is the retry
		}
	}
	return answer{result: postTransient}
}

// failedSessions reads a failed request's answer for the sessions it blames.
// ok is false when it names no session of the batch: that is an outage, not
// the failure of a session. The events of the sessions it does not name are
// resolved (tookOther) only when the answer leaves no doubt: it counts events
// as accepted or duplicate, which a collector counts for the sessions that
// did not fail only, and its list of failed sessions is complete. A session
// is never taken to be accepted merely because it is not named.
func failedSessions(batch []core.Event, ir ingestAnswer) (a answer, ok bool) {
	a = answer{result: postFailed, failed: set.Set[string]{}}
	others := false
	for i := range batch {
		if _, named := ir.FailedSessions[batch[i].SessionID]; named {
			a.failed.Add(batch[i].SessionID)
		} else {
			others = true
		}
	}
	a.took = others && ir.Accepted+ir.Duplicates > 0
	a.tookOther = a.took && !ir.FailedSessionsTruncated
	return a, len(a.failed) > 0
}

// countRejected adds the events the collector rejected, in an answer that
// took events of the batch, to the drop count of the first session it took.
// It does not count for batches made only of sdk.dropped events, or a
// collector that rejects them would feed itself forever. It returns the
// number it counted.
func (e *Exporter) countRejected(batch []core.Event, rejected int, failed set.Set[string]) int {
	if rejected <= 0 {
		return 0
	}
	for i := range batch {
		if batch[i].Kind != core.KindSDKDropped && !failed.Has(batch[i].SessionID) {
			e.rec.AddDropCount(batch[i].SessionID, uint64(rejected))
			return rejected
		}
	}
	return 0
}

// noteErr remembers why a request failed, for the delivery report. A request
// cut short by Close says nothing about the collector.
func (e *Exporter) noteErr(msg string) {
	if e.ctx.Err() == nil {
		e.lastErr.Store(&msg)
	}
}

// lastError is why the last request to the collector failed, "" when it did
// not.
func (e *Exporter) lastError() string {
	if p := e.lastErr.Load(); p != nil {
		return *p
	}
	return ""
}

// scrubError is the text of a failed request without what a URL may carry
// of credentials (userinfo, query, fragment): the URL of the request, and any
// other the text names. It is cut to what a delivery report holds.
func scrubError(err error) string {
	msg := err.Error()
	var ue *url.Error
	if errors.As(err, &ue) {
		msg = ue.Op + " " + DisplayURL(ue.URL) + ": " + ue.Err.Error()
	}
	msg = scrub.Text(msg)
	if len(msg) > maxReportError {
		msg = msg[:maxReportError]
	}
	return msg
}

// kick asks for one forced pass (every failing session is retried, whatever
// its backoff or the cooldown) without waiting for its result. It does
// nothing while a pass is running. It waits a moment for the export
// goroutine: a send that does not wait at all misses it whenever that is
// awake for its own ticker, and two tickers started together fire together.
func (e *Exporter) kick() {
	t := time.NewTimer(kickWait)
	defer t.Stop()
	select {
	case e.flushReq <- make(chan bool, 1):
	case <-e.done:
	case <-t.C:
	}
}

const kickWait = 10 * time.Millisecond

// Flush exports everything buffered and reports whether the ring was emptied
// within timeout. While the collector is failing, as a whole or for single
// sessions, it keeps trying (every failing session on each pass), pausing
// RetryBackoff between passes, and returns false when the time is up with
// events still buffered (they stay buffered for the background loop).
func (e *Exporter) Flush(timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		ack := make(chan bool, 1)
		select {
		case e.flushReq <- ack:
		case <-e.done:
			return false
		case <-deadline.C:
			return false
		}
		select {
		case ok := <-ack:
			if ok {
				return true
			}
		case <-deadline.C:
			return false
		}
		select {
		case <-time.After(e.cfg.RetryBackoff):
		case <-e.done:
			return false
		case <-deadline.C:
			return false
		}
	}
}

// Close flushes (bounded by timeout) and stops the goroutine. Events the
// collector did not accept by then are discarded with the exporter.
func (e *Exporter) Close(timeout time.Duration) {
	e.once.Do(func() {
		e.Flush(timeout)
		close(e.stop)
		e.cancel()
		<-e.done
	})
}
