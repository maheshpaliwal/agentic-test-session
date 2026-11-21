// Package runtime wires the core seams to a control plane (active-set stream)
// and a collector (batched protobuf export). Nothing here blocks the host.
package runtime

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Recorder is a bounded ring buffer implementing core.Recorder. Record never
// blocks; when full the event is dropped and counted per session.
type Recorder struct {
	mu     sync.Mutex
	buf    []slot
	head   int
	n      int
	nextID uint64
	drops  map[string]uint64
	// held are the sessions the collector keeps failing while it accepts
	// others (set by the exporter). Their events are the first to go when
	// the ring is full, see evictLocked.
	held   map[string]struct{}
	wake   chan struct{}
	wakeAt int
}

// slot is one buffered event. id numbers the events in the order they were
// recorded, so the ring is always sorted by it. sent marks an event that was
// part of a request: the collector may have it even if no answer said so,
// so it is never dropped to make room (it would count as delivered and as
// dropped).
type slot struct {
	ev   core.Event
	id   uint64
	sent bool
}

// NewRecorder creates a ring holding up to size events.
func NewRecorder(size int) *Recorder {
	if size <= 0 {
		size = 8192
	}
	return &Recorder{buf: make([]slot, size), drops: map[string]uint64{}, wake: make(chan struct{}, 1), wakeAt: size / 2}
}

// at returns the i-th oldest slot.
func (r *Recorder) at(i int) *slot { return &r.buf[(r.head+i)%len(r.buf)] }

// Record implements core.Recorder.
func (r *Recorder) Record(e core.Event) {
	r.mu.Lock()
	if r.n == len(r.buf) && !r.evictLocked(e.SessionID) {
		r.drops[e.SessionID]++
		r.mu.Unlock()
		return
	}
	r.nextID++
	s := r.at(r.n) // an empty slot: sent is false
	s.ev, s.id = e, r.nextID
	r.n++
	full := r.n >= r.wakeAt
	r.mu.Unlock()
	if full {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
}

// evictScan bounds how far evictLocked looks, and with it the time Record
// can take with the ring full: an eviction moves the events behind the one
// it drops, which was measured at about 30µs for 1000 of them, against tens
// of nanoseconds for dropping the new event. That price is paid only while
// the ring is full and a session is held. Held events further back than
// this are out of reach, and then the new event is dropped as before.
const evictScan = 1024

// evictLocked makes room in a full ring for an event of session sid by
// dropping (and counting) the newest buffered event of a held session that
// was never sent, and reports whether it did. A full ring costs one event
// per new event either way; this only decides that a session the collector
// accepts does not pay for one it keeps failing. Like the ring it gives up
// the newest event of the session that pays. An event of a held session,
// and a ring whose last evictScan events hold none to drop, fall back to
// dropping the new event.
func (r *Recorder) evictLocked(sid string) bool {
	if len(r.held) == 0 {
		return false
	}
	if _, ok := r.held[sid]; ok {
		return false
	}
	for i := r.n - 1; i >= max(0, r.n-evictScan); i-- {
		s := r.at(i)
		if _, ok := r.held[s.ev.SessionID]; !ok || s.sent {
			continue
		}
		r.drops[s.ev.SessionID]++
		for j := i; j < r.n-1; j++ {
			*r.at(j) = *r.at(j + 1)
		}
		r.n--
		*r.at(r.n) = slot{}
		return true
	}
	return false
}

// setHeld replaces the set of held sessions. The recorder keeps the map: the
// caller must not change it afterwards.
func (r *Recorder) setHeld(held map[string]struct{}) {
	r.mu.Lock()
	r.held = held
	r.mu.Unlock()
}

// take returns copies of the oldest limit events recorded as from or later
// whose session want accepts, with their ids, and marks them as sent. next
// is the id of the first of them, or the id the next
// recorded event will get when there are none: no event before next is
// wanted. take and settle are for a single consumer (the exporter goroutine).
func (r *Recorder) take(from uint64, limit int, want func(sid string) bool) (evs []core.Event, ids []uint64, next uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := r.find(from); i < r.n && len(evs) < limit; i++ {
		s := r.at(i)
		if !want(s.ev.SessionID) {
			continue
		}
		if evs == nil {
			k := min(limit, r.n-i)
			evs, ids = make([]core.Event, 0, k), make([]uint64, 0, k)
		}
		s.sent = true
		evs, ids = append(evs, s.ev), append(ids, s.id)
	}
	if len(ids) == 0 {
		return nil, nil, r.nextID + 1
	}
	return evs, ids, ids[0]
}

// find returns the position of the first buffered event with that id or a
// later one.
func (r *Recorder) find(id uint64) int {
	return sort.Search(r.n, func(i int) bool { return r.at(i).id >= id })
}

// settle ends a take: of the taken events (ids as take returned them), those
// in remove (in the same order) leave the ring, the others stay buffered.
func (r *Recorder) settle(taken, remove []uint64) {
	if len(taken) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Sent events are never evicted, so every id is still in the ring, at or
	// after the position of the one before it.
	gone := make([]int, 0, len(remove))
	i := r.find(taken[0])
	for _, id := range taken {
		for i < r.n && r.at(i).id < id {
			i++
		}
		rm := len(remove) > 0 && remove[0] == id
		if rm {
			remove = remove[1:]
		}
		if i == r.n || r.at(i).id != id {
			continue // removed behind the exporter's back (Commit, Drain)
		}
		if rm {
			gone = append(gone, i)
		}
	}
	r.removeLocked(gone)
}

// removeLocked removes the events at the given ascending positions and keeps
// the others in order, moving whichever side of the ring is shorter: the
// events before the last one removed (the usual case, a batch from the front,
// moves none), or those after the first.
func (r *Recorder) removeLocked(gone []int) {
	k := len(gone)
	if k == 0 {
		return
	}
	if first, last := gone[0], gone[k-1]; last+1-k <= r.n-first-k {
		w := last
		for i, g := last, k-1; i >= 0; i-- {
			if g >= 0 && gone[g] == i {
				g--
				continue
			}
			*r.at(w) = *r.at(i)
			w--
		}
		for i := range k {
			*r.at(i) = slot{}
		}
		r.head = (r.head + k) % len(r.buf)
	} else {
		w := first
		for i, g := first, 0; i < r.n; i++ {
			if g < k && gone[g] == i {
				g++
				continue
			}
			*r.at(w) = *r.at(i)
			w++
		}
		for i := r.n - k; i < r.n; i++ {
			*r.at(i) = slot{}
		}
	}
	r.n -= k
}

// takeDrops is TakeDrops for the sessions want accepts.
func (r *Recorder) takeDrops(want func(sid string) bool) []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []core.Event
	for sid, cnt := range r.drops {
		if want(sid) {
			out = append(out, dropEvent(sid, cnt))
			delete(r.drops, sid)
		}
	}
	return out
}

// AddDrops counts events that were given up on after they were handed to the
// exporter (a permanent export failure). Counts carried by sdk.dropped events
// are preserved.
func (r *Recorder) AddDrops(evs []core.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range evs {
		if e.Kind == core.KindSDKDropped {
			c, _ := strconv.ParseUint(e.Attrs["count"], 10, 64)
			r.drops[e.SessionID] += c
		} else {
			r.drops[e.SessionID]++
		}
	}
}

// AddDropCount adds n dropped events (e.g. rejected by the collector) to a session.
func (r *Recorder) AddDropCount(sessionID string, n uint64) {
	r.mu.Lock()
	r.drops[sessionID] += n
	r.mu.Unlock()
}

// Len is the number of buffered events.
func (r *Recorder) Len() int { r.mu.Lock(); defer r.mu.Unlock(); return r.n }

// Peek returns a copy of the oldest max events without removing them. They
// stay in the ring, in the same order, until Commit: a full ring drops new
// events, never buffered ones, so what Peek returned is still at the front.
// Peek and Commit are for a single consumer, and not for a ring an Exporter
// reads: that one uses take and settle.
func (r *Recorder) Peek(max int) []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peekLocked(max)
}

// Commit removes the oldest n events, i.e. the first n of the last Peek.
func (r *Recorder) Commit(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitLocked(n)
}

// TakeDrops returns one sdk.dropped event per session with a pending drop
// count and resets the counters. The caller owns the counts from here on: it
// either delivers the events or gives the counts back with AddDrops.
func (r *Recorder) TakeDrops() []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.takeDropsLocked(nil)
}

// Drain removes up to max events and appends one sdk.dropped event per session
// with a pending drop count. The exporter does not use it: there an event
// leaves the ring only once it was delivered.
func (r *Recorder) Drain(max int) []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.peekLocked(max)
	r.commitLocked(len(out))
	return r.takeDropsLocked(out)
}

func (r *Recorder) peekLocked(limit int) []core.Event {
	k := max(0, min(limit, r.n))
	out := make([]core.Event, 0, k+len(r.drops))
	for i := range k {
		out = append(out, r.at(i).ev)
	}
	return out
}

func (r *Recorder) commitLocked(n int) {
	n = max(0, min(n, r.n))
	for range n {
		r.buf[r.head] = slot{}
		r.head = (r.head + 1) % len(r.buf)
	}
	r.n -= n
}

func (r *Recorder) takeDropsLocked(out []core.Event) []core.Event {
	for sid, cnt := range r.drops {
		out = append(out, dropEvent(sid, cnt))
	}
	clear(r.drops)
	return out
}

// dropEvent is the sdk.dropped event that reports cnt dropped events of a
// session.
func dropEvent(sid string, cnt uint64) core.Event {
	c := core.GetConfig()
	now := time.Now().UnixNano()
	h := core.HLCNow()
	return core.Event{
		SessionID: sid, TraceID: core.NewTraceID(), SpanID: core.NewSpanID(),
		Service: c.Service, Instance: c.Instance, Kind: core.KindSDKDropped,
		HLC: h, EndHLC: h, LocalSeq: core.NextLocalSeq(), StartNs: now, EndNs: now,
		Attrs: map[string]string{"count": strconv.FormatUint(cnt, 10)},
	}
}

// dropsPending reports whether a drop count is waiting to be reported.
func (r *Recorder) dropsPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.drops) > 0
}
