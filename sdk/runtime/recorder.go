// Package runtime wires the core seams to a control plane (active-set stream)
// and a collector (batched NDJSON export). Nothing here blocks the host.
package runtime

import (
	"strconv"
	"sync"
	"time"

	"tsession/core"
)

// Recorder is a bounded ring buffer implementing core.Recorder. Record never
// blocks; when full the event is dropped and counted per session.
type Recorder struct {
	mu     sync.Mutex
	buf    []core.Event
	head   int
	n      int
	drops  map[string]uint64
	wake   chan struct{}
	wakeAt int
}

// NewRecorder creates a ring holding up to size events.
func NewRecorder(size int) *Recorder {
	if size <= 0 {
		size = 8192
	}
	return &Recorder{buf: make([]core.Event, size), drops: map[string]uint64{}, wake: make(chan struct{}, 1), wakeAt: size / 2}
}

// Record implements core.Recorder.
func (r *Recorder) Record(e core.Event) {
	r.mu.Lock()
	if r.n == len(r.buf) {
		r.drops[e.SessionID]++
		r.mu.Unlock()
		return
	}
	r.buf[(r.head+r.n)%len(r.buf)] = e
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

// AddDrops counts events that were lost after leaving the ring (export
// failure). Counts carried by sdk.dropped events are preserved.
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

// Len is the number of buffered events.
func (r *Recorder) Len() int { r.mu.Lock(); defer r.mu.Unlock(); return r.n }

// Drain removes up to max events and appends one sdk.dropped event per session
// with a pending drop count.
func (r *Recorder) Drain(max int) []core.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := min(max, r.n)
	out := make([]core.Event, 0, k+len(r.drops))
	for i := 0; i < k; i++ {
		out = append(out, r.buf[r.head])
		r.buf[r.head] = core.Event{}
		r.head = (r.head + 1) % len(r.buf)
	}
	r.n -= k
	if len(r.drops) > 0 {
		c := core.GetConfig()
		now := time.Now().UnixNano()
		for sid, cnt := range r.drops {
			h := core.HLCNow()
			out = append(out, core.Event{
				SessionID: sid, TraceID: core.NewTraceID(), SpanID: core.NewSpanID(),
				Service: c.Service, Instance: c.Instance, Kind: core.KindSDKDropped,
				HLC: h, EndHLC: h, LocalSeq: core.NextLocalSeq(), StartNs: now, EndNs: now,
				Attrs: map[string]string{"count": strconv.FormatUint(cnt, 10)},
			})
		}
		r.drops = map[string]uint64{}
	}
	return out
}
