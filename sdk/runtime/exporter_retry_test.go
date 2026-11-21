package runtime

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Collector modes for outageCollector.
const (
	modeUp        = iota
	modeDown      // 503, nothing stored
	modeAmbiguous // batch stored, but the answer is 503 (a lost response)
)

// outageCollector is a fake ingest endpoint that can be switched between
// healthy and failing. Like the real collector it is idempotent on local_seq:
// a resent event is stored once and counted in resent.
type outageCollector struct {
	mu       sync.Mutex
	mode     int
	seen     map[uint64]core.Event
	order    []uint64 // local_seq of stored events, in arrival order
	resent   int
	requests int
	failed   int
}

func newOutageCollector(t *testing.T, mode int) (*outageCollector, string) {
	t.Helper()
	c := &outageCollector{mode: mode, seen: map[uint64]core.Event{}}
	srv := httptest.NewServer(http.HandlerFunc(c.handle))
	t.Cleanup(srv.Close)
	return c, srv.URL
}

func (c *outageCollector) handle(w http.ResponseWriter, r *http.Request) {
	evs := readFrames(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	if c.mode != modeDown {
		for _, e := range evs {
			if _, dup := c.seen[e.LocalSeq]; dup {
				c.resent++
				continue
			}
			c.seen[e.LocalSeq] = e
			c.order = append(c.order, e.LocalSeq)
		}
	}
	if c.mode != modeUp {
		c.failed++
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprint(w, `{"accepted":1,"duplicates":0,"rejected":0}`)
}

func (c *outageCollector) setMode(m int) {
	c.mu.Lock()
	c.mode = m
	c.mu.Unlock()
}

func (c *outageCollector) failedRequests() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

// stored returns the stored non-drop events' local_seq in arrival order and
// the sum of the counts carried by stored sdk.dropped events.
func (c *outageCollector) stored() (seqs []uint64, dropped uint64, resent int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.order {
		e := c.seen[s]
		if e.Kind == core.KindSDKDropped {
			n, _ := strconv.ParseUint(e.Attrs["count"], 10, 64)
			dropped += n
			continue
		}
		seqs = append(seqs, s)
	}
	return seqs, dropped, c.resent
}

// seqBase keeps the tests' explicit local_seq values clear of the ones
// core.NextLocalSeq hands to sdk.dropped events.
const seqBase = 1 << 40

func recordSeqs(r *Recorder, from, to int) {
	for i := from; i <= to; i++ {
		r.Record(core.Event{SessionID: "s", Kind: core.KindAPIIn, Name: "n", LocalSeq: seqBase + uint64(i)})
	}
}

func wantSeqs(t *testing.T, got []uint64, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("stored %d events, want %d: %v", len(got), n, got)
	}
	for i, s := range got {
		if s != seqBase+uint64(i+1) {
			t.Fatalf("event %d has local_seq %d, want %d (order broken or event lost)", i, s-seqBase, i+1)
		}
	}
}

// A collector that answers 503 for longer than one round of retries must not
// cost any event: batches stay in the ring and are delivered, in order and
// once, when it recovers.
func TestExporterKeepsBatchesAcrossOutage(t *testing.T) {
	col, url := newOutageCollector(t, modeDown)
	r := NewRecorder(1000)
	x := NewExporter(r, ExporterConfig{CollectorURL: url, BatchSize: 10, FlushInterval: 5 * time.Millisecond,
		RetryBackoff: time.Millisecond, MaxCooldown: 20 * time.Millisecond})
	defer x.Close(time.Second)

	recordSeqs(r, 1, 50)
	// Well past the old limit of MaxRetries+1 attempts per batch.
	eventually(t, "several failed rounds", func() bool { return col.failedRequests() >= 9 })
	recordSeqs(r, 51, 100)
	if n := r.Len(); n != 100 {
		t.Fatalf("ring holds %d events during the outage, want 100", n)
	}
	col.setMode(modeUp)
	eventually(t, "delivery after recovery", func() bool { seqs, _, _ := col.stored(); return len(seqs) == 100 })
	if !x.Flush(time.Second) {
		t.Fatal("flush after recovery")
	}
	seqs, dropped, resent := col.stored()
	wantSeqs(t, seqs, 100)
	if dropped != 0 || resent != 0 {
		t.Fatalf("dropped=%d resent=%d, want 0 and 0", dropped, resent)
	}
	if left := r.Drain(10); len(left) != 0 {
		t.Fatalf("recorder still holds %+v", left)
	}
}

// Flush reports the truth: false while events are stuck behind an outage,
// true once they were delivered, and it keeps trying until its timeout.
func TestFlushReportsUndeliveredEvents(t *testing.T) {
	col, url := newOutageCollector(t, modeDown)
	r := NewRecorder(100)
	x := NewExporter(r, ExporterConfig{CollectorURL: url, FlushInterval: time.Hour, RetryBackoff: time.Millisecond})
	defer x.Close(100 * time.Millisecond)

	recordSeqs(r, 1, 3)
	if x.Flush(60 * time.Millisecond) {
		t.Fatal("Flush returned true during the outage")
	}
	if n := r.Len(); n != 3 {
		t.Fatalf("ring holds %d events after a failed flush, want 3", n)
	}
	if seqs, _, _ := col.stored(); len(seqs) != 0 {
		t.Fatalf("collector stored %v during the outage", seqs)
	}

	// Recovery in the middle of a Flush: it must notice within its timeout.
	go func() {
		time.Sleep(40 * time.Millisecond)
		col.setMode(modeUp)
	}()
	if !x.Flush(3 * time.Second) {
		t.Fatal("Flush returned false although the collector recovered within the timeout")
	}
	seqs, dropped, resent := col.stored()
	wantSeqs(t, seqs, 3)
	if dropped != 0 || resent != 0 || r.Len() != 0 {
		t.Fatalf("dropped=%d resent=%d len=%d", dropped, resent, r.Len())
	}
	if !x.Flush(time.Second) {
		t.Fatal("Flush of an empty ring")
	}
}

// A 400 is permanent: that batch is dropped and counted, without retries, and
// the batches behind it are delivered in the same pass.
func TestExporterDropsBatchOn400(t *testing.T) {
	var mu sync.Mutex
	var got []core.Event
	refused := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evs := readFrames(r.Body)
		mu.Lock()
		defer mu.Unlock()
		for _, e := range evs {
			if e.Attrs["bad"] == "1" {
				refused++
				http.Error(w, "bad batch", http.StatusBadRequest)
				return
			}
		}
		got = append(got, evs...)
		fmt.Fprint(w, `{"accepted":1,"duplicates":0,"rejected":0}`)
	}))
	defer srv.Close()
	r := NewRecorder(100)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, BatchSize: 2, FlushInterval: time.Hour, RetryBackoff: time.Millisecond})
	defer x.Close(time.Second)

	r.Record(core.Event{SessionID: "s", LocalSeq: seqBase + 1, Attrs: map[string]string{"bad": "1"}})
	recordSeqs(r, 2, 6)
	if !x.Flush(2 * time.Second) {
		t.Fatal("flush")
	}
	mu.Lock()
	defer mu.Unlock()
	if refused != 1 {
		t.Fatalf("refused batch was sent %d times, want 1 (no retry on 400)", refused)
	}
	var seqs []uint64
	var counts []string
	for _, e := range got {
		if e.Kind == core.KindSDKDropped {
			counts = append(counts, e.Attrs["count"])
		} else {
			seqs = append(seqs, e.LocalSeq-seqBase)
		}
	}
	if fmt.Sprint(seqs) != "[3 4 5 6]" {
		t.Fatalf("delivered %v, want [3 4 5 6]", seqs)
	}
	if fmt.Sprint(counts) != "[2]" {
		t.Fatalf("sdk.dropped counts %v, want [2]", counts)
	}
	if r.Len() != 0 {
		t.Fatalf("ring holds %d events", r.Len())
	}
}

// A collector that refuses even sdk.dropped events must not be hammered.
func TestExporterDoesNotSpinOnRefusedDropEvents(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		http.Error(w, "no", http.StatusBadRequest)
	}))
	defer srv.Close()
	r := NewRecorder(100)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, FlushInterval: time.Hour})
	defer x.Close(100 * time.Millisecond)
	recordSeqs(r, 1, 3)
	if !x.Flush(time.Second) { // the ring is empty: everything was refused
		t.Fatal("flush")
	}
	mu.Lock()
	n := requests
	mu.Unlock()
	if n != 2 { // the events, then their sdk.dropped event
		t.Fatalf("%d requests, want 2", n)
	}
	left := r.Drain(10)
	if len(left) != 1 || left[0].Kind != core.KindSDKDropped || left[0].Attrs["count"] != "3" {
		t.Fatalf("drop count not kept: %+v", left)
	}
}

// When the ring fills up during an outage the newest events are dropped, as
// before, and counted exactly once, even if the collector stored a batch
// whose answer was lost and the batch is sent again.
func TestRingOverflowDuringOutageCountsDrops(t *testing.T) {
	col, url := newOutageCollector(t, modeAmbiguous)
	r := NewRecorder(5)
	x := NewExporter(r, ExporterConfig{CollectorURL: url, BatchSize: 3, FlushInterval: 5 * time.Millisecond,
		RetryBackoff: time.Millisecond, MaxCooldown: 20 * time.Millisecond})
	defer x.Close(time.Second)

	recordSeqs(r, 1, 8) // 6, 7 and 8 do not fit
	eventually(t, "several failed rounds", func() bool { return col.failedRequests() >= 6 })
	if n := r.Len(); n != 5 {
		t.Fatalf("ring holds %d events during the outage, want 5", n)
	}
	col.setMode(modeUp)
	eventually(t, "delivery after recovery", func() bool { seqs, _, _ := col.stored(); return len(seqs) == 5 })
	if !x.Flush(time.Second) {
		t.Fatal("flush after recovery")
	}
	seqs, dropped, resent := col.stored()
	wantSeqs(t, seqs, 5) // the oldest five survive
	if dropped != 3 {
		t.Fatalf("sdk.dropped counts add up to %d, want 3", dropped)
	}
	if resent == 0 {
		t.Fatal("the stored batch was never resent; the test did not exercise deduplication")
	}
	if left := r.Drain(10); len(left) != 0 {
		t.Fatalf("recorder still holds %+v", left)
	}
}

func TestRecorderPeekCommit(t *testing.T) {
	r := NewRecorder(4)
	for i := 1; i <= 6; i++ { // 5 and 6 overflow
		r.Record(core.Event{SessionID: "s", LocalSeq: uint64(i)})
	}
	a := r.Peek(3)
	if len(a) != 3 || a[0].LocalSeq != 1 || a[2].LocalSeq != 3 || r.Len() != 4 {
		t.Fatalf("peek %+v len=%d", a, r.Len())
	}
	if b := r.Peek(3); len(b) != 3 || b[0].LocalSeq != 1 {
		t.Fatalf("second peek differs: %+v", b)
	}
	r.Commit(2)
	r.Record(core.Event{SessionID: "s", LocalSeq: 7}) // room again, wraps around
	rest := r.Peek(10)
	if len(rest) != 3 || rest[0].LocalSeq != 3 || rest[1].LocalSeq != 4 || rest[2].LocalSeq != 7 {
		t.Fatalf("after commit: %+v", rest)
	}
	d := r.TakeDrops()
	if len(d) != 1 || d[0].Kind != core.KindSDKDropped || d[0].Attrs["count"] != "2" {
		t.Fatalf("drops %+v", d)
	}
	if len(r.TakeDrops()) != 0 {
		t.Fatal("drop counters not reset")
	}
	r.Commit(100) // more than buffered: empties, does not go negative
	if r.Len() != 0 || len(r.Peek(10)) != 0 {
		t.Fatalf("len=%d", r.Len())
	}
}
