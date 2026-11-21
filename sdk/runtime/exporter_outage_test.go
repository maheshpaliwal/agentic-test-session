package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// namingCollector is a fake ingest endpoint that fails sessions one by one,
// as the collector does: the events of the sessions it fails are not stored,
// those of the others are, and a failed request answers 503 with the failed
// sessions named. It is idempotent on local_seq.
type namingCollector struct {
	mu sync.Mutex
	// fail returns the sessions to fail (id to cause) for a request holding
	// events of sids (sorted). It runs with mu held. Nil fails nobody.
	fail func(sids []string) map[string]string
	// bare makes a failed request answer without a body, as a collector
	// that does not name sessions; truncated marks the list of failed
	// sessions as incomplete; lose are sessions whose events are neither
	// stored nor named.
	bare, truncated bool
	lose            map[string]bool
	maxEvents       int // a request with more events answers 413 (0: no limit)
	// silent is a session whose requests are stored whole and then answered
	// with a bare 503: an answer that got lost.
	silent    string
	seen      map[uint64]bool
	delivered map[string][]uint64 // local_seq stored per session, in arrival order
	dropped   map[string]uint64   // sum of the stored sdk.dropped counts per session
	requests  int
}

func newNamingCollector(t *testing.T) (*namingCollector, string) {
	t.Helper()
	c := &namingCollector{seen: map[uint64]bool{}, delivered: map[string][]uint64{}, dropped: map[string]uint64{}}
	srv := httptest.NewServer(http.HandlerFunc(c.handle))
	t.Cleanup(srv.Close)
	return c, srv.URL
}

func (c *namingCollector) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	evs, _ := decodeFrames(body)
	var sids []string
	for _, e := range evs {
		if !slices.Contains(sids, e.SessionID) {
			sids = append(sids, e.SessionID)
		}
	}
	slices.Sort(sids)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxEvents > 0 && len(evs) > c.maxEvents {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	c.requests++
	var failed map[string]string
	if c.fail != nil {
		failed = c.fail(sids)
	}
	silent := c.silent != "" && slices.Contains(sids, c.silent)
	if silent {
		failed = nil
	}
	acc, dup := 0, 0
	for _, e := range evs {
		if failed[e.SessionID] != "" || (len(failed) > 0 && c.lose[e.SessionID]) {
			continue
		}
		if c.seen[e.LocalSeq] {
			dup++
			continue
		}
		c.seen[e.LocalSeq] = true
		acc++
		if e.Kind == core.KindSDKDropped {
			n, _ := strconv.ParseUint(e.Attrs["count"], 10, 64)
			c.dropped[e.SessionID] += n
		} else {
			c.delivered[e.SessionID] = append(c.delivered[e.SessionID], e.LocalSeq)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if len(failed) == 0 && !silent {
		fmt.Fprintf(w, `{"accepted":%d,"duplicates":%d}`, acc, dup)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	if c.bare || silent {
		return
	}
	reply := map[string]any{"error": "retry later", "accepted": acc, "duplicates": dup, "failed_sessions": failed}
	if c.truncated {
		reply["failed_sessions_truncated"] = true
	}
	_ = json.NewEncoder(w).Encode(reply)
}

// failing returns a rule that fails the given sessions whenever a request
// holds their events.
func failing(bad ...string) func(sids []string) map[string]string {
	return func(sids []string) map[string]string {
		f := map[string]string{}
		for _, s := range sids {
			if slices.Contains(bad, s) {
				f[s] = "storage"
			}
		}
		return f
	}
}

func (c *namingCollector) setFail(f func(sids []string) map[string]string) {
	c.mu.Lock()
	c.fail = f
	c.mu.Unlock()
}

// count returns what the collector stored for a session and the drops
// reported for it, and fails the test if its events arrived out of order.
func (c *namingCollector) count(t *testing.T, sid string) (delivered int, dropped uint64) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.IsSorted(c.delivered[sid]) {
		t.Errorf("session %s: events were not delivered in the order they were recorded", sid)
	}
	return len(c.delivered[sid]), c.dropped[sid]
}

func failAll(sids []string, cause string) map[string]string {
	f := map[string]string{}
	for _, s := range sids {
		f[s] = cause
	}
	return f
}

// load is a session's event rate in an outage scenario.
type load struct {
	sid   string
	every int           // one burst every this many ticks
	burst int           // events per burst
	from  time.Duration // the session starts this far into the outage (scaled with it)
}

// Collector failures that do not single out one session for good must cost
// no more than they did when a failed request was always resent whole, which
// is nothing as long as the ring has room: a total outage, an outage that
// begins inside a request, one request taken in the middle of an outage, and
// sessions failing at random. Every session must end with delivered equal to
// recorded and no drops reported.
//
// By default the scenarios are scaled down (a short outage, a fast exporter).
// TSESSION_OUTAGE_SECS=30 runs them for that long at 100 events per second
// with the default exporter settings.
func TestExporterOutageScenarios(t *testing.T) {
	dur, tick, cfg := 1500*time.Millisecond, 2*time.Millisecond, ExporterConfig{FlushInterval: 2 * time.Millisecond, RetryBackoff: 2 * time.Millisecond, MaxCooldown: 100 * time.Millisecond}
	if v, err := strconv.Atoi(os.Getenv("TSESSION_OUTAGE_SECS")); err == nil && v > 0 {
		dur, tick, cfg = time.Duration(v)*time.Second, 10*time.Millisecond, ExporterConfig{}
	}
	x, y := load{sid: "X", every: 1, burst: 1}, load{sid: "Y", every: 10, burst: 1}
	type scenario struct {
		name  string
		loads []load
		// fail builds the failure rule for the outage; it runs under the
		// collector's lock.
		fail func() func(sids []string) map[string]string
	}
	scenarios := []scenario{
		{"total outage, one session", []load{x}, func() func([]string) map[string]string {
			return func(sids []string) map[string]string { return failAll(sids, "shutdown") }
		}},
		{"total outage, two sessions", []load{x, y}, func() func([]string) map[string]string {
			return func(sids []string) map[string]string { return failAll(sids, "shutdown") }
		}},
		// The outage begins inside a request: the first failing answer takes
		// Y and fails X, every later one fails everything.
		{"outage begins inside a request", []load{x, y}, func() func([]string) map[string]string {
			hit := false
			return func(sids []string) map[string]string {
				if !hit && len(sids) == 2 {
					hit = true
					return map[string]string{"X": "shutdown"}
				}
				return failAll(sids, "shutdown")
			}
		}},
		// Everything fails, except that one request of a session that starts
		// a third into the outage is taken.
		{"one request taken during an outage", []load{x, y, {sid: "Z", every: 10, burst: 1, from: dur / 3}}, func() func([]string) map[string]string {
			hit := false
			return func(sids []string) map[string]string {
				if !hit && len(sids) == 1 && sids[0] == "Z" {
					hit = true
					return nil
				}
				return failAll(sids, "storage")
			}
		}},
	}
	for _, p := range []float64{0.05, 0.2, 0.5} {
		random := func() func([]string) map[string]string {
			rng := rand.New(rand.NewSource(13))
			return func(sids []string) map[string]string {
				f := map[string]string{}
				for _, s := range sids {
					if rng.Float64() < p {
						f[s] = "storage"
					}
				}
				return f
			}
		}
		even := []load{{sid: "A", every: 2, burst: 1}, {sid: "B", every: 2, burst: 1}, {sid: "C", every: 2, burst: 1}, {sid: "D", every: 2, burst: 1}}
		busy := []load{{sid: "A", every: 1, burst: 2}, {sid: "B", every: 10, burst: 1}, {sid: "C", every: 10, burst: 1}, {sid: "D", every: 10, burst: 1}}
		scenarios = append(scenarios,
			scenario{fmt.Sprintf("sessions fail at random %.0f%%", p*100), even, random},
			scenario{fmt.Sprintf("sessions fail at random %.0f%%, one busy", p*100), busy, random})
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			col, url := newNamingCollector(t)
			c := cfg
			c.CollectorURL = url
			rec := NewRecorder(0)
			exp := NewExporter(rec, c)
			defer exp.Close(time.Second)
			want := map[string]int{}
			var seq uint64
			record := func(sid string) {
				seq++
				rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, Name: "op", LocalSeq: seqBase + seq})
				want[sid]++
			}
			// Every session that exists from the start is delivered once
			// before the outage, so the collector has taken it before.
			for _, l := range sc.loads {
				if l.from == 0 {
					record(l.sid)
				}
			}
			if !exp.Flush(5 * time.Second) {
				panic("warm-up flush failed")
			}
			col.setFail(sc.fail())
			col.mu.Lock()
			before := col.requests
			col.mu.Unlock()
			start := time.Now()
			ticker := time.NewTicker(tick)
			highWater := 0
			for n := 1; time.Since(start) < dur; n++ {
				<-ticker.C
				for _, l := range sc.loads {
					if time.Since(start) >= l.from && n%l.every == 0 {
						for range l.burst {
							record(l.sid)
						}
					}
				}
				highWater = max(highWater, rec.Len())
			}
			ticker.Stop()
			col.mu.Lock()
			during := col.requests - before
			col.mu.Unlock()
			col.setFail(nil)
			flushed := exp.Flush(60 * time.Second)
			if !flushed {
				t.Error("Flush did not empty the ring after the outage")
			}
			recorded, lost := 0, 0
			for _, l := range sc.loads {
				delivered, dropped := col.count(t, l.sid)
				recorded, lost = recorded+want[l.sid], lost+want[l.sid]-delivered
				if delivered != want[l.sid] || dropped != 0 {
					t.Errorf("session %s: recorded %d, delivered %d, reported dropped %d", l.sid, want[l.sid], delivered, dropped)
				}
			}
			t.Logf("recorded %d, lost %d, ring high-water %d of %d, %d requests during the outage", recorded, lost, highWater, len(rec.buf), during)
		})
	}
}
