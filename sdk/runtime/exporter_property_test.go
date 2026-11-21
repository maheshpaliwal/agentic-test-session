package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// propBase keeps the property test's local_seq values clear of the ones
// core.NextLocalSeq hands to sdk.dropped events.
const propBase = uint64(1) << 41

// What the model collector does to every request while a mode lasts, on top
// of the sessions it fails by their own schedule.
const (
	propNormal         = iota
	propDown           // 503 without a body, nothing stored
	propAllNamed       // 503 naming every session, nothing stored
	propRefuse         // 400: the events are refused for good
	propConnDrop       // the connection is closed, before or after storing
	propSlowBefore     // stores late, around the client's timeout
	propSlowAfter      // answers late, around the client's timeout
	propCutBody        // the answer's body is cut short
	propStoreThenError // stores everything, then answers a bare 500
	propGhostName      // names a session that is not in the request as well
	propDupKey         // names a failed session twice
	propEmptyNames     // fails sessions and names none
	propTruncated      // marks the list of failed sessions as truncated
	propNamesOn200     // answers 200 and names a session anyway
	propModes
)

// lostAnswer reports whether the mode can store events without the exporter
// learning of it. Such events may be stored and, later, counted as dropped.
func lostAnswer(mode int) bool {
	switch mode {
	case propConnDrop, propSlowBefore, propSlowAfter, propCutBody, propStoreThenError, propGhostName, propEmptyNames, propTruncated:
		return true
	}
	return false
}

// propSession is how one session fails in the model collector.
type propSession struct {
	kind   int     // 0 never, 1 always, 2 flapping by request, 3 flapping by time, 4 at random, 5 for its first requests
	period int     // requests or milliseconds, for kinds 2, 3 and 5
	p      float64 // for kind 4
	n      int     // requests seen
	cause  string
}

// propCollector is a model of the collector's ingest endpoint for one case.
type propCollector struct {
	mu      sync.Mutex
	rng     *rand.Rand
	start   time.Time
	sess    map[string]*propSession
	mode    int
	healed  bool
	maxBody int // larger requests answer 413 (0: no limit)
	timeout time.Duration

	seen      map[uint64]bool
	delivered map[string][]uint64 // local_seq stored per session, in arrival order
	dropped   map[string]uint64   // sum of the stored sdk.dropped counts
	failed    map[string]bool     // the model failed or refused the session at least once
	unacked   map[string]int      // events stored by a request whose answer did not say so
	requests  int
	partials  int
	resent    int
}

func (m *propCollector) fails(sid string) (string, bool) {
	s := m.sess[sid]
	if s == nil || m.healed {
		return "", false
	}
	s.n++
	switch s.kind {
	case 1:
		return s.cause, true
	case 2:
		return s.cause, (s.n/s.period)%2 == 0
	case 3:
		return s.cause, (int(time.Since(m.start)/time.Millisecond)/s.period)%2 == 0
	case 4:
		return s.cause, m.rng.Float64() < s.p
	case 5:
		return s.cause, s.n <= s.period
	}
	return "", false
}

// store reports whether the event is new.
func (m *propCollector) store(e core.Event) bool {
	if m.seen[e.LocalSeq] {
		m.resent++
		return false
	}
	m.seen[e.LocalSeq] = true
	if e.Kind == core.KindSDKDropped {
		n, _ := strconv.ParseUint(e.Attrs["count"], 10, 64)
		m.dropped[e.SessionID] += n
	} else {
		m.delivered[e.SessionID] = append(m.delivered[e.SessionID], e.LocalSeq)
	}
	return true
}

// hijackWrite answers with raw bytes (or nothing) and closes the connection.
func hijackWrite(w http.ResponseWriter, raw string) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	c, _, err := hj.Hijack()
	if err != nil {
		return
	}
	_, _ = c.Write([]byte(raw))
	_ = c.Close()
}

func (m *propCollector) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	evs, _ := decodeFrames(body)
	var sids []string
	for _, e := range evs {
		if !slices.Contains(sids, e.SessionID) {
			sids = append(sids, e.SessionID)
		}
	}
	slices.Sort(sids)

	m.mu.Lock()
	m.requests++
	mode := m.mode
	if m.healed {
		mode = propNormal
	}
	switch mode {
	case propDown:
		m.mu.Unlock()
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	case propAllNamed:
		for _, s := range sids {
			m.failed[s] = true
		}
		m.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "x", "accepted": 0, "duplicates": 0, "failed_sessions": failAll(sids, "shutdown")})
		return
	case propRefuse:
		for _, s := range sids {
			m.failed[s] = true
		}
		m.mu.Unlock()
		http.Error(w, "bad", http.StatusBadRequest)
		return
	case propConnDrop:
		if m.rng.Intn(2) == 0 {
			m.mu.Unlock()
			hijackWrite(w, "")
			return
		}
	}
	if m.maxBody > 0 && len(body) > m.maxBody {
		m.mu.Unlock()
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	if mode == propSlowBefore {
		d := time.Duration(float64(m.timeout) * (0.5 + m.rng.Float64()))
		m.mu.Unlock()
		time.Sleep(d)
		m.mu.Lock()
	}
	failed := map[string]string{}
	if mode != propNamesOn200 && (mode != propStoreThenError || m.rng.Intn(2) == 0) {
		for _, s := range sids {
			if cause, f := m.fails(s); f {
				failed[s] = cause
				m.failed[s] = true
			}
		}
	}
	acc, dup := 0, 0
	silent := lostAnswer(mode) && (mode != propGhostName || len(failed) == 0)
	for _, e := range evs {
		if failed[e.SessionID] != "" {
			continue
		}
		if !m.store(e) {
			dup++
			continue
		}
		acc++
		if silent {
			n := uint64(1)
			if e.Kind == core.KindSDKDropped {
				n, _ = strconv.ParseUint(e.Attrs["count"], 10, 64)
			}
			m.unacked[e.SessionID] += int(n)
		}
	}
	if len(failed) > 0 && len(failed) < len(sids) {
		m.partials++
	}
	fids := slices.Sorted(maps.Keys(failed))
	code := http.StatusOK
	reply := map[string]any{"accepted": acc, "duplicates": dup}
	if len(failed) > 0 {
		code = http.StatusInternalServerError
		if failed[fids[0]] != "storage" {
			code = http.StatusServiceUnavailable
		}
		reply["error"] = "e"
		reply["failed_sessions"] = failed
	}
	var raw []byte
	switch mode {
	case propStoreThenError:
		code, reply = http.StatusInternalServerError, map[string]any{"error": "storage error"}
	case propGhostName:
		names := map[string]string{"ghost-session": "storage"}
		for k, v := range failed {
			names[k] = v
		}
		code = max(code, http.StatusInternalServerError)
		reply["error"], reply["failed_sessions"] = "e", names
	case propDupKey:
		if len(fids) > 0 {
			var b strings.Builder
			fmt.Fprintf(&b, `{"error":"e","accepted":%d,"duplicates":%d,"failed_sessions":{%q:"storage",%q:"backpressure"`, acc, dup, fids[0], fids[0])
			for _, s := range fids[1:] {
				fmt.Fprintf(&b, `,%q:%q`, s, failed[s])
			}
			raw = []byte(b.String() + "}}")
		}
	case propEmptyNames:
		if len(fids) > 0 {
			reply["failed_sessions"] = map[string]string{}
		}
	case propTruncated:
		if len(fids) > 0 {
			reply["failed_sessions_truncated"] = true
		}
	case propNamesOn200:
		if len(sids) > 0 {
			reply["failed_sessions"] = map[string]string{sids[m.rng.Intn(len(sids))]: "storage"}
		}
	}
	if raw == nil {
		raw, _ = json.Marshal(reply)
	}
	var sleep time.Duration
	if mode == propSlowAfter {
		sleep = time.Duration(float64(m.timeout) * (0.5 + m.rng.Float64()))
	}
	cutAt := 0
	if mode == propCutBody {
		cutAt = m.rng.Intn(len(raw))
	}
	m.mu.Unlock()
	time.Sleep(sleep)
	switch mode {
	case propConnDrop:
		hijackWrite(w, "")
	case propCutBody:
		hijackWrite(w, fmt.Sprintf("HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", code, len(raw), raw[:cutAt]))
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(raw)
	}
}

// propResult is the outcome of one case.
type propResult struct {
	violations []string
	url        string
	recorded   int
	requests   int
	partials   int
	resent     int
	dropped    uint64
	held       bool // a session was held at some point
	closed     bool // the case ended with Close during the faults
	strict     bool // a session that never failed was checked for exact delivery
	flushes    int  // Flush calls that returned true and were checked
}

// propCase runs one random workload against one random collector model and
// returns every invariant it broke:
//
//   - per session, delivered plus reported dropped equals recorded whenever
//     Flush returned true and at the end (more only by events the collector
//     stored without saying so);
//   - a session's events are stored in the order they were recorded, and
//     only its own;
//   - a session the collector never failed loses nothing while the ring has
//     room;
//   - after the faults end, Flush empties the ring; Close returns in time.
func propCase(seed int64) (out propResult) {
	rng := rand.New(rand.NewSource(seed))
	bad := func(f string, a ...any) { out.violations = append(out.violations, fmt.Sprintf(f, a...)) }
	timeout := 40 * time.Millisecond
	modes := rng.Intn(3) != 0 // the other cases use only the sessions' own schedules
	refusing := modes && rng.Intn(4) == 0
	if !modes || refusing {
		timeout = 2 * time.Second // no answer is lost: the counts must then be exact
	}
	m := &propCollector{rng: rand.New(rand.NewSource(seed ^ 0x5eed)), start: time.Now(), sess: map[string]*propSession{}, timeout: timeout,
		seen: map[uint64]bool{}, delivered: map[string][]uint64{}, dropped: map[string]uint64{}, failed: map[string]bool{}, unacked: map[string]int{}}
	if rng.Intn(3) == 0 {
		m.maxBody = 2048 + rng.Intn(6000)
	}
	var sids []string
	for i := range 1 + rng.Intn(6) {
		sid := fmt.Sprintf("s%d-%d", seed, i)
		sids = append(sids, sid)
		s := &propSession{cause: []string{"storage", "backpressure", "shutdown"}[rng.Intn(3)]}
		if i > 0 || rng.Intn(4) == 0 { // usually one healthy session at least
			s.kind = rng.Intn(6)
		}
		s.period = []int{1, 2, 3, 7, 20, 60}[rng.Intn(6)]
		s.p = []float64{0.05, 0.2, 0.5, 0.9}[rng.Intn(4)]
		m.sess[sid] = s
	}
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	defer srv.Close()
	// A path of its own, so that a log line names the case it comes from
	// even when a later case gets the same port.
	out.url = fmt.Sprintf("%s/case-%d", srv.URL, seed)

	ring := []int{4, 8, 16, 64, 256, 1024}[rng.Intn(6)]
	cfg := ExporterConfig{CollectorURL: out.url, BatchSize: []int{1, 2, 3, 16, 256}[rng.Intn(5)],
		FlushInterval: time.Millisecond, RetryBackoff: time.Millisecond, MaxCooldown: time.Duration(2+rng.Intn(10)) * time.Millisecond,
		HTTPTimeout: timeout}
	if rng.Intn(4) == 0 {
		cfg.MaxRetries = -1
	}
	rec := NewRecorder(ring)
	exp := NewExporter(rec, cfg)
	throttled := rng.Intn(3) != 0 // the workload waits for room in the ring
	closing := rng.Intn(5) == 0

	want := map[string]int{}
	owner := map[uint64]string{}
	next := uint64(0)
	overflowed := false
	// check holds the counts against what was recorded. Drop counts that
	// wait in the recorder are part of it; at the end none may be left.
	check := func(when string, end bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, sid := range sids {
			if end && rec.drops[sid] != 0 {
				bad("%s: session %s: %d drops are not reported", when, sid, rec.drops[sid])
			}
			sum := len(m.delivered[sid]) + int(m.dropped[sid]+rec.drops[sid])
			if sum < want[sid] || sum > want[sid]+m.unacked[sid] {
				bad("%s: session %s (kind %d): delivered %d + reported dropped %d + drops to report %d != recorded %d (stored without an answer: %d)",
					when, sid, m.sess[sid].kind, len(m.delivered[sid]), m.dropped[sid], rec.drops[sid], want[sid], m.unacked[sid])
			}
		}
	}
	steps := 100 + rng.Intn(300)
	modeLeft := 0
	for i := range steps {
		if modes {
			if modeLeft == 0 && rng.Intn(8) == 0 {
				m.mu.Lock()
				// Refused events are lost for good, which only an exact
				// count can tell from a bug: not together with lost answers.
				for {
					m.mode = 1 + rng.Intn(propModes-1)
					if (refusing && !lostAnswer(m.mode)) || (!refusing && m.mode != propRefuse) {
						break
					}
				}
				m.mu.Unlock()
				modeLeft = 1 + rng.Intn(15)
			} else if modeLeft > 0 {
				if modeLeft--; modeLeft == 0 {
					m.mu.Lock()
					m.mode = propNormal
					m.mu.Unlock()
				}
			}
		}
		for range 1 + rng.Intn(8) {
			if throttled && !overflowed {
				for deadline := time.Now().Add(300 * time.Millisecond); rec.Len() > ring/2; time.Sleep(100 * time.Microsecond) {
					if time.Now().After(deadline) {
						overflowed = true
						break
					}
				}
			}
			sid := sids[rng.Intn(len(sids))]
			next++
			size := rng.Intn(120)
			if rng.Intn(40) == 0 {
				size = 1000 + rng.Intn(9000)
			}
			rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, Name: "SECRET-NAME", LocalSeq: propBase + next,
				Attrs: map[string]string{"k": "SECRET" + strings.Repeat("x", size)}})
			want[sid]++
			owner[propBase+next] = sid
		}
		out.held = out.held || len(heldSessions(rec)) > 0
		if n := rec.Len(); n > ring {
			bad("the ring holds %d events, more than its %d", n, ring)
		}
		if rng.Intn(30) == 0 && exp.Flush(time.Duration(rng.Intn(20))*time.Millisecond) {
			out.flushes++
			check(fmt.Sprintf("Flush returned true at step %d", i), false)
		}
		if rng.Intn(3) == 0 {
			time.Sleep(time.Duration(rng.Intn(1500)) * time.Microsecond)
		}
		if closing && i > steps/2 && rng.Intn(20) == 0 {
			break
		}
	}
	if closing {
		out.closed = true
		to := time.Duration(rng.Intn(40)) * time.Millisecond
		start := time.Now()
		exp.Close(to)
		if d := time.Since(start); d > to+500*time.Millisecond { // room for a busy machine under -race
			bad("Close(%v) took %v", to, d)
		}
	} else {
		m.mu.Lock()
		m.healed = true
		m.mu.Unlock()
		if !exp.Flush(20 * time.Second) {
			bad("Flush did not empty the ring within 20s of the faults ending: %d events left", rec.Len())
		} else {
			check("at the end", true)
			if rec.Len() != 0 {
				bad("the ring holds %d events after the final Flush", rec.Len())
			}
		}
		exp.Close(50 * time.Millisecond)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sid := range sids {
		got := m.delivered[sid]
		if !slices.IsSorted(got) {
			bad("session %s: events were stored out of order", sid)
		}
		for _, q := range got {
			if owner[q] != sid {
				bad("session %s got event %d, recorded for %q", sid, q-propBase, owner[q])
			}
		}
		if len(got)+int(m.dropped[sid]) > want[sid]+m.unacked[sid] {
			bad("session %s: delivered %d + reported dropped %d > recorded %d (stored without an answer: %d)", sid, len(got), m.dropped[sid], want[sid], m.unacked[sid])
		}
		if !closing && throttled && !overflowed && !m.failed[sid] {
			out.strict = true
			if len(got) != want[sid] || m.dropped[sid] != 0 {
				bad("session %s never failed: delivered %d of %d, reported dropped %d", sid, len(got), want[sid], m.dropped[sid])
			}
		}
		out.dropped += m.dropped[sid]
		out.recorded += want[sid]
	}
	out.requests, out.partials, out.resent = m.requests, m.partials, m.resent
	if len(out.violations) > 0 {
		out.violations = append(out.violations, fmt.Sprintf("config: ring=%d batch=%d cooldown=%v retries=%d maxBody=%d throttled=%v modes=%v refusing=%v closing=%v",
			ring, cfg.BatchSize, cfg.MaxCooldown, cfg.MaxRetries, m.maxBody, throttled, modes, refusing, closing))
		for _, sid := range sids {
			s := m.sess[sid]
			out.violations = append(out.violations, fmt.Sprintf("  %s kind=%d period=%d p=%.2f cause=%s", sid, s.kind, s.period, s.p, s.cause))
		}
	}
	return out
}

// Random workloads against random collector failures, see propCase. The
// default run is a fixed set of seeds that fits `make race`;
// TSESSION_PROP_CASES and TSESSION_PROP_SEED choose how many cases to run
// and the first seed.
func TestExporterProperty(t *testing.T) {
	cases, _ := strconv.Atoi(os.Getenv("TSESSION_PROP_CASES"))
	if cases <= 0 {
		cases = 60
	}
	base, _ := strconv.ParseInt(os.Getenv("TSESSION_PROP_SEED"), 10, 64)
	if base == 0 {
		base = 1
	}
	var logs lockedBuf
	prev := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(prev)

	var (
		mu                           sync.Mutex
		wg                           sync.WaitGroup
		total                        propResult
		failed, held, closed, strict int
		seeds                        = map[string]int64{} // by collector URL
	)
	sem := make(chan struct{}, 6)
	start := time.Now()
	for i := range cases {
		seed := base + int64(i)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			o := propCase(seed)
			mu.Lock()
			defer mu.Unlock()
			seeds[o.url] = seed
			total.recorded += o.recorded
			total.requests += o.requests
			total.partials += o.partials
			total.resent += o.resent
			total.dropped += o.dropped
			total.flushes += o.flushes
			for _, c := range []struct {
				is bool
				n  *int
			}{{o.held, &held}, {o.closed, &closed}, {o.strict, &strict}, {len(o.violations) > 0, &failed}} {
				if c.is {
					*c.n++
				}
			}
			if len(o.violations) > 0 && failed <= 12 {
				t.Errorf("seed %d:\n  %s", seed, strings.Join(o.violations[:min(len(o.violations), 14)], "\n  "))
			} else if len(o.violations) > 0 {
				t.Errorf("seed %d: %d violations", seed, len(o.violations))
			}
		}()
	}
	wg.Wait()

	// The log: no event content, and per exporter at most one line about
	// held sessions and one outage and recovery (the cases are far shorter
	// than the minute between two such lines).
	lines := strings.Split(logs.String(), "\n")
	for _, line := range lines {
		if strings.Contains(line, "SECRET") {
			t.Errorf("log line carries event content: %s", line)
		}
	}
	for url, seed := range seeds {
		for _, msg := range []string{"keeps failing some sessions", "cannot deliver events"} {
			n := 0
			for _, line := range lines {
				if strings.Contains(line, msg) && (strings.Contains(line, url+" ") || strings.Contains(line, url+";")) {
					n++
				}
			}
			if n > 1 {
				t.Errorf("seed %d: %d %q lines from one exporter", seed, n, msg)
			}
		}
	}
	t.Logf("cases=%d (seeds %d..%d) failed=%d held-a-session=%d closed-early=%d strict-checks=%d events=%d requests=%d partial-answers=%d resent=%d reported-dropped=%d flush-checks=%d log-lines=%d elapsed=%v",
		cases, base, base+int64(cases)-1, failed, held, closed, strict, total.recorded, total.requests, total.partials, total.resent, total.dropped, total.flushes,
		len(lines)-1, time.Since(start).Round(time.Millisecond))
}
