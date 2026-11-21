package integration

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

// faultStore is the collector's in-memory store with injected faults: the
// whole store down, or single sessions failing. It needs no Postgres or Redis.
type faultStore struct {
	store.Store
	down atomic.Bool
	mu   sync.Mutex
	bad  map[string]bool
}

var errStoreFault = errors.New("store unavailable")

func (f *faultStore) fail(id string, bad bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bad[id] = bad
}

func (f *faultStore) check(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down.Load() || f.bad[id] {
		return errStoreFault
	}
	return nil
}

func (f *faultStore) AppendEvents(ctx context.Context, id string, evs []core.Event) error {
	if err := f.check(id); err != nil {
		return err
	}
	return f.Store.AppendEvents(ctx, id, evs)
}

func (f *faultStore) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	if err := f.check(id); err != nil {
		return nil, err
	}
	return f.Store.Events(ctx, id, from, limit)
}

func (f *faultStore) GetSeal(ctx context.Context, id string) (*store.SealInfo, error) {
	if err := f.check(id); err != nil {
		return nil, err
	}
	return f.Store.GetSeal(ctx, id)
}

func (f *faultStore) Head(ctx context.Context, id string) (uint64, error) {
	if err := f.check(id); err != nil {
		return 0, err
	}
	return f.Store.Head(ctx, id)
}

// partialEnv is a real collector over a faultStore and a real SDK recorder
// and exporter shipping to it over HTTP on 127.0.0.1.
type partialEnv struct {
	t    *testing.T
	st   *faultStore
	rec  *runtime.Recorder
	exp  *runtime.Exporter
	next uint64
	want map[string]int // events recorded per session
	secs int            // how long the fault lasts
	tick time.Duration  // one event per session and tick
}

// newPartialEnv runs a short, scaled-down scenario by default: a ring of the
// given size and a fast exporter. With TSESSION_IT_FAULT_SECS set it runs
// that many seconds at 100 events per second and session, with the default
// ring, exporter and collector settings.
func newPartialEnv(t *testing.T, ring int) *partialEnv {
	t.Helper()
	env := &partialEnv{t: t, st: &faultStore{Store: store.NewMem(), bad: map[string]bool{}}, want: map[string]int{},
		next: 1 << 40, // clear of the local_seq the SDK gives its own sdk.dropped events
		secs: 2, tick: 2 * time.Millisecond}
	cfg := runtime.ExporterConfig{FlushInterval: 5 * time.Millisecond, RetryBackoff: 5 * time.Millisecond, MaxCooldown: 100 * time.Millisecond}
	if v, err := strconv.Atoi(os.Getenv("TSESSION_IT_FAULT_SECS")); err == nil && v > 0 {
		env.secs, env.tick, ring, cfg = v, 10*time.Millisecond, 0, runtime.ExporterConfig{}
	}
	ccfg := collector.DefaultConfig()
	ccfg.Logger = slog.New(slog.DiscardHandler) // one error line per failed request otherwise
	c := collector.New(env.st, ccfg)
	srv := httptest.NewServer(c.Handler()) // 127.0.0.1
	t.Cleanup(func() { srv.Close(); c.Close() })
	cfg.CollectorURL = srv.URL
	env.rec = runtime.NewRecorder(ring)
	env.exp = runtime.NewExporter(env.rec, cfg)
	t.Cleanup(func() { env.exp.Close(time.Second) })
	return env
}

func (e *partialEnv) record(sids ...string) {
	for _, sid := range sids {
		e.next++
		h := core.HLCNow()
		e.rec.Record(core.Event{SessionID: sid, TraceID: core.NewTraceID(), SpanID: core.NewSpanID(), Service: "svc",
			Instance: "it-partial", Kind: core.KindAPIIn, Name: "op", HLC: h, EndHLC: h, LocalSeq: e.next})
		e.want[sid]++
	}
}

// run records events for the length of the fault, one per tick for each
// session in every, one every tenth tick for each in tenth, and returns the
// highest ring fill it saw.
func (e *partialEnv) run(every, tenth []string) (highWater int) {
	tick := time.NewTicker(e.tick)
	defer tick.Stop()
	start := time.Now()
	for n := 1; time.Since(start) < time.Duration(e.secs)*time.Second; n++ {
		<-tick.C
		e.record(every...)
		if n%10 == 0 {
			e.record(tenth...)
		}
		highWater = max(highWater, e.rec.Len())
	}
	return highWater
}

// stored reads a session from the store: its distinct events, whether they
// are stored in the order they were recorded, and the sum of the counts of
// its sdk.dropped events.
func (e *partialEnv) stored(sid string) (n int, ordered bool, dropped uint64) {
	e.t.Helper()
	evs, err := e.st.Store.Events(context.Background(), sid, 1, 0)
	if err != nil {
		e.t.Fatal(err)
	}
	seen, last := map[uint64]bool{}, uint64(0)
	ordered = true
	for _, ev := range evs {
		if ev.Kind == core.KindSDKDropped {
			c, _ := strconv.ParseUint(ev.Attrs["count"], 10, 64)
			dropped += c
			continue
		}
		if seen[ev.LocalSeq] {
			e.t.Errorf("session %s: event %d stored twice", sid, ev.LocalSeq)
			continue
		}
		seen[ev.LocalSeq] = true
		if ev.LocalSeq < last {
			ordered = false
		}
		last = ev.LocalSeq
	}
	return len(seen), ordered, dropped
}

// settle waits until the collector's reorder windows have released what the
// sessions are owed, then returns stored, ordered and dropped per session.
func (e *partialEnv) settle(sids ...string) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		done := true
		for _, sid := range sids {
			if n, _, dropped := e.stored(sid); uint64(n)+dropped < uint64(e.want[sid]) {
				done = false
			}
		}
		if done || time.Now().After(deadline) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// exact fails unless the session's stored events and the drops reported for
// it add up to what was recorded, and its events are stored in order.
func (e *partialEnv) exact(sid string, wantDropped bool) {
	e.t.Helper()
	n, ordered, dropped := e.stored(sid)
	e.t.Logf("session %s: recorded %d, stored %d, reported dropped %d", sid, e.want[sid], n, dropped)
	if uint64(n)+dropped != uint64(e.want[sid]) {
		e.t.Errorf("session %s: stored %d + reported dropped %d != recorded %d", sid, n, dropped, e.want[sid])
	}
	if !wantDropped && dropped != 0 {
		e.t.Errorf("session %s: %d events dropped, want none", sid, dropped)
	}
	if !ordered {
		e.t.Errorf("session %s: events are not stored in the order they were recorded", sid)
	}
}

// One session whose store partition fails for good must not keep the other
// sessions of the same process from being delivered: while it fails the
// healthy sessions lose nothing, also when (as in the scaled-down run) the
// failing session alone records more than the ring holds.
func TestFailingSessionDoesNotStarveOthers(t *testing.T) {
	env := newPartialEnv(t, 512)
	healthy := []string{"ts_ok1", "ts_ok2", "ts_ok3"}
	env.st.fail("ts_bad", true)
	high := env.run(append([]string{"ts_bad"}, healthy...), nil)
	// The failing session is still failing: only the healthy ones are owed.
	env.exp.Flush(2 * time.Second)
	env.settle(healthy...)
	t.Logf("ring high-water %d while ts_bad failed for %ds", high, env.secs)
	for _, sid := range healthy {
		env.exact(sid, false)
	}
	if n, _, _ := env.stored("ts_bad"); n != 0 {
		t.Fatalf("%d events of the failing session were stored", n)
	}

	// Once its partition is back the failing session is delivered too: what
	// the ring still holds in order, the rest reported as dropped.
	env.st.fail("ts_bad", false)
	if !env.exp.Flush(30 * time.Second) {
		t.Fatal("Flush did not empty the ring after the session recovered")
	}
	env.settle("ts_bad")
	env.exact("ts_bad", true)
	for _, sid := range healthy {
		env.exact(sid, false)
	}
}

// A store outage as the real collector shows it to an SDK: a session whose
// actor is loaded keeps being accepted into its reorder buffer, a session
// that starts during the outage fails. Nothing may be lost that the ring
// has room for, and nothing reported dropped.
func TestStoreOutageLosesNothing(t *testing.T) {
	env := newPartialEnv(t, 2048)
	for range 10 {
		env.record("ts_old")
	}
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("warm-up flush failed")
	}
	env.st.down.Store(true)
	high := env.run([]string{"ts_new"}, []string{"ts_old"})
	env.st.down.Store(false)
	if !env.exp.Flush(30 * time.Second) {
		t.Fatal("Flush did not empty the ring after the store came back")
	}
	env.settle("ts_new", "ts_old")
	t.Logf("ring high-water %d during a %ds store outage", high, env.secs)
	env.exact("ts_new", false)
	env.exact("ts_old", false)
}
