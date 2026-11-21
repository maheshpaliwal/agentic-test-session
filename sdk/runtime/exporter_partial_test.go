package runtime

import (
	"log"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// partialEnv is an exporter with short pauses against a namingCollector.
type partialEnv struct {
	t    *testing.T
	col  *namingCollector
	rec  *Recorder
	exp  *Exporter
	seq  uint64
	want map[string][]uint64 // local_seq recorded per session
}

// cooldown is MaxCooldown in these tests, and so the time a session has to
// fail while others are accepted before it is held.
const cooldown = 40 * time.Millisecond

func newPartialEnv(t *testing.T, ring, batch int) *partialEnv {
	t.Helper()
	col, url := newNamingCollector(t)
	rec := NewRecorder(ring)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: url, BatchSize: batch, FlushInterval: time.Millisecond,
		RetryBackoff: time.Millisecond, MaxCooldown: cooldown})
	t.Cleanup(func() { exp.Close(time.Second) })
	return &partialEnv{t: t, col: col, rec: rec, exp: exp, want: map[string][]uint64{}}
}

func (e *partialEnv) record(sid string, n int) {
	for range n {
		e.seq++
		e.rec.Record(core.Event{SessionID: sid, Kind: core.KindAPIIn, Name: "SECRET-NAME", LocalSeq: seqBase + e.seq})
		e.want[sid] = append(e.want[sid], seqBase+e.seq)
	}
}

// eventually waits for cond, for at most five seconds.
func (e *partialEnv) eventually(what string, cond func() bool) {
	e.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting until %s", what)
		}
	}
}

func (e *partialEnv) delivered(sid string) int {
	n, _ := e.col.count(e.t, sid)
	return n
}

// complete fails unless every recorded event of the session was delivered,
// in order, and no drop was reported for it.
func (e *partialEnv) complete(sid string) {
	e.t.Helper()
	if n, dropped := e.col.count(e.t, sid); n != len(e.want[sid]) || dropped != 0 {
		e.t.Fatalf("session %s: recorded %d, delivered %d, reported dropped %d", sid, len(e.want[sid]), n, dropped)
	}
}

// held returns the sessions the exporter holds.
func (e *partialEnv) held() []string { return heldSessions(e.rec) }

// hold makes the collector fail session sid and waits until it is held, with
// session other being accepted meanwhile.
func (e *partialEnv) hold(sid, other string) {
	e.t.Helper()
	e.col.setFail(failing(sid))
	e.record(sid, 1)
	e.eventually(sid+" is held", func() bool {
		e.record(other, 1)
		return slices.Contains(e.held(), sid)
	})
}

// The bug this guards against: a session the collector keeps failing was
// resent with its whole batch for ever, and the sessions behind it in the
// ring were never sent. Here the failing session even fills whole batches.
func TestFailingSessionDoesNotBlockOthers(t *testing.T) {
	env := newPartialEnv(t, 4096, 8)
	env.col.setFail(failing("bad"))
	env.record("bad", 50)
	for range 200 {
		env.record("ok", 1)
		env.record("bad", 1)
	}
	env.eventually("the healthy session is delivered", func() bool { return env.delivered("ok") == 200 })
	env.complete("ok")
	if env.exp.Flush(20 * time.Millisecond) {
		t.Fatal("Flush returned true with a failing session's events still buffered")
	}
	if n := env.delivered("bad"); n != 0 || env.rec.Len() != 250 {
		t.Fatalf("failing session: %d delivered, ring holds %d events, want 0 and 250", n, env.rec.Len())
	}

	env.col.setFail(nil)
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("Flush did not empty the ring after the session recovered")
	}
	env.complete("bad")
	env.complete("ok")
}

// A collector that does not name sessions (an older one) fails the request
// as a whole, and the exporter does what it always did: it sends the same
// events again, nothing from behind them, until they are taken.
func TestFailureWithoutNamesIsResentWhole(t *testing.T) {
	env := newPartialEnv(t, 4096, 4)
	env.col.bare = true
	env.col.setFail(failing("bad"))
	env.record("bad", 2)
	env.record("ok", 20)
	env.eventually("the batch was resent", func() bool {
		env.col.mu.Lock()
		defer env.col.mu.Unlock()
		return env.col.requests >= 5
	})
	// Like the collector, the fake stored the healthy events of the request
	// it failed. Only the first batch was ever sent.
	if n := env.delivered("ok"); n != 2 {
		t.Fatalf("%d events of the healthy session were delivered during the outage, want the 2 of the first batch", n)
	}
	if h := env.held(); len(h) != 0 {
		t.Fatalf("sessions %v are held although the collector named none", h)
	}
	env.col.setFail(nil)
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("Flush did not empty the ring after the outage")
	}
	env.complete("bad")
	env.complete("ok")
}

// A session is not taken to be accepted because a failed request does not
// name it: only when the answer counts accepted events and its list of
// failed sessions is complete.
func TestSessionNotNamedIsNotAssumedAccepted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		truncated bool
		other     int // events of a session the collector takes
	}{
		{"nothing counted as accepted", false, 0},
		{"list of failed sessions truncated", true, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newPartialEnv(t, 4096, 64)
			env.col.truncated, env.col.lose = tc.truncated, map[string]bool{"lost": true}
			env.col.setFail(failing("bad"))
			env.record("bad", 5)
			env.record("lost", 5)
			env.record("ok", tc.other)
			env.eventually("several requests failed", func() bool {
				env.col.mu.Lock()
				defer env.col.mu.Unlock()
				return env.col.requests >= 5
			})
			env.col.setFail(nil)
			if !env.exp.Flush(5 * time.Second) {
				t.Fatal("Flush did not empty the ring")
			}
			for _, sid := range []string{"bad", "lost", "ok"} {
				env.complete(sid)
			}
		})
	}
}

// A failing session is held only once it kept failing while other sessions
// were accepted for MaxCooldown. An outage in which the collector names
// every session holds nobody, nor does a single request taken during one.
func TestSessionIsHeldOnlyWhileOthersAreAccepted(t *testing.T) {
	t.Run("others accepted", func(t *testing.T) {
		env := newPartialEnv(t, 4096, 64)
		start := time.Now()
		env.hold("bad", "ok")
		if d := time.Since(start); d < cooldown {
			t.Fatalf("held after %v, before the others were accepted for %v", d, cooldown)
		}
		env.col.setFail(nil)
		env.eventually("the session is released", func() bool { return len(env.held()) == 0 })
		if !env.exp.Flush(5 * time.Second) {
			t.Fatal("Flush did not empty the ring")
		}
		env.complete("bad")
		env.complete("ok")
	})
	t.Run("everything fails", func(t *testing.T) {
		env := newPartialEnv(t, 4096, 64)
		env.col.setFail(func(sids []string) map[string]string { return failAll(sids, "shutdown") })
		for range 10 * cooldown / time.Millisecond {
			env.record("a", 1)
			env.record("b", 1)
			time.Sleep(time.Millisecond)
		}
		if h := env.held(); len(h) != 0 {
			t.Fatalf("sessions %v are held in an outage", h)
		}
		env.col.setFail(nil)
		if !env.exp.Flush(5 * time.Second) {
			t.Fatal("Flush did not empty the ring")
		}
		env.complete("a")
		env.complete("b")
	})
	t.Run("one request taken", func(t *testing.T) {
		env := newPartialEnv(t, 4096, 64)
		hit := false
		env.col.setFail(func(sids []string) map[string]string {
			if !hit && len(sids) == 1 && sids[0] == "c" {
				hit = true
				return nil
			}
			return failAll(sids, "storage")
		})
		for i := range 10 * cooldown / time.Millisecond {
			env.record("a", 1)
			env.record("b", 1)
			if i > 20 {
				env.record("c", 1)
			}
			time.Sleep(time.Millisecond)
		}
		if h := env.held(); len(h) != 0 {
			t.Fatalf("sessions %v are held after one accepted request", h)
		}
		env.col.setFail(nil)
		if !env.exp.Flush(5 * time.Second) {
			t.Fatal("Flush did not empty the ring")
		}
		for _, sid := range []string{"a", "b", "c"} {
			env.complete(sid)
		}
	})
}

// With the ring full, a held session pays for the room the others need: its
// newest events are dropped and counted, the other sessions lose nothing.
// What is delivered and what is reported dropped add up to what was
// recorded, and the held session keeps its oldest events, in order.
func TestFullRingDropsHeldSessionFirst(t *testing.T) {
	env := newPartialEnv(t, 64, 8)
	env.hold("bad", "ok")
	// Delivered and out of the ring, which then holds the held session only.
	okDelivered := func() bool { return bufferedOf(env.rec, "ok") == 0 }
	env.eventually("the healthy session is delivered", okDelivered)
	for env.rec.Len() < 64 {
		env.record("bad", 1)
	}
	env.record("bad", 10) // the ring is full: dropped, as always
	for range 40 {
		env.record("ok", 1) // takes the place of the newest held event
		env.eventually("the healthy session is delivered", okDelivered)
		env.record("bad", 1) // and the ring is full again
	}
	env.complete("ok")

	env.col.setFail(nil)
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("Flush did not empty the ring")
	}
	n, dropped := env.col.count(t, "bad")
	if n+int(dropped) != len(env.want["bad"]) || dropped != 50 || n < 50 {
		t.Fatalf("held session: recorded %d, delivered %d, reported dropped %d", len(env.want["bad"]), n, dropped)
	}
	env.col.mu.Lock()
	defer env.col.mu.Unlock()
	if !slices.Equal(env.col.delivered["bad"][:50], env.want["bad"][:50]) {
		t.Fatal("the held session did not keep its oldest events")
	}
}

// An event that was sent may be stored although no answer said so. It is not
// dropped to make room later: it would be delivered and counted as dropped.
// With no other event of a held session to drop, the new event is dropped,
// as it always was.
func TestSentEventIsNotDroppedForRoom(t *testing.T) {
	env := newPartialEnv(t, 16, 4)
	env.hold("bad", "ok")
	env.eventually("the healthy session is delivered", func() bool { return bufferedOf(env.rec, "ok") == 0 })
	// The held session's next requests are stored, and their answers lost.
	env.col.mu.Lock()
	env.col.silent = "bad"
	env.col.mu.Unlock()
	env.record("bad", 1)
	env.eventually("the held session's events are stored", func() bool { return env.delivered("bad") == 2 })
	// An outage, in which the healthy session fills the ring and overflows.
	env.col.mu.Lock()
	env.col.silent, env.col.bare = "", true
	env.col.mu.Unlock()
	env.col.setFail(func(sids []string) map[string]string { return failAll(sids, "storage") })
	env.record("ok", 20)

	env.col.mu.Lock()
	env.col.bare = false
	env.col.mu.Unlock()
	env.col.setFail(nil)
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("Flush did not empty the ring")
	}
	env.complete("bad")
	if n, dropped := env.col.count(t, "ok"); n+int(dropped) != len(env.want["ok"]) || dropped == 0 {
		t.Fatalf("healthy session: recorded %d, delivered %d, reported dropped %d", len(env.want["ok"]), n, dropped)
	}
}

// Held sessions are logged once, and once more when the last is released; a
// second round within the minute is not logged. The lines carry neither
// session ids nor event content.
func TestHeldSessionsAreLoggedOnce(t *testing.T) {
	var out lockedBuf
	prev := log.Writer()
	log.SetOutput(&out)
	defer log.SetOutput(prev)

	env := newPartialEnv(t, 4096, 64)
	for round := range 2 {
		env.hold("bad-session", "ok")
		env.col.setFail(nil)
		// The recorder is told before the line is written: wait for the line too.
		env.eventually("the session is released, and that is logged", func() bool {
			return len(env.held()) == 0 && strings.Contains(out.String(), "accepts all sessions again")
		})
		if h, r := strings.Count(out.String(), "keeps failing some sessions"), strings.Count(out.String(), "accepts all sessions again"); h != 1 || r != 1 {
			t.Fatalf("round %d: %d hold and %d release lines, want one each:\n%s", round, h, r, out.String())
		}
	}
	if s := out.String(); strings.Contains(s, "bad-session") || strings.Contains(s, "SECRET") {
		t.Fatalf("log carries a session id or event content:\n%s", s)
	}
}

// Flush and Close keep their bounds with a failing session: Flush reports
// false when its time is up, having delivered the others, and Close returns
// within its timeout.
func TestFlushAndCloseWithFailingSession(t *testing.T) {
	env := newPartialEnv(t, 4096, 64)
	env.col.setFail(failing("bad"))
	env.record("bad", 10)
	env.record("ok", 10)
	start := time.Now()
	if env.exp.Flush(50 * time.Millisecond) {
		t.Fatal("Flush returned true with a failing session's events buffered")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Flush(50ms) took %v", d)
	}
	env.complete("ok")
	start = time.Now()
	env.exp.Close(50 * time.Millisecond)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close(50ms) took %v", d)
	}
}

// A 413 splits the batch. When the collector fails a session in the first
// half, the session's events in the second half are not sent ahead of them.
func TestBisectKeepsFailingSessionInOrder(t *testing.T) {
	env := newPartialEnv(t, 4096, 8)
	n := 0
	env.col.maxEvents = 2
	env.col.setFail(func(sids []string) map[string]string {
		if n++; n == 1 { // the first request that is small enough
			return failing("a")(sids)
		}
		return nil
	})
	env.record("a", 4)
	env.record("b", 4)
	if !env.exp.Flush(5 * time.Second) {
		t.Fatal("Flush did not empty the ring")
	}
	env.complete("a") // count fails the test if events arrived out of order
	env.complete("b")
}
