package controlplane

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// judgeFixture is a manager with one running session that keeps as many
// reports as a session can (256 stored, 1024 beside them), all final and in
// order, and streams connected instances that announced reports and were
// never heard of; they are among the instances that reported. It returns a
// function that judges the session once, under the lock, as the end of a
// drain does.
func judgeFixture(tb testing.TB, streams int) (judge func()) {
	m := NewManager(Config{Collector: newScriptCollector().counting(), DeliveryGrace: time.Millisecond})
	tb.Cleanup(m.Close)
	s, err := m.Start(StartRequest{Name: "judge", QuiesceSeconds: 2, MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	const reports = MaxDeliveryInstances + maxDeliveryRefused
	counts := collectorCounts{known: true, n: map[string]int64{}}
	for i := range reports {
		inst := fmt.Sprintf("i-%04d", i)
		if _, err := m.ReportDelivery(s.ID, final(rep("fleet", inst, 1, 1, 0))); err != nil && !errors.Is(err, errDeliveryFull) {
			panic(err)
		}
		counts.n[deliveryKey("fleet", inst)] = 1
	}
	for i := range streams {
		follow(m, "fleet", fmt.Sprintf("i-%04d", i)) // left connected: the manager is closed with the test
	}
	time.Sleep(5 * time.Millisecond) // nothing has arrived of any stream for a tenth of the grace and more
	m.mu.Lock()
	st, live := m.delivery[s.ID], m.live[s.ID]
	unheard := len(m.unheardLocked(time.Now(), func(string) bool { return false }))
	m.mu.Unlock()
	if len(live.Delivery) != MaxDeliveryInstances || len(st.refused) != maxDeliveryRefused || unheard != streams {
		panic(fmt.Sprintf("test setup: %d reports stored, %d kept beside them, %d unheard instances", len(live.Delivery), len(st.refused), unheard))
	}
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		sess := live
		m.delivery[s.ID] = st // a judgement forgets the state
		if m.judgeDeliveryLocked(&sess, counts) || len(sess.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("reasons=%q unconfirmed=%q", sess.IncompleteReasons, sess.DeliveryUnconfirmed))
		}
	}
}

// The fourth review: a judgement went through all the reports of the session
// again for every unheard instance, building their list each time, under the
// manager's lock. With 1,000 streams and 1,280 reports, sealing 20 sessions
// took 11.7s instead of 1.2s, and GET /v1/active waited 4.4s. The reports are
// indexed once per judgement now.
//
// What is measured is memory, not time, so the test says the same on a slow
// machine: an unheard instance may cost a judgement a few allocations, not a
// copy of the session's reports (some hundred times the bound below).
func TestJudgeDeliveryDoesNotGoThroughTheReportsPerInstance(t *testing.T) {
	const streams = 1000
	measure := func(streams int) float64 {
		judge := judgeFixture(t, streams)
		judge()
		const runs = 5
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range runs {
			judge()
		}
		runtime.ReadMemStats(&after)
		return float64(after.TotalAlloc-before.TotalAlloc) / runs
	}
	base, with := measure(0), measure(streams)
	t.Logf("a judgement of 1280 reports allocates %.0f bytes without streams, %.0f with %d unheard ones", base, with, streams)
	if perStream := (with - base) / streams; perStream > 2<<10 {
		panic(fmt.Sprintf("an unheard instance costs a judgement %.0f bytes: the reports of the session are gone through once per instance", perStream))
	}
}

// BenchmarkJudgeDelivery is one judgement of a session with 1,280 reports
// while 1,000 instances that announced reports are connected and unheard.
func BenchmarkJudgeDelivery(b *testing.B) {
	judge := judgeFixture(b, 1000)
	b.ReportAllocs()
	for b.Loop() {
		judge()
	}
}

// Two lines of delivery_unconfirmed a caller who can reach the control plane
// can take out. Neither is prevented: the endpoint is not authenticated, and
// docs/PROTOCOL.md lists both among what such a caller can do. Pinned here so
// that the list stays true.
func TestDeliveryUnconfirmedLinesACallerCanRemove(t *testing.T) {
	// An instance of which nothing has arrived since it connected is named in
	// every session. One request in its name makes it one that was heard of,
	// which a session it left nothing in does not name.
	t.Run("a request in the name of an instance that was never heard of", func(t *testing.T) {
		for _, forged := range []bool{false, true} {
			e := newDeliveryEnv(t, testGrace, nil)
			e.col.counting()
			defer follow(e.m, "orders-api", "o-1")()
			if forged {
				hello(t, e.m, "orders-api", "o-1")
			}
			time.Sleep(testGrace + testGrace/4)
			s, _ := e.stop()
			if named := len(s.DeliveryUnconfirmed) == 1 && strings.Contains(s.DeliveryUnconfirmed[0], "no delivery report of it has arrived since it connected"); s.Incomplete || named == forged {
				panic(fmt.Sprintf("forged=%v: incomplete=%v unconfirmed=%q", forged, s.Incomplete, s.DeliveryUnconfirmed))
			}
		}
	})
	// An instance recorded after its last report and stopped reporting: the
	// collector has more events than the report names. A final report in its
	// name with the same counters is not stale, makes the kept report final,
	// and a final report the collector covers is complete.
	t.Run("a final report that repeats the instance's last one", func(t *testing.T) {
		for _, forged := range []bool{false, true} {
			e := newDeliveryEnv(t, testGrace, nil)
			e.col.counting().has("orders-api", "o-1", 5)
			e.report(rep("orders-api", "o-1", 3, 3, 0))
			if forged {
				e.report(final(rep("orders-api", "o-1", 3, 3, 0)))
			}
			s, _ := e.stop()
			if named := len(s.DeliveryUnconfirmed) == 1 && strings.Contains(s.DeliveryUnconfirmed[0], "the collector has 5 events, more than the 3 of the instance's last report"); s.Incomplete || named == forged {
				panic(fmt.Sprintf("forged=%v: incomplete=%v unconfirmed=%q", forged, s.Incomplete, s.DeliveryUnconfirmed))
			}
		}
	})
}
