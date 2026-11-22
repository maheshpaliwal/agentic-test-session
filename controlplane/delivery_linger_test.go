package controlplane

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The fifth review: an SDK whose reports keep failing opens its stream anew,
// to be a connection of which nothing has ever arrived, which is named in
// every session. Behind a proxy that keeps the upstream side of the stream it
// ended open (for a minute, there) the control plane has both connections, and
// went by the one heard of last, the old one: "heard of, and silent now",
// which is not named where nothing ties the instance to the session. 0 of 5
// sessions were named, with events missing. It goes by the newest connection.
func TestDeliveryUnheardGoesByTheNewestConnection(t *testing.T) {
	silence := testGrace + testGrace/4
	all := func(string) bool { return false }
	const never = "service orders-api (instance o-1): it follows the control plane, but no delivery report of it has arrived since it connected (its log says why); the collector has no events from it for this session, so whether it recorded any is unknown"
	t.Run("the old stream lingers, and nothing has arrived on the new one: unconfirmed", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "orders-api", "o-1")() // never left: the proxy keeps it open
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(silence)
		defer follow(e.m, "orders-api", "o-1")()
		time.Sleep(testGrace/10 + testGrace/20)
		e.m.mu.Lock()
		unheard := e.m.unheardLocked(time.Now(), all)
		e.m.mu.Unlock()
		if len(unheard) != 1 || !unheard[0].heard.IsZero() {
			panic(fmt.Sprintf("unheard: %+v, want the connection of which nothing has arrived", unheard))
		}
		s, _ := e.stop()
		if s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 || s.DeliveryUnconfirmed[0] != never {
			panic(fmt.Sprintf("incomplete=%v reasons=%q unconfirmed=%q", s.Incomplete, s.IncompleteReasons, s.DeliveryUnconfirmed))
		}
	})
	// The order in which the two are met must not matter (the connections are
	// kept in a map): many instances, each with a stream that lingers.
	t.Run("many instances with a stream that lingers: every one is named", func(t *testing.T) {
		const n = 12 // fewer than a list of reasons holds
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		for i := range n {
			defer follow(e.m, "fleet", fmt.Sprintf("i-%02d", i))()
			hello(t, e.m, "fleet", fmt.Sprintf("i-%02d", i))
		}
		time.Sleep(silence)
		for i := range n {
			defer follow(e.m, "fleet", fmt.Sprintf("i-%02d", i))()
		}
		time.Sleep(testGrace/10 + testGrace/20)
		s, _ := e.stop()
		if s.Incomplete || len(s.DeliveryUnconfirmed) != n {
			panic(fmt.Sprintf("incomplete=%v, %d of %d instances named: %q", s.Incomplete, len(s.DeliveryUnconfirmed), n, s.DeliveryUnconfirmed))
		}
		for _, line := range s.DeliveryUnconfirmed {
			if !strings.Contains(line, "no delivery report of it has arrived since it connected") {
				panic(fmt.Sprintf("named by its old connection: %q", line))
			}
		}
	})
	// No noise comes back with it: what arrives of a healthy instance is noted
	// on every connection it has, the one that lingers too.
	t.Run("the old stream lingers, and the new one is healthy: not named", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "orders-api", "o-1")()
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(silence)
		defer follow(e.m, "orders-api", "o-1")()
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(testGrace/10 + testGrace/20)
		for _, sub := range e.m.Subscribers() {
			if sub.DeliveryUnheard {
				panic(fmt.Sprintf("a connection of a healthy instance is marked: %+v", sub))
			}
		}
		s, _ := e.stop()
		wantComplete(t, s)
	})
	// The instance that was heard of and is silent, with a second connection
	// that says the same (both were there when its last word arrived): still
	// not named in a session nothing ties it to. Only a connection on which
	// nothing has arrived changes what is said.
	t.Run("two streams, both heard of and silent: not named", func(t *testing.T) {
		e := newDeliveryEnv(t, testGrace, nil)
		e.col.counting()
		defer follow(e.m, "orders-api", "o-1")()
		defer follow(e.m, "orders-api", "o-1")()
		hello(t, e.m, "orders-api", "o-1")
		time.Sleep(silence)
		s, _ := e.stop()
		wantComplete(t, s)
	})
	// A new connection on which nothing has arrived yet, for less than the
	// tenth of the grace an instance has to say hello: the instance is not
	// unheard, whatever its old connection says.
	t.Run("the new stream was opened a moment ago: not unheard yet", func(t *testing.T) {
		m := NewManager(Config{Collector: newScriptCollector().counting(), DeliveryGrace: time.Hour})
		t.Cleanup(m.Close)
		if _, err := m.Start(StartRequest{Name: "d", QuiesceSeconds: 2, MaxDrainSeconds: 1000}); err != nil {
			panic(err)
		}
		defer follow(m, "orders-api", "o-1")()
		hello(t, m, "orders-api", "o-1")
		defer follow(m, "orders-api", "o-1")()
		m.mu.Lock()
		defer m.mu.Unlock()
		// Five minutes on, the new connection has not had its tenth of the
		// grace. Two hours on, the session has run for two graces, the old
		// connection was last heard of two graces ago, and nothing has arrived
		// on the new one.
		if l := m.unheardLocked(time.Now().Add(5*time.Minute), all); len(l) != 0 {
			panic(fmt.Sprintf("unheard five minutes after it connected anew, with a grace of an hour: %+v", l))
		}
		if l := m.unheardLocked(time.Now().Add(2*time.Hour), all); len(l) != 1 || !l[0].heard.IsZero() {
			panic(fmt.Sprintf("two hours on: %+v, want the new connection", l))
		}
	})
}
