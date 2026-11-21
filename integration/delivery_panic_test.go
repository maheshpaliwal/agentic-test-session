package integration

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// arm makes the SDK inside the service panic at a fault point, times times
// (below zero: every time).
func (a *svcApp) arm(site string, times int) {
	resp, err := deliveryHTTP.Post(fmt.Sprintf("http://%s/fault?site=%s&times=%d", a.addr, site, times), "text/plain", nil)
	if err != nil {
		panic(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("POST /fault: " + resp.Status)
	}
}

// panics is how often the SDK inside the service has panicked at the armed
// fault point. (The service's output is not read while it runs: the process
// is still writing it.)
func (a *svcApp) panics() int {
	resp, err := deliveryHTTP.Get("http://" + a.addr + "/fault")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	n, err := strconv.Atoi(string(b))
	if err != nil {
		panic(fmt.Sprintf("GET /fault: %q", b))
	}
	return n
}

// log ends the service and returns what it wrote.
func (a *svcApp) log() string {
	a.kill()
	return a.out.String()
}

// alive reports whether the service still answers.
func (a *svcApp) alive() bool {
	resp, err := deliveryHTTP.Get("http://" + a.addr + "/healthz")
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// A panic in one of the SDK's goroutines used to end the service it runs
// in. Now the service goes on, and what the panic cost the recording is said
// in the session: never a complete session over a service that had stopped
// recording.
func TestDelivery_APanicInTheSDKNeitherEndsTheHostNorPassesForAGreenRun(t *testing.T) {
	// stopped is the reason of a session the service stopped recording in.
	stopped := func(t *testing.T, r deliveryRun, service, how string) {
		t.Helper()
		if r.err != nil || !r.info.Incomplete || len(r.info.IncompleteReasons) != 1 {
			panic(fmt.Sprintf("a session that ran while the service did not record: %s", r))
		}
		if why := r.info.IncompleteReasons[0]; !strings.HasPrefix(why, "service "+service+" (instance ") ||
			!strings.Contains(why, "stopped recording after an internal error") || !strings.Contains(why, how) {
			panic("the reason: " + why)
		}
	}

	// The subscriber panics once: recording stops until the subscriber, started
	// again a second later, has the active sessions anew. The host answers
	// throughout; the events of that second are missing, and the session says so.
	t.Run("the subscriber is started again", func(t *testing.T) {
		t.Parallel()
		e := newDeliveryBase(t)
		a := e.startSvc("orders-api", svcOpts{})
		e.connected("orders-api")
		s, err := e.begin(30)
		if err != nil {
			panic(err)
		}
		if err := a.burst(s, 50); err != nil {
			panic(err)
		}
		a.arm("subscriber.apply", 1)
		other, err := e.begin(30) // a new active set: applying it panics
		if err != nil {
			panic(err)
		}
		if err := a.burst(s, 50); err != nil { // not recorded: the host must not notice
			panic(fmt.Sprintf("a request of the host while the SDK does not record: %v", err))
		}
		// The subscriber is started again a second after the panic and fetches
		// the active sessions anew: a session started then is recorded in full
		// and is complete.
		time.Sleep(3 * time.Second)
		if !a.alive() {
			panic("the host is gone")
		}
		later, err := e.begin(30)
		if err != nil {
			panic(err)
		}
		if err := a.burst(later, 20); err != nil {
			panic(err)
		}
		if next := e.finish(later, 21); next.err != nil || next.info.Incomplete || next.got != 21 || len(next.info.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("a session started after recording had resumed: %s", next))
		}
		// The sessions that ran while the service did not record say so.
		r := e.finish(s, 102)
		stopped(t, r, "orders-api", "its subscriber had panicked and it recorded nothing for a time; it records again now")
		if r.got >= 102 {
			panic(fmt.Sprintf("the events sent while the subscriber was down were recorded: %s", r))
		}
		if o := e.finish(other, 0); !o.info.Incomplete || len(o.info.IncompleteReasons) != 1 ||
			!strings.Contains(o.info.IncompleteReasons[0], "stopped recording after an internal error") {
			panic(fmt.Sprintf("the other session that ran meanwhile: %s", o))
		}
		if out := a.log(); strings.Count(out, "the subscriber goroutine panicked") != 1 || strings.Contains(out, "stays stopped") ||
			!strings.Contains(out, "the application is not affected") {
			t.Errorf("the service's log:\n%s", out)
		}
	})

	// The subscriber panics every time and stays stopped after half a minute
	// of restarts. The session that ran meanwhile used to seal complete with
	// nothing unconfirmed and none of the service's events.
	t.Run("the subscriber stays stopped", func(t *testing.T) {
		t.Parallel()
		e := newDeliveryBase(t)
		a := e.startSvc("orders-api", svcOpts{})
		e.connected("orders-api")
		a.arm("subscriber.apply", -1)
		s, err := e.begin(30) // a new active set: applying it panics, and so does every one after it
		if err != nil {
			panic(err)
		}
		if err := a.burst(s, 50); err != nil {
			panic(fmt.Sprintf("a request of the host while the SDK does not record: %v", err))
		}
		// Six panics: five restarts (after 1, 2, 4, 8 and 16 seconds), then it
		// stays stopped.
		for deadline := time.Now().Add(90 * time.Second); a.panics() < 6; time.Sleep(100 * time.Millisecond) {
			if time.Now().After(deadline) {
				panic(fmt.Sprintf("the subscriber never gave up: %d panics", a.panics()))
			}
		}
		time.Sleep(time.Second) // the control plane is told
		if err := a.burst(s, 50); err != nil || !a.alive() {
			panic(fmt.Sprintf("the host after the subscriber gave up: %v, alive %v", err, a.alive()))
		}
		r := e.finish(s, 102)
		stopped(t, r, "orders-api", "its subscriber panicked repeatedly and stays stopped")
		if r.got != 0 {
			panic(fmt.Sprintf("events were recorded without an active set: %s", r))
		}
		if out := a.log(); strings.Count(out, "the subscriber goroutine panicked") != 6 || !strings.Contains(out, "stays stopped: nothing more is recorded") {
			t.Errorf("the service's log:\n%s", out)
		}
	})

	// The reporter panics under the recorder's lock, which every request of
	// the host that records an event takes. The host answers, the reporter is
	// started again, and the sessions are complete.
	t.Run("the reporter panics under the recorder's lock", func(t *testing.T) {
		t.Parallel()
		e := newDeliveryBase(t)
		a := e.startSvc("orders-api", svcOpts{})
		e.connected("orders-api")
		first, err := e.begin(30)
		if err != nil {
			panic(err)
		}
		if err := a.burst(first, 5); err != nil {
			panic(err)
		}
		a.arm("recorder.forget", 2) // the reporter forgets a session when it has ended
		if r := e.finish(first, 6); r.err != nil || r.short() {
			panic(fmt.Sprintf("the first session: %s", r))
		}
		for deadline := time.Now().Add(30 * time.Second); a.panics() < 2; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				panic(fmt.Sprintf("the reporter never got to forget the session: %d panics", a.panics()))
			}
		}
		s, err := e.begin(30)
		if err != nil {
			panic(err)
		}
		done := make(chan error, 1)
		go func() { done <- a.burst(s, 100) }()
		select {
		case err := <-done:
			if err != nil {
				panic(err)
			}
		case <-time.After(20 * time.Second):
			panic("a request of the host hangs: the reporter left the recorder's lock held when it panicked")
		}
		if r := e.finish(s, 101); r.err != nil || r.info.Incomplete || r.short() || len(r.info.DeliveryUnconfirmed) != 0 {
			panic(fmt.Sprintf("the session after the reporter was started again: %s", r))
		}
		if out := a.log(); strings.Count(out, "the delivery reporter goroutine panicked") != 2 || strings.Contains(out, "stays stopped") {
			t.Errorf("the service's log:\n%s", out)
		}
	})
}
