package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
)

// outageSim runs a reporter through simulated time: a clock the test moves, a
// transport that answers the reports without a connection, and a subscriber
// whose loop is not started, whose streams the test "opens" the way
// Subscriber.run and stream do. A day of failures takes a second, and no
// socket is opened for any of it.
type outageSim struct {
	now time.Time
	sub *Subscriber
	rec *Recorder
	rep *reporter
	// answer is the status a report is answered with; 0: no answer.
	answer func() int
	posts  int // reports sent
	// streams counts the streams opened, the first included; ended those the
	// reporter ended to open another (Subscriber.reopen).
	streams, ended int
}

const (
	simTick   = 250 * time.Millisecond
	simPeriod = 5 * time.Second // the heartbeat period: a grace of 10s, the default
)

type simTransport func(*http.Request) (*http.Response, error)

func (f simTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newOutageSim(t *testing.T, answer func() int) *outageSim {
	logTo(t) // an outage a minute, for a day: not on the terminal
	sim := &outageSim{now: time.Now(), answer: answer}
	sim.sub = &Subscriber{base: "http://control-plane.invalid", reportsAs: "svc-1", opened: make(chan struct{}, 1)}
	sim.sub.client = &http.Client{Transport: simTransport(func(r *http.Request) (*http.Response, error) {
		sim.posts++
		code := sim.answer()
		if code == 0 {
			return nil, errors.New("no answer")
		}
		return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"results":[]}`)), Request: r}, nil
	})}
	sim.sub.announce.Store(true)
	// A session of somebody else runs all the time: a heartbeat is due every period.
	sim.sub.snap.Store(&snapshot{active: set.Of("someone-elses"), reports: true, heartbeat: simPeriod})
	sim.rec = NewRecorder(16)
	exp := NewExporter(sim.rec, ExporterConfig{CollectorURL: newTestCollector(t).srv.URL, FlushInterval: time.Hour})
	sim.rep = manualReporter(sim.rec, exp, sim.sub, "svc", "svc-1", reporterConfig{Tick: time.Hour, Now: func() time.Time { return sim.now }})
	t.Cleanup(func() {
		sim.answer = func() int { return 0 }
		exp.Close(10 * time.Millisecond)
		sim.rep.close(10 * time.Millisecond)
	})
	sim.open()
	return sim
}

// open is a stream that delivered its first set.
func (sim *outageSim) open() {
	sim.streams++
	cancel := context.CancelFunc(func() { sim.ended++ })
	sim.sub.streamCancel.Store(&cancel)
	sim.sub.streaming.Store(true)
	sim.sub.connects.Add(1)
}

// run moves the clock by d, a tick at a time, with a pass of the reporter at
// every tick. A stream the reporter ended is opened again at once, as the
// subscriber's loop does, unless connect says no (a stream that is slow to
// open). each, when not nil, runs before every pass.
func (sim *outageSim) run(d time.Duration, connect func() bool, each func()) {
	for end := sim.now.Add(d); sim.now.Before(end); {
		sim.now = sim.now.Add(simTick)
		if each != nil {
			each()
		}
		sim.rep.pass()
		if sim.sub.again.Load() && (connect == nil || connect()) {
			sim.sub.again.Store(false)
			sim.open()
		}
	}
}

// The fifth review: the first report of a process arrived at the control
// plane and its answer was lost; nothing arrived since. The control plane had
// heard of the process, and names such a one only where something ties it to
// the session; the process did not open its stream anew, because no report
// had been answered on it. 0 of 6 sessions named it. A stream on which a
// request was lost (not refused) is opened anew, once.
//
// And the bound: whatever the outage looks like and however long it lasts,
// the streams this process opens for it are counted here, over a day.
func TestReporterOpensAtMostOneStreamPerOutageAndGrace(t *testing.T) {
	const day = 24 * time.Hour
	const gap = 2 * simPeriod // a grace; longer than the default ReopenGap
	bound := func(sim *outageSim, d time.Duration) {
		if most := 1 + int(d/gap); sim.ended > most {
			panic(fmt.Sprintf("%d streams opened anew in %v, more than one per %v (%d reports sent)", sim.ended, d, gap, sim.posts))
		}
		if sim.posts > int(d/time.Second)+8 {
			panic(fmt.Sprintf("%d reports in %v", sim.posts, d))
		}
	}
	for _, c := range []struct {
		what   string
		answer int
		want   int // streams the process opens anew in a day of it
	}{
		{"the answer is lost on the way back (504), from the first report on", http.StatusGatewayTimeout, 1},
		{"no answer at all, from the first report on", 0, 1},
		// Nothing of the process arrived, and the control plane names it as it is.
		{"refused (403), from the first report on", http.StatusForbidden, 0},
	} {
		t.Run(c.what, func(t *testing.T) {
			sim := newOutageSim(t, func() int { return c.answer })
			// Not before the grace is up, and with the request made when it is.
			sim.run(2*simPeriod-simTick, nil, nil)
			if sim.ended != 0 {
				panic(fmt.Sprintf("a stream opened anew after %v of failures, before the grace of %v", 2*simPeriod-simTick, 2*simPeriod))
			}
			sim.run(simPeriod, nil, nil)
			if sim.ended != c.want {
				panic(fmt.Sprintf("%d streams opened anew when the reports had failed for a grace and a period, want %d", sim.ended, c.want))
			}
			sim.run(day, nil, nil)
			if sim.ended != c.want || sim.streams != 1+c.want {
				panic(fmt.Sprintf("%d streams opened anew in a day of one outage (%d streams in all), want %d", sim.ended, sim.streams, c.want))
			}
			bound(sim, day)
			// The outage ends, and the next one is another: once more.
			sim.answer = func() int { return http.StatusOK }
			sim.run(time.Minute, nil, nil)
			sim.answer = func() int { return http.StatusBadGateway }
			sim.run(day, nil, nil)
			if sim.ended != c.want+1 {
				panic(fmt.Sprintf("%d streams opened anew after a second outage, want %d", sim.ended, c.want+1))
			}
		})
	}
	// The worst there is: the stream breaks by itself every three seconds (a
	// proxy that cuts it), so every stream is a new one, not one this process
	// opened, and on every one a request is lost. One more stream per grace is
	// the most this process adds to that.
	t.Run("streams that keep breaking, and every report lost", func(t *testing.T) {
		sim := newOutageSim(t, func() int { return 0 })
		last := sim.now
		sim.run(day, nil, func() {
			if sim.now.Sub(last) >= 3*time.Second && !sim.sub.again.Load() {
				last = sim.now
				sim.open()
			}
		})
		bound(sim, day)
		if sim.ended < int(day/gap)/2 {
			panic(fmt.Sprintf("%d streams opened anew: the case is not the one the test means", sim.ended))
		}
	})
	// An endpoint that answers one report a minute and fails the others: each
	// answer ends an outage, each silence of a grace is one.
	t.Run("one report in a minute is answered", func(t *testing.T) {
		var sim *outageSim
		answered := time.Time{}
		sim = newOutageSim(t, func() int {
			if sim.now.Sub(answered) >= time.Minute {
				answered = sim.now
				return http.StatusOK
			}
			return http.StatusServiceUnavailable
		})
		sim.run(day, nil, nil)
		bound(sim, day)
		// An answer every minute, or up to a period later: the outages.
		if most, least := int(day/time.Minute), int(day/(time.Minute+simPeriod)); sim.ended < least-1 || sim.ended > most {
			panic(fmt.Sprintf("%d streams opened anew, want one per outage: %d to %d", sim.ended, least, most))
		}
	})
	// A control plane that asks for a heartbeat every millisecond (a grace of
	// 2ms): the gap between two streams is ReopenGap, not its grace.
	t.Run("a control plane with a grace of 2ms", func(t *testing.T) {
		sim := newOutageSim(t, func() int { return 0 })
		sim.sub.snap.Store(&snapshot{active: set.Of("someone-elses"), reports: true, heartbeat: time.Millisecond})
		sim.run(time.Hour, nil, func() { sim.open() }) // a new stream before every pass
		if most := 1 + int(time.Hour/(5*time.Second)); sim.ended > most || sim.ended < most/2 {
			panic(fmt.Sprintf("%d streams opened anew in an hour, want one per 5s (ReopenGap): at most %d", sim.ended, most))
		}
	})
}

// While the stream this process opened anew has not delivered its first set
// (a proxy that is slow to connect, a control plane that is busy), the
// reports go on failing, and none of those failures ends the stream again:
// that would be the stream that is being opened, and with requests that fail
// faster than a stream opens the process would never have one. (The fifth
// review: taking the askNow guard out failed no test.)
func TestReporterDoesNotEndTheStreamItIsOpeningAnew(t *testing.T) {
	sim := newOutageSim(t, func() int { return http.StatusOK })
	sim.run(simPeriod, nil, nil) // its reports arrive
	sim.answer = func() int { return 0 }
	never := func() bool { return false }
	sim.run(3*simPeriod, never, nil)
	if sim.ended != 1 || !sim.rep.askNow {
		panic(fmt.Sprintf("%d streams ended when the reports had failed for the grace, want 1 (askNow %v)", sim.ended, sim.rep.askNow))
	}
	// Ten minutes in which the new stream does not come up: sixty graces.
	sim.run(10*time.Minute, never, nil)
	if sim.ended != 1 {
		panic(fmt.Sprintf("the stream that was being opened was ended %d more times", sim.ended-1))
	}
	// It comes up: asked at once, and it is the one stream of this outage.
	sim.answer = func() int { return http.StatusBadGateway }
	sim.sub.again.Store(false)
	sim.open()
	before := sim.posts
	sim.run(simTick, nil, nil)
	if sim.streams != 2 || sim.posts != before+1 || sim.rep.askNow || !sim.rep.reopened {
		panic(fmt.Sprintf("%d streams, %d reports on the new one at once (askNow %v, reopened %v)", sim.streams, sim.posts-before, sim.rep.askNow, sim.rep.reopened))
	}
	sim.run(time.Hour, nil, nil)
	if sim.ended != 1 {
		panic(fmt.Sprintf("%d streams opened anew in one outage", sim.ended))
	}
}

// Nothing to end: while no stream is open the process is not connected, and
// the stream that comes up is a new connection by itself. It is not taken for
// one this process opened anew, so a request lost on it still counts.
func TestReporterDoesNotCountAStreamItCouldNotEnd(t *testing.T) {
	sim := newOutageSim(t, func() int { return http.StatusOK })
	sim.run(simPeriod, nil, nil)
	sim.answer = func() int { return 0 }
	sim.sub.streamCancel.Store(nil) // the stream is down
	sim.sub.streaming.Store(false)
	// Without a stream no heartbeat is sent; the report of a session this
	// process recorded for is, and gets no answer.
	sim.rec.Record(core.Event{SessionID: "someone-elses", Kind: core.KindAPIIn, LocalSeq: seqBase + 1})
	before := sim.posts
	sim.run(time.Minute, nil, nil)
	if sim.posts < before+6 {
		panic(fmt.Sprintf("test setup: %d reports failed without a stream", sim.posts-before))
	}
	if sim.rep.askNow || !sim.rep.reopenedAt.IsZero() || sim.ended != 0 {
		panic(fmt.Sprintf("without a stream: askNow %v, opened anew at %v", sim.rep.askNow, sim.rep.reopenedAt))
	}
	sim.open() // it is back by itself, and the reports still get no answer
	sim.run(3*simPeriod, nil, nil)
	if sim.ended != 1 {
		panic(fmt.Sprintf("%d streams opened anew on the stream that came back, want 1", sim.ended))
	}
}
