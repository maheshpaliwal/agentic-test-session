package runtime

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// The pause after a failed report is never longer than the heartbeat period,
// whatever RetryBase has grown to: when the outage is over the control plane
// hears of this process as soon as it would of a healthy one. (The fourth
// review: taking the bound out failed no test.)
func TestReporterRetryPauseIsBoundedByTheHeartbeatPeriod(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusForbidden} {
		// The control plane names the period: 50ms. The reporter's own pause
		// would be an hour.
		cp := newReportCP(t, `{"active":["someone-elses"],"delivery_reports":true,"delivery_heartbeat_ms":50}`)
		newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase = time.Hour })
		eventually(t, "the hello", func() bool { return cp.beats() >= 1 })
		cp.setStatus(code)
		// One attempt is the heartbeat that is due, a second the one made when
		// the control plane's grace is up (see reporter.failed); every further
		// one waited for the pause.
		eventually(t, fmt.Sprintf("HTTP %d: an attempt per heartbeat period, not one per hour", code), func() bool { return cp.postCount() >= 6 })
		before := cp.beats()
		cp.setStatus(0)
		eventually(t, "a heartbeat within a period of the end of the outage", func() bool { return cp.beats() > before })
	}
}

// The first session after none ends the pause a failed request left behind:
// the control plane counts the silence of this process from the moment the
// session began. (The fourth review: taking that out failed no test.)
func TestReporterRetriesAtOnceWhenASessionBeginsAfterNone(t *testing.T) {
	for _, code := range []int{http.StatusBadGateway, http.StatusForbidden} {
		cp := newReportCP(t, `{"active":[]}`)
		cp.setStatus(code)
		// The pause after the first failure is an hour, and so is the heartbeat
		// period that bounds it.
		newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase = time.Hour })
		eventually(t, "the hello, which fails", func() bool { return cp.postCount() >= 1 })
		cp.setStatus(0)
		time.Sleep(50 * time.Millisecond)
		if n := cp.beats(); n != 0 {
			panic(fmt.Sprintf("HTTP %d: %d requests got through during a pause of an hour", code, n))
		}
		cp.set(`{"active":["someone-elses"]}`)
		eventually(t, fmt.Sprintf("HTTP %d: a request when a session begins, whatever the pause", code), func() bool { return cp.beats() == 1 })
	}
}

// Whether the reports have failed for the whole grace is asked when the grace
// is up, not at whatever later moment the growing pause ends: the pause that
// would go past that moment ends at it. (With the defaults: failures after 0,
// 1, 3 and 7 seconds, and then at 10, not at 12.)
func TestReporterAsksAgainWhenTheGraceIsUp(t *testing.T) {
	cp := newReportCP(t, `{"active":["someone-elses"],"delivery_reports":true}`)
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: newTestCollector(t).srv.URL, FlushInterval: time.Hour})
	sub := newSubscriber(cp.srv.URL, "svc", "raw", "svc-1")
	// A heartbeat period of an hour: a grace of two.
	rep := manualReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: time.Hour, RetryBase: 50 * time.Minute, Heartbeat: time.Hour})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the first set of the stream", func() bool { return sub.connects.Load() == 1 })
	rep.pass() // its reports arrive
	rep.failed(http.StatusBadGateway)
	if want := rep.failedAt.Add(50 * time.Minute); !rep.retryAt.Equal(want) || !rep.failingSince.Equal(rep.failedAt) {
		panic(fmt.Sprintf("after the first failure at %v: the next request at %v, want %v; failing since %v", rep.failedAt, rep.retryAt, want, rep.failingSince))
	}
	// Ninety minutes later the pause would be an hour, the heartbeat period.
	rep.failingSince = rep.failingSince.Add(-90 * time.Minute)
	rep.failed(http.StatusBadGateway)
	if want := rep.failingSince.Add(2 * time.Hour); !rep.retryAt.Equal(want) || rep.askNow {
		panic(fmt.Sprintf("failing since %v, failed again at %v: the next request at %v, want %v, when the grace is up (stream opened anew: %v)",
			rep.failingSince, rep.failedAt, rep.retryAt, want, rep.askNow))
	}
	// That request fails as well: the stream is opened anew, and the pause is
	// the usual one.
	rep.failingSince = rep.failingSince.Add(-30 * time.Minute)
	rep.failed(http.StatusBadGateway)
	if want := rep.failedAt.Add(time.Hour); !rep.retryAt.Equal(want) || !rep.askNow {
		panic(fmt.Sprintf("after the grace: the next request at %v, want %v (stream opened anew: %v)", rep.retryAt, want, rep.askNow))
	}
	// The new stream is asked at once, an hour before the pause ends: the
	// control plane names a connection it has not heard of within a second,
	// and the outage may just be over (here it is).
	eventually(t, "a second stream, and its first set", func() bool { return len(cp.streams()) == 2 && sub.connects.Load() == 2 })
	before := cp.beats()
	rep.pass()
	if n := cp.beats(); n != before+1 || rep.askNow || rep.failures != 0 {
		panic(fmt.Sprintf("on the stream it opened anew: %d requests got through, want 1 (failures %d)", n-before, rep.failures))
	}
}

// The time in which the reports fail does not include a time without a
// session in which nothing was tried; what failed before it still counts. A
// report endpoint that is down whenever a session runs is noticed after a
// grace of sessions, and a refusal long ago, with nothing tried since, does
// not make the next failure the end of a grace.
func TestReporterDoesNotCountTheTimeWithoutASessionAsFailing(t *testing.T) {
	cp := newReportCP(t, `{"active":["one"],"delivery_reports":true}`)
	rec := NewRecorder(16)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: newTestCollector(t).srv.URL, FlushInterval: time.Hour})
	sub := newSubscriber(cp.srv.URL, "svc", "raw", "svc-1")
	rep := manualReporter(rec, exp, sub, "svc", "svc-1", reporterConfig{Tick: time.Hour, RetryBase: time.Hour, Heartbeat: time.Hour})
	t.Cleanup(func() { exp.Close(10 * time.Millisecond); rep.close(10 * time.Millisecond); sub.Close() })
	eventually(t, "the first set of the stream", func() bool { return sub.connects.Load() == 1 })
	rep.pass() // its reports arrive
	sessions := func(n int, set string) {
		cp.set(set)
		eventually(t, "the active set "+set, func() bool { return len(sub.snap.Load().active) == n })
		rep.pass()
	}
	rep.failed(http.StatusBadGateway)
	for _, c := range []struct {
		what               string
		lastFailure, ended time.Duration // how long ago the last report failed, and the last session ended
		want               time.Duration // of the 90 minutes since the first failure, the time that counts
	}{
		// Tried and failed while no session ran (the hello of a new stream,
		// say): what was not tried after that does not count.
		{"the last failure after the last session ended", 30 * time.Minute, 45 * time.Minute, 60 * time.Minute},
		// The session ended while the reports were failing: the time up to
		// its end counts, the time without one does not.
		{"the last session ended after the last failure", 30 * time.Minute, 10 * time.Minute, 80 * time.Minute},
	} {
		sessions(0, `{"active":[],"delivery_reports":true}`)
		now := time.Now()
		rep.failingSince, rep.failedAt, rep.idleAt = now.Add(-90*time.Minute), now.Add(-c.lastFailure), now.Add(-c.ended)
		before := time.Now()
		sessions(1, `{"active":["next"],"delivery_reports":true}`)
		after := time.Now()
		// The pass in which the session began lies between before and after.
		if lo, hi := before.Add(-c.want), after.Add(-c.want); rep.failingSince.Before(lo) || rep.failingSince.After(hi) {
			panic(fmt.Sprintf("%s: a session begins between %v and %v: failing since %v, want %v before the session began",
				c.what, before, after, rep.failingSince, c.want))
		}
	}
}

// A refused report is not sent again while no session runs: an idle service
// behind a proxy that answers 403 used to ask again every five seconds for as
// long as it ran (the fourth review: 8 requests in 20 seconds, per instance,
// with no session anywhere). It is tried when a session begins and while one
// runs, and once on every stream that is opened.
func TestReporterDoesNotRetryARefusedReportWithoutASession(t *testing.T) {
	cp := newReportCP(t, `{"active":[]}`)
	cp.setStatus(http.StatusForbidden)
	// A pause of 5ms that doubles to 160ms: without the rule, a request every
	// 160ms at least.
	newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase = 5 * time.Millisecond })
	eventually(t, "the hello, which is refused", func() bool { return cp.postCount() >= 1 })
	quiet := func(when string) int {
		time.Sleep(100 * time.Millisecond) // a request that was on its way
		n := cp.postCount()
		time.Sleep(600 * time.Millisecond)
		if got := cp.postCount(); got != n {
			panic(fmt.Sprintf("%s: %d more refused requests in 600ms from a process that follows no session", when, got-n))
		}
		return n
	}
	if n := quiet("after the first refusal"); n != 1 {
		panic(fmt.Sprintf("%d requests, want the one at the connect", n))
	}
	// A session: the control plane waits for a word of this process.
	cp.set(`{"active":["someone-elses"]}`)
	eventually(t, "attempts while a session runs", func() bool { return cp.postCount() >= 4 })
	cp.set(`{"active":[]}`)
	n := quiet("after the session")
	// A new stream: what is behind it may take the report. One attempt.
	cp.drop <- struct{}{}
	eventually(t, "one attempt on the new stream", func() bool { return cp.postCount() == n+1 })
	quiet("after the attempt on the new stream")
	// A failure that is not a refusal (the control plane restarts) is retried
	// as before: it ends by itself.
	cp.setStatus(http.StatusBadGateway)
	cp.drop <- struct{}{}
	eventually(t, "attempts against a control plane that fails", func() bool { return cp.postCount() >= n+5 })
	cp.setStatus(0)
	eventually(t, "it arrives", func() bool { return cp.beats() == 1 })
}

// The reports of a process fail while its stream works. For less than the
// control plane's grace (two heartbeat periods) nothing else happens: they are
// retried. Once they have failed for all of it the stream is opened anew,
// once: the control plane then has a connection it has never heard of, which
// it names in every session, where it says nothing of an instance it has
// heard of and that is merely silent (frozen, or idle behind an endpoint that
// was down for a moment).
func TestReporterOpensTheStreamAnewWhenItsReportsFailForTheGrace(t *testing.T) {
	t.Run("shorter than the grace: the stream stays", func(t *testing.T) {
		// A grace of a minute, and a few hundred milliseconds of failures.
		cp := newReportCP(t, `{"active":["someone-elses"],"delivery_reports":true,"delivery_heartbeat_ms":30000}`)
		newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase = 5 * time.Millisecond })
		eventually(t, "the hello", func() bool { return cp.beats() >= 1 })
		cp.setStatus(http.StatusServiceUnavailable)
		// The process has something to say all the time: a heartbeat would not
		// be due for half a minute.
		cp.drop <- struct{}{}
		eventually(t, "the outage is noticed, and more", func() bool { return cp.postCount() >= 4 })
		cp.setStatus(0)
		eventually(t, "the reports arrive again", func() bool { return cp.beats() >= 2 })
		time.Sleep(300 * time.Millisecond)
		if s := cp.streams(); len(s) != 2 {
			panic(fmt.Sprintf("%d streams after an outage far shorter than the grace, want the two the test opened", len(s)))
		}
	})
	// Reports that were refused on the stream from the start: the control
	// plane has not heard of this process on its connection, and names it
	// already. Another stream would change nothing, take the process off the
	// control plane's list for a moment, and hold the drain of a session it
	// took part in for another grace. (It did: a service that delivered its
	// events and whose reports were refused from the start left its first
	// session complete.)
	t.Run("refused from the start, never heard of on its stream: the stream stays", func(t *testing.T) {
		const period = 50 * time.Millisecond
		cp := newReportCP(t, fmt.Sprintf(`{"active":["someone-elses"],"delivery_reports":true,"delivery_heartbeat_ms":%d}`, period.Milliseconds()))
		cp.setStatus(http.StatusForbidden)
		newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase = 5 * time.Millisecond })
		eventually(t, "refused requests", func() bool { return cp.postCount() >= 6 })
		time.Sleep(10 * period) // five graces
		if s := cp.streams(); len(s) != 1 {
			panic(fmt.Sprintf("%d streams of a process whose reports never arrived, want the one", len(s)))
		}
	})
	// Reports that were lost from the start, not refused (the fifth review: a
	// gateway that passed the first one on and answered 504): the control
	// plane may have heard of this process, which cannot know. One more
	// stream, on which it has not; and no further one.
	t.Run("lost from the start: once", func(t *testing.T) {
		const period = 50 * time.Millisecond
		cp := newReportCP(t, fmt.Sprintf(`{"active":["someone-elses"],"delivery_reports":true,"delivery_heartbeat_ms":%d}`, period.Milliseconds()))
		cp.setStatus(http.StatusGatewayTimeout)
		began := time.Now()
		// A stream at most every ReopenGap, were the rule broken: far more than 2.
		newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase, c.ReopenGap = 5*time.Millisecond, time.Millisecond })
		eventually(t, "a second stream", func() bool { return len(cp.streams()) == 2 })
		if d := time.Since(began); d < 2*period {
			panic(fmt.Sprintf("the stream was opened anew after %v, before the grace of %v was up", d, 2*period))
		}
		n := cp.postCount()
		eventually(t, "attempts on the new stream", func() bool { return cp.postCount() >= n+3 })
		time.Sleep(10 * period) // five graces
		if s := cp.streams(); len(s) != 2 {
			panic(fmt.Sprintf("%d streams of a process whose reports got no answer from the start, want 2", len(s)))
		}
	})
	for _, code := range []int{http.StatusServiceUnavailable, http.StatusForbidden} {
		t.Run(fmt.Sprintf("HTTP %d for the grace: once", code), func(t *testing.T) {
			const period = 100 * time.Millisecond // a grace of 200ms
			cp := newReportCP(t, fmt.Sprintf(`{"active":["someone-elses"],"delivery_reports":true,"delivery_heartbeat_ms":%d}`, period.Milliseconds()))
			// Two streams are never closer than a grace, or ReopenGap (5s by
			// default: longer than the test waits for the second outage).
			newReportEnvOn(t, cp, nil, func(c *reporterConfig) { c.RetryBase, c.ReopenGap = 5*time.Millisecond, time.Millisecond })
			eventually(t, "the hello", func() bool { return cp.beats() >= 1 })
			failedAt := time.Now()
			cp.setStatus(code)
			eventually(t, "a second stream", func() bool { return len(cp.streams()) == 2 })
			if d := time.Since(failedAt); d < 2*period {
				panic(fmt.Sprintf("the stream was opened anew after %v of failures, before the grace of %v was up", d, 2*period))
			}
			// The new stream announces the reports like the first, and the
			// reports are tried on it: it may lead somewhere else.
			if s := cp.streams(); s[1] != "svc-1-abcdef" {
				panic(fmt.Sprintf("streams %q", s))
			}
			n := cp.postCount()
			eventually(t, "attempts on the new stream", func() bool { return cp.postCount() >= n+3 })
			// Once per outage, however long it lasts.
			time.Sleep(10 * period)
			if s := cp.streams(); len(s) != 2 {
				panic(fmt.Sprintf("%d streams during one outage, want 2", len(s)))
			}
			// The outage ends, and another begins: once more.
			before := cp.beats()
			cp.setStatus(0)
			eventually(t, "the reports arrive again", func() bool { return cp.beats() > before })
			cp.setStatus(code)
			eventually(t, "a third stream, for the second outage", func() bool { return len(cp.streams()) == 3 })
		})
	}
}
