package runtime

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"slices"
	"time"
)

// deliveryReport is the body of POST /v1/sessions/{id}/delivery: what this
// process recorded for a session and what became of it. The counts are
// cumulative, so a report replaces the ones before it.
type deliveryReport struct {
	Service   string `json:"service,omitempty"`
	Instance  string `json:"instance"`
	Recorded  uint64 `json:"recorded"`
	Delivered uint64 `json:"delivered"`
	Dropped   uint64 `json:"dropped"`
	Buffered  uint64 `json:"buffered"`
	// DeliveredTotal is what this process delivered for all sessions since it
	// started. The control plane reads a rising total as "this instance is
	// getting events through", also while the events of the session reported
	// here wait behind those of others.
	DeliveredTotal uint64 `json:"delivered_total"`
	LastError      string `json:"last_error,omitempty"`
	// Final: sent after this process saw the session draining and had the
	// time to flush, or while it shuts down.
	Final bool `json:"final,omitempty"`
}

// counts is the report without the total: what says whether the session
// itself changed.
func (d deliveryReport) counts() deliveryReport {
	d.DeliveredTotal = 0
	return d
}

// maxReportError bounds LastError.
const maxReportError = 256

// reporterConfig tunes a reporter. Zero values get defaults.
type reporterConfig struct {
	Tick time.Duration // how often the accounts are looked at (default 250ms)
	// Interval is the least time between two reports of a session that is
	// not draining (default 5s). A draining one is reported whenever it
	// changed.
	Interval time.Duration
	// FirstDelay is how long the first report of a session waits for the
	// exporter to deliver what was recorded (default 1s), so that it does not
	// say "undelivered" about events that are a flush interval old.
	FirstDelay time.Duration
	// DrainFlush is how long a session that turned draining has to deliver
	// what it holds before its reports are final all the same (default 2s).
	DrainFlush time.Duration
	// KickEvery is how often the exporter is made to retry while a draining
	// session has undelivered events (default 1s): the control plane waits
	// only so long for them, and the exporter's own pause can be longer. It
	// is also the least time between two reports of a session that say
	// nothing new about it: one that only carries a new total, and one sent
	// while the stream is down (see pass).
	KickEvery time.Duration
	Timeout   time.Duration // per report request (default 2s)
	// Burst is the most reports one pass sends (default 32). With the default
	// Tick that is 128 reports a second: many sessions that change at once
	// are reported over a few passes, the draining ones first.
	Burst int
	// RetryBase is the pause after a first failed report; it doubles with
	// every further one (default 1s), up to 4 times as long for a control
	// plane that does not answer or fails, 32 times for one that refuses.
	RetryBase time.Duration
}

// reporter sends the delivery accounts of the recorder to the control plane:
// events that never reach the collector are invisible there, and the control
// plane is the one service this process is known to reach (it follows its
// active set). One goroutine; nothing here is on the path of a request.
type reporter struct {
	cfg      reporterConfig
	rec      *Recorder
	exp      *Exporter
	sub      *Subscriber
	service  string
	instance string

	sessions map[string]*reportState
	// idle are the draining sessions this process recorded nothing for and
	// has said so (see pass).
	idle map[string]*reportState
	// off is set when the control plane does not know the endpoint (an older
	// one answers 404). It lasts until the stream is opened again, which is
	// when the control plane may have been replaced.
	off      bool
	connects uint64
	failures int
	retryAt  time.Time // no report before this, after a failed one
	kickedAt time.Time
	urgentOf *snapshot // the active set whose draining sessions the exporter was told of
	// down, outageLogged and lastOutageLog make the reporter say once when
	// its reports stop arriving and once when they arrive again, at most one
	// such pair per outageLogGap.
	down          bool
	outageLogged  bool
	lastOutageLog time.Time

	stop chan struct{}
	done chan struct{}
}

// reportState is what the reporter knows about one session with an account.
type reportState struct {
	seen   time.Time      // when the account was first seen
	sent   bool           // a report was accepted
	last   deliveryReport // the last one that was
	sentAt time.Time
	resend bool // the control plane may have lost what was sent
	// drainAt is when the session was first seen draining. final is set once
	// it then had nothing left to deliver, or DrainFlush to deliver it: its
	// reports are final from then on.
	drainAt time.Time
	final   bool
	// closed: the control plane takes no more reports of this session.
	closed bool
}

func newReporter(rec *Recorder, exp *Exporter, sub *Subscriber, service, instance string, cfg reporterConfig) *reporter {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&cfg.Tick, 250*time.Millisecond)
	def(&cfg.Interval, 5*time.Second)
	def(&cfg.FirstDelay, time.Second)
	def(&cfg.DrainFlush, 2*time.Second)
	def(&cfg.KickEvery, time.Second)
	def(&cfg.Timeout, 2*time.Second)
	def(&cfg.RetryBase, time.Second)
	if cfg.Burst <= 0 {
		cfg.Burst = 32
	}
	r := &reporter{cfg: cfg, rec: rec, exp: exp, sub: sub, service: service, instance: instance,
		sessions: map[string]*reportState{}, idle: map[string]*reportState{}, stop: make(chan struct{}), done: make(chan struct{})}
	go r.loop()
	return r
}

func (r *reporter) loop() {
	defer close(r.done)
	t := time.NewTicker(r.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.pass()
		}
	}
}

// report builds the report of an account.
func (r *reporter) report(a account, final bool, total uint64) deliveryReport {
	d := deliveryReport{Service: r.service, Instance: r.instance, Recorded: a.recorded, Delivered: a.delivered,
		Dropped: a.dropped, Buffered: a.buffered(), DeliveredTotal: total, Final: final}
	if d.Buffered > 0 || d.Dropped > 0 {
		d.LastError = r.exp.lastError()
	}
	return d
}

// due is a report a pass wants to send.
type due struct {
	sid      string
	st       *reportState
	d        deliveryReport
	draining bool
}

// pass looks at the accounts once and sends what is due. It never waits for
// the exporter: one session's undeliverable events must not delay the report
// of another.
//
// A session is reported for the first time once its events had the time to
// be delivered, then at most every Interval while it changes. When it turns
// draining its counts are sent at once if they changed (so the control plane
// knows what to wait for) and the exporter is made to send now, the events
// of draining sessions first; from then on every change is reported at once.
// The reports are final once nothing is left to deliver, or after DrainFlush.
// Events recorded later in the drain make a new final report that replaces
// the last. While a draining session still holds events, its report is sent
// again, every KickEvery at most, when only the total moved: that tells the
// control plane that this process is working through a backlog. A session
// that left the active set is forgotten.
//
// A draining session this process recorded nothing for is reported once,
// as that, while the stream is up: this process told the control plane, when
// it opened the stream, that it reports, and the control plane then expects
// to hear from it for every draining session. An instance it does not hear
// from is one whose reports do not arrive.
//
// While the stream is down this process learns of a drain late, from a poll,
// or not at all. Its reports then do not wait for FirstDelay or Interval:
// what the control plane was not told before it seals a session, it cannot
// hold against the collector.
//
// A pass sends Burst reports at most, the draining sessions first and of
// those the ones reported longest ago, and none after one that failed.
//
// The set read here is the raw one: under the kill switch IsActive is false
// for every session, and what was recorded before must still be accounted.
func (r *reporter) pass() {
	accts := r.rec.accounts()
	sn := r.sub.snap.Load()
	if sn != r.urgentOf {
		r.urgentOf = sn
		r.exp.setUrgent(sn.draining)
	}
	if len(accts) == 0 && len(r.sessions) == 0 && len(sn.draining) == 0 && len(r.idle) == 0 {
		return
	}
	if c := r.sub.connects.Load(); c != r.connects {
		r.connects, r.off = c, false
		for _, st := range r.sessions {
			st.resend = true
		}
		clear(r.idle)
	}
	now := time.Now()
	streaming := r.sub.streaming.Load()
	total := r.rec.deliveredTotal()
	stuck, turned := false, false
	var todo []due
	for sid, a := range accts {
		if _, live := sn.active[sid]; !live {
			r.rec.forget(sid)
			delete(accts, sid)
			continue
		}
		st := r.sessions[sid]
		if st == nil {
			st = &reportState{seen: now}
			r.sessions[sid] = st
		}
		_, draining := sn.draining[sid]
		if draining && st.drainAt.IsZero() {
			st.drainAt, turned = now, true
		}
		if draining && (a.buffered() == 0 || now.Sub(st.drainAt) >= r.cfg.DrainFlush) {
			st.final = true
		}
		d := r.report(a, st.final, total)
		stuck = stuck || draining && d.Buffered > 0
		changed := !st.sent || st.resend || d.counts() != st.last.counts()
		paced := now.Sub(st.sentAt) >= r.cfg.KickEvery
		send := false
		switch {
		case draining:
			send = changed || d.Buffered > 0 && d.DeliveredTotal != st.last.DeliveredTotal && paced
		case !changed:
		case !st.sent:
			send = d.Buffered == 0 || !streaming || now.Sub(st.seen) >= r.cfg.FirstDelay
		default:
			send = st.resend || now.Sub(st.sentAt) >= r.cfg.Interval || !streaming && paced
		}
		if send {
			todo = append(todo, due{sid, st, d, draining})
		}
	}
	for sid := range r.sessions {
		if _, ok := accts[sid]; !ok {
			delete(r.sessions, sid)
		}
	}
	for sid := range r.idle {
		if _, ok := sn.draining[sid]; !ok {
			delete(r.idle, sid)
		}
	}
	if streaming {
		for sid := range sn.draining {
			if _, has := accts[sid]; has {
				continue
			}
			st := r.idle[sid]
			if st == nil {
				st = &reportState{seen: now}
				r.idle[sid] = st
			}
			if !st.sent && !st.closed {
				todo = append(todo, due{sid, st, deliveryReport{Service: r.service, Instance: r.instance, DeliveredTotal: total, Final: true}, true})
			}
		}
	}
	slices.SortFunc(todo, func(a, b due) int {
		if a.draining != b.draining {
			if a.draining {
				return -1
			}
			return 1
		}
		return cmp.Or(a.st.sentAt.Compare(b.st.sentAt), cmp.Compare(a.sid, b.sid))
	})
	for i, x := range todo {
		if i == r.cfg.Burst || !r.send(x.sid, x.st, x.d) {
			break
		}
	}
	if stuck && (turned || now.Sub(r.kickedAt) >= r.cfg.KickEvery) {
		r.kickedAt = now
		r.exp.kick()
	}
}

// send posts one report, unless reporting is off, paused after a failure, or
// over for this session. It reports false when the pass should send no more:
// reporting is off or paused.
func (r *reporter) send(sid string, st *reportState, d deliveryReport) bool {
	if r.off || time.Now().Before(r.retryAt) {
		return false
	}
	if st.closed {
		return true
	}
	switch code := r.post(sid, d, r.cfg.Timeout); {
	case code/100 == 2:
		st.sent, st.last, st.sentAt, st.resend = true, d, time.Now(), false
		r.up()
	case code == http.StatusNotFound || code == http.StatusMethodNotAllowed:
		// A control plane without the endpoint: say nothing, it is not an error.
		r.off = true
		return false
	case code == http.StatusConflict:
		st.closed = true // the session is finished or being sealed
		r.up()
	default:
		// No answer, a failing control plane, or something on the way that
		// refuses the request (a proxy answering 403, say). The control plane
		// has not got the report: say so in the log, and try again.
		r.failed(code)
		return false
	}
	return true
}

// failed notes a report that did not arrive and pauses reporting: RetryBase
// after the first failure, twice as long after each further one, up to 4
// times RetryBase. A request that was refused (4xx) will be refused again
// until someone changes something, so its pause grows to 32 times RetryBase.
// The outage is logged once: at the first refusal, and at the second failure
// in a row otherwise (a single lost request is retried without a word).
func (r *reporter) failed(code int) {
	refused := code/100 == 4
	r.failures++
	steps := 2
	if refused {
		steps = 5
	}
	r.retryAt = time.Now().Add(r.cfg.RetryBase << min(r.failures-1, steps))
	if r.down || !refused && r.failures < 2 {
		return
	}
	r.down = true
	if now := time.Now(); r.lastOutageLog.IsZero() || now.Sub(r.lastOutageLog) >= outageLogGap {
		r.lastOutageLog, r.outageLogged = now, true
		what := "did not answer"
		if code != 0 {
			what = fmt.Sprintf("answered HTTP %d", code)
		}
		log.Printf("tsession: cannot send delivery reports to the control plane at %s (it %s); they are retried, and until they arrive the sessions this service takes part in are marked incomplete",
			DisplayURL(r.sub.base), what)
	}
}

// up notes that the control plane answered a report.
func (r *reporter) up() {
	r.failures = 0
	if r.down {
		r.down = false
		if r.outageLogged {
			r.outageLogged = false
			log.Printf("tsession: delivery reports reach the control plane at %s again", DisplayURL(r.sub.base))
		}
	}
}

// post sends one report and returns the status code, 0 when there was no
// answer. The body of the answer is not read for more than to reuse the
// connection.
func (r *reporter) post(sid string, d deliveryReport, timeout time.Duration) int {
	body, err := json.Marshal(d)
	if err != nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.sub.base+"/v1/sessions/"+url.PathEscape(sid)+"/delivery", bytes.NewReader(body))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.sub.client.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<12)) // let the connection be reused
	resp.Body.Close()
	return resp.StatusCode
}

// close stops the reporter and, within timeout, sends a last report for
// every session it has an account of. It is called once the exporter is
// closed: what that could not deliver is discarded with it, so it is
// reported as dropped, and the control plane need not wait for it. (An event
// the collector stored without this process seeing the answer is among them;
// the control plane goes by what the collector has.)
func (r *reporter) close(timeout time.Duration) {
	close(r.stop)
	<-r.done
	if r.off {
		return
	}
	deadline := time.Now().Add(timeout)
	sn := r.sub.snap.Load()
	total := r.rec.deliveredTotal()
	for sid, a := range r.rec.accounts() {
		st := r.sessions[sid]
		if _, live := sn.active[sid]; !live || st != nil && st.closed {
			continue
		}
		left := time.Until(deadline)
		if left <= 0 {
			return
		}
		d := r.report(a, true, total)
		d.Dropped, d.Buffered = d.Dropped+d.Buffered, 0
		if code := r.post(sid, d, min(left, r.cfg.Timeout)); code == 0 || code == http.StatusNotFound {
			return // unreachable, or without the endpoint
		}
	}
}
