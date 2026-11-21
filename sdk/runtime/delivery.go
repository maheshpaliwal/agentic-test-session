package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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
	LastError string `json:"last_error,omitempty"`
	// Final: sent after this process saw the session draining and had the
	// time to flush, or while it shuts down.
	Final bool `json:"final,omitempty"`
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
	// only so long for them, and the exporter's own pause can be longer.
	KickEvery time.Duration
	Timeout   time.Duration // per report request (default 2s)
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
	// off is set when the control plane does not know the endpoint (an older
	// one answers 404). It lasts until the stream is opened again, which is
	// when the control plane may have been replaced.
	off      bool
	connects uint64
	failures int
	retryAt  time.Time // no report before this, after a failed one
	kickedAt time.Time

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
	r := &reporter{cfg: cfg, rec: rec, exp: exp, sub: sub, service: service, instance: instance,
		sessions: map[string]*reportState{}, stop: make(chan struct{}), done: make(chan struct{})}
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
func (r *reporter) report(a account, final bool) deliveryReport {
	d := deliveryReport{Service: r.service, Instance: r.instance, Recorded: a.recorded, Delivered: a.delivered,
		Dropped: a.dropped, Buffered: a.buffered(), Final: final}
	if d.Buffered > 0 || d.Dropped > 0 {
		d.LastError = r.exp.lastError()
	}
	return d
}

// pass looks at the accounts once and sends what is due. It never waits for
// the exporter: one session's undeliverable events must not delay the report
// of another.
//
// A session is reported for the first time once its events had the time to
// be delivered, then at most every Interval while it changes. When it turns
// draining its counts are sent at once if they changed (so the control plane
// knows what to wait for) and the exporter is made to send now; from then on
// every change is reported at once. The reports are final once nothing is
// left to deliver, or after DrainFlush. Events recorded later in the drain
// make a new final report that replaces the last. A session that left the
// active set is forgotten.
//
// The set read here is the raw one: under the kill switch IsActive is false
// for every session, and what was recorded before it must still be accounted.
func (r *reporter) pass() {
	accts := r.rec.accounts()
	if len(accts) == 0 && len(r.sessions) == 0 {
		return
	}
	sn := r.sub.snap.Load()
	if c := r.sub.connects.Load(); c != r.connects {
		r.connects, r.off = c, false
		for _, st := range r.sessions {
			st.resend = true
		}
	}
	now := time.Now()
	stuck, turned := false, false
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
		d := r.report(a, st.final)
		stuck = stuck || draining && d.Buffered > 0
		changed := !st.sent || st.resend || d != st.last
		switch {
		case !changed:
		case draining:
			r.send(sid, st, d)
		case !st.sent:
			if d.Buffered == 0 || now.Sub(st.seen) >= r.cfg.FirstDelay {
				r.send(sid, st, d)
			}
		case st.resend || now.Sub(st.sentAt) >= r.cfg.Interval:
			r.send(sid, st, d)
		}
	}
	for sid := range r.sessions {
		if _, ok := accts[sid]; !ok {
			delete(r.sessions, sid)
		}
	}
	if stuck && (turned || now.Sub(r.kickedAt) >= r.cfg.KickEvery) {
		r.kickedAt = now
		r.exp.kick()
	}
}

// send posts one report, unless reporting is off, paused after a failure, or
// over for this session.
func (r *reporter) send(sid string, st *reportState, d deliveryReport) {
	if r.off || st.closed || time.Now().Before(r.retryAt) {
		return
	}
	switch code := r.post(sid, d, r.cfg.Timeout); {
	case code/100 == 2:
		st.sent, st.last, st.sentAt, st.resend = true, d, time.Now(), false
		r.failures = 0
	case code == http.StatusNotFound || code == http.StatusMethodNotAllowed:
		// A control plane without the endpoint: say nothing, it is not an error.
		r.off = true
	case code == http.StatusConflict:
		st.closed = true // the session is finished or being sealed
	case code/100 == 4:
		// Refused as it is; the same report will be refused again.
		st.sent, st.last, st.sentAt, st.resend = true, d, time.Now(), false
	default:
		// No answer, or a failing control plane: try again, ever less often.
		r.failures = min(r.failures+1, 3)
		r.retryAt = time.Now().Add(time.Second << (r.failures - 1))
	}
}

// post sends one report and returns the status code, 0 when there was no
// answer.
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
// reported as dropped, and the control plane need not wait for it.
func (r *reporter) close(timeout time.Duration) {
	close(r.stop)
	<-r.done
	if r.off {
		return
	}
	deadline := time.Now().Add(timeout)
	sn := r.sub.snap.Load()
	for sid, a := range r.rec.accounts() {
		st := r.sessions[sid]
		if _, live := sn.active[sid]; !live || st != nil && st.closed {
			continue
		}
		left := time.Until(deadline)
		if left <= 0 {
			return
		}
		d := r.report(a, true)
		d.Dropped, d.Buffered = d.Dropped+d.Buffered, 0
		if code := r.post(sid, d, min(left, r.cfg.Timeout)); code == 0 || code == http.StatusNotFound {
			return // unreachable, or without the endpoint
		}
	}
}
