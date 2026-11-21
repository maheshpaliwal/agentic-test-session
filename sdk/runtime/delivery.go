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

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
	"github.com/maheshpaliwal/agentic-test-session/internal/ident"
)

// deliveryReport is what this process recorded for a session and what became
// of it. The counts are cumulative, so a report replaces the ones before it.
// It is sent as one entry of a batch (POST /v1/delivery, see deliveryBatch);
// to a control plane without that endpoint it is the body of
// POST /v1/sessions/{id}/delivery.
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

// deliveryBatch is the body of POST /v1/delivery: the reports of this process
// for any number of sessions. One without reports says only that the reports
// of this process arrive.
type deliveryBatch struct {
	Service        string        `json:"service,omitempty"`
	Instance       string        `json:"instance"`
	DeliveredTotal uint64        `json:"delivered_total"`
	Reports        []batchReport `json:"reports"`
	// Stopped is set when this process stopped recording after an internal
	// error, and says how (see Subscriber.panicked). The control plane marks
	// the sessions that run when it hears this incomplete; one that does not
	// know the field ignores it.
	Stopped string `json:"stopped,omitempty"`
}

// batchReport is one report of a batch: a deliveryReport without what the
// batch says once, with the session it is about.
type batchReport struct {
	Session   string `json:"session"`
	Recorded  uint64 `json:"recorded"`
	Delivered uint64 `json:"delivered"`
	Dropped   uint64 `json:"dropped"`
	Buffered  uint64 `json:"buffered"`
	LastError string `json:"last_error,omitempty"`
	Final     bool   `json:"final,omitempty"`
}

// batchAnswer is the answer to a batch: what became of each report, in order.
type batchAnswer struct {
	Results []struct {
		Status string `json:"status"`
	} `json:"results"`
}

// maxReportError bounds LastError.
const maxReportError = 256

// How the reports are sent.
const (
	// modeBatch: all that is due in one request (POST /v1/delivery).
	modeBatch = iota
	// modeSingle: a control plane that does not know the batch endpoint and
	// may know the older one, a request per session.
	modeSingle
	// modeOff: a control plane that knows neither.
	modeOff
)

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
	// Batch is the most reports one request carries (default 256, which is
	// also the most a control plane takes). Many sessions that change at once
	// are reported in a few requests, the draining ones first.
	Batch int
	// Burst is the most requests one pass sends to a control plane that takes
	// one report per request (default 32).
	Burst int
	// PassBudget is how long a pass goes on sending batches (default Tick):
	// after a request that took it past that, the rest waits for the next
	// pass.
	PassBudget time.Duration
	// RetryBase is the pause after a first failed report; it doubles with
	// every further one (default 1s), up to 4 times as long for a control
	// plane that does not answer or fails, 32 times for one that refuses.
	RetryBase time.Duration
	// Heartbeat is how often a request is sent that only says that the
	// reports of this process arrive, while the active set names a session and
	// no report was sent meanwhile (default 5s). The control plane names a
	// process it has not heard of for twice as long; it can ask for another
	// period in the active set, which then counts.
	Heartbeat time.Duration
	// DrainRetry is the pause after a failed report while the report of a
	// draining session is waiting, whatever RetryBase has grown to (default
	// 200ms, so that the next pass tries again: a request every 250ms). The
	// control plane waits only so long for the report, and an outage of the
	// report endpoint that ends within the drain must not cost a session its
	// report.
	DrainRetry time.Duration
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
	// mode is how reports are sent (modeBatch, modeSingle, modeOff). It falls
	// back when the control plane does not know an endpoint (an older one
	// answers 404), until the stream is opened again, which is when the
	// control plane may have been replaced.
	mode int
	// hello is set while the control plane has not been told, since the
	// stream was last opened, that the reports of this process arrive. A
	// batch says so, also one without reports.
	hello bool
	// beatAt is when the control plane last answered a batch, with reports or
	// without: the next heartbeat is due a period after it.
	beatAt time.Time
	// busy: the active set named a session at the last pass.
	busy     bool
	connects uint64
	failures int
	failedAt time.Time // when the last report failed
	retryAt  time.Time // no report before this, after a failed one (see paused)
	kickedAt time.Time
	urgentOf *snapshot // the active set whose draining sessions the exporter was told of
	// toldAt is when the control plane last took a batch that said that
	// recording stopped; wasBlind: the set was unknown at the last pass.
	toldAt   time.Time
	wasBlind bool
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
	def(&cfg.DrainRetry, 200*time.Millisecond)
	def(&cfg.Heartbeat, 5*time.Second)
	def(&cfg.PassBudget, cfg.Tick)
	if cfg.Burst <= 0 {
		cfg.Burst = 32
	}
	if cfg.Batch <= 0 || cfg.Batch > maxBatchReports {
		cfg.Batch = maxBatchReports
	}
	// The names as the control plane compares them: the stream's headers
	// reach it trimmed, and a report must name the same instance.
	r := &reporter{cfg: cfg, rec: rec, exp: exp, sub: sub, service: ident.Clean(service), instance: ident.Clean(instance),
		sessions: map[string]*reportState{}, stop: make(chan struct{}), done: make(chan struct{})}
	startLoop("delivery reporter", "the control plane no longer hears what this process delivered, and names it in its sessions",
		r.stop, r.loop, func() { close(r.done) }, nil)
	return r
}

// loop may be started again after a panic (startLoop): it closes nothing.
func (r *reporter) loop() {
	t := time.NewTicker(r.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
			r.pass()
		case <-r.sub.opened:
			// The stream was opened, or the active set names a session after
			// naming none: say at once that the reports of this process arrive,
			// not a tick later. The control plane names a process it has not
			// heard of a second after it connected, or after the first session
			// began.
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
// A session this process recorded nothing for is not reported. The control
// plane does not expect to hear of it: it looks for the report of a process
// only when the collector has events of that process for the session.
//
// When the stream is opened, one request tells the control plane that the
// reports of this process arrive, also when nothing is due (see hello). The
// same request is its heartbeat: while the active set names a session, of
// this process or not, one is sent whenever the control plane has not
// answered a batch for a heartbeat period (see beat). The control plane judges
// a process by how long it has not heard of it, so one whose reports stop
// arriving in the middle of a session is noticed. A process that follows no
// session sends nothing.
//
// While the stream is down this process learns of a drain late, from a poll,
// or not at all. Its reports then do not wait for FirstDelay or Interval:
// what the control plane was not told before it seals a session, it cannot
// hold against the collector.
//
// Everything that is due goes into one request, Batch reports at most; what
// does not fit follows at once, the draining sessions first and of those the
// ones reported longest ago. Nothing is sent after a request that failed.
//
// The set read here is the raw one: under the kill switch IsActive is false
// for every session, and what was recorded before must still be accounted.
//
// When the subscriber panicked this process has stopped recording and does
// not know the set (snapshot.blind). The control plane is told (see
// Subscriber.panicked), also when nothing else is due. Until the set is
// known again no account is forgotten, and what was recorded before is
// reported on like that of a running session.
func (r *reporter) pass() {
	faultpoint.Hit("reporter.pass")
	accts := r.rec.accounts()
	sn := r.sub.snap.Load()
	if sn != r.urgentOf {
		r.urgentOf = sn
		r.exp.setUrgent(sn.draining)
	}
	if c := r.sub.connects.Load(); c != r.connects {
		r.connects, r.mode, r.hello = c, modeBatch, true
		for _, st := range r.sessions {
			st.resend = true
		}
	}
	streaming := r.sub.streaming.Load()
	now := time.Now()
	r.renote(sn, now)
	if busy := len(sn.active) > 0; busy != r.busy {
		// The first session after none: the control plane expects to hear of
		// this process now, so a pause that refused requests left behind (there
		// was nothing to send, and nothing to end it) is over.
		if r.busy = busy; busy {
			r.retryAt = time.Time{}
		}
	}
	hello := r.mode == modeBatch && (streaming && (r.hello || r.beat(sn, now)) || r.sub.stopNote.Load() != nil)
	if len(accts) == 0 && len(r.sessions) == 0 && !hello {
		return
	}
	total := r.rec.deliveredTotal()
	stuck, turned, urgent := false, false, false
	var todo []due
	for sid, a := range accts {
		if _, live := sn.active[sid]; !live && !sn.blind {
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
		case st.closed:
		case draining:
			send = changed || d.Buffered > 0 && d.DeliveredTotal != st.last.DeliveredTotal && paced
			urgent = urgent || changed
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
	slices.SortFunc(todo, func(a, b due) int {
		if a.draining != b.draining {
			if a.draining {
				return -1
			}
			return 1
		}
		return cmp.Or(a.st.sentAt.Compare(b.st.sentAt), cmp.Compare(a.sid, b.sid))
	})
	r.flush(todo, hello, total, urgent)
	if stuck && (turned || now.Sub(r.kickedAt) >= r.cfg.KickEvery) {
		r.kickedAt = now
		r.exp.kick()
	}
}

// renote makes the control plane hear again that this process does not
// record, while that is so and passing: every KickEvery while the subscriber
// is being started again, and once when it has the set again. The control
// plane marks the sessions that run when it is told, so one telling at the
// panic would miss a session that began during the restarts, which take up
// to half a minute. A subscriber that stays stopped is told once (see
// Subscriber.panicked): from then on this process is like a service that is
// not connected, and takes no part in later sessions.
func (r *reporter) renote(sn *snapshot, now time.Time) {
	switch {
	case sn.blind && !r.sub.gaveUp.Load():
		if r.sub.stopNote.Load() == nil && now.Sub(r.toldAt) >= r.cfg.KickEvery {
			r.sub.note(noteRestarting)
		}
	case r.wasBlind && !sn.blind:
		r.sub.note(noteResumed)
	}
	r.wasBlind = sn.blind
}

// beat reports whether a heartbeat is due: the active set names a session,
// and the control plane has not answered a batch of this process for the
// heartbeat period (the one it asked for, or cfg.Heartbeat). A tick early
// rather than late (a quarter of the period at most): the control plane
// counts from when it heard of this process, and the next pass is a tick
// away.
func (r *reporter) beat(sn *snapshot, now time.Time) bool {
	if len(sn.active) == 0 {
		return false
	}
	period := r.period(sn)
	return now.Sub(r.beatAt) >= period-min(r.cfg.Tick, period/4)
}

// period is the heartbeat period: the one the control plane asked for, or
// cfg.Heartbeat.
func (r *reporter) period(sn *snapshot) time.Duration {
	if sn.heartbeat > 0 {
		return sn.heartbeat
	}
	return r.cfg.Heartbeat
}

// paused reports whether a failed request keeps this pass from sending. The
// pause grows with every failure in a row (see failed); while the report of a
// draining session waits (urgent) it is DrainRetry at most.
func (r *reporter) paused(urgent bool) bool {
	now := time.Now()
	if !now.Before(r.retryAt) {
		return false
	}
	return !urgent || now.Sub(r.failedAt) < r.cfg.DrainRetry
}

// flush sends what a pass found due, in the order given. With hello a
// request is sent even when nothing is due.
func (r *reporter) flush(todo []due, hello bool, total uint64, urgent bool) {
	if r.mode == modeOff || r.paused(urgent) {
		return
	}
	began := time.Now()
	for i := 0; len(todo) > 0 || hello; i++ {
		if r.mode == modeSingle {
			if i == r.cfg.Burst || len(todo) == 0 || !r.send(todo[0].sid, todo[0].st, todo[0].d) {
				return
			}
			todo = todo[1:]
			continue
		}
		n := min(len(todo), r.cfg.Batch)
		if !r.sendBatch(todo[:n], total) {
			// Not sent. A control plane without the endpoint is tried again at
			// the next pass, a report per request.
			return
		}
		todo, hello = todo[n:], false
		// A request that took long has kept the accounts waiting: the rest
		// is sent by the next pass, from what the accounts say then.
		if time.Since(began) >= r.cfg.PassBudget {
			return
		}
	}
}

// maxBatchReports is the most reports a control plane takes in one request,
// and maxBatchAnswer bounds what is read of its answer.
const (
	maxBatchReports = 256
	maxBatchAnswer  = 64 << 10
)

// sendBatch posts the reports of chunk in one request and notes what became
// of each. It reports false when nothing more should be sent now: the request
// failed, or the control plane does not know the endpoint.
func (r *reporter) sendBatch(chunk []due, total uint64) bool {
	b := deliveryBatch{Service: r.service, Instance: r.instance, DeliveredTotal: total, Reports: make([]batchReport, len(chunk))}
	note := r.sub.stopNote.Load()
	if note != nil {
		b.Stopped = *note
	}
	for i, x := range chunk {
		b.Reports[i] = batchReport{Session: x.sid, Recorded: x.d.Recorded, Delivered: x.d.Delivered, Dropped: x.d.Dropped,
			Buffered: x.d.Buffered, LastError: x.d.LastError, Final: x.d.Final}
	}
	code, answer := r.postBatch(b, r.cfg.Timeout)
	switch {
	case code/100 == 2:
		now := time.Now()
		for i, x := range chunk {
			status := "accepted" // an answer that says nothing of a report: the control plane has it
			if i < len(answer.Results) {
				status = answer.Results[i].Status
			}
			switch status {
			case "accepted", "stale", "full":
				// full: the session keeps reports of as many instances as it
				// can, and this is another one. The control plane goes by what
				// a refused report says, so a change is still reported: events
				// that are lost later must not go unsaid.
				x.st.sent, x.st.last, x.st.sentAt, x.st.resend = true, x.d, now, false
			case "failed": // the control plane could not store it: sent again
			default: // closed, unknown, invalid: no report of this session will be taken
				x.st.closed = true
			}
		}
		r.hello, r.beatAt = false, time.Now()
		if note != nil {
			r.toldAt = r.beatAt
		}
		r.sub.stopNote.CompareAndSwap(note, nil) // told; a newer note stays
		r.sub.setAnnounce(true)
		r.up()
		return true
	case r.noEndpoint(code):
		// A control plane without the endpoint. It may know the older one; it
		// does not expect to hear from this process either way, and one that
		// knows the older endpoint only would take the stream's announcement
		// for a promise to report every draining session.
		r.mode = modeSingle
		r.sub.setAnnounce(false)
		return false
	default:
		r.failed(code)
		return false
	}
}

// send posts one report to a control plane that takes one per request. It
// reports false when no more should be sent now.
func (r *reporter) send(sid string, st *reportState, d deliveryReport) bool {
	switch code := r.post(sid, d, r.cfg.Timeout); {
	case code/100 == 2:
		st.sent, st.last, st.sentAt, st.resend = true, d, time.Now(), false
		r.up()
	case r.noEndpoint(code):
		// A control plane without the endpoint: say nothing, it is not an error.
		r.mode = modeOff
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

// noEndpoint reports whether the answer code to a report means that the
// control plane does not know the endpoint: 404 or 405 from one that has not
// said that it takes reports (see snapshot.reports). From one that has, the
// answer comes from something on the way (a proxy that passes the stream and
// answers 404 to the POST) and is a refusal like any other: logged, retried,
// and the sessions of this process end unconfirmed or incomplete. Taking it
// for "no endpoint" would turn the reports off without a word, and a service
// with a wrong collector URL behind such a proxy would leave no trace at all.
func (r *reporter) noEndpoint(code int) bool {
	return (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) && !r.sub.snap.Load().reports
}

// failed notes a report that did not arrive and pauses reporting: RetryBase
// after the first failure, twice as long after each further one, up to 4
// times RetryBase. A request that was refused (4xx) will be refused again
// until someone changes something, so its pause grows to 32 times RetryBase;
// no pause is longer than a heartbeat period, and the first session after
// none is a reason to try at once (see pass).
// While the report of a draining session waits the pause is DrainRetry (see
// paused). The outage is logged once: at the first refusal, and at the second
// failure in a row otherwise (a single lost request is retried without a
// word).
func (r *reporter) failed(code int) {
	refused := code/100 == 4
	r.failures++
	steps := 2
	if refused {
		steps = 5
	}
	r.failedAt = time.Now()
	// Never longer than a heartbeat period: when the outage is over, the
	// control plane hears of this process as soon as it would of a healthy one.
	r.retryAt = r.failedAt.Add(min(r.cfg.RetryBase<<min(r.failures-1, steps), r.period(r.sub.snap.Load())))
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
		log.Printf("tsession: cannot send delivery reports to the control plane at %s (it %s); they are retried, and until they arrive the sessions this service takes part in are marked incomplete, or say that its delivery is unconfirmed",
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
// answer.
func (r *reporter) post(sid string, d deliveryReport, timeout time.Duration) int {
	code, _ := r.do("/v1/sessions/"+url.PathEscape(sid)+"/delivery", d, timeout, nil)
	return code
}

// postBatch sends one batch and returns the status code, 0 when there was no
// answer, and what the control plane said of each report.
func (r *reporter) postBatch(b deliveryBatch, timeout time.Duration) (int, batchAnswer) {
	var a batchAnswer
	code, _ := r.do("/v1/delivery", b, timeout, &a)
	return code, a
}

// do posts body as JSON to path of the control plane and returns the status
// code, 0 when there was no answer. The answer is decoded into out when it is
// a 2xx and out is not nil; it is not read for more than that, and to reuse
// the connection.
func (r *reporter) do(path string, body any, timeout time.Duration, out any) (int, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.sub.base+path, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.sub.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode/100 == 2 {
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxBatchAnswer)).Decode(out)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<12)) // let the connection be reused
	return resp.StatusCode, nil
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
	if r.mode == modeOff {
		return
	}
	deadline := time.Now().Add(timeout)
	sn := r.sub.snap.Load()
	total := r.rec.deliveredTotal()
	var last []batchReport
	for sid, a := range r.rec.accounts() {
		st := r.sessions[sid]
		if _, live := sn.active[sid]; !live && !sn.blind || st != nil && st.closed {
			continue
		}
		d := r.report(a, true, total)
		d.Dropped, d.Buffered = d.Dropped+d.Buffered, 0
		last = append(last, batchReport{Session: sid, Recorded: d.Recorded, Delivered: d.Delivered, Dropped: d.Dropped, LastError: d.LastError, Final: true})
	}
	slices.SortFunc(last, func(a, b batchReport) int { return cmp.Compare(a.Session, b.Session) })
	for len(last) > 0 {
		left := time.Until(deadline)
		if left <= 0 {
			return
		}
		n := 1
		code := 0
		if r.mode == modeBatch {
			n = min(len(last), r.cfg.Batch)
			code, _ = r.postBatch(deliveryBatch{Service: r.service, Instance: r.instance, DeliveredTotal: total, Reports: last[:n]}, min(left, r.cfg.Timeout))
			if r.noEndpoint(code) {
				r.mode = modeSingle
				continue
			}
		} else {
			x := last[0]
			code = r.post(x.Session, deliveryReport{Service: r.service, Instance: r.instance, Recorded: x.Recorded, Delivered: x.Delivered,
				Dropped: x.Dropped, DeliveredTotal: total, LastError: x.LastError, Final: true}, min(left, r.cfg.Timeout))
		}
		if code == 0 || code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
			return // unreachable, without the endpoint, or refused on the way
		}
		last = last[n:]
	}
}
