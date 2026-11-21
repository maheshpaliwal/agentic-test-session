package controlplane

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
	"github.com/maheshpaliwal/agentic-test-session/internal/ident"
	"github.com/maheshpaliwal/agentic-test-session/internal/scrub"
)

// Delivery is what one SDK instance last reported about the events it
// recorded for a session: how many there were and what became of them. The
// SDK sends it straight to the control plane (POST /v1/delivery, many
// sessions in one request), because events that never reach the collector
// cannot be seen there. The counts are cumulative for the life of the
// instance, so a later report replaces an earlier one.
//
// A report is the instance's own word. The verdict on a session does not rest
// on it alone: at the end of the drain it is held against what the collector
// has from that instance (see deliveryVerdict).
type Delivery struct {
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance"`
	// Recorded = Delivered + Dropped + Buffered.
	Recorded  int64 `json:"recorded"`
	Delivered int64 `json:"delivered"` // answered for by the collector, as far as the instance saw
	Dropped   int64 `json:"dropped"`   // given up by the instance: ring overflow, refused by the collector, discarded at shutdown
	Buffered  int64 `json:"buffered"`  // still held by the instance
	// DeliveredTotal is what the instance has delivered for all sessions
	// together since it started. It rises while the instance gets events of
	// any session through to the collector, which is how a drain tells an
	// instance that is working through a backlog from one that is stuck.
	// Zero from an SDK that does not send it.
	DeliveredTotal int64 `json:"delivered_total,omitempty"`
	// LastError is why the instance's last request to the collector failed;
	// empty once a request succeeded.
	LastError string `json:"last_error,omitempty"`
	// Final marks a report sent after the instance learned that the session
	// was draining (or was shutting down itself) and had tried to flush. A
	// report without it is a periodic one: the instance may have recorded or
	// delivered more since.
	Final bool `json:"final,omitempty"`
	// At is when the control plane received the report.
	At time.Time `json:"at"`
}

// PendingDelivery sums what one instance last reported of the sessions that
// are ACTIVE or DRAINING now: the events it had not got to the collector.
// It is the instance's own word and as old as its newest report (At).
type PendingDelivery struct {
	Sessions    int   `json:"sessions"`    // running sessions it reported events held or given up of
	Undelivered int64 `json:"undelivered"` // events it still held
	Dropped     int64 `json:"dropped"`     // events it gave up
	// LastError is why its last request to the collector failed, by its
	// newest report; empty when that request succeeded.
	LastError string    `json:"last_error,omitempty"`
	At        time.Time `json:"at"`
}

// pendingLocked is the PendingDelivery of every instance that has one, by
// deliveryKey. It walks the live sessions, never the store.
func (m *Manager) pendingLocked() map[string]*PendingDelivery {
	out := map[string]*PendingDelivery{}
	// newest is the newest report of each instance: its error is the current
	// one, since an instance has one exporter for all its sessions.
	newest := map[string]Delivery{}
	add := func(d Delivery) {
		key := deliveryKey(d.Service, d.Instance)
		if n, ok := newest[key]; !ok || d.At.After(n.At) {
			newest[key] = d
		}
		if d.Buffered == 0 && d.Dropped == 0 {
			return
		}
		p := out[key]
		if p == nil {
			p = &PendingDelivery{}
			out[key] = p
		}
		p.Sessions++
		p.Undelivered += d.Buffered
		p.Dropped += d.Dropped
	}
	for id, s := range m.live {
		for _, d := range s.Delivery {
			add(d)
		}
		if st := m.delivery[id]; st != nil {
			for _, d := range st.refused {
				add(d)
			}
		}
	}
	for key, n := range newest {
		p := out[key]
		if p == nil {
			if n.LastError == "" {
				continue
			}
			p = &PendingDelivery{}
			out[key] = p
		}
		p.LastError, p.At = n.LastError, n.At
	}
	return out
}

// DeliveryBatch is the body of POST /v1/delivery: the reports of one SDK
// instance for any number of sessions. One without reports says only that
// the instance's reports arrive (it is sent when the instance connects).
type DeliveryBatch struct {
	Service        string `json:"service,omitempty"`
	Instance       string `json:"instance"`
	DeliveredTotal int64  `json:"delivered_total,omitempty"`
	// Reports holds at most MaxDeliveryBatch entries of at most 4 KiB each.
	Reports []json.RawMessage `json:"reports"`
	// Stopped is set by an instance that stopped recording after an internal
	// error (a loop of its SDK panicked), and says how. It is noted on every
	// session that is ACTIVE or DRAINING when it arrives; what the verdict of
	// such a session makes of it depends on whether anything ties the instance
	// to the session (see stoppedLocked and stoppedReasons).
	Stopped string `json:"stopped,omitempty"`
	// Collector is set by an instance that looked whether it can reach its
	// collector, and says what it found (see CollectorReach). It is kept with
	// the instance's connection to the active set, for GET /v1/subscribers;
	// no session is judged by it.
	Collector *CollectorReach `json:"collector,omitempty"`
}

// CollectorReach is the word of an SDK instance on whether it can reach its
// collector: it asks the collector's /healthz while it has nothing to send.
// It is how a collector URL that leads nowhere shows before a session runs
// and an event is lost to it (`tracectl doctor`).
type CollectorReach struct {
	Reachable bool `json:"reachable"`
	// Error is why not, in the instance's words (cleaned and bounded); empty
	// when it can.
	Error string `json:"error,omitempty"`
	// At is when the control plane was last told.
	At time.Time `json:"at"`
}

// SessionDelivery is one entry of DeliveryBatch.Reports.
type SessionDelivery struct {
	Session   string `json:"session"`
	Recorded  int64  `json:"recorded"`
	Delivered int64  `json:"delivered"`
	Dropped   int64  `json:"dropped"`
	Buffered  int64  `json:"buffered"`
	LastError string `json:"last_error,omitempty"`
	Final     bool   `json:"final,omitempty"`
}

// DeliveryResult is what became of one report of a batch, in the order of
// the batch.
type DeliveryResult struct {
	Session string `json:"session,omitempty"`
	// Status is one of the Delivery* constants.
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// The statuses of a DeliveryResult.
const (
	DeliveryAccepted = "accepted"
	// DeliveryStale: the report lies behind the one already kept for the
	// instance (an old one that arrived late, see staleReport); ignored.
	DeliveryStale = "stale"
	// DeliveryClosed: the session is finished or about to be sealed.
	DeliveryClosed = "closed"
	// DeliveryUnknown: no such session.
	DeliveryUnknown = "unknown"
	// DeliveryFull: the session already keeps reports of MaxDeliveryInstances
	// instances, so this one is not stored with it. It counts all the same:
	// it is kept for the verdict like a stored one (see deliveryState.refused).
	DeliveryFull    = "full"
	DeliveryInvalid = "invalid"
	// DeliveryFailed: the report could not be stored; send it again.
	DeliveryFailed = "failed"
)

const (
	// maxDeliveryBody bounds one delivery report.
	maxDeliveryBody = 4 << 10
	// MaxDeliveryBatch bounds the reports of one batch.
	MaxDeliveryBatch = 256
	// maxDeliveryBatchBody bounds the body of a batch.
	maxDeliveryBatchBody = MaxDeliveryBatch*maxDeliveryBody + 1<<10
	// MaxDeliveryInstances bounds the instances one session keeps reports of.
	MaxDeliveryInstances = 256
	// maxDeliveryRefused bounds the instances whose reports a session keeps
	// for its verdict without storing them, beyond MaxDeliveryInstances.
	maxDeliveryRefused = 1024
	maxDeliveryError   = 256
	// maxDeliveryCount bounds a counter of a report (2^53: exact in JSON).
	maxDeliveryCount = 1 << 53
	// maxDeliveryReasons bounds the instances named in incomplete_reasons and
	// in delivery_unconfirmed.
	maxDeliveryReasons = 16
	// maxStoppedInstances bounds the instances a session keeps the word of
	// that they stopped recording (Session.StoppedInstances). They are stored
	// with the session, so there are far fewer of them than of the reports
	// kept in memory.
	maxStoppedInstances = 64
	// maxInstanceProgress bounds Manager.progress.
	maxInstanceProgress = 4096
	// maxCountsAsked bounds the instances a drain asks the collector about by
	// name (see Manager.countsFor).
	maxCountsAsked = 4096
)

// errDeliveryFull is ErrBadState for a report refused for MaxDeliveryInstances.
var errDeliveryFull = fmt.Errorf("%w: session already keeps delivery reports of %d instances", ErrBadState, MaxDeliveryInstances)

// deliveryState is the part of a live session's delivery accounting that is
// not stored with it.
type deliveryState struct {
	// progress is, per instance that reported, when it was last seen to get
	// somewhere with this session: its first report, a report that delivered
	// more, or a collector count that rose. The clock is the control plane's.
	progress map[string]time.Time
	// have is what the collector had from each reporting instance when the
	// drain last looked.
	have map[string]int64
	// short are the instances that sent a final report with events they still
	// held. A later report may take that back, and is believed when the
	// collector's count for the instance agrees; without that count it is not.
	short map[string]struct{}
	// closed is set when the drain stops looking at reports: the verdict is
	// made from what is stored then, and later reports are refused.
	closed bool
	// refused holds the last report of every instance whose reports are not
	// stored with the session because it already kept MaxDeliveryInstances
	// (the instance is answered "full"). They are replaced, waited for and
	// judged against the collector's count like the stored ones: a session
	// whose list someone filled must not lose the word of the instance that
	// came after. At most maxDeliveryRefused of them; refusedMore when there
	// were more. Of those nothing is kept but refusedShort: one of them said
	// that events were dropped, or was final and still held some.
	refused      map[string]Delivery
	refusedMore  bool
	refusedShort bool
	// silent names the connected instances whose events the collector has and
	// of which no report had arrived when the reports were closed (see
	// silentLocked).
	silent []Subscriber
}

// instanceProgress is when an instance was last seen to deliver events of any
// session, by the total it reports.
type instanceProgress struct {
	total int64
	at    time.Time
}

func deliveryKey(service, instance string) string { return service + "\x00" + instance }

func (m *Manager) deliveryStateLocked(id string) *deliveryState {
	st := m.delivery[id]
	if st == nil {
		st = &deliveryState{progress: map[string]time.Time{}, have: map[string]int64{}, short: map[string]struct{}{}}
		m.delivery[id] = st
	}
	return st
}

// cleanDeliveryError makes a self-reported error text safe to store and
// show: printable ASCII, no URL credentials, bounded.
func cleanDeliveryError(s string) string {
	s = scrub.Text(ident.CleanN(s, 4*maxDeliveryError))
	if len(s) > maxDeliveryError {
		s = s[:maxDeliveryError]
	}
	return s
}

// ReportDelivery records what an SDK instance says about its events of a
// running session. It reports false, without an error, for a report that
// lies behind the one already kept for that instance (an old report that
// arrived late, see staleReport): it is ignored. The errors are ErrNotFound,
// ErrInvalid, and ErrBadState for a session that takes no reports (any more):
// one that is finished or about to be sealed, or one that already stores
// MaxDeliveryInstances instances and is told of another (an instance it
// stores can still report). The report of such an instance is not stored
// with the session, and counts for the verdict all the same.
//
// A report that says nothing was recorded is accepted and not stored.
func (m *Manager) ReportDelivery(id string, d Delivery) (bool, error) {
	d.Service, d.Instance = ident.Clean(d.Service), ident.Clean(d.Instance)
	if d.Instance == "" {
		return false, fmt.Errorf("%w: instance is required", ErrInvalid)
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heardLocked(deliveryKey(d.Service, d.Instance), now, true, nil)
	return m.reportLocked(id, d, now)
}

// ReportDeliveries is ReportDelivery for the reports of one instance for many
// sessions (see DeliveryBatch): one result per report, in order. The error is
// ErrInvalid for a batch that names no instance or holds more than
// MaxDeliveryBatch reports; nothing of it is looked at then.
func (m *Manager) ReportDeliveries(b DeliveryBatch) ([]DeliveryResult, error) {
	service, instance := ident.Clean(b.Service), ident.Clean(b.Instance)
	if instance == "" {
		return nil, fmt.Errorf("%w: instance is required", ErrInvalid)
	}
	if len(b.Reports) > MaxDeliveryBatch {
		return nil, fmt.Errorf("%w: a batch holds at most %d reports", ErrInvalid, MaxDeliveryBatch)
	}
	// Decoded before the lock is taken.
	reports := make([]SessionDelivery, len(b.Reports))
	out := make([]DeliveryResult, len(b.Reports))
	for i, raw := range b.Reports {
		if len(raw) > maxDeliveryBody {
			out[i] = DeliveryResult{Status: DeliveryInvalid, Error: fmt.Sprintf("report exceeds %d bytes", maxDeliveryBody)}
			continue
		}
		if err := json.Unmarshal(raw, &reports[i]); err != nil {
			out[i] = DeliveryResult{Status: DeliveryInvalid, Error: err.Error()}
			continue
		}
		out[i].Session = ident.Clean(reports[i].Session)
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heardLocked(deliveryKey(service, instance), now, len(reports) > 0, b.Collector)
	// After the reports of the batch: they are what ties the instance to a session.
	if why := cleanDeliveryError(b.Stopped); why != "" {
		defer m.stoppedLocked(service, instance, why, now)
	}
	for i, r := range reports {
		if out[i].Status != "" {
			continue
		}
		accepted, err := m.reportLocked(r.Session, Delivery{Service: service, Instance: instance, Recorded: r.Recorded, Delivered: r.Delivered,
			Dropped: r.Dropped, Buffered: r.Buffered, DeliveredTotal: b.DeliveredTotal, LastError: r.LastError, Final: r.Final}, now)
		switch {
		case err == nil && accepted:
			out[i].Status = DeliveryAccepted
		case err == nil:
			out[i].Status = DeliveryStale
		case errors.Is(err, errDeliveryFull):
			out[i].Status = DeliveryFull
		case errors.Is(err, ErrBadState):
			out[i].Status = DeliveryClosed
		case errors.Is(err, ErrNotFound):
			out[i].Status = DeliveryUnknown
		case errors.Is(err, ErrInvalid):
			out[i].Status, out[i].Error = DeliveryInvalid, err.Error()
		default:
			out[i].Status, out[i].Error = DeliveryFailed, err.Error()
		}
	}
	return out, nil
}

// heardLocked notes that a request with delivery reports arrived from the
// instance behind key: its reports get through. One without reports is the
// instance's heartbeat: it sends one when it connects, and every
// heartbeatPeriod while the control plane has a session. One with reports
// (reports) is also a sign that the instance is at work on its sessions. Both
// are noted on the instance's connection to the active set, which is what the
// drain of a session looks at (see unheard and silentLocked).
//
// reach, when the request carried it, is what the instance says about
// reaching its collector. It is kept on the connection too, and goes with it:
// an instance that connects anew says it again, and one that is gone is not
// listed at all.
func (m *Manager) heardLocked(key string, now time.Time, reports bool, reach *CollectorReach) {
	if reach != nil {
		why := ""
		if !reach.Reachable {
			why = cmp.Or(cleanDeliveryError(reach.Error), "it did not say why")
		}
		reach = &CollectorReach{Reachable: reach.Reachable, Error: why, At: now.UTC()}
	}
	for ch, sub := range m.subs {
		if sub.stream && sub.DeliveryInstance != "" && deliveryKey(sub.Service, sub.DeliveryInstance) == key {
			sub.heard, sub.LastDeliveryReport = now, now.UTC()
			if reports {
				sub.reported = now
			}
			if reach != nil {
				sub.Collector = reach // shared and never changed
			}
			m.subs[ch] = sub
		}
	}
}

// stoppedLocked notes that an instance said that it stopped recording after
// an internal error, on every session that runs now. A session that starts
// later is not marked: the instance says again when it stops again, and one
// that stays stopped records nothing and follows no session, like a service
// that was never connected.
//
// The note is the caller's own word and proves nothing about a session: an
// environment has many services that take no part in a given session, and
// anyone who reaches the control plane can send one in any name. So it does
// not make a session incomplete by itself. That is decided when the session
// is judged, by whether anything ties the instance to it (see
// stoppedReasons).
//
// It is stored with the session (Session.StoppedInstances), not kept in
// memory: a control plane that is restarted between the note and the end of
// the session must still see it, and a restart is what follows a spent panic
// budget. A session is written only when the note says something new about
// it, so an instance that repeats itself every second costs nothing.
//
// A session keeps at most maxStoppedInstances notes. When that many are kept,
// the note of an instance that is known (it follows the active set, or the
// session has a report of it) takes the place of one that is not: made-up
// names must not keep the word of a real instance out. That notes were left
// out is said once (Session.StoppedInstancesMore).
func (m *Manager) stoppedLocked(service, instance, why string, now time.Time) {
	n := StoppedInstance{Service: service, Instance: instance, Why: why, At: now.UTC(),
		Connected: m.reporterConnectedLocked(deliveryKey(service, instance))}
	noted := 0
	for id, s := range m.live {
		list, more, changed := noteStopped(s, m.delivery[id], n)
		if !changed {
			continue
		}
		s.StoppedInstances, s.StoppedInstancesMore = list, more
		if err := m.putLocked(s); err != nil {
			m.log.Error("persist the note that a service stopped recording", "id", id, "err", err)
		}
		noted++
	}
	if noted > 0 {
		m.log.Error("a service says that it stopped recording after an internal error; that is noted on the sessions that run now",
			"service", service, "instance", instance, "connected", n.Connected, "sessions", noted, "why", why)
	}
}

func cmpStopped(a, b StoppedInstance) int {
	return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.Instance, b.Instance))
}

// noteStopped adds n to the notes of s, or replaces the note of the same
// instance when n says something else. It returns the new list (the stored
// one is shared with copies of the session and never changed in place),
// whether notes were left out, and whether anything changed. st may be nil.
func noteStopped(s Session, st *deliveryState, n StoppedInstance) (list []StoppedInstance, more, changed bool) {
	if st == nil {
		st = &deliveryState{}
	}
	known := func(x StoppedInstance) bool {
		_, reported := st.reportOf(s, x.Service, x.Instance)
		return x.Connected || reported
	}
	list, more = s.StoppedInstances, s.StoppedInstancesMore
	if i := slices.IndexFunc(list, func(x StoppedInstance) bool { return cmpStopped(x, n) == 0 }); i >= 0 {
		old := list[i]
		if old.Why == n.Why && (old.Connected || !n.Connected) {
			return list, more, false
		}
		n.At, n.Connected = old.At, old.Connected || n.Connected
		list = slices.Clone(list)
		list[i] = n
		return list, more, true
	}
	if len(list) >= maxStoppedInstances {
		i := -1
		if known(n) {
			i = slices.IndexFunc(list, func(x StoppedInstance) bool { return !known(x) })
		}
		if i < 0 {
			return list, true, !more
		}
		list, more = slices.Delete(slices.Clone(list), i, i+1), true
	}
	list = append(slices.Clone(list), n)
	slices.SortFunc(list, cmpStopped)
	return list, more, true
}

// stoppedReasons reads the notes of the instances that said that they stopped
// recording while the session ran (see stoppedLocked). kept is the report
// kept for each instance, counts what the collector has.
//
//   - Something ties the instance to the session, a delivery report of it or
//     events of it at the collector: it took part and recorded nothing from
//     then on, so the session is incomplete (shortfalls).
//   - Nothing does: it may be a service the session never touched, or a name
//     somebody made up; it may also be one that stopped before the first
//     request of the session reached it, and so missed events it should have
//     recorded. Those cannot be told apart, so it is a warning (unconfirmed).
//     The instances that followed the active set when they said it come
//     first: the list is cut (capReasons).
func stoppedReasons(s Session, kept map[string]Delivery, counts collectorCounts) (shortfalls, unconfirmed []string) {
	const bug = "; this is a bug in tsession, and the log of that service has the panic"
	var strangers []string
	for _, n := range s.StoppedInstances {
		said := who(n.Service, n.Instance) + " stopped recording after an internal error (" + n.Why + ")"
		_, reported := kept[deliveryKey(n.Service, n.Instance)]
		have, ok := counts.of(n.Service, n.Instance)
		switch {
		case reported || ok && have > 0:
			shortfalls = append(shortfalls, said+", so events of this session it would have recorded since are missing"+bug)
		case n.Connected:
			unconfirmed = append(unconfirmed, said+" while this session ran; no delivery report and no events of it are known for the session, "+
				"so whether it took part is unknown: it may have missed events it should have recorded"+bug)
		default:
			strangers = append(strangers, said+" while this session ran; it did not follow the control plane then, and no delivery report and no events of it "+
				"are known for the session, so whether it took part is unknown: it may have missed events it should have recorded"+bug)
		}
	}
	unconfirmed = append(unconfirmed, strangers...)
	if s.StoppedInstancesMore {
		unconfirmed = append(unconfirmed, fmt.Sprintf("more than %d instances said that they stopped recording while this session ran; what the others said is not kept", maxStoppedInstances))
	}
	return shortfalls, unconfirmed
}

// heartbeatPeriod is how often an instance that follows the active set is
// asked to say that its reports arrive while a session runs (see
// ActiveSet.DeliveryHeartbeatMs): half the grace, so that a healthy instance
// is never a whole grace without a word. Zero when the check is off.
func (m *Manager) heartbeatPeriod() time.Duration {
	return max(m.cfg.DeliveryGrace/2, 0)
}

// unheard reports whether nothing has arrived for too long from an instance
// that follows the active set and said that it sends delivery reports:
// something between it and here refuses its reports, now if not from the
// start. That is
//
//   - no report (of any session) and no heartbeat at all since it connected,
//     DeliveryGrace/10 ago or longer: it sends one when it connects; or
//   - none for DeliveryGrace, two heartbeat periods, in all of which the
//     control plane had a session. Without a session an instance sends no
//     heartbeat, and its first is due when it learns that there is one; so
//     its silence counts from then, and a first heartbeat that is slow on its
//     way costs a session nothing.
//
// The third review's F5: the question used to be asked once per connection
// ("has it ever been heard of"), so an instance whose reports stopped
// arriving after the first was never named.
func (m *Manager) unheard(sub Subscriber, now time.Time) bool {
	if !sub.stream || sub.DeliveryInstance == "" || m.cfg.DeliveryGrace < 0 {
		return false
	}
	if sub.heard.IsZero() {
		return now.Sub(sub.since) >= m.cfg.DeliveryGrace/10
	}
	return !m.busySince.IsZero() && now.Sub(m.busySince) >= m.cfg.DeliveryGrace && now.Sub(sub.heard) >= m.cfg.DeliveryGrace
}

// staleReport reports whether d, a report of the instance prev is of, is an
// older one: the instance had recorded less when it sent d, or as much and
// had delivered or dropped less. The counters of an instance only rise, and
// recorded rises with every event, so recorded alone orders two reports that
// differ in it.
//
// A lower delivered or dropped next to a higher recorded does not make a
// report stale. Nothing an instance sends looks like that, but a report in
// its name by somebody else may, sent before its own: {recorded 1, dropped 1,
// final} would otherwise make every report of the real instance stale, and
// the session complete with the collector holding that one event.
func staleReport(d, prev Delivery) bool {
	if d.Recorded != prev.Recorded {
		return d.Recorded < prev.Recorded
	}
	return d.Delivered < prev.Delivered || d.Dropped < prev.Dropped
}

// reportOf is the report kept for an instance: the one stored with the
// session, or the one kept beside it (see deliveryState.refused).
func (st *deliveryState) reportOf(s Session, service, instance string) (Delivery, bool) {
	if i := slices.IndexFunc(s.Delivery, func(x Delivery) bool { return x.Service == service && x.Instance == instance }); i >= 0 {
		return s.Delivery[i], true
	}
	d, ok := st.refused[deliveryKey(service, instance)]
	return d, ok
}

// reports lists every report kept for the session: the stored ones in their
// order, then the ones kept beside them, by service and instance.
func (st *deliveryState) reports(s Session) []Delivery {
	if len(st.refused) == 0 {
		return s.Delivery
	}
	list := slices.Clone(s.Delivery)
	for _, key := range slices.Sorted(maps.Keys(st.refused)) {
		list = append(list, st.refused[key])
	}
	return list
}

// reportLocked is ReportDelivery for a report whose names are clean.
func (m *Manager) reportLocked(id string, d Delivery, now time.Time) (bool, error) {
	d.LastError = cleanDeliveryError(d.LastError)
	for _, n := range []int64{d.Recorded, d.Delivered, d.Dropped, d.Buffered, d.DeliveredTotal} {
		if n < 0 || n > maxDeliveryCount {
			return false, fmt.Errorf("%w: counters must be 0..2^53", ErrInvalid)
		}
	}
	if d.Recorded != d.Delivered+d.Dropped+d.Buffered {
		return false, fmt.Errorf("%w: recorded must equal delivered + dropped + buffered", ErrInvalid)
	}
	d.At = now.UTC()
	s, ok := m.live[id]
	if !ok {
		s, err := m.stored(id)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("%w: session is %s; delivery reports are accepted for running sessions only", ErrBadState, s.State)
	}
	st := m.deliveryStateLocked(id)
	if st.closed {
		return false, fmt.Errorf("%w: session is being sealed; delivery reports are no longer accepted", ErrBadState)
	}
	key := deliveryKey(d.Service, d.Instance)
	m.noteProgressLocked(key, d.DeliveredTotal, now)
	i := slices.IndexFunc(s.Delivery, func(x Delivery) bool { return x.Service == d.Service && x.Instance == d.Instance })
	prev, known := Delivery{}, i >= 0
	if known {
		prev = s.Delivery[i]
	}
	// beside: the report is kept for the verdict, not stored with the session.
	beside := false
	if !known {
		prev, known = st.refused[key]
		beside = known
	}
	switch {
	case known:
		if staleReport(d, prev) {
			return false, nil
		}
		// A report sent before the instance knew of the drain cannot take back
		// one sent after: the two name the same events. One that names more is
		// a later one and speaks for itself, so a final report in the instance's
		// name by somebody else does not make its later ones final.
		d.Final = d.Final || prev.Final && d.Recorded == prev.Recorded
		d.DeliveredTotal = max(d.DeliveredTotal, prev.DeliveredTotal)
	case d.Recorded == 0:
		return true, nil
	case len(s.Delivery) >= MaxDeliveryInstances:
		if len(st.refused) >= maxDeliveryRefused {
			st.refusedMore = true
			st.refusedShort = st.refusedShort || d.Dropped > 0 || d.Final && d.Buffered > 0
			return false, errDeliveryFull
		}
		beside = true
	}
	if !known || prev.Buffered == 0 || d.Delivered > prev.Delivered {
		st.progress[key] = now
	}
	if d.Final && d.Buffered > 0 {
		st.short[key] = struct{}{}
	}
	if beside {
		if st.refused == nil {
			st.refused = map[string]Delivery{}
		}
		st.refused[key] = d
		return false, errDeliveryFull
	}
	// The stored slice is shared with copies of the session: never changed in place.
	list := make([]Delivery, 0, len(s.Delivery)+1)
	list = append(list, s.Delivery...)
	if i >= 0 {
		list[i] = d
	} else {
		list = append(list, d)
		slices.SortFunc(list, func(a, b Delivery) int {
			return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.Instance, b.Instance))
		})
	}
	s.Delivery = list
	if err := m.putLocked(s); err != nil {
		return false, err
	}
	return true, nil
}

// noteProgressLocked records that the instance behind key reported total
// events delivered for all its sessions: more than it reported before means
// that it is getting events through.
func (m *Manager) noteProgressLocked(key string, total int64, now time.Time) {
	p, ok := m.progress[key]
	if ok && total <= p.total {
		return
	}
	if !ok && len(m.progress) >= maxInstanceProgress {
		// Forgetting costs an instance nothing but a grace that starts over.
		clear(m.progress)
	}
	m.progress[key] = instanceProgress{total: total, at: now}
}

// pruneProgressLocked forgets the instances that have not delivered anything
// for a long time.
func (m *Manager) pruneProgressLocked(now time.Time) {
	keep := max(10*m.cfg.DeliveryGrace, 10*time.Minute)
	for k, p := range m.progress {
		if now.Sub(p.at) > keep {
			delete(m.progress, k)
		}
	}
}

// reporterConnectedLocked reports whether an instance that sends delivery
// reports under key follows the active set right now.
func (m *Manager) reporterConnectedLocked(key string) bool {
	for _, sub := range m.subs {
		if sub.stream && sub.DeliveryInstance != "" && deliveryKey(sub.Service, sub.DeliveryInstance) == key {
			return true
		}
	}
	return false
}

// silentLocked looks for the instances the session should have heard from
// and has not: those that follow the active set over a stream, said when they
// opened it that they send delivery reports (Subscriber.DeliveryInstance),
// and of which the collector has events of this session that no report of
// theirs covers. Such an instance took part, and it reports a session at once
// when it learns of the drain, so something is wrong with it or between it
// and here.
//
// An instance the collector has no events from is not looked at, whether it
// reported or not: nothing shows that it took part, and an environment has
// many connected instances that did not.
//
// It is waited for (wait) until it has had DeliveryGrace since its last sign
// of life: the drain's begin, its connecting, or a report of it for any
// session, whichever is latest. Its heartbeat is not one: that says that the
// instance reaches the control plane, not that it is at work on a session,
// and it comes more often than the grace is long. silent lists those that
// sent no report at all for the session and have had that time.
func (m *Manager) silentLocked(s Session, st *deliveryState, begin, now time.Time, cur collectorCounts) (silent []Subscriber, wait bool) {
	seen := map[string]struct{}{}
	for _, sub := range m.subs {
		if !sub.stream || sub.DeliveryInstance == "" {
			continue
		}
		key := deliveryKey(sub.Service, sub.DeliveryInstance)
		have, ok := cur.of(sub.Service, sub.DeliveryInstance)
		if !ok || have == 0 {
			continue
		}
		d, reported := st.reportOf(s, sub.Service, sub.DeliveryInstance)
		if reported && (d.Final || have <= d.Recorded) {
			continue
		}
		since := begin
		for _, t := range []time.Time{sub.since, sub.reported} {
			if t.After(since) {
				since = t
			}
		}
		if now.Sub(since) < m.cfg.DeliveryGrace {
			wait = true
			continue
		}
		if _, dup := seen[key]; !dup && !reported {
			seen[key] = struct{}{}
			silent = append(silent, sub)
		}
	}
	slices.SortFunc(silent, func(a, b Subscriber) int {
		return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.DeliveryInstance, b.DeliveryInstance))
	})
	return silent, wait
}

// unheardLocked lists the instances of which nothing has arrived for too
// long (see unheard). skip names the ones that are accounted for otherwise.
// An instance with several connections is unheard when all of them are, and
// is listed with the one it opened last.
//
// The newest, not the one heard of last: a request marks every connection the
// instance has at that moment (see heardLocked), so the connections that were
// heard of all say the same, and one that says something else is a later one
// on which nothing has arrived. That is the stream an SDK opens anew when its
// reports keep failing, so as to be named; behind a proxy that keeps the
// stream it ended open for a while, the old connection is still here, and
// going by it the instance was "heard of, and silent now", which is not named
// where nothing ties it to the session (the fifth review: 0 of 5 sessions
// named behind such a proxy, 4 of 4 without it). A healthy instance with a
// stream that lingers is not listed: what arrives of it is noted on both.
func (m *Manager) unheardLocked(now time.Time, skip func(key string) bool) []Subscriber {
	var out []Subscriber
	best := map[string]Subscriber{}
	heardOf := map[string]struct{}{}
	for _, sub := range m.subs {
		if !sub.stream || sub.DeliveryInstance == "" {
			continue
		}
		key := deliveryKey(sub.Service, sub.DeliveryInstance)
		if !m.unheard(sub, now) {
			heardOf[key] = struct{}{}
			continue
		}
		if b, ok := best[key]; !ok || sub.since.After(b.since) {
			best[key] = sub
		}
	}
	for key, sub := range best {
		if _, ok := heardOf[key]; ok || skip(key) {
			continue
		}
		out = append(out, sub)
	}
	slices.SortFunc(out, func(a, b Subscriber) int {
		return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.DeliveryInstance, b.DeliveryInstance))
	})
	return out
}

// uncountedLocked counts the connected, reporting instances without a report
// for the session of which the collector's count is not known although the
// collector counts per instance: its list was cut short and it did not answer
// for them by name.
func (m *Manager) uncountedLocked(s Session, st *deliveryState, counts collectorCounts) int {
	if !counts.known {
		return 0
	}
	seen := map[string]struct{}{}
	for _, sub := range m.subs {
		if !sub.stream || sub.DeliveryInstance == "" {
			continue
		}
		if _, reported := st.reportOf(s, sub.Service, sub.DeliveryInstance); reported {
			continue
		}
		if _, ok := counts.of(sub.Service, sub.DeliveryInstance); !ok {
			seen[deliveryKey(sub.Service, sub.DeliveryInstance)] = struct{}{}
		}
	}
	return len(seen)
}

// collectorCounts is what the collector has of a session from the SDK
// instances the control plane needs to know it of.
type collectorCounts struct {
	// known is false when the collector gave no counts: an older collector,
	// or no answer.
	known bool
	// truncated: the collector's list was cut, so an instance that is not in
	// n may have sent events all the same, unless it was asked about by name.
	truncated bool
	n         map[string]int64 // by deliveryKey
}

// countsOf reads the counts of a drain-status answer. The names are cleaned
// the way the names of a report are, so the two compare.
func countsOf(st DrainStatus, ok bool) collectorCounts {
	if !ok || st.Instances == nil {
		return collectorCounts{}
	}
	c := collectorCounts{known: true, truncated: st.InstancesTruncated, n: make(map[string]int64, len(st.Instances))}
	for _, i := range st.Instances {
		c.n[deliveryKey(ident.Clean(i.Service), ident.Clean(i.Instance))] += i.Events
	}
	return c
}

// of is the number of events the collector has from an instance. ok is false
// when that is not known.
func (c collectorCounts) of(service, instance string) (n int64, ok bool) {
	if !c.known {
		return 0, false
	}
	if n, ok := c.n[deliveryKey(service, instance)]; ok {
		return n, true
	}
	return 0, !c.truncated
}

// owed reports whether the instance behind d may still deliver events the
// collector does not have: what the drain waits for. With a collector count
// that is the events the instance recorded and did not give up, less what the
// collector has; without one it is the instance's own word.
func (c collectorCounts) owed(d Delivery) bool {
	if have, ok := c.of(d.Service, d.Instance); ok {
		return have < d.Recorded-d.Dropped
	}
	return d.Buffered > 0
}

// InstanceCounter is a Collector that can be asked how many events of a
// session it has from named instances. A drain asks when the collector's own
// list (DrainStatus.Instances) is cut short: a list can be filled with
// instances nobody asked about, an answer to a question cannot.
type InstanceCounter interface {
	// InstanceCounts returns one count per instance asked about, in order,
	// zero for an instance without events. At most MaxInstanceCountsAsk
	// instances per call.
	InstanceCounts(ctx context.Context, id string, instances []InstanceCount) ([]InstanceCount, error)
}

// MaxInstanceCountsAsk bounds the instances of one InstanceCounts call.
const MaxInstanceCountsAsk = 512

// countsFor reads the counts of a drain-status answer and, when the
// collector's list was cut short, asks it by name about the instances this
// session's verdict needs and the list does not hold: the ones that reported
// and the ones that follow the active set. Whatever the collector does not
// answer for stays unknown.
func (m *Manager) countsFor(ctx context.Context, id string, st DrainStatus, ok bool) collectorCounts {
	c := countsOf(st, ok)
	ic, can := m.cfg.Collector.(InstanceCounter)
	if !c.known || !c.truncated || !can {
		return c
	}
	var ask []InstanceCount
	seen := map[string]struct{}{}
	add := func(service, instance string) {
		key := deliveryKey(service, instance)
		if _, dup := seen[key]; dup || len(ask) >= maxCountsAsked {
			return
		}
		seen[key] = struct{}{}
		if _, has := c.n[key]; !has {
			ask = append(ask, InstanceCount{Service: service, Instance: instance})
		}
	}
	// The unlock is deferred: a drain is started again after a panic, and
	// one in here that left the lock held would stop the whole control plane.
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		faultpoint.Hit("countsFor.locked")
		for _, d := range m.live[id].Delivery {
			add(d.Service, d.Instance)
		}
		if st := m.delivery[id]; st != nil {
			for _, d := range st.refused {
				add(d.Service, d.Instance)
			}
		}
		for _, sub := range m.subs {
			if sub.stream && sub.DeliveryInstance != "" {
				add(sub.Service, sub.DeliveryInstance)
			}
		}
	}()
	for chunk := range slices.Chunk(ask, MaxInstanceCountsAsk) {
		got, err := ic.InstanceCounts(ctx, id, chunk)
		if err != nil || len(got) != len(chunk) {
			if ctx.Err() == nil {
				m.log.Error("instance counts failed", "id", id, "err", err)
			}
			break
		}
		for i, q := range chunk {
			c.n[deliveryKey(q.Service, q.Instance)] = got[i].Events
		}
	}
	return c
}

// deliverySettled reports whether the drain of id, which began at begin, has
// nothing left to wait for:
//
//   - no instance that reported still owes the collector events (see owed),
//     except those that have got nowhere for DeliveryGrace: no report that
//     delivered more of this session, no rise of the total the instance
//     delivered for all its sessions, and no rise of what the collector has
//     from it. An instance that is working through a backlog, of this session
//     or of others, is waited for; the drain's max_drain bounds that.
//   - no connected instance has events of the session at the collector that
//     its reports do not cover, or it has had DeliveryGrace since its last
//     sign of life (see silentLocked).
//
// The grace counts from begin at the earliest, so every instance gets all of
// it after it was told to flush. cur is what the collector has now. With
// force nothing is looked at. When it reports true the session's reports are
// closed: the verdict is made from what is stored now.
func (m *Manager) deliverySettled(id string, begin time.Time, cur collectorCounts, force bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.live[id]
	if !ok {
		return true // aborted meanwhile: nothing to close
	}
	st := m.deliveryStateLocked(id)
	now := time.Now()
	wait := false
	if m.cfg.DeliveryGrace > 0 {
		for _, d := range st.reports(s) {
			key := deliveryKey(d.Service, d.Instance)
			if have, ok := cur.of(d.Service, d.Instance); ok {
				if prev, seen := st.have[key]; seen && have > prev {
					st.progress[key] = now
				}
				st.have[key] = have
			}
			if !cur.owed(d) {
				continue
			}
			since := begin
			for _, t := range []time.Time{st.progress[key], m.progress[key].at} {
				if t.After(since) {
					since = t
				}
			}
			wait = wait || now.Sub(since) < m.cfg.DeliveryGrace
		}
		var w bool
		st.silent, w = m.silentLocked(s, st, begin, now, cur)
		wait = wait || w
	}
	if wait && !force {
		return false
	}
	st.closed = true
	return true
}

func who(service, instance string) string {
	if service == "" {
		return "instance " + instance
	}
	return fmt.Sprintf("service %s (instance %s)", service, instance)
}

// deliveryVerdict reads the reports a session ended with against what the
// collector has from each reporting instance. shortfalls name the instances
// whose events did not all reach the collector: the session is incomplete.
// unconfirmed name those of which that cannot be said either way. Neither
// list is bounded here.
//
// Neither the instance's word nor its silence decides. With a collector count
// for the instance (have), and R its last report:
//
//   - have >= R.recorded and R is final: complete, whatever the instance
//     believed. It may have counted events as undelivered, or as dropped at
//     its shutdown, that the collector had stored without the answer getting
//     back.
//   - have >= R.recorded, R not final and nothing dropped: complete as far as
//     R goes. If the collector has more than R.recorded, the instance
//     recorded after R and stopped reporting: unconfirmed, because nothing
//     says how many more there were.
//   - otherwise R.dropped > 0: incomplete.
//   - otherwise have < R.recorded: incomplete, final or not. The drain has
//     waited for the instance as long as it got anywhere.
//
// Without a count the instance's word is all there is, and a word alone never
// makes a session complete:
//
//   - dropped events, or a final report that still holds some: incomplete.
//   - a final report that held some, taken back by a later one (short):
//     incomplete, when the collector counts per instance and only this count
//     is missing. A collector that has the events would have said so.
//   - anything else that recorded events: unconfirmed. When the collector
//     gives no counts at all (an older one) that is said once for the
//     session, not once per instance.
//
// What this cannot see: an instance that is killed after a report that was
// in order, with events recorded since that reached nobody. Its last report
// and the collector agree.
func deliveryVerdict(list []Delivery, counts collectorCounts, short map[string]struct{}) (shortfalls, unconfirmed []string) {
	unchecked := 0
	for _, d := range list {
		w := who(d.Service, d.Instance)
		tail := ""
		if d.Dropped > 0 {
			tail += fmt.Sprintf(", %d dropped by the SDK", d.Dropped)
		}
		if !d.Final {
			tail += "; the instance stopped reporting before the session ended"
		}
		if d.LastError != "" {
			// The remedy with the finding: the reader may never have seen this product.
			tail += " (last error: " + d.LastError + "; check the TSESSION_COLLECTOR_URL of that service)"
		}
		have, ok := counts.of(d.Service, d.Instance)
		switch {
		case !ok:
			why := "the collector's count for the instance was not available"
			if !counts.known {
				why = "the collector gave none"
			}
			msg := fmt.Sprintf("%s: %d of %d recorded events were delivered by the instance's own count (%s)", w, d.Delivered, d.Recorded, why)
			if d.Buffered > 0 {
				msg += fmt.Sprintf(", %d still undelivered", d.Buffered)
			}
			_, took := short[deliveryKey(d.Service, d.Instance)]
			switch {
			case d.Dropped > 0, d.Buffered > 0 && d.Final:
				shortfalls = append(shortfalls, msg+tail)
			case took && counts.known:
				shortfalls = append(shortfalls, msg+tail+"; an earlier final report of it held undelivered events, and nothing but its own word says that they arrived since")
			case d.Buffered > 0:
				unconfirmed = append(unconfirmed, msg+tail+", so whether the rest arrived is unknown")
			case d.Recorded == 0:
			case !counts.known:
				unchecked++
			default:
				unconfirmed = append(unconfirmed, msg+tail+", so that could not be checked")
			}
		case have >= d.Recorded && d.Final:
		case have >= d.Recorded && d.Dropped == 0:
			if have > d.Recorded {
				unconfirmed = append(unconfirmed, fmt.Sprintf("%s: the collector has %d events, more than the %d of the instance's last report, and the instance stopped reporting before the session ended, so whether everything it recorded after that report arrived is unknown",
					w, have, d.Recorded))
			}
		default:
			shortfalls = append(shortfalls, fmt.Sprintf("%s: %d of %d recorded events reached the collector", w, min(have, d.Recorded), d.Recorded)+tail)
		}
	}
	if unchecked > 0 {
		unconfirmed = append(unconfirmed, fmt.Sprintf("the collector does not count events per instance (an older collector), so the delivery reports of %d instance(s) could not be checked against it; upgrade the collector to the control plane's version", unchecked))
	}
	return shortfalls, unconfirmed
}

// capReasons bounds a list of reasons, saying how many it leaves out.
func capReasons(list []string) []string {
	if n := len(list) - maxDeliveryReasons; n > 0 {
		list = append(list[:maxDeliveryReasons:maxDeliveryReasons], fmt.Sprintf("and %d more", n))
	}
	return list
}

// unreported is the reason for an instance whose events the collector has
// and of which no report for the session arrived: it took part, and nothing
// says how many events it recorded. The session is incomplete for it although
// nothing may be missing, which the control plane cannot know without the
// report.
func unreported(service, instance string, have int64) string {
	from := instance
	if service != "" {
		from = service + "/" + instance
	}
	return fmt.Sprintf("the collector has %d event(s) from %s, which did not report how many it recorded; its log says why its reports do not arrive", have, from)
}

// judgeDeliveryLocked writes the verdict on the delivery of a session that is
// being finished into it and forgets the session's delivery state. counts is
// what the collector had once the reports were closed. It reports whether the
// session is incomplete because of it.
//
// The session is incomplete (incomplete_reasons) for:
//
//   - an instance whose last report and the collector's count show a
//     shortfall (see deliveryVerdict). The reports that are not stored with
//     the session, for MaxDeliveryInstances, are judged like the stored ones;
//   - an instance that is connected, whose events of the session the
//     collector has, and of which no report for the session arrived in the
//     time it had (see silentLocked): it took part, and nothing says whether
//     those events are all it recorded (see unreported);
//   - a report beyond the ones that are kept that said that events were lost;
//   - an instance that said, while the session ran, that it stopped recording
//     after an internal error, when a report of it or events of it at the
//     collector tie it to the session (see stoppedReasons). One that nothing
//     ties to the session is unconfirmed.
//
// It is unconfirmed (delivery_unconfirmed, a warning) for what deliveryVerdict
// lists as such, and for:
//
//   - a connected instance of which nothing at all has arrived since it
//     connected, no report and no heartbeat (see unheard), and of which the
//     session has no report: something refuses its reports, and whether it
//     took part cannot be known;
//   - a connected instance that was heard of and then not for too long, when
//     something ties it to the session: its last report for the session was
//     not final and agrees with the collector (whether it recorded more since
//     cannot be known), or the collector has events of it and no report
//     arrived. One that was heard of, is silent now, and of which the session
//     has neither a report nor events is not named: in an environment of many
//     services one that is frozen for a while, or whose reports fail for a
//     few seconds, would otherwise put a line into every session that ends
//     meanwhile, none of which it took part in. The price: a process that is
//     cut off from the control plane altogether after its first word, stream
//     and reports, is not named either (an SDK that only loses its reports
//     opens its stream anew, and is then one that was never heard of);
//   - the instances beyond the MaxDeliveryInstances whose reports are stored
//     and the maxDeliveryRefused whose reports are kept: nothing is known of
//     them.
//
// Each list names at most maxDeliveryReasons instances. With the check turned
// off (DeliveryGrace < 0) shortfalls are listed as unconfirmed, and the
// connected instances are not looked at.
func (m *Manager) judgeDeliveryLocked(s *Session, counts collectorCounts) bool {
	st := m.delivery[s.ID]
	if st == nil {
		st = &deliveryState{}
	}
	reports := st.reports(*s)
	shortfalls, unconfirmed := deliveryVerdict(reports, counts, st.short)
	silent := map[string]struct{}{}
	for _, sub := range st.silent {
		key := deliveryKey(sub.Service, sub.DeliveryInstance)
		// Still connected, with its events at the collector: one that has left
		// since is like one that was never connected.
		have, ok := counts.of(sub.Service, sub.DeliveryInstance)
		if !ok || have == 0 || !m.reporterConnectedLocked(key) {
			continue
		}
		silent[key] = struct{}{}
		shortfalls = append(shortfalls, unreported(sub.Service, sub.DeliveryInstance, have))
	}
	// kept is the report of each instance, by its key. It is built once per
	// judgement: the lock is held here, and looking through the reports again
	// for every unheard instance (a thousand streams, 1280 reports) stalled
	// every request to the control plane for seconds.
	kept := make(map[string]Delivery, len(reports))
	for _, d := range reports {
		// The first one counts: a stored report comes before one kept beside.
		key := deliveryKey(d.Service, d.Instance)
		if _, dup := kept[key]; !dup {
			kept[key] = d
		}
	}
	if m.cfg.DeliveryGrace >= 0 {
		now := time.Now()
		// stopped: the instance has a report for the session that is not final
		// and that the verdict above had nothing to say about.
		stopped := map[string]Delivery{}
		for _, sub := range m.unheardLocked(now, func(key string) bool {
			if _, convicted := silent[key]; convicted {
				return true
			}
			d, reported := kept[key]
			if !reported {
				return false
			}
			if have, ok := counts.of(d.Service, d.Instance); !d.Final && d.Dropped == 0 && ok && have == d.Recorded {
				stopped[key] = d
				return false
			}
			return true
		}) {
			msg := who(sub.Service, sub.DeliveryInstance) + ": it follows the control plane, but "
			if sub.heard.IsZero() {
				msg += "no delivery report of it has arrived since it connected (its log says why)"
			} else {
				msg += fmt.Sprintf("neither a delivery report nor a heartbeat of it has arrived for %s (its log says why)", now.Sub(sub.heard).Round(time.Second))
			}
			if d, ok := stopped[deliveryKey(sub.Service, sub.DeliveryInstance)]; ok {
				unconfirmed = append(unconfirmed, msg+fmt.Sprintf("; its last report for this session, of %d events, was not final, so whether it recorded more since is unknown", d.Recorded))
				continue
			}
			have, ok := counts.of(sub.Service, sub.DeliveryInstance)
			if !sub.heard.IsZero() && (!ok || have == 0) {
				// Heard of before and silent since, with no report for this
				// session and no events of it at the collector: nothing ties it
				// to the session. It is frozen, or its reports stopped arriving
				// a moment ago, and it is idle as far as anyone can tell; the
				// list of followers marks it (Subscriber.DeliveryUnheard). An
				// SDK whose reports keep failing opens its stream anew, and is
				// then one that was never heard of, which is named.
				continue
			}
			switch {
			case !ok:
				msg += ", so whether it recorded anything for this session is unknown"
			case have == 0:
				msg += "; the collector has no events from it for this session, so whether it recorded any is unknown"
			default:
				msg += fmt.Sprintf("; the collector has %d events from it, so whether that is all it recorded is unknown", have)
			}
			unconfirmed = append(unconfirmed, msg)
		}
		if n := m.uncountedLocked(*s, st, counts); n > 0 {
			unconfirmed = append(unconfirmed, fmt.Sprintf("the collector's list of instances was cut short and it did not answer for %d connected instance(s) that sent no report, so whether they took part is unknown", n))
		}
	}
	stoppedShort, stoppedUnknown := stoppedReasons(*s, kept, counts)
	shortfalls = append(shortfalls, stoppedShort...)
	if st.refusedShort {
		shortfalls = append(shortfalls, fmt.Sprintf("more than %d instances reported, and a report of one beyond that said that events were dropped or left undelivered; which instance is not kept",
			MaxDeliveryInstances+maxDeliveryRefused))
	}
	if st.refusedMore {
		unconfirmed = append(unconfirmed, fmt.Sprintf("more than %d instances reported; the reports of the ones beyond that are not accounted for", MaxDeliveryInstances+maxDeliveryRefused))
	}
	if m.cfg.DeliveryGrace < 0 {
		shortfalls, unconfirmed = nil, append(shortfalls, unconfirmed...)
	}
	// Last, where the cut falls first: a note is anybody's word, and those
	// that nothing ties to the session must not push out what was found.
	unconfirmed = append(unconfirmed, stoppedUnknown...)
	delete(m.delivery, s.ID)
	s.IncompleteReasons, s.DeliveryUnconfirmed = capReasons(shortfalls), capReasons(unconfirmed)
	return len(shortfalls) > 0
}

// decodeBody reads a JSON body of at most limit bytes into v, and answers
// the request itself when it cannot.
func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit)).Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": fmt.Sprintf("body exceeds %d bytes", limit)})
			return false
		}
		writeErr(w, fmt.Errorf("%w: %v", ErrInvalid, err))
		return false
	}
	return true
}

// hDelivery serves POST /v1/sessions/{id}/delivery: one report.
func (m *Manager) hDelivery(w http.ResponseWriter, r *http.Request) {
	var d Delivery
	if !decodeBody(w, r, maxDeliveryBody, &d) {
		return
	}
	accepted, err := m.ReportDelivery(r.PathValue("id"), d)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": accepted})
}

// hDeliveryBatch serves POST /v1/delivery: the reports of one instance for
// many sessions.
func (m *Manager) hDeliveryBatch(w http.ResponseWriter, r *http.Request) {
	var b DeliveryBatch
	if !decodeBody(w, r, maxDeliveryBatchBody, &b) {
		return
	}
	results, err := m.ReportDeliveries(b)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string][]DeliveryResult{"results": results})
}
