package controlplane

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/scrub"
)

// Delivery is what one SDK instance last reported about the events it
// recorded for a session: how many there were and what became of them. The
// SDK sends it straight to the control plane (POST /v1/sessions/{id}/delivery),
// because events that never reach the collector cannot be seen there. The
// counts are cumulative for the life of the instance, so a later report
// replaces an earlier one.
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

const (
	// maxDeliveryBody bounds the body of a delivery report.
	maxDeliveryBody = 4 << 10
	// MaxDeliveryInstances bounds the instances one session keeps reports of.
	MaxDeliveryInstances = 256
	maxDeliveryError     = 256
	// maxDeliveryCount bounds a counter of a report (2^53: exact in JSON).
	maxDeliveryCount = 1 << 53
	// maxDeliveryReasons bounds the instances named in incomplete_reasons and
	// delivery_unconfirmed.
	maxDeliveryReasons = 16
	// maxInstanceProgress bounds Manager.progress.
	maxInstanceProgress = 4096
)

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
	// idle are the connected instances that reported that they recorded
	// nothing for the session. Such a report is not stored.
	idle map[string]struct{}
	// closed is set when the drain stops looking at reports: the verdict is
	// made from what is stored then, and later reports are refused.
	closed bool
	// overflow is set when a report was refused for MaxDeliveryInstances.
	overflow bool
	// silent names the connected instances that owed a report when the reports
	// were closed and had sent none (see silentLocked).
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
		st = &deliveryState{progress: map[string]time.Time{}, have: map[string]int64{}, idle: map[string]struct{}{}}
		m.delivery[id] = st
	}
	return st
}

// cleanDeliveryError makes a self-reported error text safe to store and
// show: printable ASCII, no URL credentials, bounded.
func cleanDeliveryError(s string) string {
	s = scrub.Text(identityN(s, 4*maxDeliveryError))
	if len(s) > maxDeliveryError {
		s = s[:maxDeliveryError]
	}
	return s
}

// ReportDelivery records what an SDK instance says about its events of a
// running session. It reports false, without an error, for a report whose
// counters lie behind the ones already stored for that instance (an old
// report that arrived late): it is ignored. The errors are ErrNotFound,
// ErrInvalid, and ErrBadState for a session that takes no reports (any more):
// one that is finished or about to be sealed, or one that already keeps
// MaxDeliveryInstances instances and is told of another (an instance it
// keeps can still report).
//
// A report that says nothing was recorded is not stored. It is remembered
// when it comes from an instance that follows the active set: that instance
// has then accounted for the session (see silentLocked).
func (m *Manager) ReportDelivery(id string, d Delivery) (bool, error) {
	d.Service, d.Instance, d.LastError = identity(d.Service), identity(d.Instance), cleanDeliveryError(d.LastError)
	if d.Instance == "" {
		return false, fmt.Errorf("%w: instance is required", ErrInvalid)
	}
	for _, n := range []int64{d.Recorded, d.Delivered, d.Dropped, d.Buffered, d.DeliveredTotal} {
		if n < 0 || n > maxDeliveryCount {
			return false, fmt.Errorf("%w: counters must be 0..2^53", ErrInvalid)
		}
	}
	if d.Recorded != d.Delivered+d.Dropped+d.Buffered {
		return false, fmt.Errorf("%w: recorded must equal delivered + dropped + buffered", ErrInvalid)
	}
	now := time.Now()
	d.At = now.UTC()

	m.mu.Lock()
	defer m.mu.Unlock()
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
	var prev Delivery
	switch {
	case i >= 0:
		prev = s.Delivery[i]
		if d.Recorded < prev.Recorded || d.Delivered < prev.Delivered || d.Dropped < prev.Dropped {
			return false, nil
		}
		// A report sent before the instance knew of the drain cannot take back
		// one sent after.
		d.Final = d.Final || prev.Final
		d.DeliveredTotal = max(d.DeliveredTotal, prev.DeliveredTotal)
	case d.Recorded == 0:
		if m.reporterConnectedLocked(key) {
			st.idle[key] = struct{}{}
		}
		return true, nil
	case len(s.Delivery) >= MaxDeliveryInstances:
		st.overflow = true
		return false, fmt.Errorf("%w: session already keeps delivery reports of %d instances", ErrBadState, MaxDeliveryInstances)
	}
	if i < 0 || prev.Buffered == 0 || d.Delivered > prev.Delivered {
		st.progress[key] = now
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

// silentLocked lists the instances that owe the session a delivery report
// and have sent none: those that follow the active set over a stream and
// said, when they opened it, that they report (Subscriber.DeliveryInstance).
// Such an instance reports every draining session, also one it recorded
// nothing for, so its silence means that its reports do not arrive. Only
// those that have had DeliveryGrace for it, counted from begin or from when
// they connected, are listed; wait is set while one has not had it yet.
func (m *Manager) silentLocked(s Session, st *deliveryState, begin, now time.Time) (silent []Subscriber, wait bool) {
	seen := map[string]struct{}{}
	for _, sub := range m.subs {
		if !sub.stream || sub.DeliveryInstance == "" {
			continue
		}
		key := deliveryKey(sub.Service, sub.DeliveryInstance)
		if _, idle := st.idle[key]; idle {
			continue
		}
		if slices.ContainsFunc(s.Delivery, func(d Delivery) bool { return d.Service == sub.Service && d.Instance == sub.DeliveryInstance }) {
			continue
		}
		since := begin
		if sub.ConnectedAt.After(since) {
			since = sub.ConnectedAt
		}
		if now.Sub(since) < m.cfg.DeliveryGrace {
			wait = true
			continue
		}
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			silent = append(silent, sub)
		}
	}
	slices.SortFunc(silent, func(a, b Subscriber) int {
		return cmp.Or(cmp.Compare(a.Service, b.Service), cmp.Compare(a.DeliveryInstance, b.DeliveryInstance))
	})
	return silent, wait
}

// collectorCounts is what the collector has of a session from every SDK
// instance, as its drain-status said.
type collectorCounts struct {
	// known is false when the collector gave no counts: an older collector,
	// or no answer.
	known     bool
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
		c.n[deliveryKey(identity(i.Service), identity(i.Instance))] += i.Events
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

// deliverySettled reports whether the drain of id, which began at begin, has
// nothing left to wait for:
//
//   - no instance that reported still owes the collector events (see owed),
//     except those that have got nowhere for DeliveryGrace: no report that
//     delivered more of this session, no rise of the total the instance
//     delivered for all its sessions, and no rise of what the collector has
//     from it. An instance that is working through a backlog, of this session
//     or of others, is waited for; the drain's max_drain bounds that.
//   - every instance that owes a report has sent one, or has had the grace
//     for it (see silentLocked).
//
// The grace counts from begin at the earliest, so every instance gets all of
// it after it was told to flush. cur is the collector's latest drain-status.
// With force nothing is looked at. When it reports true the session's reports
// are closed: the verdict is made from what is stored now.
func (m *Manager) deliverySettled(id string, begin time.Time, cur collectorCounts, force bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.live[id]
	if !ok {
		return true // aborted meanwhile: nothing to close
	}
	st := m.deliveryStateLocked(id)
	now := time.Now()
	check := m.cfg.DeliveryGrace > 0
	wait := false
	if check {
		for _, d := range s.Delivery {
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
		st.silent, w = m.silentLocked(s, st, begin, now)
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
// unconfirmed name those of which that cannot be said either way.
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
// Without a count (an older collector, or more instances than its answer
// lists) the instance's word is all there is: dropped events, or a final
// report that still holds some, are incomplete; a report that is not final
// and holds some is unconfirmed.
//
// What this cannot see: an instance that is killed after a report that was
// in order, with events recorded since that reached nobody. Its last report
// and the collector agree.
func deliveryVerdict(list []Delivery, counts collectorCounts) (shortfalls, unconfirmed []string) {
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
			tail += " (last error: " + d.LastError + ")"
		}
		have, ok := counts.of(d.Service, d.Instance)
		switch {
		case !ok:
			msg := fmt.Sprintf("%s: %d of %d recorded events were delivered by the instance's own count (the collector gave none)", w, d.Delivered, d.Recorded)
			if d.Buffered > 0 {
				msg += fmt.Sprintf(", %d still undelivered", d.Buffered)
			}
			switch {
			case d.Dropped > 0, d.Buffered > 0 && d.Final:
				shortfalls = append(shortfalls, msg+tail)
			case d.Buffered > 0:
				unconfirmed = append(unconfirmed, msg+tail+", so whether the rest arrived is unknown")
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
	return capReasons(shortfalls), capReasons(unconfirmed)
}

func capReasons(list []string) []string {
	if n := len(list) - maxDeliveryReasons; n > 0 {
		list = append(list[:maxDeliveryReasons:maxDeliveryReasons], fmt.Sprintf("and %d more instance(s)", n))
	}
	return list
}

// judgeDeliveryLocked writes the verdict on the reports of a session that is
// being finished into it and forgets the session's delivery state. counts is
// what the collector had once the reports were closed. It reports whether the
// session is incomplete because of it. With the check turned off
// (DeliveryGrace < 0) shortfalls are listed as unconfirmed.
func (m *Manager) judgeDeliveryLocked(s *Session, counts collectorCounts) bool {
	shortfalls, unconfirmed := deliveryVerdict(s.Delivery, counts)
	st := m.delivery[s.ID]
	if st != nil {
		// An instance that follows the active set and reports nothing for a
		// draining session: its reports do not arrive, so nothing vouches for
		// its events. Still connected now, or it is simply gone.
		var silent []string
		for _, sub := range st.silent {
			key := deliveryKey(sub.Service, sub.DeliveryInstance)
			if !m.reporterConnectedLocked(key) {
				continue
			}
			msg := who(sub.Service, sub.DeliveryInstance) + ": it follows the control plane but none of its delivery reports arrived (its log says why), so what it recorded for the session is not accounted for"
			if have, ok := counts.of(sub.Service, sub.DeliveryInstance); ok {
				msg += fmt.Sprintf("; the collector has %d events from it", have)
			}
			silent = append(silent, msg)
		}
		shortfalls = capReasons(append(shortfalls, silent...))
	}
	if m.cfg.DeliveryGrace < 0 {
		shortfalls, unconfirmed = nil, append(shortfalls, unconfirmed...)
	}
	if st != nil && st.overflow {
		unconfirmed = append(unconfirmed, fmt.Sprintf("more than %d instances reported; the others are not accounted for", MaxDeliveryInstances))
	}
	delete(m.delivery, s.ID)
	s.IncompleteReasons, s.DeliveryUnconfirmed = shortfalls, unconfirmed
	return len(shortfalls) > 0
}

// hDelivery serves POST /v1/sessions/{id}/delivery.
func (m *Manager) hDelivery(w http.ResponseWriter, r *http.Request) {
	var d Delivery
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxDeliveryBody)).Decode(&d); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": fmt.Sprintf("body exceeds %d bytes", maxDeliveryBody)})
			return
		}
		writeErr(w, fmt.Errorf("%w: %v", ErrInvalid, err))
		return
	}
	accepted, err := m.ReportDelivery(r.PathValue("id"), d)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": accepted})
}
