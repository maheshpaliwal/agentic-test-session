package controlplane

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"time"
)

// Delivery is what one SDK instance last reported about the events it
// recorded for a session: how many there were and what became of them. The
// SDK sends it straight to the control plane (POST /v1/sessions/{id}/delivery),
// because events that never reach the collector cannot be seen there. The
// counts are cumulative for the life of the instance, so a later report
// replaces an earlier one.
type Delivery struct {
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance"`
	// Recorded = Delivered + Dropped + Buffered.
	Recorded  int64 `json:"recorded"`
	Delivered int64 `json:"delivered"` // accepted by the collector (duplicates included)
	Dropped   int64 `json:"dropped"`   // lost for good: ring overflow, refused by the collector, discarded at shutdown
	Buffered  int64 `json:"buffered"`  // still held by the instance
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
)

// deliveryState is the part of a live session's delivery accounting that is
// not stored with it.
type deliveryState struct {
	// stuck is, per instance that holds undelivered events, since when it has
	// held some without delivering any. The clock is the control plane's.
	stuck map[string]time.Time
	// closed is set when the drain stops looking at reports: the verdict is
	// made from what is stored then, and later reports are refused.
	closed bool
	// overflow is set when a report was refused for MaxDeliveryInstances.
	overflow bool
}

func deliveryKey(service, instance string) string { return service + "\x00" + instance }

func (m *Manager) deliveryStateLocked(id string) *deliveryState {
	st := m.delivery[id]
	if st == nil {
		st = &deliveryState{stuck: map[string]time.Time{}}
		m.delivery[id] = st
	}
	return st
}

// urlUserinfo matches the userinfo of a URL inside a text.
var urlUserinfo = regexp.MustCompile(`://[^/\s@]*@`)

// cleanDeliveryError makes a self-reported error text safe to store and
// show: printable ASCII, no URL credentials, bounded.
func cleanDeliveryError(s string) string {
	s = urlUserinfo.ReplaceAllString(identityN(s, 4*maxDeliveryError), "://")
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
// MaxDeliveryInstances instances.
func (m *Manager) ReportDelivery(id string, d Delivery) (bool, error) {
	d.Service, d.Instance, d.LastError = identity(d.Service), identity(d.Instance), cleanDeliveryError(d.LastError)
	if d.Instance == "" {
		return false, fmt.Errorf("%w: instance is required", ErrInvalid)
	}
	for _, n := range []int64{d.Recorded, d.Delivered, d.Dropped, d.Buffered} {
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
	i := slices.IndexFunc(s.Delivery, func(x Delivery) bool { return x.Service == d.Service && x.Instance == d.Instance })
	var prev Delivery
	if i >= 0 {
		prev = s.Delivery[i]
		if d.Recorded < prev.Recorded || d.Delivered < prev.Delivered || d.Dropped < prev.Dropped {
			return false, nil
		}
		// A report sent before the instance knew of the drain cannot take back
		// one sent after.
		d.Final = d.Final || prev.Final
	} else if len(s.Delivery) >= MaxDeliveryInstances {
		st.overflow = true
		return false, fmt.Errorf("%w: session already keeps delivery reports of %d instances", ErrBadState, MaxDeliveryInstances)
	}
	switch {
	case d.Buffered == 0:
		delete(st.stuck, key)
	case i < 0 || prev.Buffered == 0 || d.Delivered > prev.Delivered:
		st.stuck[key] = now
	default:
		if _, ok := st.stuck[key]; !ok { // the control plane restarted since
			st.stuck[key] = now
		}
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

// deliverySettled reports whether the drain of id, which began at begin, has
// nothing left to wait for among the instances that reported: none holds
// undelivered events, except those that have held some for DeliveryGrace
// without delivering any (counted from begin at the earliest, so every
// instance gets the whole grace after it was told to flush). With force it
// does not look. When it reports true the session's reports are closed: the
// verdict is made from what is stored now.
func (m *Manager) deliverySettled(id string, begin time.Time, force bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.live[id]
	if !ok {
		return true // aborted meanwhile: nothing to close
	}
	st := m.deliveryStateLocked(id)
	if !force && m.cfg.DeliveryGrace > 0 {
		now := time.Now()
		for _, d := range s.Delivery {
			if d.Buffered == 0 {
				continue
			}
			since := st.stuck[deliveryKey(d.Service, d.Instance)]
			if since.Before(begin) {
				since = begin
			}
			if now.Sub(since) < m.cfg.DeliveryGrace {
				return false
			}
		}
	}
	st.closed = true
	return true
}

// deliveryVerdict reads the reports a session ended with. shortfalls name the
// instances whose events are known not to have reached the collector: the
// session is incomplete. unconfirmed name those whose last word left events
// undelivered without saying that they were lost.
//
// A false "incomplete" on a healthy run costs the trust the gate lives on, so
// a shortfall needs evidence from the instance itself:
//
//   - it dropped events; or
//   - its last report is final (it knew of the drain and had flushed) and
//     still holds events: the drain waited DeliveryGrace for them; or
//   - it never delivered an event of the session, was failing to reach the
//     collector when it last reported, and then went silent.
//
// An instance that went silent holding events after it had delivered some is
// unconfirmed: it may have delivered the rest and been killed before it
// could say so.
func deliveryVerdict(list []Delivery) (shortfalls, unconfirmed []string) {
	for _, d := range list {
		who := "instance " + d.Instance
		if d.Service != "" {
			who = fmt.Sprintf("service %s (instance %s)", d.Service, d.Instance)
		}
		msg := fmt.Sprintf("%s: %d of %d recorded events reached the collector", who, d.Delivered, d.Recorded)
		if d.Buffered > 0 {
			msg += fmt.Sprintf(", %d still undelivered", d.Buffered)
		}
		if d.Dropped > 0 {
			msg += fmt.Sprintf(", %d dropped", d.Dropped)
		}
		if d.LastError != "" {
			msg += " (last error: " + d.LastError + ")"
		}
		switch {
		case d.Dropped > 0, d.Buffered > 0 && d.Final, d.Buffered > 0 && d.Delivered == 0 && d.LastError != "":
			shortfalls = append(shortfalls, msg)
		case d.Buffered > 0:
			unconfirmed = append(unconfirmed, msg+"; it stopped reporting before the session ended, so whether the rest arrived is unknown")
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
// being finished into it and forgets the session's delivery state. It
// reports whether the session is incomplete because of it. With the check
// turned off (DeliveryGrace < 0) shortfalls are listed as unconfirmed.
func (m *Manager) judgeDeliveryLocked(s *Session) bool {
	shortfalls, unconfirmed := deliveryVerdict(s.Delivery)
	if m.cfg.DeliveryGrace < 0 {
		shortfalls, unconfirmed = nil, append(shortfalls, unconfirmed...)
	}
	if st := m.delivery[s.ID]; st != nil && st.overflow {
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
