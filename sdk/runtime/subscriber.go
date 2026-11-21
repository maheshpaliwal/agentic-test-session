package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
)

type activeSet struct {
	Active    []string `json:"active"`
	Exclusive string   `json:"exclusive"`
	Kill      bool     `json:"kill"`
	// Bindings is optional: older control planes omit it.
	Bindings []core.Binding `json:"bindings"`
	// Draining is optional too: the ids in Active that are DRAINING.
	Draining []string `json:"draining"`
	// DeliveryReports is optional as well: the control plane says that it
	// takes delivery reports. One that does not say so may still take them
	// (the first that did, did not say it).
	DeliveryReports bool `json:"delivery_reports"`
	// DeliveryHeartbeatMs is optional: how often the control plane wants to
	// hear from this process while the set names a session.
	DeliveryHeartbeatMs int64 `json:"delivery_heartbeat_ms"`
}

type snapshot struct {
	active    map[string]struct{}
	draining  map[string]struct{}
	exclusive string
	kill      bool
	bindings  core.BindingSet
	// reports: the control plane said that it takes delivery reports. A 404
	// to a report then does not mean "a control plane without the endpoint".
	reports bool
	// heartbeat is how often the control plane asked to hear from this
	// process while the set names a session; zero when it did not say.
	heartbeat time.Duration
}

// Subscriber tracks the control plane's active set and implements
// core.SessionSet. It follows the SSE stream and falls back to polling the
// snapshot endpoint while the stream is down.
type Subscriber struct {
	base       string
	maxBackoff time.Duration
	idle       time.Duration // no bytes for this long => reconnect
	client     *http.Client  // own transport: unaffected by DefaultTransport patching
	service    string        // reported to the control plane, for its list of connected services
	instance   string
	// reportsAs is the instance name this process sends delivery reports
	// under; empty when it sends none. The stream announces it while announce
	// is set: not to a control plane that turned out not to take the reports
	// (see setAnnounce).
	reportsAs string
	announce  atomic.Bool
	// streamCancel ends the stream that is open, nil when none is.
	streamCancel atomic.Pointer[context.CancelFunc]
	snap         atomic.Pointer[snapshot]
	ready        atomic.Bool
	streaming    atomic.Bool
	// connects counts the streams that were opened and have delivered the
	// active set: a new one may lead to another control plane (a restart, an
	// upgrade), and what that one says of itself is in the set. opened is
	// signalled for each, and when the set names a session after naming none.
	connects atomic.Uint64
	opened   chan struct{}

	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber starts following controlPlaneURL in the background.
func NewSubscriber(controlPlaneURL string) *Subscriber {
	return NewSubscriberFor(controlPlaneURL, "", "")
}

// NewSubscriberFor is NewSubscriber for a process that names itself: the
// control plane lists service and instance among its connected services
// (GET /v1/subscribers, `tracectl doctor`).
func NewSubscriberFor(controlPlaneURL, service, instance string) *Subscriber {
	return newSubscriber(controlPlaneURL, service, instance, "")
}

// newSubscriber is NewSubscriberFor for a process that also sends delivery
// reports, as instance reportsAs (see core.HeaderDeliveryInstance).
func newSubscriber(controlPlaneURL, service, instance, reportsAs string) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Subscriber{
		base:       strings.TrimRight(controlPlaneURL, "/"),
		maxBackoff: 5 * time.Second,
		idle:       45 * time.Second,
		client:     &http.Client{Transport: newPlainTransport()},
		service:    service,
		instance:   instance,
		reportsAs:  reportsAs,
		opened:     make(chan struct{}, 1),
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	s.snap.Store(&snapshot{})
	s.announce.Store(reportsAs != "")
	go s.run(ctx)
	return s
}

// setAnnounce says whether the stream names the instance this process sends
// delivery reports under. The announcement tells the control plane to expect
// reports from this process, so it is taken back when the control plane does
// not take them (an older one), and made again when it does. A change opens
// the stream anew: the announcement is a header of the request.
func (s *Subscriber) setAnnounce(on bool) {
	if s.reportsAs == "" || s.announce.Swap(on) == on {
		return
	}
	if cancel := s.streamCancel.Load(); cancel != nil {
		(*cancel)()
	}
}

// IsActive reports whether id is ACTIVE/DRAINING and the kill switch is off.
func (s *Subscriber) IsActive(id string) bool {
	sn := s.snap.Load()
	if sn.kill {
		return false
	}
	_, ok := sn.active[id]
	return ok
}

// Exclusive returns the exclusive session id, if any (never while killed).
func (s *Subscriber) Exclusive() (string, bool) {
	sn := s.snap.Load()
	if sn.kill || sn.exclusive == "" {
		return "", false
	}
	return sn.exclusive, true
}

// AnyActive implements core.AnyActiver: some session is ACTIVE/DRAINING or an
// exclusive session is set, and the kill switch is off. False until the first
// state arrives from the control plane.
func (s *Subscriber) AnyActive() bool { return s.snap.Load().anyActive() }

func (sn *snapshot) anyActive() bool {
	return !sn.kill && (len(sn.active) > 0 || sn.exclusive != "")
}

// MatchAttrs implements core.Binder. It extracts only the attribute keys that
// active bindings use (none when there are none) and never matches while killed.
func (s *Subscriber) MatchAttrs(a core.Attributes) (string, bool) {
	id, _, ok := s.MatchAttrsKey(a)
	return id, ok
}

// MatchAttrsKey implements core.KeyBinder.
func (s *Subscriber) MatchAttrsKey(a core.Attributes) (string, core.BindingKey, bool) {
	sn := s.snap.Load()
	if sn.kill || sn.bindings.Empty() {
		return "", core.BindingKey{}, false
	}
	return sn.bindings.Match(a)
}

// MatchAttrsDetail implements core.DetailBinder (priority, fallback, ambiguity).
func (s *Subscriber) MatchAttrsDetail(a core.Attributes) core.BindingMatch {
	sn := s.snap.Load()
	if sn.kill || sn.bindings.Empty() {
		return core.BindingMatch{}
	}
	return sn.bindings.MatchDetail(a)
}

// BindingKeys lists the attribute keys currently needed (empty when killed).
func (s *Subscriber) BindingKeys() []core.BindingKey {
	sn := s.snap.Load()
	if sn.kill {
		return nil
	}
	return sn.bindings.Keys()
}

// Ready is true once any state has been received from the control plane.
func (s *Subscriber) Ready() bool { return s.ready.Load() }

// Close stops the subscriber.
func (s *Subscriber) Close() {
	s.cancel()
	<-s.done
}

func (s *Subscriber) apply(a activeSet) {
	sn := &snapshot{active: set.Of(a.Active...), draining: set.Of(a.Draining...), exclusive: a.Exclusive, kill: a.Kill,
		bindings: core.NewBindingSet(a.Bindings), reports: a.DeliveryReports,
		heartbeat: time.Duration(max(a.DeliveryHeartbeatMs, 0)) * time.Millisecond}
	old := s.snap.Swap(sn)
	s.ready.Store(true)
	if len(old.active) == 0 && len(sn.active) > 0 {
		// The first session after none: the reporter looks at once whether a
		// heartbeat is due, instead of at its next tick (see reporter.pass).
		s.wake()
	}
}

// wake makes the reporter look at its accounts now.
func (s *Subscriber) wake() {
	select {
	case s.opened <- struct{}{}:
	default:
	}
}

func (s *Subscriber) run(ctx context.Context) {
	defer close(s.done)
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		got := s.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		if got {
			backoff = 200 * time.Millisecond
		}
		s.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, s.maxBackoff)
	}
}

func (s *Subscriber) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/v1/active", nil)
	if err != nil {
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var a activeSet
	if json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&a) == nil {
		s.apply(a)
	}
}

// stream follows one SSE connection until it ends; reports whether any event
// was applied.
func (s *Subscriber) stream(parent context.Context) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Before the announcement is read: a change after this ends the stream.
	s.streamCancel.Store(&cancel)
	defer s.streamCancel.Store(nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/v1/active/stream", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Accept", "text/event-stream")
	if s.service != "" {
		req.Header.Set(core.HeaderService, s.service)
	}
	if s.instance != "" {
		req.Header.Set(core.HeaderInstance, s.instance)
	}
	if s.reportsAs != "" && s.announce.Load() {
		req.Header.Set(core.HeaderDeliveryInstance, s.reportsAs)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	s.streaming.Store(true)
	defer s.streaming.Store(false)

	watchdog := time.AfterFunc(s.idle, cancel)
	defer watchdog.Stop()

	got := false
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	var data strings.Builder
	for sc.Scan() {
		watchdog.Reset(s.idle)
		line := sc.Text()
		switch {
		case line == "":
			if data.Len() > 0 {
				var a activeSet
				if json.Unmarshal([]byte(data.String()), &a) == nil {
					s.apply(a)
					if !got {
						// The first set of this stream: the reporter starts over
						// with the control plane behind it, and knows from the set
						// whether that one says that it takes reports.
						s.connects.Add(1)
						s.wake()
					}
					got = true
				}
				data.Reset()
			}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return got
}
