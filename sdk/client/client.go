// Package client is a Go client for tsession. Test code uses it to start and
// stop sessions on the control plane, tag outgoing requests, and read what a
// session recorded from the collector (Summary, Events, Mutations, History,
// Trace, Export). Subscribers and Session.WaitPropagated report which
// services are connected and whether they were sent a new session.
//
// A handle to a session comes from Start (a new session), Attach (an existing
// one, by id), AttachEnv (the one a surrounding `tracectl run` put in
// TSESSION_ID) or Detached (by id, without asking the control plane).
package client

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// DefaultURL and DefaultCollectorURL are the loopback addresses the control
// plane and the collector listen on unless configured otherwise.
const (
	DefaultURL          = "http://127.0.0.1:7070"
	DefaultCollectorURL = "http://127.0.0.1:7071"
)

// ErrConflict is returned by Start on HTTP 409: an exclusive session is already
// running, or a binding is held by another active session (error text carries the conflicting id).
var ErrConflict = errors.New("tsession: conflicts with a running session")

// Options configure Start. Zero values use control-plane defaults.
type Options struct {
	BaseURL string // default $TSESSION_CONTROLPLANE_URL or DefaultURL
	// CollectorURL is where the session's results are read from (Summary,
	// Events, Entities, ...); default $TSESSION_COLLECTOR_URL or
	// DefaultCollectorURL. Starting and stopping sessions does not use it.
	CollectorURL    string
	Name            string
	Mode            string // "header" (default) or "exclusive"
	TTLSeconds      int
	QuiesceSeconds  int
	MaxDrainSeconds int
	InitiatedBy     string // default "client"
	// Bindings attribute requests carrying matching attributes to this session
	// (no x-tsession-id needed). Not allowed with Mode "exclusive". jwt_claim
	// bindings read an UNVERIFIED JWT payload: traffic can be spoofed; test
	// environments only.
	Bindings     []Binding
	HTTPClient   *http.Client
	PollInterval time.Duration // Wait polling; default 200ms
}

// Binding is {source, name, value}; source is header|jwt_claim|cookie|query.
type Binding struct {
	Source string `json:"source"`
	Name   string `json:"name"`
	Value  string `json:"value"`
}

// Bind builds a Binding.
func Bind(source, name, value string) Binding { return Binding{source, name, value} }

// BindHeader binds requests carrying the header name: value.
func BindHeader(name, value string) Binding { return Bind("header", name, value) }

// BindJWT binds requests whose bearer token has the claim set to value. The
// token is NOT verified: attribution only, never a security boundary.
func BindJWT(claim, value string) Binding { return Bind("jwt_claim", claim, value) }

// BindCookie binds requests carrying the cookie name=value.
func BindCookie(name, value string) Binding { return Bind("cookie", name, value) }

// BindQuery binds requests with the query parameter name=value.
func BindQuery(name, value string) Binding { return Bind("query", name, value) }

// ParseBinding parses "header:x-user-id=u_1", "jwt:sub=u_1" (also jwt_claim),
// "cookie:sid=x" or "query:uid=1".
func ParseBinding(s string) (Binding, error) {
	src, rest, ok := strings.Cut(s, ":")
	name, val, ok2 := strings.Cut(rest, "=")
	if src == "jwt" {
		src = "jwt_claim"
	}
	switch src {
	case "header", "jwt_claim", "cookie", "query":
	default:
		ok = false
	}
	if !ok || !ok2 || name == "" || val == "" {
		return Binding{}, fmt.Errorf("tsession: invalid binding %q (want source:name=value; source header|jwt|cookie|query)", s)
	}
	return Binding{src, name, val}, nil
}

// Info is the control plane's view of a session.
type Info struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	NameAuto        bool      `json:"name_auto"`
	Mode            string    `json:"mode"`
	State           string    `json:"state"`
	InitiatedBy     string    `json:"initiated_by"`
	CreatedAt       time.Time `json:"created_at"`
	StartedAt       time.Time `json:"started_at"`
	EndedAt         time.Time `json:"ended_at"`
	TTLSeconds      int       `json:"ttl_seconds"`
	QuiesceSeconds  int       `json:"quiesce_seconds"`
	MaxDrainSeconds int       `json:"max_drain_seconds"`
	Incomplete      bool      `json:"incomplete"`
	DroppedEvents   int64     `json:"dropped_events"`
	BundleURI       string    `json:"bundle_uri"`
	// Reason is "ttl" when the session was ended by its TTL.
	Reason string `json:"reason,omitempty"`
	// SealError is set when the collector could not seal the session; the
	// session is then SEALED with Incomplete set.
	SealError string `json:"seal_error,omitempty"`
	// SealPending is set while the control plane is still retrying a seal
	// that failed; SealError and BundleURI change once a retry succeeds.
	SealPending bool `json:"seal_pending,omitempty"`
	// DrainTimedOut is set when the drain ended at MaxDrainSeconds with
	// asynchronous work still outstanding.
	DrainTimedOut bool      `json:"drain_timed_out,omitempty"`
	Bindings      []Binding `json:"bindings,omitempty"`
	// Delivery is the last delivery report of every SDK instance that sent
	// one: what it recorded for the session and what became of it. Services
	// with an older SDK, or that were never connected, are not listed. It is
	// filled by Get and the calls that end a session, not by List: a list of
	// sessions carries the verdict below without the reports behind it.
	Delivery []Delivery `json:"delivery,omitempty"`
	// IncompleteReasons says why the control plane marked the session
	// incomplete: one line per instance whose events did not all reach the
	// collector, by the collector's own count of what it has from that
	// instance. Such a session is Incomplete even when the collector's
	// summary is not: the missing events never got there.
	IncompleteReasons []string `json:"incomplete_reasons,omitempty"`
	// DeliveryUnconfirmed names instances of which the control plane cannot
	// say whether all their events arrived: one that recorded more after its
	// last report and then stopped reporting, say, or one that follows the
	// control plane and whose reports never arrive. A warning: the session is
	// not Incomplete because of it, and it is not known to be complete
	// (tsessiontest.RequireDeliveryConfirmed and tracectl's
	// --fail-on-unconfirmed fail on it).
	DeliveryUnconfirmed []string `json:"delivery_unconfirmed,omitempty"`
}

// Delivery is one SDK instance's account of the events it recorded for a
// session: Recorded = Delivered + Dropped + Buffered.
type Delivery struct {
	Service   string `json:"service,omitempty"`
	Instance  string `json:"instance"`
	Recorded  int64  `json:"recorded"`
	Delivered int64  `json:"delivered"` // accepted by the collector, as far as the instance saw
	Dropped   int64  `json:"dropped"`   // given up by the instance
	Buffered  int64  `json:"buffered"`  // still held by the instance when it last reported
	// DeliveredTotal is what the instance had delivered for all its sessions
	// together when it reported.
	DeliveredTotal int64 `json:"delivered_total,omitempty"`
	// LastError is why the instance's last request to the collector failed.
	LastError string `json:"last_error,omitempty"`
	// Final is set when the instance reported after it had seen the session
	// draining, or while it shut down; otherwise it may have recorded or
	// delivered more since.
	Final bool `json:"final,omitempty"`
	// At is when the control plane received the report.
	At time.Time `json:"at"`
}

// Done reports whether the session reached a terminal state.
func (i Info) Done() bool {
	return i.State == "SEALED" || i.State == "ABORTED" || i.State == "EXPIRED"
}

// Session is a handle to a control-plane session.
type Session struct {
	ID string

	base string // control plane
	col  string // collector
	hc   *http.Client
	poll time.Duration

	mu   sync.Mutex
	info Info
}

// baseURL picks the explicit value, else the environment variable, else def.
func baseURL(explicit, env, def string) string {
	if explicit == "" {
		explicit = os.Getenv(env)
	}
	explicit = cmp.Or(explicit, def)
	return strings.TrimRight(explicit, "/")
}

// scrubURL removes what may be a credential from the URL a transport error
// names: the user name and password of the URL and its query. Such errors
// are printed by tracectl and by tests, and end up in CI logs. The error is
// changed in place, so it still unwraps to the cause.
func scrubURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		ue.URL = DisplayURL(ue.URL)
	}
	return err
}

// ScrubURLError is for callers that make their own requests to the services
// and print the error: it removes the user name, password and query from the
// URL a transport error names, in place, and returns err.
func ScrubURLError(err error) error { return scrubURL(err) }

// DisplayURL is raw as it may be printed: scheme, host and path only. A user
// name, a password and a query may each be a credential.
func DisplayURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparseable URL>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

func newSession(o Options, info Info) *Session {
	hc := o.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	poll := o.PollInterval
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	return &Session{
		ID:   info.ID,
		base: baseURL(o.BaseURL, "TSESSION_CONTROLPLANE_URL", DefaultURL),
		col:  baseURL(o.CollectorURL, "TSESSION_COLLECTOR_URL", DefaultCollectorURL),
		hc:   hc, poll: poll, info: info,
	}
}

// Start creates and activates a session.
func Start(ctx context.Context, o Options) (*Session, error) {
	o.InitiatedBy = cmp.Or(o.InitiatedBy, "client")
	body := map[string]any{
		"name": o.Name, "mode": o.Mode, "ttl_seconds": o.TTLSeconds,
		"quiesce_seconds": o.QuiesceSeconds, "max_drain_seconds": o.MaxDrainSeconds,
		"initiated_by": o.InitiatedBy,
	}
	if len(o.Bindings) > 0 {
		body["bindings"] = o.Bindings
	}
	s := newSession(o, Info{})
	var info Info
	if err := s.do(ctx, http.MethodPost, "/v1/sessions", body, &info); err != nil {
		return nil, err
	}
	s.ID, s.info = info.ID, info
	return s, nil
}

// Attach returns a handle for an existing session id.
func Attach(ctx context.Context, o Options, id string) (*Session, error) {
	s := newSession(o, Info{ID: id})
	if _, err := s.Get(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// SessionEnv names the environment variable in which `tracectl run -- <command>`
// gives its command the id of the session it started: the ambient session.
// Nothing joins that session by itself; the command tags its requests with it
// (see AttachEnv).
const SessionEnv = "TSESSION_ID"

// ErrNoSessionEnv is returned by AttachEnv when SessionEnv is unset or empty:
// the process was not started by `tracectl run`.
var ErrNoSessionEnv = errors.New("tsession: " + SessionEnv + " is not set (no surrounding `tracectl run`)")

// AttachEnv returns a handle to the ambient session, the one named by
// SessionEnv, so that code run by `tracectl run` can tag its requests with it:
//
//	sess, err := client.AttachEnv(ctx, client.Options{})
//	httpClient := &http.Client{Transport: sess.Transport(nil)}
//
// It fails with ErrNoSessionEnv when the variable is not set, and with another
// error when the control plane does not know the session or the session is
// not ACTIVE (a session that is not active records nothing). Of o only the
// URLs, HTTPClient and PollInterval are used; `tracectl run` passes the URLs
// it was given on its command line to the command in TSESSION_CONTROLPLANE_URL
// and TSESSION_COLLECTOR_URL, which an empty Options picks up.
//
// The session belongs to whoever started it: do not Stop, Abort or Delete it.
// It is still recording, so what Summary, Events and Mutations return is a
// moment's view of everything the whole command has done so far, with
// transaction outcomes and asynchronous work unresolved until the seal.
func AttachEnv(ctx context.Context, o Options) (*Session, error) {
	id := os.Getenv(SessionEnv)
	if id == "" {
		return nil, ErrNoSessionEnv
	}
	s, err := Attach(ctx, o, id)
	if err != nil {
		return nil, fmt.Errorf("tsession: session %q named by %s: %w", id, SessionEnv, err)
	}
	if st := s.Info().State; st != "ACTIVE" {
		return nil, fmt.Errorf("tsession: session %s named by %s is %s, not ACTIVE: it records nothing", id, SessionEnv, st)
	}
	return s, nil
}

// Detached returns a handle to a session by id without asking the control
// plane. It is for reading results (Summary, Events, ...) of a session the
// control plane no longer knows, such as one it forgot after its retention
// or a restart, while the collector still holds the events. Info is empty
// apart from the id, and the lifecycle calls (Stop, Wait, ...) will fail.
func Detached(o Options, id string) *Session {
	return newSession(o, Info{ID: id})
}

// Info returns the last fetched session state without a network call.
func (s *Session) Info() Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info
}

// Get refreshes and returns the session.
func (s *Session) Get(ctx context.Context) (Info, error) {
	var info Info
	if err := s.do(ctx, http.MethodGet, "/v1/sessions/"+url.PathEscape(s.ID), nil, &info); err != nil {
		return Info{}, err
	}
	s.set(info)
	return info, nil
}

// Stop moves the session to DRAINING; the control plane then seals it.
func (s *Session) Stop(ctx context.Context) (Info, error) {
	var info Info
	if err := s.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(s.ID)+"/stop", nil, &info); err != nil {
		return Info{}, err
	}
	s.set(info)
	return info, nil
}

// Rename relabels the session (allowed in any state, including SEALED). The
// name may contain templates ({bind}, {bind.<n>}, {initiated_by}, {time},
// {date}); the server expands and sanitises it.
func (s *Session) Rename(ctx context.Context, name string) (Info, error) {
	var info Info
	if err := s.do(ctx, http.MethodPatch, "/v1/sessions/"+url.PathEscape(s.ID), map[string]string{"name": name}, &info); err != nil {
		return Info{}, err
	}
	s.set(info)
	return info, nil
}

// Abort discards the session.
func (s *Session) Abort(ctx context.Context) (Info, error) {
	var info Info
	if err := s.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(s.ID)+"/abort", nil, &info); err != nil {
		return Info{}, err
	}
	s.set(info)
	return info, nil
}

// Delete removes the session and everything recorded for it (events, seal,
// bundle). Only a finished session can be deleted: stop or abort it first.
func (s *Session) Delete(ctx context.Context) error {
	return s.do(ctx, http.MethodDelete, "/v1/sessions/"+url.PathEscape(s.ID), nil, nil)
}

// Wait blocks until the session is SEALED (or otherwise terminal) or ctx ends.
// It returns an error if the terminal state is not SEALED.
func (s *Session) Wait(ctx context.Context) (Info, error) {
	for {
		info, err := s.Get(ctx)
		if err != nil {
			return info, err
		}
		if info.Done() {
			if info.State != "SEALED" {
				return info, fmt.Errorf("tsession: session %s ended in state %s", info.ID, info.State)
			}
			return info, nil
		}
		select {
		case <-ctx.Done():
			return info, ctx.Err()
		case <-time.After(s.poll):
		}
	}
}

// StopAndWait is Stop followed by Wait.
func (s *Session) StopAndWait(ctx context.Context) (Info, error) {
	if _, err := s.Stop(ctx); err != nil {
		return Info{}, err
	}
	return s.Wait(ctx)
}

// Header returns headers that tag a request as part of this session.
// Each call uses a fresh trace id.
func (s *Session) Header() http.Header {
	h := http.Header{}
	s.Inject(h)
	return h
}

// Inject adds x-tsession-id, x-tsession-traceparent and x-tsession-hlc to h.
// It also sets traceparent, unless h already has one from the caller's own
// tracing (see core.WriteTraceparent).
func (s *Session) Inject(h http.Header) {
	h.Set(core.KeySession, s.ID)
	core.WriteTraceparent(core.HTTPHeaderCarrier(h), "00-"+core.NewTraceID()+"-"+core.NewSpanID()+"-01",
		strconv.FormatUint(core.HLCNow(), 10))
}

// Transport wraps rt (default http.DefaultTransport) so every request it
// carries is tagged with this session's headers.
func (s *Session) Transport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		s.Inject(r.Header)
		return rt.RoundTrip(r)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (s *Session) set(i Info) {
	s.mu.Lock()
	s.info = i
	s.mu.Unlock()
}

// do calls the control plane.
func (s *Session) do(ctx context.Context, method, path string, in, out any) error {
	return s.call(ctx, s.base, method, path, in, out)
}

// call sends one JSON request to base+path and decodes the reply into out
// (skipped when out is nil). Non-2xx replies become errors carrying the
// server's message.
func (s *Session) call(ctx context.Context, base, method, path string, in, out any) error {
	_, err := s.callHeader(ctx, base, method, path, in, out)
	return err
}

// callHeader is call, and returns the headers of a 2xx reply as well.
func (s *Session) callHeader(ctx context.Context, base, method, path string, in, out any) (http.Header, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, scrubURL(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error              string   `json:"error"`
			ConflictingSession string   `json:"conflicting_session"`
			SessionID          string   `json:"session_id"`
			Binding            *Binding `json:"binding"`
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = json.Unmarshal(b, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(b))
		}
		if id := cmp.Or(e.ConflictingSession, e.SessionID); id != "" {
			e.Error += " (held by session " + id + ")"
		}
		if e.Binding != nil {
			e.Error += " binding " + e.Binding.Source + ":" + e.Binding.Name
		}
		if resp.StatusCode == http.StatusConflict && method == http.MethodPost && path == "/v1/sessions" {
			return nil, fmt.Errorf("%w: %s", ErrConflict, e.Error)
		}
		return nil, fmt.Errorf("tsession: %s %s: %s: %s", method, path, resp.Status, e.Error)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)) // let the connection be reused
		return resp.Header, nil
	}
	return resp.Header, json.NewDecoder(resp.Body).Decode(out)
}

// Subscriber is one process following the control plane's active set,
// normally the SDK inside an instrumented service. Service and Instance are
// empty for SDKs that do not report them.
type Subscriber struct {
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance,omitempty"`
	// DeliveryInstance is the instance name the process sends its delivery
	// reports under (the one its events carry); empty for an SDK that sends
	// none.
	DeliveryInstance string    `json:"delivery_instance,omitempty"`
	ConnectedAt      time.Time `json:"connected_at"`
	// LastDeliveryReport is when a delivery report of the process last
	// reached the control plane (it sends one when it connects); zero when
	// none has since it connected. A process that names a DeliveryInstance
	// and stays at zero cannot get its reports through.
	LastDeliveryReport time.Time `json:"last_delivery_report,omitzero"`
	// Generation is the newest version of the active set sent to it.
	Generation uint64 `json:"generation"`
}

// Subscribers lists the processes currently connected to the control plane.
// A service that is missing here cannot record anything.
func Subscribers(ctx context.Context, o Options) ([]Subscriber, error) {
	var out []Subscriber
	return out, newSession(o, Info{}).do(ctx, http.MethodGet, "/v1/subscribers", nil, &out)
}

// WaitPropagated blocks until every service connected to the control plane
// has been sent the current set of active sessions, and returns how many are
// connected. Call it after Start when the very first request must be
// recorded: a service only records a session once it has heard of it.
//
// "Sent" means written to the service's connection; the service applies it a
// moment later, so this narrows the window to network latency rather than
// closing it. It returns ctx's error when ctx ends first (a context without
// a deadline is given one of 30s), and (0, nil) at once against a control
// plane too old to report delivery. A count of 0 means no service is
// connected, so nothing was told about the session.
func (s *Session) WaitPropagated(ctx context.Context) (int, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		// A follower that never catches up must not hold a caller forever.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxPropagationWait)
		defer cancel()
	}
	connected := 0
	for {
		// Read the target again on every pass: a control plane that restarted
		// counts from zero, and waiting for the old number would never end.
		var active struct {
			Generation uint64 `json:"generation"`
		}
		if err := s.do(ctx, http.MethodGet, "/v1/active", nil, &active); err != nil {
			if ctx.Err() != nil {
				return connected, ctx.Err()
			}
			return connected, err
		}
		if active.Generation == 0 {
			return 0, nil
		}
		var subs []Subscriber
		if err := s.do(ctx, http.MethodGet, "/v1/subscribers", nil, &subs); err != nil {
			switch {
			case ctx.Err() != nil:
				return connected, ctx.Err()
			case strings.Contains(err.Error(), ": 404 "):
				return 0, nil
			}
			return connected, err
		}
		connected = len(subs)
		behind := false
		for _, sub := range subs {
			behind = behind || sub.Generation < active.Generation
		}
		if !behind {
			return connected, nil
		}
		select {
		case <-ctx.Done():
			return connected, ctx.Err()
		case <-time.After(min(s.poll, 20*time.Millisecond)):
		}
	}
}

// maxPropagationWait bounds WaitPropagated when its context has no deadline.
const maxPropagationWait = 30 * time.Second

// ListOptions selects the sessions ListPage returns.
type ListOptions struct {
	// State and Name keep only the sessions in that state and with exactly
	// that name; empty means any.
	State string
	Name  string
	// Limit is the page size, 1..1000: the newest matching sessions are
	// returned, newest first. Zero asks for every matching session, oldest
	// first, like List.
	Limit int
	// Before is the next value ListPage returned for the previous page.
	Before string
}

// ListPage returns one page of the sessions known to the control plane:
// the newest lo.Limit sessions matching lo, newest first, and next, the
// value to pass as lo.Before for the page after it (empty when there are no
// older sessions). Unlike List its cost does not grow with the number of
// sessions the control plane has ever recorded.
//
// Paging until next is empty returns no session twice, and every session
// that exists and matches during the whole run exactly once; a page can hold
// fewer sessions than lo.Limit while next is set.
//
// A control plane older than this call ignores Name, Limit and Before: it
// returns every session (in lo.State), oldest first, and next is empty. A
// caller that may talk to one should check the names it gets, and expect
// more than lo.Limit sessions.
func ListPage(ctx context.Context, o Options, lo ListOptions) (infos []Info, next string, err error) {
	q := url.Values{}
	if lo.State != "" {
		q.Set("state", lo.State)
	}
	if lo.Name != "" {
		q.Set("name", lo.Name)
	}
	if lo.Limit != 0 {
		q.Set("limit", strconv.Itoa(lo.Limit))
	}
	if lo.Before != "" {
		q.Set("before", lo.Before)
	}
	path := "/v1/sessions"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	s := newSession(o, Info{})
	h, err := s.callHeader(ctx, s.base, http.MethodGet, path, nil, &infos)
	if err != nil {
		return nil, "", err
	}
	return infos, h.Get("X-Tsession-Next"), nil
}

// List returns the sessions known to the control plane, oldest first,
// optionally filtered by state. It returns all of them, which can be many:
// see ListPage for the newest ones or for the sessions with a given name.
func List(ctx context.Context, o Options, state string) ([]Info, error) {
	s := newSession(o, Info{})
	path := "/v1/sessions"
	if state != "" {
		path += "?state=" + url.QueryEscape(state)
	}
	var out []Info
	if err := s.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SetKill turns the control plane's global kill switch on or off. While it
// is on, every connected SDK records nothing, for any session; sessions keep
// their state and record again once it is off. The switch is held in the
// control plane's memory: a restart turns it off.
func SetKill(ctx context.Context, o Options, on bool) error {
	return newSession(o, Info{}).do(ctx, http.MethodPost, "/v1/kill", map[string]bool{"kill": on}, nil)
}

// KillSwitch reports whether the global kill switch is on (see SetKill).
func KillSwitch(ctx context.Context, o Options) (bool, error) {
	var active struct {
		Kill bool `json:"kill"`
	}
	err := newSession(o, Info{}).do(ctx, http.MethodGet, "/v1/active", nil, &active)
	return active.Kill, err
}
