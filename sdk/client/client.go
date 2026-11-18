// Package client is a Go client for the tsession control plane. Test code
// uses it to start a session, tag outgoing requests, and wait for the seal.
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
var ErrConflict = errors.New("tsession: exclusive session already running")

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
	SealError string    `json:"seal_error,omitempty"`
	Bindings  []Binding `json:"bindings,omitempty"`
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

// Inject adds x-tsession-id, traceparent and x-tsession-hlc to h.
func (s *Session) Inject(h http.Header) {
	h.Set(core.KeySession, s.ID)
	h.Set(core.KeyTraceparent, "00-"+core.NewTraceID()+"-"+core.NewSpanID()+"-01")
	h.Set(core.KeyHLC, strconv.FormatUint(core.HLCNow(), 10))
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
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
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
			return fmt.Errorf("%w: %s", ErrConflict, e.Error)
		}
		return fmt.Errorf("tsession: %s %s: %s: %s", method, path, resp.Status, e.Error)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)) // let the connection be reused
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// List returns sessions known to the control plane, optionally filtered by state.
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
