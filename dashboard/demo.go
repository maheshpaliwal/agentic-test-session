package dashboard

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Session is the control plane's session, in its wire form: the dashboard's
// own backends answer with the very type the control plane encodes.
type Session = controlplane.Session

// Binding is one attribute binding of a session. jwt_claim values are read from
// an UNVERIFIED JWT payload: traffic can be spoofed.
type Binding = controlplane.Binding

func validBindings(bs []Binding) string {
	if len(bs) > 8 {
		return "at most 8 bindings"
	}
	for _, b := range bs {
		switch b.Source {
		case "header", "jwt_claim", "cookie", "query":
		default:
			return "binding source must be header, jwt_claim, cookie or query"
		}
		if b.Name == "" || len(b.Name) > 64 || b.Value == "" || len(b.Value) > 256 {
			return "binding name (1-64 bytes) and value (1-256 bytes) are required"
		}
	}
	return ""
}

func bindingKey(b Binding) string {
	n := b.Name
	if b.Source == "header" {
		n = strings.ToLower(n)
	}
	return b.Source + "\x00" + n + "\x00" + b.Value
}

type demoSess struct {
	Session
	// events are the session's events: as the services sent them (demo) or
	// as the collector served them when the file was written (file).
	events  []core.Event
	live    bool // events are revealed progressively while ACTIVE
	started time.Time
	stopAt  time.Time
	sealed  bool // static terminal session
	// file is set for a session read from an export: the collector's view of
	// it, computed once (its events already carry transaction outcomes and
	// flags, and are served as they are). path is the file it came from.
	file *colView
	path string
}

// Demo serves the control-plane and collector APIs from memory: synthetic
// sessions (demo mode) or sessions read from exported files. Every read
// endpoint answers in the shape of the real service (docs/PROTOCOL.md).
type Demo struct {
	mu   sync.Mutex
	sess []*demoSess
	seq  int
	now  func() time.Time
	// gen stands in for the control plane's ActiveSet.Generation: it counts
	// the changes to the set of running sessions.
	gen uint64
	// readOnly refuses every change (sessions opened from files).
	readOnly bool
}

const liveStep = 1500 * time.Millisecond

// ts is t as the demo's sessions carry their times: UTC, whole seconds.
func ts(t time.Time) time.Time { return t.UTC().Truncate(time.Second) }

// NewDemo builds the demo world: one sealed rich session, one live session
// that keeps "receiving" events, and one sealed incomplete session.
func NewDemo() *Demo { return newDemoAt(time.Now) }

func newDemoAt(now func() time.Time) *Demo {
	d := &Demo{now: now, gen: 1}
	t := now().UTC().Truncate(time.Second)

	base1 := t.Add(-20 * time.Minute)
	full := demoEvents("ts_demo0001", base1)
	last := time.Unix(0, full[len(full)-1].EndNs)
	d.sess = append(d.sess, &demoSess{
		Session: Session{ID: "ts_demo0001", Name: "checkout happy path + failures", Mode: "header", State: "SEALED",
			InitiatedBy: "dashboard", CreatedAt: ts(base1.Add(-time.Second)), StartedAt: ts(base1),
			EndedAt: ts(last.Add(4 * time.Second)), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60,
			BundleURI: "file:///var/tsession/bundles/ts_demo0001.json"},
		events: full, sealed: true})

	base2 := t
	d.sess = append(d.sess, &demoSess{
		Session: Session{ID: "ts_demo0002", Name: "dashboard " + base2.Format("15:04:05"), Mode: "exclusive", State: "ACTIVE",
			NameAuto: true, InitiatedBy: "dashboard", CreatedAt: ts(base2), StartedAt: ts(base2), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60},
		events: demoEvents("ts_demo0002", base2), live: true, started: base2})

	base3 := t.Add(-2 * time.Hour)
	part := demoEvents("ts_demo0003", base3)[:14]
	drop := core.Event{SessionID: "ts_demo0003", TraceID: "tr_demo01", SpanID: "sp_dropped", Service: "orders-api", Instance: "orders-api-1",
		Kind: core.KindSDKDropped, Name: "sdk.dropped", LocalSeq: 999, Seq: uint64(len(part) + 1),
		StartNs: part[len(part)-1].EndNs + 1e6, EndNs: part[len(part)-1].EndNs + 1e6, Attrs: map[string]string{"count": "12"}, Flags: []string{"unattributed"}}
	drop.HLC, drop.EndHLC = uint64(drop.StartNs), uint64(drop.EndNs)
	part = append(part, drop)
	d.sess = append(d.sess, &demoSess{
		Session: Session{ID: "ts_demo0003", Name: "nightly regression (buffer overflow)", Mode: "header", State: "SEALED",
			InitiatedBy: "api", CreatedAt: ts(base3), StartedAt: ts(base3), EndedAt: ts(time.Unix(0, drop.EndNs).Add(4 * time.Second)),
			TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60, Incomplete: true, DroppedEvents: 12,
			Reason: "ttl", SealError: "collector seal: synthetic timeout after 3 attempts"},
		events: part, sealed: true})
	base4 := t.Add(-time.Minute)
	d.sess = append(d.sess, &demoSess{
		Session: Session{ID: "ts_demo0004", Name: "alice's Android session (bound by JWT sub)", Mode: "header", State: "ACTIVE",
			InitiatedBy: "dashboard", CreatedAt: ts(base4), StartedAt: ts(base4), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60,
			Bindings: []Binding{{Source: "jwt_claim", Name: "sub", Value: "user_synth_alice"}, {Source: "header", Name: "x-device-id", Value: "dev_synth_001"}}},
		events: demoEvents("ts_demo0004", base4), live: true, started: base4})
	base5 := t.Add(-8 * time.Minute)
	act := activityEvents("ts_demo0005", base5)
	d.sess = append(d.sess, &demoSess{
		Session: Session{ID: "ts_demo0005", Name: "multi-trigger activity flow", Mode: "header", State: "SEALED",
			InitiatedBy: "dashboard", CreatedAt: ts(base5.Add(-time.Second)), StartedAt: ts(base5),
			EndedAt: ts(time.Unix(0, act[len(act)-1].EndNs).Add(4 * time.Second)), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60},
		events: act, sealed: true})
	d.seq = 5
	return d
}

// session returns s as the control plane would report it now, and the events
// recorded for it so far.
func (d *Demo) session(s *demoSess) (Session, []core.Event) {
	now := d.now()
	sess := s.Session
	evs := s.events
	if !s.sealed {
		if !s.stopAt.IsZero() {
			switch {
			case now.Before(s.stopAt.Add(2 * time.Second)):
				sess.State = controlplane.StateDraining
			default:
				sess.State = controlplane.StateSealed
				sess.EndedAt = ts(s.stopAt.Add(2 * time.Second))
			}
		}
		if s.live && sess.State == controlplane.StateActive {
			n := 3 + int(now.Sub(s.started)/liveStep)
			if n < len(evs) {
				evs = evs[:n]
			}
		}
	}
	return sess, evs
}

// view returns a snapshot of the session: the control plane's record of it
// and the collector's view of its events.
func (d *Demo) view(s *demoSess) (Session, colView) {
	sess, evs := d.session(s)
	if s.file != nil {
		return sess, *s.file
	}
	// The collector holds a seal for a session that was stopped and drained;
	// an aborted one is never sealed.
	cv := newColView(sess.ID, evs, sess.State == controlplane.StateSealed)
	if cv.sealed {
		// What the control plane copies from the seal.
		sess.Incomplete = sess.Incomplete || cv.incomplete
		sess.DroppedEvents = max(sess.DroppedEvents, cv.dropped)
	}
	return sess, cv
}

func (d *Demo) find(id string) *demoSess {
	for _, s := range d.sess {
		if s.ID == id {
			return s
		}
	}
	return nil
}

// Recorded returns the events of session id as its services sent them, before
// the collector resolves transaction outcomes and orphans on a read; nil for
// an unknown session. It exists so that a test can hand the demo's events to
// a real collector and compare the two.
func (d *Demo) Recorded(id string) []core.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.find(id)
	if s == nil {
		return nil
	}
	_, evs := d.session(s)
	return slices.Clone(evs)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeCPErr answers with the status and body the control plane gives err.
func writeCPErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, controlplane.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, controlplane.ErrConflict), errors.Is(err, controlplane.ErrBadState):
		code = http.StatusConflict
	case errors.Is(err, controlplane.ErrInvalid):
		code = http.StatusBadRequest
	}
	writeJSON(w, code, m{"error": err.Error()})
}

func notFound(w http.ResponseWriter) { writeCPErr(w, controlplane.ErrNotFound) }

// healthz answers a probe of the service behind one of the two prefixes
// (`tracectl doctor` asks both).
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}

// ControlPlane returns the control-plane API (paths without /api/cp).
func (d *Demo) ControlPlane() http.Handler {
	if d.readOnly {
		return d.readOnlyAPI(d.controlPlane())
	}
	return d.controlPlane()
}

func (d *Demo) controlPlane() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	// The list is the control plane's (docs/PROTOCOL.md): state, name, limit
	// and before; every session oldest first without a limit, and with one
	// the newest first plus the X-Tsession-Next and X-Tsession-Total headers.
	// The sessions are put into the control plane's own in-memory store, so
	// the order, the cursor and the errors are its own.
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		q := controlplane.PageQuery{State: controlplane.State(v.Get("state")), Name: v.Get("name"), Before: v.Get("before")}
		if v.Has("limit") {
			n, err := strconv.Atoi(v.Get("limit"))
			if err != nil || n < 1 || n > controlplane.MaxPageLimit {
				writeCPErr(w, fmt.Errorf("%w: limit must be 1..%d", controlplane.ErrInvalid, controlplane.MaxPageLimit))
				return
			}
			q.Limit = n
		}
		st := controlplane.NewMemStore()
		d.mu.Lock()
		for _, s := range d.sess {
			sess, _ := d.view(s)
			_ = st.Put(sess)
		}
		d.mu.Unlock()
		p, err := st.ListPage(q)
		if err != nil {
			writeCPErr(w, err)
			return
		}
		if q.Limit > 0 {
			if p.Next != "" {
				w.Header().Set(controlplane.HeaderNext, p.Next)
			}
			if p.Total >= 0 {
				w.Header().Set(controlplane.HeaderTotal, strconv.Itoa(p.Total))
			}
		}
		if p.Sessions == nil {
			p.Sessions = []Session{}
		}
		writeJSON(w, 200, p.Sessions)
	})
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Name        string    `json:"name"`
			Mode        string    `json:"mode"`
			InitiatedBy string    `json:"initiated_by"`
			Bindings    []Binding `json:"bindings"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil {
			writeJSON(w, 400, m{"error": "bad json"})
			return
		}
		in.Mode = cmp.Or(in.Mode, "header")
		if in.Mode != "header" && in.Mode != "exclusive" {
			writeJSON(w, 400, m{"error": "mode must be header or exclusive"})
			return
		}
		if msg := validBindings(in.Bindings); msg != "" {
			writeJSON(w, 400, m{"error": msg})
			return
		}
		if len(in.Bindings) > 0 && in.Mode == "exclusive" {
			writeJSON(w, 400, m{"error": "bindings and exclusive mode are mutually exclusive"})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, s := range d.sess {
			v, _ := d.session(s)
			if v.State != controlplane.StateActive {
				continue
			}
			for _, have := range v.Bindings {
				for _, want := range in.Bindings {
					if bindingKey(have) == bindingKey(want) {
						writeJSON(w, 409, m{"error": "binding " + want.Source + ":" + want.Name + " is already held by session " + v.ID, "session_id": v.ID})
						return
					}
				}
			}
		}
		if in.Mode == "exclusive" {
			for _, s := range d.sess {
				if v, _ := d.session(s); v.Mode == controlplane.ModeExclusive && (v.State == controlplane.StateActive || v.State == controlplane.StateDraining) {
					writeJSON(w, 409, m{"error": "exclusive session " + v.ID + " is " + string(v.State)})
					return
				}
			}
		}
		d.seq++
		d.gen++
		now := ts(d.now())
		ns := &demoSess{Session: Session{ID: fmt.Sprintf("ts_demo%04d", d.seq), Name: in.Name, Mode: controlplane.Mode(in.Mode), State: controlplane.StateActive,
			InitiatedBy: "dashboard", CreatedAt: now, StartedAt: now, TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60, Bindings: in.Bindings}}
		d.sess = append(d.sess, ns)
		writeJSON(w, 201, ns.Session)
	})
	mux.HandleFunc("PATCH /v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil {
			writeJSON(w, 400, m{"error": "bad json"})
			return
		}
		name := strings.TrimSpace(in.Name)
		if name == "" || len([]rune(name)) > 120 {
			writeJSON(w, 400, m{"error": "name must be 1-120 characters"})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return
		}
		s.Name, s.NameAuto = name, false
		v, _ := d.view(s)
		writeJSON(w, 200, v)
	})
	mux.HandleFunc("GET /v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return
		}
		v, _ := d.view(s)
		writeJSON(w, 200, v)
	})
	mux.HandleFunc("DELETE /v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return
		}
		v, _ := d.session(s)
		if !v.State.Terminal() { // same rule as the control plane: only finished sessions
			writeJSON(w, 409, m{"error": "invalid state transition: session is " + string(v.State) + "; only finished sessions can be deleted"})
			return
		}
		d.sess = slices.DeleteFunc(d.sess, func(x *demoSess) bool { return x == s })
		writeJSON(w, 200, m{"deleted": true, "id": v.ID})
	})
	stop := func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return
		}
		if v, _ := d.session(s); v.State != controlplane.StateActive {
			writeJSON(w, 409, m{"error": "session is " + string(v.State)})
			return
		}
		s.stopAt = d.now()
		d.gen++
		if strings.HasSuffix(r.URL.Path, "/abort") {
			s.State = controlplane.StateAborted
			s.sealed = true
			s.EndedAt = ts(s.stopAt)
		}
		v, _ := d.view(s)
		writeJSON(w, 200, v)
	}
	mux.HandleFunc("POST /v1/sessions/{id}/stop", stop)
	mux.HandleFunc("POST /v1/sessions/{id}/abort", stop)
	// Like the proxy, the demo does not expose the global kill switch.
	mux.HandleFunc("POST /v1/kill", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusForbidden, m{"error": "endpoint not exposed by dashboard"})
	})
	mux.HandleFunc("GET /v1/subscribers", func(w http.ResponseWriter, r *http.Request) {
		if d.readOnly { // sessions opened from files have no services behind them
			writeJSON(w, 200, []controlplane.Subscriber{})
			return
		}
		d.mu.Lock()
		gen := d.gen
		d.mu.Unlock()
		since := ts(d.now().Add(-10 * time.Minute))
		writeJSON(w, 200, []controlplane.Subscriber{
			{Service: "orders-api", Instance: "orders-api-1", ConnectedAt: since, Generation: gen},
			{Service: "orders-worker", Instance: "orders-worker-1", ConnectedAt: since, Generation: gen},
			{Service: "shipping-api", Instance: "shipping-api-1", ConnectedAt: since, Generation: gen},
		})
	})
	mux.HandleFunc("GET /v1/active", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		a := controlplane.ActiveSet{Active: []string{}, Generation: d.gen}
		for _, s := range d.sess {
			if v, _ := d.session(s); v.State == controlplane.StateActive || v.State == controlplane.StateDraining {
				a.Active = append(a.Active, v.ID)
				if v.Mode == controlplane.ModeExclusive {
					a.Exclusive = v.ID
				}
			}
		}
		slices.Sort(a.Active)
		for _, id := range a.Active {
			if v, _ := d.session(d.find(id)); v.State == controlplane.StateActive {
				for _, b := range v.Bindings {
					a.Bindings = append(a.Bindings, controlplane.ActiveBinding{SessionID: id, Binding: b})
				}
			}
		}
		writeJSON(w, 200, a)
	})
	return mux
}

// The by-id contract of the collector's GET /v1/sessions.
const (
	headerUnanswered = "X-Tsession-Unanswered"
	maxListIDs       = 1000
)

var demoSessionID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$`)

// demoListIDs reads the ids parameter the way the collector does: comma
// separated, empty elements skipped, repeats counted once, at most
// maxListIDs valid session ids.
func demoListIDs(vals []string) ([]string, error) {
	var ids []string
	for _, v := range vals {
		for id := range strings.SplitSeq(v, ",") {
			if id == "" || slices.Contains(ids, id) {
				continue
			}
			if !demoSessionID.MatchString(id) {
				return nil, errors.New("ids: invalid session id")
			}
			if len(ids) == maxListIDs {
				return nil, fmt.Errorf("ids: more than %d session ids", maxListIDs)
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// Collector returns the collector's read API (paths without /api/col). Like
// the collector it answers for any session id: one it does not hold reads as
// a session with no events.
func (d *Demo) Collector() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	get := func(r *http.Request) colView {
		d.mu.Lock()
		defer d.mu.Unlock()
		id := r.PathValue("id")
		if s := d.find(id); s != nil {
			_, cv := d.view(s)
			return cv
		}
		return colView{id: id}
	}
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			// r.URL.Query() would drop what it cannot parse, and a damaged ids
			// parameter would then be answered with every session.
			writeJSON(w, 400, m{"error": "query: " + err.Error()})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if raw, ok := q["ids"]; ok {
			// By id (docs/PROTOCOL.md): the asked sessions that have events, in
			// the order asked, without first_ns and last_ns.
			ids, err := demoListIDs(raw)
			if err != nil {
				writeJSON(w, 400, m{"error": err.Error()})
				return
			}
			out := []sessionCount{}
			for _, id := range ids {
				if s := d.find(id); s != nil {
					if _, cv := d.view(s); len(cv.evs) > 0 {
						out = append(out, sessionCount{ID: id, EventCount: len(cv.evs), Sealed: cv.sealed})
					}
				}
			}
			w.Header().Set(headerUnanswered, "0")
			writeJSON(w, 200, out)
			return
		}
		// Every session that has events, by id.
		out := []sessionInfo{}
		for _, s := range d.sess {
			if _, cv := d.view(s); len(cv.evs) > 0 {
				out = append(out, cv.info())
			}
		}
		slices.SortFunc(out, func(a, b sessionInfo) int { return cmp.Compare(a.ID, b.ID) })
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, get(r).page(r.URL.Query().Get("from_seq"), r.URL.Query().Get("limit")))
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		cv := get(r)
		writeJSON(w, 200, buildTrace(cv.id, cv.evs))
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, listEntities(get(r).evs))
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, entityHistory(get(r).evs, r.PathValue("store"), r.PathValue("entity"), r.PathValue("pk")))
	})
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, get(r).summary())
	})
	if d.readOnly {
		return d.readOnlyAPI(mux)
	}
	return mux
}
