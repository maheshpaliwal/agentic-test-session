package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"tsession/core"
)

// Session mirrors the control-plane session JSON (docs/PROTOCOL.md).
type Session struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Mode            string `json:"mode"`
	State           string `json:"state"`
	InitiatedBy     string `json:"initiated_by"`
	CreatedAt       string `json:"created_at"`
	StartedAt       string `json:"started_at"`
	EndedAt         string `json:"ended_at"`
	TTLSeconds      int    `json:"ttl_seconds"`
	QuiesceSeconds  int    `json:"quiesce_seconds"`
	MaxDrainSeconds int    `json:"max_drain_seconds"`
	Incomplete      bool   `json:"incomplete"`
	DroppedEvents   int    `json:"dropped_events"`
	BundleURI       string `json:"bundle_uri"`
}

type demoSess struct {
	Session
	events  []core.Event
	live    bool // events are revealed progressively while ACTIVE
	started time.Time
	stopAt  time.Time
	sealed  bool // static terminal session
}

// Demo serves fake control-plane and collector APIs backed by synthetic data.
type Demo struct {
	mu   sync.Mutex
	sess []*demoSess
	seq  int
	now  func() time.Time
}

const liveStep = 1500 * time.Millisecond

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// NewDemo builds the demo world: one sealed rich session, one live session
// that keeps "receiving" events, and one sealed incomplete session.
func NewDemo() *Demo { return newDemoAt(time.Now) }

func newDemoAt(now func() time.Time) *Demo {
	d := &Demo{now: now}
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
		Session: Session{ID: "ts_demo0002", Name: "live checkout (events streaming in)", Mode: "exclusive", State: "ACTIVE",
			InitiatedBy: "dashboard", CreatedAt: ts(base2), StartedAt: ts(base2), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60},
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
			TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60, Incomplete: true, DroppedEvents: 12},
		events: part, sealed: true})
	d.seq = 3
	return d
}

// view returns a snapshot of the session with state and visible events resolved.
func (d *Demo) view(s *demoSess) (Session, []core.Event) {
	now := d.now()
	sess := s.Session
	evs := s.events
	if !s.sealed {
		if !s.stopAt.IsZero() {
			switch {
			case now.Before(s.stopAt.Add(2 * time.Second)):
				sess.State = "DRAINING"
			default:
				sess.State = "SEALED"
				sess.EndedAt = ts(s.stopAt.Add(2 * time.Second))
			}
		}
		if s.live && sess.State == "ACTIVE" {
			n := 3 + int(now.Sub(s.started)/liveStep)
			if n < len(evs) {
				evs = evs[:n]
			}
		}
	}
	return sess, resolveTx(evs)
}

func (d *Demo) find(id string) *demoSess {
	for _, s := range d.sess {
		if s.ID == id {
			return s
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func notFound(w http.ResponseWriter) { writeJSON(w, 404, m{"error": "not found"}) }

// ControlPlane returns the fake control-plane API (paths without /api/cp).
func (d *Demo) ControlPlane() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		want := r.URL.Query().Get("state")
		out := []Session{}
		for i := len(d.sess) - 1; i >= 0; i-- {
			s, _ := d.view(d.sess[i])
			if want == "" || s.State == want {
				out = append(out, s)
			}
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Name        string `json:"name"`
			Mode        string `json:"mode"`
			InitiatedBy string `json:"initiated_by"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil {
			writeJSON(w, 400, m{"error": "bad json"})
			return
		}
		if in.Mode == "" {
			in.Mode = "header"
		}
		if in.Mode != "header" && in.Mode != "exclusive" {
			writeJSON(w, 400, m{"error": "mode must be header or exclusive"})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if in.Mode == "exclusive" {
			for _, s := range d.sess {
				if v, _ := d.view(s); v.Mode == "exclusive" && (v.State == "ACTIVE" || v.State == "DRAINING") {
					writeJSON(w, 409, m{"error": "exclusive session " + v.ID + " is " + v.State})
					return
				}
			}
		}
		d.seq++
		now := d.now().UTC()
		ns := &demoSess{Session: Session{ID: fmt.Sprintf("ts_demo%04d", d.seq), Name: in.Name, Mode: in.Mode, State: "ACTIVE",
			InitiatedBy: "dashboard", CreatedAt: ts(now), StartedAt: ts(now), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60}}
		d.sess = append(d.sess, ns)
		writeJSON(w, 201, ns.Session)
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
	stop := func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return
		}
		if v, _ := d.view(s); v.State != "ACTIVE" {
			writeJSON(w, 409, m{"error": "session is " + v.State})
			return
		}
		s.stopAt = d.now()
		if r.URL.Path[len(r.URL.Path)-5:] == "abort" {
			s.State = "ABORTED"
			s.sealed = true
			s.EndedAt = ts(s.stopAt)
		}
		v, _ := d.view(s)
		writeJSON(w, 200, v)
	}
	mux.HandleFunc("POST /v1/sessions/{id}/stop", stop)
	mux.HandleFunc("POST /v1/sessions/{id}/abort", stop)
	mux.HandleFunc("GET /v1/active", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		act, excl := []string{}, ""
		for _, s := range d.sess {
			if v, _ := d.view(s); v.State == "ACTIVE" || v.State == "DRAINING" {
				act = append(act, v.ID)
				if v.Mode == "exclusive" {
					excl = v.ID
				}
			}
		}
		writeJSON(w, 200, m{"active": act, "exclusive": excl, "kill": false})
	})
	return mux
}

// Collector returns the fake collector read API (paths without /api/col).
func (d *Demo) Collector() http.Handler {
	mux := http.NewServeMux()
	get := func(w http.ResponseWriter, r *http.Request) (Session, []core.Event, bool) {
		d.mu.Lock()
		defer d.mu.Unlock()
		s := d.find(r.PathValue("id"))
		if s == nil {
			notFound(w)
			return Session{}, nil, false
		}
		v, evs := d.view(s)
		return v, evs, true
	}
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		defer d.mu.Unlock()
		out := []m{}
		for _, s := range d.sess {
			v, evs := d.view(s)
			e := m{"id": v.ID, "event_count": len(evs), "sealed": v.State == "SEALED" || v.State == "ABORTED"}
			if len(evs) > 0 {
				e["first_ns"], e["last_ns"] = evs[0].StartNs, evs[len(evs)-1].EndNs
			}
			out = append(out, e)
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		v, evs, ok := get(w, r)
		if !ok {
			return
		}
		from, _ := strconv.Atoi(r.URL.Query().Get("from_seq"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 5000 {
			limit = 1000
		}
		out := []core.Event{}
		next := uint64(from)
		for _, e := range evs { // events with Seq > from_seq
			if int(e.Seq) > from && len(out) < limit {
				out = append(out, e)
				next = e.Seq
			}
		}
		writeJSON(w, 200, m{"events": out, "next_seq": next, "sealed": v.State == "SEALED" || v.State == "ABORTED"})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		if _, evs, ok := get(w, r); ok {
			writeJSON(w, 200, m{"session_id": r.PathValue("id"), "roots": BuildTrace(evs)})
		}
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities", func(w http.ResponseWriter, r *http.Request) {
		if _, evs, ok := get(w, r); ok {
			writeJSON(w, 200, entityList(evs))
		}
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities/{store}/{entity}/{pk...}", func(w http.ResponseWriter, r *http.Request) {
		if _, evs, ok := get(w, r); ok {
			pk, _ := url.PathUnescape(r.PathValue("pk"))
			writeJSON(w, 200, entityHistory(evs, r.PathValue("store"), r.PathValue("entity"), pk))
		}
	})
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		if v, evs, ok := get(w, r); ok {
			writeJSON(w, 200, summarize(evs, v))
		}
	})
	return mux
}
