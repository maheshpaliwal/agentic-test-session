package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Handler returns the REST + SSE API.
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", m.hStart)
	mux.HandleFunc("GET /v1/sessions", m.hList)
	mux.HandleFunc("GET /v1/sessions/{id}", m.hGet)
	mux.HandleFunc("PATCH /v1/sessions/{id}", m.hRename)
	mux.HandleFunc("DELETE /v1/sessions/{id}", m.hDelete)
	mux.HandleFunc("POST /v1/sessions/{id}/stop", m.transition(m.Stop))
	mux.HandleFunc("POST /v1/sessions/{id}/abort", m.transition(m.Abort))
	mux.HandleFunc("POST /v1/sessions/{id}/delivery", m.hDelivery)
	mux.HandleFunc("GET /v1/active", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, m.Active())
	})
	mux.HandleFunc("GET /v1/active/stream", m.hStream)
	mux.HandleFunc("GET /v1/subscribers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, m.Subscribers())
	})
	mux.HandleFunc("POST /v1/kill", m.hKill)
	mux.Handle("GET /metrics", m.tel.reg.Handler())
	guarded := core.LoopbackGuard(mux, core.GuardConfig{
		AllowedHosts:   m.cfg.AllowedHosts,
		AllowedOrigins: m.cfg.AllowedOrigins,
	})
	// /healthz is static and unauthenticated by design (container healthchecks
	// use the pod IP as Host); it exposes nothing and is outside the guard.
	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
	root.Handle("/", guarded)
	return root
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	var bc *BindingConflictError
	if errors.As(err, &bc) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "binding_conflict", "conflicting_session": bc.Session, "binding": bc.Binding,
		})
		return
	}
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrConflict), errors.Is(err, ErrBadState):
		code = http.StatusConflict
	case errors.Is(err, ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, ErrCollector):
		code = http.StatusBadGateway
	case errors.Is(err, ErrStore):
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (m *Manager) hStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, fmt.Errorf("%w: %v", ErrInvalid, err))
		return
	}
	s, err := m.Start(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, s)
}

// Response headers of GET /v1/sessions. The body stays a JSON array of
// sessions, so what a page needs besides them travels here.
const (
	// HeaderNext is the cursor of the next page (the "before" parameter),
	// present on a response to a request with "limit" when older sessions
	// exist.
	HeaderNext = "X-Tsession-Next"
	// HeaderTotal is the number of sessions matching the filters of a request
	// with "limit", whatever the page; absent when the store cannot tell.
	HeaderTotal = "X-Tsession-Total"
	// HeaderUnreadable is the number of sessions that belong in the response
	// and are missing from it because the store could not read them; absent
	// when there is none.
	HeaderUnreadable = "X-Tsession-Unreadable"
)

// hList serves GET /v1/sessions[?state=&name=&limit=&before=]. Without limit
// it answers as it always has: every session (matching state and name),
// oldest first.
func (m *Manager) hList(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	q := PageQuery{State: State(v.Get("state")), Name: v.Get("name"), Before: v.Get("before")}
	if v.Has("limit") {
		n, err := strconv.Atoi(v.Get("limit"))
		if err != nil || n < 1 || n > MaxPageLimit {
			writeErr(w, fmt.Errorf("%w: limit must be 1..%d", ErrInvalid, MaxPageLimit))
			return
		}
		q.Limit = n
	}
	p, err := m.ListPage(q)
	if err != nil {
		writeErr(w, err)
		return
	}
	h := w.Header()
	if q.Limit > 0 {
		if p.Next != "" {
			h.Set(HeaderNext, p.Next)
		}
		if p.Total >= 0 {
			h.Set(HeaderTotal, strconv.Itoa(p.Total))
		}
	}
	if p.Unreadable > 0 {
		h.Set(HeaderUnreadable, strconv.Itoa(p.Unreadable))
	}
	if p.Sessions == nil {
		p.Sessions = []Session{}
	}
	writeJSON(w, http.StatusOK, p.Sessions)
}

func (m *Manager) hGet(w http.ResponseWriter, r *http.Request) {
	s, err := m.stored(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (m *Manager) hRename(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name *string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.Name == nil {
		writeErr(w, fmt.Errorf("%w: body must be {\"name\":\"...\"}", ErrInvalid))
		return
	}
	s, err := m.Rename(r.PathValue("id"), *body.Name)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

func (m *Manager) hDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := m.Delete(r.Context(), id); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": id})
}

func (m *Manager) transition(f func(string) (Session, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s, err := f(r.PathValue("id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, s)
	}
}

func (m *Manager) hKill(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Kill bool `json:"kill"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&body); err != nil {
		writeErr(w, fmt.Errorf("%w: %v", ErrInvalid, err))
		return
	}
	m.SetKill(body.Kill)
	writeJSON(w, http.StatusOK, m.Active())
}

// identity cleans a self-reported service or instance name: printable ASCII
// only, at most 128 bytes. It is shown to operators, never trusted.
func identity(s string) string { return identityN(s, 128) }

// identityN is identity with another length limit.
func identityN(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, s)
	if len(s) > limit {
		s = s[:limit]
	}
	return s
}

func (m *Manager) hStream(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	ch, cancel := m.subscribe(identity(r.Header.Get(core.HeaderService)), identity(r.Header.Get(core.HeaderInstance)), true)
	defer cancel()
	sent := uint64(0) // generation written but not yet flushed
	hb := time.NewTicker(m.cfg.HeartbeatPeriod)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-m.rootCtx.Done():
			return
		case a := <-ch:
			b, _ := json.Marshal(a)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return // the connection is gone: nothing was delivered
			}
			sent = a.Generation
		case <-hb.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
		}
		// Flush through the controller: unlike Flusher.Flush it reports a
		// connection that is gone, and that must not count as a delivery.
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		m.delivered(ch, sent)
	}
}
