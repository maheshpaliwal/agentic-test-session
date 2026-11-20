package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func (m *Manager) hList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, m.List(State(r.URL.Query().Get("state"))))
}

func (m *Manager) hGet(w http.ResponseWriter, r *http.Request) {
	s, ok := m.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, ErrNotFound)
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
func identity(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return -1
		}
		return r
	}, s)
	if len(s) > 128 {
		s = s[:128]
	}
	return s
}

func (m *Manager) hStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
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
		fl.Flush()
		m.delivered(ch, sent)
	}
}
