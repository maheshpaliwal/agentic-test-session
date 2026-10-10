package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"tsession/collector/store"
	"tsession/core"
)

// Handler returns the collector HTTP API (ingest, read and internal endpoints).
func (c *Collector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/ingest", c.handleIngest)
	mux.HandleFunc("GET /v1/sessions", c.handleSessions)
	mux.HandleFunc("GET /v1/sessions/{id}/events", c.handleEvents)
	mux.HandleFunc("GET /v1/sessions/{id}/trace", c.handleTrace)
	mux.HandleFunc("GET /v1/sessions/{id}/entities", c.handleEntities)
	mux.HandleFunc("GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}", c.handleHistory)
	mux.HandleFunc("GET /v1/sessions/{id}/summary", c.handleSummary)
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", c.handleDrain)
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", c.handleSeal)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (c *Collector) handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, c.cfg.MaxBodyBytes)
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 0, min(64<<10, c.cfg.MaxLineBytes)), c.cfg.MaxLineBytes)
	var evs []core.Event
	bad := 0
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e core.Event
		if err := json.Unmarshal(line, &e); err != nil {
			bad++
			continue
		}
		evs = append(evs, e)
	}
	if err := sc.Err(); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) || errors.Is(err, bufio.ErrTooLong) {
			writeErr(w, http.StatusRequestEntityTooLarge, errors.New("payload too large"))
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := c.Ingest(r.Context(), r.Header.Get("X-TSession-Instance"), evs)
	res.Rejected += bad
	if err != nil {
		c.log.Error("ingest", "err", err)
		writeErr(w, http.StatusInternalServerError, errors.New("storage error"))
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (c *Collector) handleSessions(w http.ResponseWriter, r *http.Request) {
	l, err := c.st.ListSessions(r.Context())
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if l == nil {
		l = []store.SessionInfo{}
	}
	writeJSON(w, 200, l)
}

// view loads a session's events with tx outcomes and orphan flags applied.
func (c *Collector) view(ctx context.Context, id string) ([]core.Event, error) {
	evs, err := c.st.Events(ctx, id, 0, 0)
	if err != nil {
		return nil, err
	}
	return c.resolveView(ctx, id, evs, nil)
}

func (c *Collector) resolveView(ctx context.Context, id string, evs []core.Event, outcomes map[string]bool) ([]core.Event, error) {
	si, err := c.st.GetSeal(ctx, id)
	if err != nil {
		return nil, err
	}
	if outcomes == nil {
		if outcomes, err = c.st.TxOutcomes(ctx, id); err != nil {
			return nil, err
		}
	}
	var orph map[string]bool
	if si != nil {
		orph = setOf(si.Orphaned)
	}
	return resolve(evs, outcomes, si != nil, orph), nil
}

func (c *Collector) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	from, _ := strconv.ParseUint(r.URL.Query().Get("from_seq"), 10, 64)
	if from < 1 {
		from = 1
	}
	limit := 1000
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = min(v, 10000)
	}
	evs, err := c.st.Events(r.Context(), id, from, limit)
	if err == nil {
		evs, err = c.resolveView(r.Context(), id, evs, nil)
	}
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	next := from
	if n := len(evs); n > 0 {
		next = evs[n-1].Seq + 1
	}
	si, _ := c.st.GetSeal(r.Context(), id)
	if evs == nil {
		evs = []core.Event{}
	}
	writeJSON(w, 200, map[string]any{"events": evs, "next_seq": next, "sealed": si != nil})
}

func (c *Collector) handleTrace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	evs, err := c.view(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, BuildTrace(id, evs))
}

func (c *Collector) handleEntities(w http.ResponseWriter, r *http.Request) {
	evs, err := c.view(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, ListEntities(evs))
}

func (c *Collector) handleHistory(w http.ResponseWriter, r *http.Request) {
	evs, err := c.view(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, History(evs, r.PathValue("store"), r.PathValue("entity"), r.PathValue("pk")))
}

func (c *Collector) handleSummary(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	evs, err := c.view(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	s, err := c.summary(r.Context(), id, evs)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, s)
}

func (c *Collector) handleDrain(w http.ResponseWriter, r *http.Request) {
	ds, err := c.DrainStatus(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, ds)
}

func (c *Collector) handleSeal(w http.ResponseWriter, r *http.Request) {
	res, err := c.Seal(r.Context(), r.PathValue("id"))
	if errors.Is(err, errBadID) {
		writeErr(w, 400, err)
		return
	}
	if err != nil {
		c.log.Error("seal", "err", err)
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, res)
}
