package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto"
)

// Handler returns the collector HTTP API (ingest, read and internal
// endpoints) behind core.LoopbackGuard (Host/Origin/Content-Type checks).
// /v1/internal/* additionally requires a loopback peer or one inside
// Config.InternalAllow.
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
	mux.HandleFunc("DELETE /v1/internal/sessions/{id}", c.handleDelete)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/internal/") && !c.internalPeerOK(r) {
			writeErr(w, http.StatusForbidden, errors.New("internal endpoint: peer not allowed"))
			return
		}
		mux.ServeHTTP(w, r)
	})
	guarded := core.LoopbackGuard(h, core.GuardConfig{
		AllowedHosts:   c.cfg.AllowedHosts,
		AllowedOrigins: c.cfg.AllowedOrigins,
		// Ingest is the only collector POST with a body (seal sends none), so
		// protobuf is the only allowed type; the 415 keeps the JSON error shape.
		BodyContentTypes: []string{tsessionproto.ContentType},
		OnBadContentType: func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusUnsupportedMediaType, errBadContentType)
		},
	})
	// /healthz is exempt from the Host guard (GET only; reveals nothing) so
	// container probes using service names work.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("ok"))
			return
		}
		guarded.ServeHTTP(w, r)
	})
}

func (c *Collector) internalPeerOK(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	for _, n := range c.internal {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// errBadContentType is the 415 message for ingest bodies that are not framed
// protobuf (NDJSON ingest was removed).
var errBadContentType = errors.New("unsupported Content-Type: POST /v1/ingest requires " + tsessionproto.ContentType)

// decodeProtoBatch reads a stream of length-delimited Event messages. A bad
// message is rejected alone; broken framing ends the stream with one
// invalid_proto, keeping the events decoded so far.
func decodeProtoBatch(body io.Reader, maxItem int, res *IngestResult) ([]core.Event, error) {
	dec := tsessionproto.NewDecoder(body, maxItem)
	var evs []core.Event
	for {
		e, err := dec.Next()
		switch {
		case err == nil:
			evs = append(evs, e)
		case err == io.EOF:
			return evs, nil
		case errors.Is(err, tsessionproto.ErrOversize):
			res.reject("oversize_message", 1)
		case errors.Is(err, tsessionproto.ErrInvalid):
			res.reject("invalid_proto", 1)
		case errors.Is(err, tsessionproto.ErrCorrupt):
			res.reject("invalid_proto", 1)
			return evs, nil
		default:
			return evs, err
		}
	}
}

func (c *Collector) handleIngest(w http.ResponseWriter, r *http.Request) {
	// The guard only vets bodies; an empty request must be refused too.
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != tsessionproto.ContentType {
		writeErr(w, http.StatusUnsupportedMediaType, errBadContentType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, c.cfg.MaxBodyBytes)
	var res IngestResult
	evs, err := decodeProtoBatch(r.Body, c.cfg.MaxMessageBytes, &res)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, errors.New("payload too large"))
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	ir, err := c.Ingest(r.Context(), r.Header.Get("X-TSession-Instance"), evs)
	res.Accepted, res.Duplicates = ir.Accepted, ir.Duplicates
	for k, n := range ir.RejectedReasons {
		res.reject(k, n)
	}
	if err != nil {
		c.log.Error("ingest", "err", err)
		switch {
		case errors.Is(err, errBackpressure):
			writeErr(w, http.StatusServiceUnavailable, errors.New("session buffer full, retry later"))
			return
		case errors.Is(err, errSeqClosed):
			writeErr(w, http.StatusServiceUnavailable, errors.New("collector is shutting down, retry later"))
			return
		}
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

// resolvePage applies tx outcomes and orphan flags to one page of a session's
// events and reports whether the session is sealed. Outcomes come from the
// store because the deciding commit/rollback may lie outside the page.
func (c *Collector) resolvePage(ctx context.Context, id string, evs []core.Event) (out []core.Event, sealed bool, err error) {
	si, err := c.st.GetSeal(ctx, id)
	if err != nil {
		return nil, false, err
	}
	outcomes, err := c.st.TxOutcomes(ctx, id)
	if err != nil {
		return nil, false, err
	}
	var orph map[string]bool
	if si != nil {
		orph = setOf(si.Orphaned)
	}
	return resolve(evs, outcomes, si != nil, orph), si != nil, nil
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
	sealed := false
	if err == nil {
		evs, sealed, err = c.resolvePage(r.Context(), id, evs)
	}
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	next := from
	if n := len(evs); n > 0 {
		next = evs[n-1].Seq + 1
	}
	if evs == nil {
		evs = []core.Event{}
	}
	writeJSON(w, 200, map[string]any{"events": evs, "next_seq": next, "sealed": sealed})
}

func (c *Collector) handleTrace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v, err := c.viewOf(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, v.getTrace(id))
}

func (c *Collector) handleEntities(w http.ResponseWriter, r *http.Request) {
	v, err := c.viewOf(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, v.getEntities())
}

func (c *Collector) handleHistory(w http.ResponseWriter, r *http.Request) {
	v, err := c.viewOf(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, History(v.evs, r.PathValue("store"), r.PathValue("entity"), r.PathValue("pk")))
}

func (c *Collector) handleSummary(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v, err := c.viewOf(r.Context(), id)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	s, err := c.summary(r.Context(), id, v.getSummary(id))
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

func (c *Collector) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	switch err := c.Delete(r.Context(), id); {
	case errors.Is(err, errBadID):
		writeErr(w, http.StatusBadRequest, err)
	case errors.Is(err, errSeqClosed):
		writeErr(w, http.StatusServiceUnavailable, errors.New("collector is shutting down, retry later"))
	case err != nil:
		c.log.Error("delete session", "session", id, "err", err)
		writeErr(w, http.StatusInternalServerError, err)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "session_id": id})
	}
}
