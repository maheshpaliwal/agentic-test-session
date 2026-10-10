package thttp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"tsession/core"
)

// tracedHandler is the idempotency marker for server-side wrapping.
type tracedHandler struct{ http.Handler }

// Handler wraps h so each request in an active session records an api.in span.
// When the SDK is disabled, or h is already wrapped, h is returned as is.
func Handler(h http.Handler) http.Handler {
	if !core.Enabled() {
		return h
	}
	if _, ok := h.(*tracedHandler); ok {
		return h
	}
	return &tracedHandler{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(h, w, r)
	})}
}

// HandlerFunc is Handler for a plain function.
func HandlerFunc(f http.HandlerFunc) http.Handler { return Handler(f) }

// WrapMux wraps a ServeMux; the span name uses the matched route pattern.
func WrapMux(m *http.ServeMux) http.Handler { return Handler(m) }

func serve(h http.Handler, w http.ResponseWriter, r *http.Request) {
	name := r.Method + " " + r.URL.Path
	ctx, span := core.BeginIngress(r.Context(), core.KindAPIIn, name, core.HTTPHeaderCarrier(r.Header))
	if span == nil {
		h.ServeHTTP(w, r)
		return
	}

	reqCap := newLimitBuf()
	r2 := r.WithContext(ctx)
	if r.Body != nil && r.Body != http.NoBody {
		r2.Body = &teeBody{rc: r.Body, cl: r.Body.Close, cap: reqCap}
	}
	rw := &recWriter{ResponseWriter: w, cap: newLimitBuf()}

	defer func() {
		rec := recover()
		route := r2.Pattern // set by ServeMux on r2 during routing
		if route != "" {
			if strings.Contains(route, " ") {
				span.SetName(route)
			} else {
				span.SetName(r.Method + " " + route)
			}
		}
		span.SetReq(message{
			Method:  r.Method,
			URL:     r.URL.RequestURI(),
			Headers: redactHeaders(r.Header),
			Body:    reqCap.body(),
		})
		status := rw.status
		if status == 0 {
			status = http.StatusOK
		}
		if rec != nil {
			status = http.StatusInternalServerError
			span.SetErr(fmt.Errorf("panic: %v", rec))
		} else if status >= 500 {
			span.SetErr(fmt.Errorf("HTTP %d", status))
		}
		span.SetResp(message{Status: status, Headers: redactHeaders(w.Header()), Body: rw.cap.body()})
		if reqCap.truncated || rw.cap.truncated {
			span.AddFlag("truncated")
		}
		span.End()
		if rec != nil {
			panic(rec)
		}
	}()
	h.ServeHTTP(rw, r2)
}

// recWriter records status and a capped copy of the response body. It always
// exposes Flusher/Hijacker/Pusher/ReaderFrom, degrading gracefully when the
// underlying writer lacks them; Unwrap supports http.ResponseController.
type recWriter struct {
	http.ResponseWriter
	status int
	cap    *limitBuf
}

func (w *recWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *recWriter) WriteHeader(code int) {
	if w.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.cap.add(p[:n])
	return n, err
}

func (w *recWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *recWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (w *recWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *recWriter) ReadFrom(src io.Reader) (int64, error) {
	// Route through Write so the body is captured; the writer hides ReadFrom
	// to avoid recursion.
	return io.Copy(struct{ io.Writer }{w}, src)
}
