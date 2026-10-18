package thttp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

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
	ctx, span := core.BeginIngressAttrs(r.Context(), core.KindAPIIn, name, core.HTTPHeaderCarrier(r.Header), reqAttrs{r})
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

	var once sync.Once
	finish := func(rec any, stream bool) {
		once.Do(func() {
			route := r2.Pattern // set by ServeMux on r2 during routing
			if route != "" {
				if strings.Contains(route, " ") {
					span.SetName(route)
				} else {
					span.SetName(r.Method + " " + route)
				}
			}
			span.SetReq(message{
				Method:   r.Method,
				URL:      core.RedactURLBound(r.URL),
				Headers:  redactHeaders(r.Header),
				Body:     reqCap.body(r.Header.Get("Content-Type")),
				BodySize: reqCap.size(),
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
			if stream {
				span.SetAttr("stream", "true")
			}
			span.SetResp(message{Status: status, Headers: redactHeaders(w.Header()),
				Body: rw.cap.body(w.Header().Get("Content-Type")), BodySize: rw.cap.size()})
			if reqCap.truncated || rw.cap.truncated {
				span.AddFlag("truncated")
			}
			span.End()
		})
	}
	// Long-lived responses (SSE, hijacked connections) end their span at the
	// first write/hijack instead of when the handler eventually returns.
	rw.onStream = func() { finish(nil, true) }

	defer func() {
		rec := recover()
		finish(rec, false)
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
	// onStream ends the span early for SSE / hijacked responses.
	onStream func()
}

func (w *recWriter) maybeStream() {
	if w.onStream != nil && strings.HasPrefix(strings.ToLower(w.Header().Get("Content-Type")), "text/event-stream") {
		w.onStream()
	}
}

func (w *recWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *recWriter) WriteHeader(code int) {
	if w.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
	w.maybeStream()
}

func (w *recWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.cap.add(p[:n])
	w.maybeStream()
	return n, err
}

func (w *recWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	w.maybeStream()
}

func (w *recWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		c, rw, err := hj.Hijack()
		if err == nil {
			if w.status == 0 {
				w.status = http.StatusSwitchingProtocols
			}
			if w.onStream != nil {
				w.onStream()
			}
		}
		return c, rw, err
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

// reqAttrs adapts *http.Request to core.Attributes. It is a single-pointer
// struct (no allocation when boxed) and extracts lazily: the SessionSet only
// asks for keys that active bindings use. jwt_claim reads the Authorization
// Bearer token, UNVERIFIED; the raw token is never logged or recorded.
type reqAttrs struct{ r *http.Request }

func (a reqAttrs) Get(source, name string) string {
	switch source {
	case core.SourceHeader:
		return a.r.Header.Get(name)
	case core.SourceCookie:
		if c, err := a.r.Cookie(name); err == nil {
			return c.Value
		}
	case core.SourceQuery:
		return a.r.URL.Query().Get(name)
	case core.SourceJWTClaim:
		return core.JWTClaimFromAuthorization(a.r.Header.Get("Authorization"), name)
	}
	return ""
}
