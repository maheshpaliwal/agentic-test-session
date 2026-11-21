package core

import (
	"context"
	"net/http"
	"strconv"
	"strings"
)

// SpanContext is what flows through context.Context and over the wire.
type SpanContext struct {
	SessionID string
	TraceID   string
	SpanID    string // the span that is the parent of whatever starts next
	HLC       uint64
}

type ctxKey struct{}

// ContextWith returns a copy of ctx carrying sc; spans begun from it become
// children of sc.SpanID.
func ContextWith(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}

// FromContext returns the span context in ctx. ok is false when ctx carries
// none (the request is not part of a session).
func FromContext(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(SpanContext)
	return sc, ok && sc.SessionID != ""
}

// Wire keys (lowercase). Transports map these onto headers/metadata/attributes.
//
// KeyTraceparent is the W3C Trace Context header, which the application's own
// tracing (OpenTelemetry, say) uses as well. KeyTSessionTraceparent carries
// the same format under a name only tsession writes; it is the one tsession
// reads first. See WriteTraceparent for who writes which.
const (
	KeyTraceparent         = "traceparent"            // 00-<32hex trace>-<16hex span>-01
	KeyTSessionTraceparent = "x-tsession-traceparent" // same format, tsession's own
	KeySession             = "x-tsession-id"
	KeyHLC                 = "x-tsession-hlc"
)

// HeaderService and HeaderInstance are sent by the SDK when it follows the
// control plane's active set, so the control plane can list which services
// are connected. They identify the process, not a request.
const (
	HeaderService  = "X-Tsession-Service"
	HeaderInstance = "X-Tsession-Instance"
)

// Carrier abstracts a header-like key/value store. Keys are lowercase.
type Carrier interface {
	Get(key string) string
	Set(key, val string)
}

// MapCarrier is a Carrier backed by a map; keys are stored lowercase. Queue
// wrappers use it for message headers and side-channel records.
type MapCarrier map[string]string

// Get implements Carrier.
func (m MapCarrier) Get(k string) string { return m[strings.ToLower(k)] }

// Set implements Carrier.
func (m MapCarrier) Set(k, v string) { m[strings.ToLower(k)] = v }

// HTTPHeaderCarrier adapts http.Header (also usable for gRPC metadata copies).
type HTTPHeaderCarrier http.Header

// Get implements Carrier.
func (h HTTPHeaderCarrier) Get(k string) string { return http.Header(h).Get(k) }

// Set implements Carrier.
func (h HTTPHeaderCarrier) Set(k, v string) { http.Header(h).Set(k, v) }

// Inject writes the current span context (if any) into c: the session id,
// the clock, and the trace context as described at WriteTraceparent.
func Inject(ctx context.Context, c Carrier) {
	sc, ok := FromContext(ctx)
	if !ok || !Enabled() {
		return
	}
	span := sc.SpanID
	if span == "" {
		span = NewSpanID()
	}
	WriteTraceparent(c, "00-"+sc.TraceID+"-"+span+"-01")
	c.Set(KeySession, sc.SessionID)
	c.Set(KeyHLC, strconv.FormatUint(HLCNow(), 10))
}

// WriteTraceparent writes tsession's trace context tp (the traceparent format)
// into c. It always sets KeyTSessionTraceparent, replacing an earlier value.
// It sets KeyTraceparent only when that cannot destroy a value the application
// wrote:
//
//   - c has no traceparent, or
//   - c's traceparent equals the x-tsession-traceparent c held before this
//     call: tsession wrote both and they were copied here together (a
//     consumer that republishes a message with its headers, a proxy that
//     forwards them, a second Inject on one carrier). Leaving it would hand a
//     stale parent to a reader that knows only traceparent; or
//   - Config.OverwriteTraceparent is set.
//
// Any other traceparent belongs to the application and is left as it is.
func WriteTraceparent(c Carrier, tp string) {
	app := c.Get(KeyTraceparent)
	own := c.Get(KeyTSessionTraceparent)
	c.Set(KeyTSessionTraceparent, tp)
	if app == "" || app == own || cfg.Load().OverwriteTraceparent {
		c.Set(KeyTraceparent, tp)
	}
}

// parseTraceparent returns the ids in a traceparent value.
func parseTraceparent(v string) (trace, span string, ok bool) {
	parts := strings.Split(v, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// ExtractSpanContext parses c without validating the session is active.
//
// The trace context is read from x-tsession-traceparent. When that key is
// absent (the sender runs an SDK from before the key existed) or does not
// parse, it is read from traceparent instead. That fallback cannot tell
// tsession's value from the application's: see the compatibility note in
// docs/PROTOCOL.md.
func ExtractSpanContext(c Carrier) (SpanContext, bool) {
	sid := c.Get(KeySession)
	if sid == "" {
		return SpanContext{}, false
	}
	sc := SpanContext{SessionID: sid}
	trace, span, ok := parseTraceparent(c.Get(KeyTSessionTraceparent))
	if !ok {
		trace, span, ok = parseTraceparent(c.Get(KeyTraceparent))
	}
	if ok {
		sc.TraceID, sc.SpanID = trace, span
	} else {
		sc.TraceID = NewTraceID()
	}
	if h := c.Get(KeyHLC); h != "" {
		sc.HLC, _ = strconv.ParseUint(h, 10, 64)
	}
	return sc, true
}
