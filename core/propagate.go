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
// tsession's trace context under a name only tsession writes, bound to the
// KeyHLC value written with it; it is the one tsession reads first. See
// WriteTraceparent for who writes which, and ownTraceparent for the binding.
const (
	KeyTraceparent         = "traceparent"            // 00-<32hex trace>-<16hex span>-01
	KeyTSessionTraceparent = "x-tsession-traceparent" // <traceparent format>;<x-tsession-hlc value>
	KeySession             = "x-tsession-id"
	KeyHLC                 = "x-tsession-hlc"
)

// ownSep separates the trace context from the clock value in
// KeyTSessionTraceparent. It cannot occur in a traceparent, so the value is
// never mistaken for one.
const ownSep = ";"

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

// PresenceCarrier is a Carrier that can tell a key it does not hold from one
// it holds with a value Get returns as "": an empty value, or one that is not
// text (an SQS attribute of type Binary, an AMQP header holding a number).
// WriteTraceparent asks it whether the carrier has a traceparent, so that an
// application's is left alone whatever its value. A carrier whose values are
// always non-empty strings need not implement it.
type PresenceCarrier interface {
	Carrier
	// Has reports whether the carrier holds key, whatever its value.
	Has(key string) bool
}

// MapCarrier is a Carrier backed by a map; keys are stored lowercase. Queue
// wrappers use it for message headers and side-channel records.
type MapCarrier map[string]string

// Get implements Carrier.
func (m MapCarrier) Get(k string) string { return m[strings.ToLower(k)] }

// Has implements PresenceCarrier.
func (m MapCarrier) Has(k string) bool { _, ok := m[strings.ToLower(k)]; return ok }

// Set implements Carrier.
func (m MapCarrier) Set(k, v string) { m[strings.ToLower(k)] = v }

// HTTPHeaderCarrier adapts http.Header (also usable for gRPC metadata copies).
type HTTPHeaderCarrier http.Header

// Get implements Carrier.
func (h HTTPHeaderCarrier) Get(k string) string { return http.Header(h).Get(k) }

// Has implements PresenceCarrier. A header sent with an empty value counts,
// and so does one stored under a key that is not in canonical form
// (h["traceparent"] = ..., which http.Header.Get does not find but which is
// sent all the same).
func (h HTTPHeaderCarrier) Has(k string) bool {
	ck := canonicalKey(k)
	if _, ok := h[ck]; ok {
		return true
	}
	_, ok := h.variant(ck)
	return ok
}

// Set implements Carrier. A header stored under a key that is not in
// canonical form is replaced where it is, so that the request never carries
// the header twice.
func (h HTTPHeaderCarrier) Set(k, v string) {
	ck := canonicalKey(k)
	if _, ok := h[ck]; !ok {
		if name, ok := h.variant(ck); ok {
			ck = name
		}
	}
	h[ck] = []string{v}
}

// canonicalKey is http.CanonicalHeaderKey, without the allocation for the
// keys Inject writes on every traced request.
func canonicalKey(k string) string {
	switch k {
	case KeyTraceparent:
		return "Traceparent"
	case KeyTSessionTraceparent:
		return "X-Tsession-Traceparent"
	case KeySession:
		return "X-Tsession-Id"
	case KeyHLC:
		return "X-Tsession-Hlc"
	}
	return http.CanonicalHeaderKey(k)
}

// variant returns a key of h that differs from the canonical key ck in case
// only.
func (h HTTPHeaderCarrier) variant(ck string) (string, bool) {
	for name := range h {
		if len(name) == len(ck) && name != ck && strings.EqualFold(name, ck) {
			return name, true
		}
	}
	return "", false
}

// Inject writes the current span context (if any) into c: the session id, and
// the trace context and the clock as described at WriteTraceparent.
func Inject(ctx context.Context, c Carrier) {
	sc, ok := FromContext(ctx)
	if !ok || !Enabled() {
		return
	}
	span := sc.SpanID
	if span == "" {
		span = NewSpanID()
	}
	WriteTraceparent(c, "00-"+sc.TraceID+"-"+span+"-01", strconv.FormatUint(HLCNow(), 10))
	c.Set(KeySession, sc.SessionID)
}

// WriteTraceparent writes tsession's trace context tp (the traceparent format)
// and the sender's clock hlc (decimal, the KeyHLC value) into c.
//
// It always sets KeyHLC to hlc and KeyTSessionTraceparent to tp, ";", hlc,
// replacing earlier values. The clock value inside KeyTSessionTraceparent
// binds that key to this write: a hop that does not know the key (an SDK from
// before it existed) and passes it on unchanged still replaces KeyHLC, and a
// reader then sees that the key is out of date (see ownTraceparent).
//
// It sets KeyTraceparent to tp only when that cannot destroy a value the
// application wrote:
//
//   - c has no traceparent (the key is not there: see PresenceCarrier), or
//   - c's traceparent equals the trace context in the x-tsession-traceparent c
//     held before this call: tsession wrote both and they were copied here
//     together (a consumer that republishes a message with its headers, a
//     proxy that forwards them, a second Inject on one carrier). Leaving it
//     would hand a stale parent to a reader that knows only traceparent; or
//   - Config.OverwriteTraceparent is set.
//
// Any other traceparent belongs to the application and is left as it is,
// whatever it holds: an empty value, or one that is not text.
func WriteTraceparent(c Carrier, tp, hlc string) {
	app := c.Get(KeyTraceparent)
	absent := app == ""
	if absent {
		if p, ok := c.(PresenceCarrier); ok {
			absent = !p.Has(KeyTraceparent)
		}
	}
	own, _, _ := strings.Cut(c.Get(KeyTSessionTraceparent), ownSep)
	c.Set(KeyTSessionTraceparent, tp+ownSep+hlc)
	c.Set(KeyHLC, hlc)
	if absent || (app != "" && app == own) || cfg.Load().OverwriteTraceparent {
		c.Set(KeyTraceparent, tp)
	}
}

// ownTraceparent returns the trace context in the x-tsession-traceparent
// value own of a carrier whose x-tsession-hlc is hlc, or "" when own is not
// current: the clock value it was written with is not the carrier's.
//
// Every write sets both keys, and no later hop produces the same clock value
// (it has observed the one it received, so its own is greater). A difference
// therefore means that a hop which does not know x-tsession-traceparent wrote
// its own context into traceparent and x-tsession-hlc afterwards and passed
// this key on as it received it.
//
// A carrier without x-tsession-hlc was not rewritten by such a hop, which
// always writes one; something on the way dropped that key, and own is used.
func ownTraceparent(own, hlc string) string {
	tp, bound, ok := strings.Cut(own, ownSep)
	if !ok || bound == "" || (hlc != "" && hlc != bound) {
		return ""
	}
	return tp
}

// parseTraceparent returns the ids in a traceparent value: four "-"-separated
// fields, of which the second is a trace id of 32 hex digits and the third a
// span id of 16, neither all zeros (which W3C Trace Context defines as
// invalid). The version and flags fields are not interpreted.
func parseTraceparent(v string) (trace, span string, ok bool) {
	parts := strings.Split(v, "-")
	if len(parts) != 4 || !validID(parts[1], 32) || !validID(parts[2], 16) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// validID reports whether s is n hex digits that are not all zeros. Upper
// case is accepted: the ids are recorded as they arrive, as before.
func validID(s string, n int) bool {
	if len(s) != n {
		return false
	}
	zero := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '0':
		case '1' <= c && c <= '9', 'a' <= c && c <= 'f', 'A' <= c && c <= 'F':
			zero = false
		default:
			return false
		}
	}
	return !zero
}

// ExtractSpanContext parses c without validating the session is active.
//
// The trace context is read from x-tsession-traceparent when that key is
// current (see ownTraceparent). When it is absent (the sender runs an SDK from
// before the key existed), out of date (such an SDK passed the carrier on) or
// does not parse, the trace context is read from traceparent instead. That
// fallback cannot tell tsession's value from the application's: see the
// compatibility note in docs/PROTOCOL.md.
func ExtractSpanContext(c Carrier) (SpanContext, bool) {
	sid := c.Get(KeySession)
	if sid == "" {
		return SpanContext{}, false
	}
	sc := SpanContext{SessionID: sid}
	h := c.Get(KeyHLC)
	trace, span, ok := parseTraceparent(ownTraceparent(c.Get(KeyTSessionTraceparent), h))
	if !ok {
		trace, span, ok = parseTraceparent(c.Get(KeyTraceparent))
	}
	if ok {
		sc.TraceID, sc.SpanID = trace, span
	} else {
		sc.TraceID = NewTraceID()
	}
	if h != "" {
		sc.HLC, _ = strconv.ParseUint(h, 10, 64)
	}
	return sc, true
}
