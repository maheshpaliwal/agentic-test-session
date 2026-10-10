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

func ContextWith(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}

func FromContext(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(SpanContext)
	return sc, ok && sc.SessionID != ""
}

// Wire keys (lowercase). Transports map these onto headers/metadata/attributes.
const (
	KeyTraceparent = "traceparent" // 00-<32hex trace>-<16hex span>-01
	KeySession     = "x-tsession-id"
	KeyHLC         = "x-tsession-hlc"
)

// Carrier abstracts a header-like key/value store. Keys are lowercase.
type Carrier interface {
	Get(key string) string
	Set(key, val string)
}

type MapCarrier map[string]string

func (m MapCarrier) Get(k string) string { return m[strings.ToLower(k)] }
func (m MapCarrier) Set(k, v string)     { m[strings.ToLower(k)] = v }

// HTTPHeaderCarrier adapts http.Header (also usable for gRPC metadata copies).
type HTTPHeaderCarrier http.Header

func (h HTTPHeaderCarrier) Get(k string) string { return http.Header(h).Get(k) }
func (h HTTPHeaderCarrier) Set(k, v string)     { http.Header(h).Set(k, v) }

// Inject writes the current span context (if any) into c.
func Inject(ctx context.Context, c Carrier) {
	sc, ok := FromContext(ctx)
	if !ok || !Enabled() {
		return
	}
	span := sc.SpanID
	if span == "" {
		span = NewSpanID()
	}
	c.Set(KeyTraceparent, "00-"+sc.TraceID+"-"+span+"-01")
	c.Set(KeySession, sc.SessionID)
	c.Set(KeyHLC, strconv.FormatUint(HLCNow(), 10))
}

// ExtractSpanContext parses c without validating the session is active.
func ExtractSpanContext(c Carrier) (SpanContext, bool) {
	sid := c.Get(KeySession)
	if sid == "" {
		return SpanContext{}, false
	}
	sc := SpanContext{SessionID: sid}
	if parts := strings.Split(c.Get(KeyTraceparent), "-"); len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		sc.TraceID, sc.SpanID = parts[1], parts[2]
	} else {
		sc.TraceID = NewTraceID()
	}
	if h := c.Get(KeyHLC); h != "" {
		sc.HLC, _ = strconv.ParseUint(h, 10, 64)
	}
	return sc, true
}
