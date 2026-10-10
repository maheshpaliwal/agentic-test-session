package core

import (
	"context"
	"sync"
	"time"
)

// Span is an in-flight event. All methods are safe on a nil *Span, so wrappers
// never need nil checks: when the SDK is off or no session applies, Begin*
// returns a nil span and the original ctx.
type Span struct {
	mu    sync.Mutex
	ev    Event
	ended bool
}

func startSpan(ctx context.Context, parent SpanContext, kind Kind, name string) (context.Context, *Span) {
	c := GetConfig()
	sid := NewSpanID()
	if parent.HLC > 0 {
		HLCObserve(parent.HLC)
	}
	s := &Span{ev: Event{
		SessionID: parent.SessionID,
		TraceID:   parent.TraceID,
		SpanID:    sid,
		ParentID:  parent.SpanID,
		Service:   c.Service,
		Instance:  c.Instance,
		Kind:      kind,
		Name:      name,
		HLC:       HLCNow(),
		StartNs:   time.Now().UnixNano(),
	}}
	child := SpanContext{SessionID: parent.SessionID, TraceID: parent.TraceID, SpanID: sid}
	return ContextWith(ctx, child), s
}

// Begin starts a child span of whatever session span is in ctx.
func Begin(ctx context.Context, kind Kind, name string) (context.Context, *Span) {
	if !Enabled() {
		return ctx, nil
	}
	parent, ok := FromContext(ctx)
	if !ok || !activeSessions().IsActive(parent.SessionID) {
		return ctx, nil
	}
	return startSpan(ctx, parent, kind, name)
}

// BeginIngress starts a span from an incoming carrier (HTTP headers, gRPC
// metadata, message headers). If the carrier has no session and the control
// plane runs an exclusive session, the request is stamped with it. For
// worker.start the publisher span is recorded as a link.
func BeginIngress(ctx context.Context, kind Kind, name string, c Carrier) (context.Context, *Span) {
	if !Enabled() {
		return ctx, nil
	}
	sc, ok := ExtractSpanContext(c)
	if ok {
		if !activeSessions().IsActive(sc.SessionID) {
			return ctx, nil
		}
	} else {
		id, ex := activeSessions().Exclusive()
		if !ex {
			return ctx, nil
		}
		sc = SpanContext{SessionID: id, TraceID: NewTraceID()}
	}
	pubSpan := sc.SpanID
	ctx, s := startSpan(ctx, sc, kind, name)
	if kind == KindWorkerStart && pubSpan != "" {
		s.AddLink(pubSpan)
	}
	return ctx, s
}

func (s *Span) with(f func(e *Event)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		f(&s.ev)
	}
}

func (s *Span) SpanID() string {
	if s == nil {
		return ""
	}
	return s.ev.SpanID
}
func (s *Span) SetName(n string) { s.with(func(e *Event) { e.Name = n }) }
func (s *Span) SetReq(v any)     { p := Payload(v); s.with(func(e *Event) { e.Req = p }) }
func (s *Span) SetResp(v any)    { p := Payload(v); s.with(func(e *Event) { e.Resp = p }) }
func (s *Span) SetTx(id string)  { s.with(func(e *Event) { e.TxID = id }) }
func (s *Span) SetAttempt(n int) { s.with(func(e *Event) { e.Attempt = n }) }
func (s *Span) AddLink(spanID string) {
	s.with(func(e *Event) { e.LinkIDs = append(e.LinkIDs, spanID) })
}
func (s *Span) AddMutation(m Mutation) {
	s.with(func(e *Event) { e.Mutations = append(e.Mutations, m) })
}
func (s *Span) AddRead(r ReadSummary) { s.with(func(e *Event) { e.Reads = append(e.Reads, r) }) }
func (s *Span) AddFlag(f string)      { s.with(func(e *Event) { e.Flags = append(e.Flags, f) }) }
func (s *Span) SetErr(err error) {
	if err != nil {
		s.with(func(e *Event) { e.Err = err.Error() })
	}
}
func (s *Span) SetAttr(k, v string) {
	s.with(func(e *Event) {
		if e.Attrs == nil {
			e.Attrs = map[string]string{}
		}
		e.Attrs[k] = v
	})
}

// End finalizes and records the span. Idempotent.
func (s *Span) End() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.ev.EndNs = time.Now().UnixNano()
	s.ev.EndHLC = HLCNow()
	s.ev.LocalSeq = nextLocalSeq()
	ev := s.ev
	s.mu.Unlock()
	defer func() { _ = recover() }() // never let recording break the host app
	record(ev)
}
