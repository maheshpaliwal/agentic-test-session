package tgrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Request/response metadata (authorization, cookie, x-api-key, ...) is never
// recorded: only the trace context is read from / written to it, and incoming
// metadata is consulted for attribute bindings (docs/BINDING.md).

// maxStreamMsgs bounds how many stream messages are kept per direction. The
// bytes kept per direction are bounded too, by Config.MaxInlineBytes.
const maxStreamMsgs = 50

// flagTruncated marks a stream span whose message log was capped.
const flagTruncated = "truncated"

// mdCarrier adapts gRPC metadata to core.Carrier.
type mdCarrier struct{ md metadata.MD }

func (c mdCarrier) Get(key string) string {
	if v := c.md.Get(strings.ToLower(key)); len(v) > 0 {
		return v[0]
	}
	return ""
}

// Has implements core.PresenceCarrier: a key sent with an empty value counts.
func (c mdCarrier) Has(key string) bool { return len(c.md.Get(strings.ToLower(key))) > 0 }

func (c mdCarrier) Set(key, val string) { c.md.Set(strings.ToLower(key), val) }

// inMD reads the incoming metadata of ctx key by key. Unlike
// metadata.FromIncomingContext it does not copy the whole of it: a call that
// carries no session header, which is every call outside a session, costs one
// lookup.
type inMD struct{ ctx context.Context }

func (c *inMD) Get(key string) string {
	if v := metadata.ValueFromIncomingContext(c.ctx, strings.ToLower(key)); len(v) > 0 {
		return v[0]
	}
	return ""
}

// Has implements core.PresenceCarrier.
func (c *inMD) Has(key string) bool {
	return len(metadata.ValueFromIncomingContext(c.ctx, strings.ToLower(key))) > 0
}

// Set is never called: incoming metadata is only read.
func (c *inMD) Set(string, string) {}

// inAttrs adapts incoming metadata to core.Attributes for session bindings:
// header bindings match metadata keys, jwt_claim reads the (UNVERIFIED) bearer
// token in the authorization metadata. Cookie and query sources do not exist
// in gRPC and never match. Lookups are lazy: nothing is read unless an active
// binding asks for the key.
type inAttrs struct{ c *inMD }

func (a inAttrs) Get(source, name string) string {
	switch source {
	case core.SourceHeader:
		return a.c.Get(name)
	case core.SourceJWTClaim:
		return core.JWTClaimFromAuthorization(a.c.Get("authorization"), name)
	}
	return ""
}

// beginIngress starts the api.in span for an incoming call. While no session
// is active nothing is read at all.
func beginIngress(ctx context.Context, method string) (context.Context, *core.Span) {
	if !core.AnyActive() {
		return ctx, nil
	}
	in := &inMD{ctx}
	return core.BeginIngressAttrs(ctx, core.KindAPIIn, method, in, inAttrs{in})
}

// toJSON renders a message as JSON (protojson for protobuf, encoding/json
// otherwise) with sensitive field names masked at every depth via
// core.RedactAny. Redaction happens here, on the full message, in addition to
// the structural pass core.Payload applies later. Non-JSON output is scrubbed
// with core.RedactText.
func toJSON(m any) (out json.RawMessage) {
	// A message whose own marshalling panics must not take the RPC down.
	defer func() {
		if recover() != nil {
			out, _ = json.Marshal("<unmarshalable>")
		}
	}()
	var raw []byte
	if pm, ok := m.(proto.Message); ok {
		if b, err := protojson.Marshal(pm); err == nil {
			raw = b
		}
	}
	if raw == nil {
		if b, err := json.Marshal(m); err == nil {
			raw = b
		}
	}
	if raw == nil {
		b, _ := json.Marshal("<unmarshalable>")
		return b
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep int64 precision
	var v any
	if err := dec.Decode(&v); err != nil {
		b, _ := json.Marshal(core.RedactText(string(raw)))
		return b
	}
	out, err := json.Marshal(core.RedactAny(v))
	if err != nil {
		b, _ := json.Marshal("<unmarshalable>")
		return b
	}
	return out
}

func finish(span *core.Span, req, resp any, err error) {
	defer core.Recover() // recording must never break the RPC
	span.SetReq(req)
	if resp != nil {
		span.SetResp(resp)
	}
	code := status.Code(err)
	span.SetAttr("grpc.code", code.String())
	switch {
	case err == nil || code == codes.OK:
	case failure(code):
		span.SetErr(err)
	default:
		// The caller's doing, like an HTTP 4xx: recorded, not a failure.
		span.SetAttr("grpc.message", core.RedactText(status.Convert(err).Message()))
	}
	span.End()
}

// failure reports whether a status means the callee failed, as an HTTP 5xx
// does: the codes gRPC maps to 500, 501, 503 and 504, and Unknown, which is
// also what an error that is not a status reads as. The others (NotFound,
// InvalidArgument, PermissionDenied, Canceled, ...) answer what the caller
// asked or did, as an HTTP 4xx does, and a session with such calls has not
// failed for it.
func failure(c codes.Code) bool {
	switch c {
	case codes.Unknown, codes.DeadlineExceeded, codes.Unimplemented, codes.Internal, codes.Unavailable, codes.DataLoss:
		return true
	}
	return false
}

// finishStream is finish for a streaming call: req and resp are the message
// logs of the two directions, and a capped log flags the span truncated.
func finishStream(span *core.Span, req, resp *msgLog, err error) {
	if req.truncated() || resp.truncated() {
		span.AddFlag(flagTruncated)
	}
	finish(span, req.payload(), resp.payload(), err)
}

// msgLog collects a sequence of stream messages, capped both in count
// (maxStreamMsgs) and in retained bytes (Config.MaxInlineBytes), so a long or
// heavy stream cannot pin memory until it ends. more reports that something
// was dropped or cut.
type msgLog struct {
	mu    sync.Mutex
	msgs  []json.RawMessage
	bytes int
	more  bool
}

func (l *msgLog) add(m any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	room := core.GetConfig().MaxInlineBytes - l.bytes
	if len(l.msgs) >= maxStreamMsgs || room <= 0 {
		l.more = true // dropped without marshalling
		return
	}
	js := toJSON(m)
	if len(js) > room {
		js = headString(js, room)
		l.more = true
	}
	l.bytes += len(js)
	l.msgs = append(l.msgs, js)
}

// headString returns the head of js (an already redacted message) as a JSON
// string of at most max bytes, or the empty JSON string when max is too small
// to hold anything. Quoting and escaping grow the text, so the cut is
// shortened until the encoded form fits.
func headString(js []byte, max int) []byte {
	for n := max; n > 0; {
		out, _ := json.Marshal(strings.ToValidUTF8(string(js[:n]), ""))
		if len(out) <= max {
			return out
		}
		n -= len(out) - max
	}
	return []byte(`""`)
}

func (l *msgLog) truncated() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.more
}

func (l *msgLog) payload() any {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.msgs) == 0 {
		return nil
	}
	return append([]json.RawMessage(nil), l.msgs...)
}

// UnaryServerInterceptor records an api.in span per unary call.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tgrpc.UnaryServerInterceptor")
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(ctx, req)
		}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx, span := beginIngress(ctx, info.FullMethod)
		if span == nil {
			return h(ctx, req)
		}
		var resp any
		var err error
		defer func() {
			if r := recover(); r != nil {
				finish(span, toJSON(req), nil, status.Errorf(codes.Internal, "panic: %v", r))
				panic(r)
			}
			var respJSON any
			if resp != nil {
				respJSON = toJSON(resp)
			}
			finish(span, toJSON(req), respJSON, err)
		}()
		resp, err = h(ctx, req)
		return resp, err
	}
}

type serverStream struct {
	grpc.ServerStream
	ctx     context.Context
	in, out msgLog
}

func (s *serverStream) Context() context.Context { return s.ctx }

func (s *serverStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.in.add(m)
	}
	return err
}

func (s *serverStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.out.add(m)
	}
	return err
}

// StreamServerInterceptor records one api.in span per streaming call, with
// the (capped) sequences of received and sent messages as Req and Resp.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tgrpc.StreamServerInterceptor")
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, ss)
		}
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		ctx, span := beginIngress(ss.Context(), info.FullMethod)
		if span == nil {
			return h(srv, ss)
		}
		w := &serverStream{ServerStream: ss, ctx: ctx}
		var err error
		defer func() {
			if r := recover(); r != nil {
				finishStream(span, &w.in, &w.out, status.Errorf(codes.Internal, "panic: %v", r))
				panic(r)
			}
			finishStream(span, &w.in, &w.out, err)
		}()
		err = h(srv, w)
		return err
	}
}

// outgoing returns ctx with the span context injected into outgoing metadata.
func outgoing(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	core.Inject(ctx, mdCarrier{md})
	return metadata.NewOutgoingContext(ctx, md)
}

// UnaryClientInterceptor records an api.out span per unary call and propagates
// the session context in metadata.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tgrpc.UnaryClientInterceptor")
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return inv(ctx, method, req, reply, cc, opts...)
		}
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := core.Begin(ctx, core.KindAPIOut, method)
		if span == nil {
			return inv(ctx, method, req, reply, cc, opts...)
		}
		span.SetAttr("target", cc.Target())
		err := inv(outgoing(ctx), method, req, reply, cc, opts...)
		var respJSON any
		if err == nil && reply != nil {
			respJSON = toJSON(reply)
		}
		finish(span, toJSON(req), respJSON, err)
		return err
	}
}

// clientStream logs the messages of a client-side stream and ends the span
// with the RPC's final status, which gRPC reports through grpc.OnFinish.
type clientStream struct {
	grpc.ClientStream
	span    *core.Span
	in, out msgLog
	once    sync.Once

	mu      sync.Mutex
	recving int   // RecvMsg calls in flight
	done    bool  // the RPC has completed
	err     error // its final status (nil for OK)
}

// end records the span, once, with the final status.
func (s *clientStream) end() {
	s.once.Do(func() {
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		if errors.Is(err, io.EOF) {
			err = nil
		}
		// out holds what we sent (Req); in holds what we received (Resp).
		finishStream(s.span, &s.out, &s.in, err)
	})
}

// onFinish is the grpc.OnFinish callback: gRPC calls it exactly once with the
// RPC's status when the call completes (final RecvMsg, error, cancellation).
// gRPC may call it from inside the RecvMsg that delivers the last message
// (the single response of a client-streaming call); the span is then ended by
// that RecvMsg, after the message has been logged.
func (s *clientStream) onFinish(err error) {
	s.mu.Lock()
	s.done, s.err = true, err
	recving := s.recving > 0
	s.mu.Unlock()
	if !recving {
		s.end()
	}
}

// fail ends the span for a stream that could not be created.
func (s *clientStream) fail(err error) {
	s.mu.Lock()
	if !s.done {
		s.done, s.err = true, err
	}
	s.mu.Unlock()
	s.end()
}

// SendMsg logs a sent message. A send error does not end the span: gRPC
// reports io.EOF here when the stream is already broken, and the real status
// only arrives through RecvMsg / OnFinish.
func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err == nil {
		s.out.add(m)
	}
	return err
}

func (s *clientStream) RecvMsg(m any) error {
	s.mu.Lock()
	s.recving++
	s.mu.Unlock()

	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		s.in.add(m)
	}

	s.mu.Lock()
	s.recving--
	done := s.done && s.recving == 0
	s.mu.Unlock()
	if done {
		s.end()
	}
	return err
}

// StreamClientInterceptor records one api.out span per streaming call. The
// span ends when the RPC completes (final message, error or context
// cancellation) and carries its final status.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tgrpc.StreamClientInterceptor")
		return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, str grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return str(ctx, desc, cc, method, opts...)
		}
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, str grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, span := core.Begin(ctx, core.KindAPIOut, method)
		if span == nil {
			return str(ctx, desc, cc, method, opts...)
		}
		span.SetAttr("target", cc.Target())
		w := &clientStream{span: span}
		// Full slice expression: never append into the caller's backing array.
		opts = append(opts[:len(opts):len(opts)], grpc.OnFinish(w.onFinish))
		cs, err := str(outgoing(ctx), desc, cc, method, opts...)
		if err != nil {
			w.fail(err)
			return nil, err
		}
		w.ClientStream = cs
		return w, nil
	}
}
