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

	"tsession/core"
)

// Request/response metadata (authorization, cookie, x-api-key, ...) is never
// recorded, only the trace context is read from / written to it.

// maxStreamMsgs bounds how many stream messages are kept per direction.
const maxStreamMsgs = 50

// mdCarrier adapts gRPC metadata to core.Carrier.
type mdCarrier struct{ md metadata.MD }

func (c mdCarrier) Get(key string) string {
	if v := c.md.Get(strings.ToLower(key)); len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c mdCarrier) Set(key, val string) { c.md.Set(strings.ToLower(key), val) }

// toJSON renders a message as JSON (protojson for protobuf, encoding/json
// otherwise) with sensitive field names masked at every depth via
// core.RedactAny. Redaction happens here, on the full message, in addition to
// the structural pass core.Payload applies later. Non-JSON output is scrubbed
// with core.RedactText.
func toJSON(m any) json.RawMessage {
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
	span.SetReq(req)
	if resp != nil {
		span.SetResp(resp)
	}
	code := status.Code(err)
	span.SetAttr("grpc.code", code.String())
	if err != nil && code != codes.OK {
		span.SetErr(err)
	}
	span.End()
}

// msgLog collects a capped sequence of stream messages.
type msgLog struct {
	mu   sync.Mutex
	msgs []json.RawMessage
	more bool
}

func (l *msgLog) add(m any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.msgs) >= maxStreamMsgs {
		l.more = true
		return
	}
	l.msgs = append(l.msgs, toJSON(m))
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
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(ctx, req)
		}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		ctx, span := core.BeginIngress(ctx, core.KindAPIIn, info.FullMethod, mdCarrier{md.Copy()})
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
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, ss)
		}
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		ctx, span := core.BeginIngress(ss.Context(), core.KindAPIIn, info.FullMethod, mdCarrier{md.Copy()})
		if span == nil {
			return h(srv, ss)
		}
		w := &serverStream{ServerStream: ss, ctx: ctx}
		var err error
		defer func() {
			if r := recover(); r != nil {
				finish(span, w.in.payload(), w.out.payload(), status.Errorf(codes.Internal, "panic: %v", r))
				panic(r)
			}
			finish(span, w.in.payload(), w.out.payload(), err)
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

type clientStream struct {
	grpc.ClientStream
	span    *core.Span
	in, out msgLog
	once    sync.Once
}

func (s *clientStream) end(err error) {
	s.once.Do(func() {
		if errors.Is(err, io.EOF) {
			err = nil
		}
		// out holds what we sent (Req); in holds what we received (Resp).
		finish(s.span, s.out.payload(), s.in.payload(), err)
	})
}

func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.end(err)
		return err
	}
	s.out.add(m)
	return nil
}

func (s *clientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.end(err)
		return err
	}
	s.in.add(m)
	return nil
}

// StreamClientInterceptor records one api.out span per streaming call. The
// span ends when the stream finishes (EOF, error or context cancellation).
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	if !core.Enabled() {
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
		cs, err := str(outgoing(ctx), desc, cc, method, opts...)
		if err != nil {
			finish(span, nil, nil, err)
			return nil, err
		}
		w := &clientStream{ClientStream: cs, span: span}
		go func() {
			<-cs.Context().Done()
			w.end(nil)
		}()
		return w, nil
	}
}
