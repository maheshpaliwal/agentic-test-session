package tgrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// streamDesc has the stream shapes the Echo service lacks: client-streaming
// calls that fail early or succeed, and a server stream that ends in an error.
var streamDesc = grpc.ServiceDesc{
	ServiceName: "test.Streams",
	HandlerType: (*svc)(nil),
	Streams: []grpc.StreamDesc{
		{StreamName: "Reject", ClientStreams: true, Handler: func(_ any, ss grpc.ServerStream) error {
			return status.Error(codes.PermissionDenied, "no")
		}},
		{StreamName: "Count", ClientStreams: true, Handler: func(_ any, ss grpc.ServerStream) error {
			n := 0
			for {
				if err := ss.RecvMsg(new(wrapperspb.StringValue)); err == io.EOF {
					break
				} else if err != nil {
					return err
				}
				n++
			}
			return ss.SendMsg(wrapperspb.String(strings.Repeat("x", n)))
		}},
		{StreamName: "Fail", ServerStreams: true, Handler: func(_ any, ss grpc.ServerStream) error {
			if err := ss.RecvMsg(new(wrapperspb.StringValue)); err != nil {
				return err
			}
			return status.Error(codes.NotFound, "nope")
		}},
	},
}

// setupStreams is setup with streamDesc (and the Echo service) registered.
func setupStreams(t *testing.T, s core.SessionSet) (*core.MemRecorder, *grpc.ClientConn) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(s)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor()), grpc.StreamInterceptor(StreamServerInterceptor()))
	srv.RegisterService(&testDesc, struct{}{})
	srv.RegisterService(&streamDesc, struct{}{})
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(StreamClientInterceptor()))
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})
	return rec, conn
}

func sessionCtx() context.Context {
	return core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
}

func eventsOf(rec *core.MemRecorder, k core.Kind) []core.Event {
	var out []core.Event
	for _, e := range rec.Events() {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

// A server that rejects a client-streaming call makes SendMsg return io.EOF;
// the real status only arrives with RecvMsg. The span must carry that status.
func TestClientStreamSendEOFKeepsFinalStatus(t *testing.T) {
	rec, conn := setupStreams(t, sess("s1"))
	cs, err := conn.NewStream(sessionCtx(), &grpc.StreamDesc{StreamName: "Reject", ClientStreams: true}, "/test.Streams/Reject")
	if err != nil {
		panic(err)
	}
	var sendErr error
	waitFor(t, func() bool {
		sendErr = cs.SendMsg(wrapperspb.String("x"))
		return sendErr != nil
	})
	if !errors.Is(sendErr, io.EOF) {
		panic(fmt.Sprintf("SendMsg err = %v, want io.EOF", sendErr))
	}
	if err := cs.RecvMsg(new(wrapperspb.StringValue)); status.Code(err) != codes.PermissionDenied {
		panic(fmt.Sprintf("RecvMsg err = %v", err))
	}
	waitFor(t, func() bool { return len(eventsOf(rec, core.KindAPIOut)) == 1 })
	out := eventsOf(rec, core.KindAPIOut)[0]
	// PermissionDenied answers the caller, as an HTTP 403 does: recorded, not a failure.
	if out.Attrs["grpc.code"] != "PermissionDenied" || out.Attrs["grpc.message"] == "" || out.Err != "" {
		panic(fmt.Sprintf("span lost the final status: code=%s message=%q err=%q", out.Attrs["grpc.code"], out.Attrs["grpc.message"], out.Err))
	}
}

// gRPC cancels the stream context before a failing RecvMsg returns; nothing
// may end the span as OK in that window.
func TestClientStreamErrorStatusNeverLost(t *testing.T) {
	rec, conn := setupStreams(t, sess("s1"))
	const n = 500
	for i := 0; i < n; i++ {
		cs, err := conn.NewStream(sessionCtx(), &grpc.StreamDesc{StreamName: "Fail", ServerStreams: true}, "/test.Streams/Fail")
		if err != nil {
			panic(err)
		}
		if err := cs.SendMsg(wrapperspb.String("x")); err != nil {
			panic(err)
		}
		cs.CloseSend()
		if err := cs.RecvMsg(new(wrapperspb.StringValue)); status.Code(err) != codes.NotFound {
			panic(fmt.Sprintf("RecvMsg err = %v", err))
		}
	}
	waitFor(t, func() bool { return len(eventsOf(rec, core.KindAPIOut)) == n })
	for _, e := range eventsOf(rec, core.KindAPIOut) {
		if e.Attrs["grpc.code"] != "NotFound" {
			panic(fmt.Sprintf("api.out recorded %s for a NotFound stream", e.Attrs["grpc.code"]))
		}
	}
}

// The single response of a client-streaming call is delivered by the RecvMsg
// during which gRPC completes the RPC; it must still be part of the span.
func TestClientStreamingResponseRecorded(t *testing.T) {
	rec, conn := setupStreams(t, sess("s1"))
	cs, err := conn.NewStream(sessionCtx(), &grpc.StreamDesc{StreamName: "Count", ClientStreams: true}, "/test.Streams/Count")
	if err != nil {
		panic(err)
	}
	for _, m := range []string{"a", "b", "c"} {
		if err := cs.SendMsg(wrapperspb.String(m)); err != nil {
			panic(err)
		}
	}
	cs.CloseSend()
	reply := new(wrapperspb.StringValue)
	if err := cs.RecvMsg(reply); err != nil || reply.Value != "xxx" {
		panic(fmt.Sprintf("reply %q err %v", reply.Value, err))
	}
	waitFor(t, func() bool { return len(eventsOf(rec, core.KindAPIOut)) == 1 })
	out := eventsOf(rec, core.KindAPIOut)[0]
	if out.Attrs["grpc.code"] != "OK" || out.Err != "" {
		panic(fmt.Sprintf("code=%s err=%q", out.Attrs["grpc.code"], out.Err))
	}
	if got := string(out.Req.Inline); got != `["a","b","c"]` {
		panic(fmt.Sprintf("req = %s", got))
	}
	if got := string(out.Resp.Inline); got != `["xxx"]` {
		panic(fmt.Sprintf("resp = %s", got))
	}
}

// A caller that abandons a stream by cancelling its context gets a span with
// the Canceled status, not OK.
func TestClientStreamCancelRecorded(t *testing.T) {
	rec, conn := setupStreams(t, sess("s1"))
	ctx, cancel := context.WithCancel(sessionCtx())
	if _, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "Count", ClientStreams: true}, "/test.Streams/Count"); err != nil {
		panic(err)
	}
	cancel()
	waitFor(t, func() bool { return len(eventsOf(rec, core.KindAPIOut)) == 1 })
	if out := eventsOf(rec, core.KindAPIOut)[0]; out.Attrs["grpc.code"] != "Canceled" {
		panic(fmt.Sprintf("code = %s", out.Attrs["grpc.code"]))
	}
}

func TestStreamLogCapsRetainedBytes(t *testing.T) {
	core.Configure(core.Config{Service: "svc", Enabled: true, MaxInlineBytes: 1000})
	t.Cleanup(func() { core.Configure(core.Config{}) })

	var l msgLog
	for i := 0; i < 10; i++ {
		l.add(wrapperspb.String(strings.Repeat("m", 400)))
	}
	if l.bytes > 1000 || !l.more {
		panic(fmt.Sprintf("retained %d bytes in %d msgs (more=%v), want <= 1000", l.bytes, len(l.msgs), l.more))
	}
	if len(l.msgs) != 3 {
		panic(fmt.Sprintf("kept %d msgs, want 2 whole and 1 cut", len(l.msgs)))
	}
	for _, m := range l.msgs {
		if !strings.HasPrefix(string(m), `"`) || !strings.HasSuffix(string(m), `"`) {
			panic(fmt.Sprintf("entry is not a JSON string: %.40s", m))
		}
	}

	// One message larger than the whole budget is cut, not kept whole.
	var big msgLog
	big.add(wrapperspb.String(strings.Repeat("b", 5000)))
	if big.bytes > 1000 || len(big.msgs) != 1 || !big.more {
		panic(fmt.Sprintf("oversized message retained %d bytes", big.bytes))
	}
}

func TestStreamSpanFlaggedWhenLogCapped(t *testing.T) {
	rec, conn := setupStreams(t, sess("s1"))
	cs, err := conn.NewStream(sessionCtx(), &grpc.StreamDesc{StreamName: "Count", ClientStreams: true}, "/test.Streams/Count")
	if err != nil {
		panic(err)
	}
	for i := 0; i < maxStreamMsgs+1; i++ {
		if err := cs.SendMsg(wrapperspb.String("m")); err != nil {
			panic(err)
		}
	}
	cs.CloseSend()
	if err := cs.RecvMsg(new(wrapperspb.StringValue)); err != nil {
		panic(err)
	}
	waitFor(t, func() bool { return len(rec.Events()) == 2 })
	for _, e := range rec.Events() {
		flagged := false
		for _, f := range e.Flags {
			flagged = flagged || f == flagTruncated
		}
		if !flagged {
			panic(fmt.Sprintf("%s span not flagged truncated: %v", e.Kind, e.Flags))
		}
	}
}

func TestIngressAttributeBinding(t *testing.T) {
	// {"sub":"u2"} with alg none; the signature is not verified (BINDING.md).
	const jwt = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1MiJ9."
	s := core.StaticSessions{Active: map[string]bool{"s1": true, "s2": true}, Bindings: []core.Binding{
		{SessionID: "s1", Source: core.SourceHeader, Name: "X-User-Id", Value: "u1"},
		{SessionID: "s2", Source: core.SourceJWTClaim, Name: "sub", Value: "u2"},
	}}
	rec, conn := setupStreams(t, s)

	call := func(kv ...string) {
		t.Helper()
		ctx := metadata.AppendToOutgoingContext(context.Background(), kv...)
		if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("hi"), new(wrapperspb.StringValue)); err != nil {
			panic(err)
		}
	}
	call("x-user-id", "u1")
	call("authorization", "Bearer "+jwt)
	call("x-user-id", "someone-else")
	call()

	in := eventsOf(rec, core.KindAPIIn)
	if len(in) != 2 {
		panic(fmt.Sprintf("want 2 bound api.in spans, got %d: %+v", len(in), in))
	}
	if in[0].SessionID != "s1" || in[0].Attrs["bound_by"] != "header:x-user-id" {
		panic(fmt.Sprintf("header binding: %+v", in[0]))
	}
	if in[1].SessionID != "s2" || in[1].Attrs["bound_by"] != "jwt_claim:sub" {
		panic(fmt.Sprintf("jwt binding: %+v", in[1]))
	}

	// Streams bind the same way.
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-user-id", "u1")
	cs, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "Fail", ServerStreams: true}, "/test.Streams/Fail")
	if err != nil {
		panic(err)
	}
	cs.SendMsg(wrapperspb.String("x"))
	cs.CloseSend()
	cs.RecvMsg(new(wrapperspb.StringValue))
	waitFor(t, func() bool { return len(eventsOf(rec, core.KindAPIIn)) == 3 })
	if e := eventsOf(rec, core.KindAPIIn)[2]; e.SessionID != "s1" || e.Name != "/test.Streams/Fail" {
		panic(fmt.Sprintf("stream binding: %+v", e))
	}
	for _, e := range rec.Events() {
		if strings.Contains(string(e.Req.Inline)+string(e.Resp.Inline), jwt) {
			panic("token recorded")
		}
	}
}
