package tgrpc

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

const appTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// otelLikeMD stands in for OpenTelemetry's gRPC client instrumentation: it
// sets traceparent in the outgoing metadata, replacing any value there.
func otelLikeMD(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	md.Set("traceparent", appTraceparent)
	return metadata.NewOutgoingContext(ctx, md)
}

func otelLikeUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return inv(otelLikeMD(ctx), method, req, reply, cc, opts...)
}

func otelLikeStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, str grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return str(otelLikeMD(ctx), desc, cc, method, opts...)
}

// seenMD collects the metadata the server received, per call.
type seenMD struct {
	mu sync.Mutex
	md []metadata.MD
}

func (s *seenMD) add(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.md = append(s.md, md.Copy())
	s.mu.Unlock()
}

func (s *seenMD) last(t *testing.T) metadata.MD {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.md) == 0 {
		t.Fatal("server saw no call")
	}
	return s.md[len(s.md)-1]
}

// setupTraceparent is setup with the application's stand-in interceptor
// chained before (appFirst) or after tsession's on the client.
func setupTraceparent(t *testing.T, appFirst bool) (*core.MemRecorder, *grpc.ClientConn, *seenMD) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(sess("s1"))

	seen := &seenMD{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			seen.add(ctx)
			return h(ctx, req)
		}, UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			seen.add(ss.Context())
			return h(srv, ss)
		}, StreamServerInterceptor()))
	srv.RegisterService(&testDesc, struct{}{})
	go srv.Serve(lis)

	unary := []grpc.UnaryClientInterceptor{UnaryClientInterceptor(), otelLikeUnary}
	stream := []grpc.StreamClientInterceptor{StreamClientInterceptor(), otelLikeStream}
	if appFirst {
		unary[0], unary[1] = unary[1], unary[0]
		stream[0], stream[1] = stream[1], stream[0]
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithChainStreamInterceptor(stream...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		core.Configure(core.Config{})
		core.SetRecorder(nil)
		core.SetSessions(nil)
	})
	return rec, conn, seen
}

func assertTraceparentKept(t *testing.T, rec *core.MemRecorder, md metadata.MD, root core.SpanContext) {
	t.Helper()
	if got := md.Get("traceparent"); len(got) != 1 || got[0] != appTraceparent {
		t.Fatalf("application traceparent arrived as %q, want exactly %q", got, appTraceparent)
	}
	evs := rec.Events()
	in, out := byKind(evs, core.KindAPIIn), byKind(evs, core.KindAPIOut)
	if in == nil || out == nil {
		t.Fatalf("events: %+v", evs)
	}
	hlc := md.Get(core.KeyHLC)
	if len(hlc) != 1 {
		t.Fatalf("%s = %q", core.KeyHLC, hlc)
	}
	own, want := md.Get(core.KeyTSessionTraceparent), "00-"+root.TraceID+"-"+out.SpanID+"-01;"+hlc[0]
	if len(own) != 1 || own[0] != want {
		t.Fatalf("%s = %q, want %q", core.KeyTSessionTraceparent, own, want)
	}
	if in.ParentID != out.SpanID || in.TraceID != root.TraceID {
		t.Fatalf("server span parent %q trace %q, want parent %q trace %q", in.ParentID, in.TraceID, out.SpanID, root.TraceID)
	}
}

// With the application's tracing on the same connection, before or after
// tsession's interceptor, the server receives the application's traceparent
// untouched and its api.in span is a child of the client's api.out span.
func TestClientKeepsApplicationTraceparent(t *testing.T) {
	for name, appFirst := range map[string]bool{"application tracing runs first": true, "application tracing runs last": false} {
		t.Run(name+"/unary", func(t *testing.T) {
			rec, conn, seen := setupTraceparent(t, appFirst)
			root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
			ctx := core.ContextWith(context.Background(), root)
			if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("hi"), new(wrapperspb.StringValue)); err != nil {
				t.Fatal(err)
			}
			assertTraceparentKept(t, rec, seen.last(t), root)
		})
		t.Run(name+"/stream", func(t *testing.T) {
			rec, conn, seen := setupTraceparent(t, appFirst)
			root := core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()}
			ctx := core.ContextWith(context.Background(), root)
			cs, err := conn.NewStream(ctx, &testDesc.Streams[0], "/test.Echo/Repeat")
			if err != nil {
				t.Fatal(err)
			}
			if err := cs.SendMsg(wrapperspb.String("x")); err != nil {
				t.Fatal(err)
			}
			if err := cs.CloseSend(); err != nil {
				t.Fatal(err)
			}
			for {
				if err := cs.RecvMsg(new(wrapperspb.StringValue)); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, func() bool { return len(rec.Events()) >= 2 })
			assertTraceparentKept(t, rec, seen.last(t), root)
		})
	}
}

// A caller whose SDK predates x-tsession-traceparent sends the parent in
// traceparent only.
func TestServerReadsTraceparentFromOlderSDK(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	trace, span := core.NewTraceID(), core.NewSpanID()
	ctx := metadata.AppendToOutgoingContext(context.Background(), core.KeySession, "s1",
		core.KeyTraceparent, "00-"+trace+"-"+span+"-01")
	if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("hi"), new(wrapperspb.StringValue)); err != nil {
		t.Fatal(err)
	}
	in := byKind(rec.Events(), core.KindAPIIn)
	if in == nil || in.ParentID != span || in.TraceID != trace {
		t.Fatalf("api.in = %+v", in)
	}
}

// A call that a service on an older SDK forwarded with the metadata it
// received from a new one: its span is in traceparent, with a new
// x-tsession-hlc, next to the x-tsession-traceparent it copied. The parent is
// the forwarder's span.
func TestServerIgnoresOwnKeyCopiedByOlderSDK(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	trace, sender, forwarder := core.NewTraceID(), core.NewSpanID(), core.NewSpanID()
	ctx := metadata.AppendToOutgoingContext(context.Background(), core.KeySession, "s1",
		core.KeyTSessionTraceparent, "00-"+trace+"-"+sender+"-01;1000",
		core.KeyTraceparent, "00-"+trace+"-"+forwarder+"-01",
		core.KeyHLC, "2000")
	if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("hi"), new(wrapperspb.StringValue)); err != nil {
		t.Fatal(err)
	}
	in := byKind(rec.Events(), core.KindAPIIn)
	if in == nil || in.ParentID != forwarder || in.TraceID != trace {
		t.Fatalf("api.in = %+v, want parent %s (the forwarder; the first sender is %s)", in, forwarder, sender)
	}
}
