package tgrpc

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type svc interface{}

var testDesc = grpc.ServiceDesc{
	ServiceName: "test.Echo",
	HandlerType: (*svc)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		Handler: func(_ any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			h := func(ctx context.Context, req any) (any, error) {
				v := req.(*wrapperspb.StringValue).Value
				if v == "fail" {
					return nil, status.Error(codes.NotFound, "nope")
				}
				return wrapperspb.String("echo:" + v), nil
			}
			if ic == nil {
				return h(ctx, in)
			}
			return ic(ctx, in, &grpc.UnaryServerInfo{FullMethod: "/test.Echo/Echo"}, h)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Repeat",
		ServerStreams: true,
		Handler: func(_ any, ss grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := ss.RecvMsg(in); err != nil {
				return err
			}
			for i := 0; i < 2; i++ {
				if err := ss.SendMsg(wrapperspb.String(in.Value)); err != nil {
					return err
				}
			}
			return nil
		},
	}},
}

func setup(t *testing.T, s core.StaticSessions) (*core.MemRecorder, *grpc.ClientConn) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(s)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor()), grpc.StreamInterceptor(StreamServerInterceptor()))
	srv.RegisterService(&testDesc, struct{}{})
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(StreamClientInterceptor()))
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
	return rec, conn
}

func sess(ids ...string) core.StaticSessions {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return core.StaticSessions{Active: m}
}

func byKind(evs []core.Event, k core.Kind) *core.Event {
	for i := range evs {
		if evs[i].Kind == k {
			return &evs[i]
		}
	}
	return nil
}

func TestUnaryPropagation(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	root := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID(), SpanID: core.NewSpanID()})
	out := new(wrapperspb.StringValue)
	if err := conn.Invoke(root, "/test.Echo/Echo", wrapperspb.String("hi"), out); err != nil {
		t.Fatal(err)
	}
	if out.Value != "echo:hi" {
		t.Fatalf("resp %q", out.Value)
	}
	evs := rec.Events()
	in, ao := byKind(evs, core.KindAPIIn), byKind(evs, core.KindAPIOut)
	if in == nil || ao == nil || len(evs) != 2 {
		t.Fatalf("events: %+v", evs)
	}
	if in.TraceID != ao.TraceID || in.ParentID != ao.SpanID || in.SessionID != "s1" {
		t.Fatalf("linkage: in=%+v out=%+v", in, ao)
	}
	if in.Name != "/test.Echo/Echo" || !strings.Contains(string(in.Req.Inline), "hi") || !strings.Contains(string(ao.Resp.Inline), "echo:hi") {
		t.Fatalf("payloads: %s | %s", in.Req.Inline, ao.Resp.Inline)
	}
	if in.Attrs["grpc.code"] != "OK" || in.Err != "" {
		t.Fatalf("status: %+v", in)
	}
}

func TestUnaryErrorCode(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	root := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	err := conn.Invoke(root, "/test.Echo/Echo", wrapperspb.String("fail"), new(wrapperspb.StringValue))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err %v", err)
	}
	for _, e := range rec.Events() {
		if e.Attrs["grpc.code"] != "NotFound" || e.Err == "" {
			t.Fatalf("event %s: %+v", e.Kind, e)
		}
	}
}

func TestUnknownSessionAndHeaderIngress(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	ctx := metadata.AppendToOutgoingContext(context.Background(), core.KeySession, "bogus")
	if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("x"), new(wrapperspb.StringValue)); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("unknown session recorded %d", n)
	}
	// Plain client, session header only: server span, no client span.
	ctx = metadata.AppendToOutgoingContext(context.Background(), core.KeySession, "s1")
	if err := conn.Invoke(ctx, "/test.Echo/Echo", wrapperspb.String("y"), new(wrapperspb.StringValue)); err != nil {
		t.Fatal(err)
	}
	evs := rec.Events()
	if len(evs) != 1 || evs[0].Kind != core.KindAPIIn || evs[0].SessionID != "s1" {
		t.Fatalf("%+v", evs)
	}
}

func TestExclusive(t *testing.T) {
	rec, conn := setup(t, core.StaticSessions{Active: map[string]bool{"ex": true}, ExclusiveID: "ex"})
	if err := conn.Invoke(context.Background(), "/test.Echo/Echo", wrapperspb.String("z"), new(wrapperspb.StringValue)); err != nil {
		t.Fatal(err)
	}
	evs := rec.Events()
	if len(evs) != 1 || evs[0].SessionID != "ex" {
		t.Fatalf("%+v", evs)
	}
}

func TestStream(t *testing.T) {
	rec, conn := setup(t, sess("s1"))
	root := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	cs, err := conn.NewStream(root, &grpc.StreamDesc{StreamName: "Repeat", ServerStreams: true}, "/test.Echo/Repeat")
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(wrapperspb.String("r")); err != nil {
		t.Fatal(err)
	}
	cs.CloseSend()
	n := 0
	for {
		m := new(wrapperspb.StringValue)
		if err := cs.RecvMsg(m); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("got %d msgs", n)
	}
	// The server span ends after its handler returns; wait for both events.
	waitFor(t, func() bool { return len(rec.Events()) == 2 })
	evs := rec.Events()
	in, ao := byKind(evs, core.KindAPIIn), byKind(evs, core.KindAPIOut)
	if in == nil || ao == nil || in.ParentID != ao.SpanID || in.TraceID != ao.TraceID {
		t.Fatalf("%+v", evs)
	}
	if !strings.Contains(string(in.Resp.Inline), `"r","r"`) && !strings.Contains(string(in.Resp.Inline), `"r"`) {
		t.Fatalf("resp %s", in.Resp.Inline)
	}
}

func TestDisabledPassThrough(t *testing.T) {
	core.Configure(core.Config{Enabled: false})
	defer core.Configure(core.Config{})
	u := UnaryServerInterceptor()
	called := false
	resp, err := u(context.Background(), "req", &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		called = true
		return "ok", nil
	})
	if !called || resp != "ok" || err != nil {
		t.Fatal("pass-through broken")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout")
}

func TestMessagesRedactedAndStreamCap(t *testing.T) {
	st, err := structpb.NewStruct(map[string]any{"user": "ann", "password": "pw-grpc-1",
		"nested": map[string]any{"api_key": "key-grpc-2", "big": float64(9007199254740993)}})
	if err != nil {
		t.Fatal(err)
	}
	got := string(toJSON(st))
	if strings.Contains(got, "pw-grpc-1") || strings.Contains(got, "key-grpc-2") || !strings.Contains(got, "ann") {
		t.Fatalf("%s", got)
	}
	if got := string(toJSON(map[string]any{"Authorization": "Bearer abc", "n": 1})); strings.Contains(got, "abc") {
		t.Fatal(got)
	}
	var l msgLog
	for i := 0; i < maxStreamMsgs+25; i++ {
		l.add(wrapperspb.String("m"))
	}
	if len(l.msgs) != maxStreamMsgs || !l.more {
		t.Fatalf("cap not applied: %d", len(l.msgs))
	}
}
