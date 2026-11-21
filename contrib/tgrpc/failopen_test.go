package tgrpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type panicsOnMarshal struct{ M map[string]any }

func (panicsOnMarshal) MarshalJSON() ([]byte, error) {
	panic("marshal failed badly")
}

// Recording a message must never take the RPC down, whatever its marshalling does.
func TestUnaryServerSurvivesUnmarshalableMessages(t *testing.T) {
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(&core.MemRecorder{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()
	ic := UnaryServerInterceptor()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(core.KeySession, "s1"))
	msgs := map[string]any{
		"nil element in repeated message":    &descriptorpb.FileDescriptorProto{MessageType: []*descriptorpb.DescriptorProto{nil}},
		"typed nil proto":                    (*descriptorpb.FileDescriptorProto)(nil),
		"nil struct value":                   &structpb.ListValue{Values: []*structpb.Value{nil}},
		"host type whose MarshalJSON panics": panicsOnMarshal{},
	}
	for name, m := range msgs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: panic escaped the interceptor into the host RPC: %v", name, r)
				}
			}()
			_, err := ic(ctx, m, &grpc.UnaryServerInfo{FullMethod: "/svc/M"}, func(ctx context.Context, req any) (any, error) { return m, nil })
			t.Logf("%s: ok err=%v", name, err)
		}()
	}
}
