package tgrpc_test

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tgrpc"
)

// The setup of the package comment: keep the two in step.
func Example() {
	// After the SDK has started (sdk/autoinit, or runtime.Start in main).
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tgrpc.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(tgrpc.StreamServerInterceptor()))
	defer srv.Stop()

	conn, err := grpc.NewClient("dns:///orders:9090",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(tgrpc.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(tgrpc.StreamClientInterceptor()))
	if err != nil {
		return
	}
	defer conn.Close()
}
