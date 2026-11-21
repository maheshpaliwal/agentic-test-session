// Package tgrpc provides gRPC client and server interceptors that join the
// active test session: server calls become api.in spans and client calls
// api.out spans, with trace context carried in gRPC metadata.
//
//	srv := grpc.NewServer(
//		grpc.ChainUnaryInterceptor(tgrpc.UnaryServerInterceptor()),
//		grpc.ChainStreamInterceptor(tgrpc.StreamServerInterceptor()))
//
//	conn, err := grpc.NewClient(target,
//		grpc.WithChainUnaryInterceptor(tgrpc.UnaryClientInterceptor()),
//		grpc.WithChainStreamInterceptor(tgrpc.StreamClientInterceptor()))
//
// Build the interceptors after the SDK has started (sdk/autoinit, or
// runtime.Start in main): one built earlier passes every call through and
// records nothing, which the SDK reports when it starts.
//
// Failures. A call that ends with any status other than OK is recorded with
// its error, so it counts as a failed event; the status is in the attribute
// grpc.code. That includes the statuses a caller causes or expects: NotFound,
// InvalidArgument, AlreadyExists, PermissionDenied, Canceled. HTTP is
// recorded differently: a 4xx response is not a failure, only a 5xx is. A
// lookup that misses therefore fails a session over gRPC and passes it over
// HTTP; filter on grpc.code when reading failures.
//
// Messages are recorded as JSON with sensitive field names masked (core.RedactAny);
// stream messages are capped at 50 per direction. Metadata is never recorded.
//
// The interceptors have no double-wrap marker: unlike thttp/tredis/tmongo,
// installing the same interceptor twice on one server or client produces two
// spans per call. Install each interceptor exactly once.
package tgrpc
