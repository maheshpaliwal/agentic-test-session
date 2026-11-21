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
// Failures. Every call's status is in the attribute grpc.code. A call counts
// as a failed event when its status says the callee failed, as an HTTP 5xx
// does: Unknown (also any error that is not a status), DeadlineExceeded,
// Unimplemented, Internal, Unavailable, DataLoss. The statuses a caller
// causes or expects (NotFound, InvalidArgument, AlreadyExists,
// PermissionDenied, Unauthenticated, FailedPrecondition, Aborted,
// OutOfRange, ResourceExhausted, Canceled) are recorded with their message
// in grpc.message and do not fail a session, as an HTTP 4xx does not: a
// lookup that misses is the same event over either protocol.
//
// Messages are recorded as JSON with sensitive field names masked (core.RedactAny);
// stream messages are capped at 50 per direction. Metadata is never recorded.
//
// The interceptors have no double-wrap marker: unlike thttp/tredis/tmongo,
// installing the same interceptor twice on one server or client produces two
// spans per call. Install each interceptor exactly once.
package tgrpc
