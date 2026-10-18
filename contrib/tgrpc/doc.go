// Package tgrpc provides gRPC client and server interceptors that join the
// active test session: server calls become api.in spans and client calls
// api.out spans, with trace context carried in gRPC metadata.
//
// Messages are recorded as JSON with sensitive field names masked (core.RedactAny);
// stream messages are capped at 50 per direction. Metadata is never recorded.
//
// The interceptors have no double-wrap marker: unlike thttp/tredis/tmongo,
// installing the same interceptor twice on one server or client produces two
// spans per call. Install each interceptor exactly once.
package tgrpc
