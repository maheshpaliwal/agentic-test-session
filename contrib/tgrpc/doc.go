// Package tgrpc provides gRPC client and server interceptors that join the
// active test session: server calls become api.in spans and client calls
// api.out spans, with trace context carried in gRPC metadata.
package tgrpc
