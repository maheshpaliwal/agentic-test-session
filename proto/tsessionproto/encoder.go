package tsessionproto

import "github.com/maheshpaliwal/agentic-test-session/core"

// Encoder is a runtime.BatchEncoder backed by the generated protobuf code. It
// is the reference implementation: the SDK default (runtime.ProtobufEncoder,
// hand-written so the root module stays stdlib-only) is conformance-tested
// against it, and non-SDK producers that already depend on protobuf can use it
// directly.
type Encoder struct{}

// ContentType returns the ingest media type, application/x-protobuf.
func (Encoder) ContentType() string { return ContentType }

// Encode frames batch as length-delimited Event messages.
func (Encoder) Encode(batch []core.Event) ([]byte, error) { return Encode(batch) }
