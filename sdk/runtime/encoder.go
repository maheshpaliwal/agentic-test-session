package runtime

import "github.com/maheshpaliwal/agentic-test-session/core"

// BatchEncoder serialises a batch of events for POST /v1/ingest. The default
// is ProtobufEncoder, the only format the collector accepts. It is an
// extension point for tests and custom transports; any replacement must
// produce what the collector decodes for its Content-Type.
type BatchEncoder interface {
	// ContentType is sent as the request Content-Type; the collector picks its
	// decoder from it.
	ContentType() string
	// Encode must not retain batch. An error drops the batch (counted like
	// any failed export).
	Encode(batch []core.Event) ([]byte, error)
}
