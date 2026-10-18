package tsessionproto

import "github.com/maheshpaliwal/agentic-test-session/core"

// Encoder is a runtime.BatchEncoder that posts framed protobuf instead of
// NDJSON. The collector selects the decoder from the Content-Type.
type Encoder struct{}

func (Encoder) ContentType() string { return ContentType }

func (Encoder) Encode(batch []core.Event) ([]byte, error) { return Encode(batch) }
