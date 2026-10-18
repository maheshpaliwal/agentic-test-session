package runtime

import (
	"bytes"
	"encoding/json"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// BatchEncoder serialises a batch of events for POST /v1/ingest. The default
// is NDJSON. Protobuf is opt-in from the separate proto module, which keeps
// this module stdlib-only:
//
//	runtime.Start(runtime.Config{Encoder: tsessionproto.Encoder{}})
//
// (import "github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto").
// sdk/autoinit always uses NDJSON.
type BatchEncoder interface {
	// ContentType is sent as the request Content-Type; the collector picks its
	// decoder from it.
	ContentType() string
	// Encode must not retain batch. An error drops the batch (counted like
	// any failed export).
	Encode(batch []core.Event) ([]byte, error)
}

// NDJSONEncoder is the default encoder: one JSON event per line.
type NDJSONEncoder struct{}

func (NDJSONEncoder) ContentType() string { return "application/x-ndjson" }

func (NDJSONEncoder) Encode(batch []core.Event) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range batch {
		_ = enc.Encode(&batch[i])
	}
	return buf.Bytes(), nil
}
