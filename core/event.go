// Package core holds the SDK's shared contracts: event model, session context,
// propagation, spans and the recorder seam. It is stdlib-only.
package core

import "encoding/json"

type Kind string

const (
	KindAPIIn       Kind = "api.in"
	KindAPIOut      Kind = "api.out"
	KindDBRead      Kind = "db.read"
	KindDBWrite     Kind = "db.write"
	KindDBTxBegin   Kind = "db.tx.begin"
	KindDBTxCommit  Kind = "db.tx.commit"
	KindDBTxRollbck Kind = "db.tx.rollback"
	KindCacheRead   Kind = "cache.read"
	KindCacheWrite  Kind = "cache.write"
	KindQueuePub    Kind = "queue.publish"
	KindQueueCons   Kind = "queue.consume"
	KindWorkerStart Kind = "worker.start"
	KindWorkerEnd   Kind = "worker.end"
	KindWorkerRetry Kind = "worker.retry"
	KindWorkerDLQ   Kind = "worker.dlq"
	KindSessStart   Kind = "session.start"
	KindSessStop    Kind = "session.stop"
	// KindSDKDropped is emitted by the exporter when events were dropped
	// (buffer overflow). Attrs["count"] holds the number dropped.
	KindSDKDropped Kind = "sdk.dropped"
)

// PayloadRef is an inline (size-capped, redacted) JSON payload. BlobHash is
// reserved for a future blob store; v1 truncates instead of spilling.
type PayloadRef struct {
	Inline    json.RawMessage `json:"inline,omitempty"`
	BlobHash  string          `json:"blob_hash,omitempty"`
	Size      int             `json:"size,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
	Redacted  bool            `json:"redacted,omitempty"`
}

func (p PayloadRef) IsZero() bool { return len(p.Inline) == 0 && p.BlobHash == "" }

type FieldChange struct {
	Field string `json:"field"`
	Old   any    `json:"old,omitempty"`
	New   any    `json:"new,omitempty"`
}

type ReadSummary struct {
	Store     string     `json:"store"`
	Entity    string     `json:"entity,omitempty"`
	Statement string     `json:"statement,omitempty"`
	Args      PayloadRef `json:"args,omitempty"`
	RowCount  int        `json:"row_count,omitempty"`
	PKs       []string   `json:"pks,omitempty"`
	Sample    PayloadRef `json:"sample,omitempty"`
}

type Mutation struct {
	Store     string         `json:"store"`  // postgres|mysql|mongo|dynamodb|redis
	Entity    string         `json:"entity"` // table / collection / key pattern
	PK        map[string]any `json:"pk,omitempty"`
	Op        string         `json:"op"` // insert|update|delete|upsert|expire|...
	Before    PayloadRef     `json:"before,omitempty"`
	After     PayloadRef     `json:"after,omitempty"`
	Diff      []FieldChange  `json:"diff,omitempty"`
	Source    string         `json:"source,omitempty"` // sdk | cdc
	Effective *bool          `json:"effective,omitempty"`
}

// Event is one recorded step. The SDK leaves Seq zero; the collector assigns it.
type Event struct {
	SessionID string   `json:"session_id"`
	TraceID   string   `json:"trace_id"`
	SpanID    string   `json:"span_id"`
	ParentID  string   `json:"parent_id,omitempty"`
	LinkIDs   []string `json:"link_ids,omitempty"` // follows-from (worker -> publisher span)
	Service   string   `json:"service"`
	Instance  string   `json:"instance"`
	Kind      Kind     `json:"kind"`
	Name      string   `json:"name,omitempty"` // "GET /orders", SQL text, job name, ...

	HLC      uint64 `json:"hlc"`       // hybrid logical clock at span start
	EndHLC   uint64 `json:"end_hlc"`   // at span end; collector orders by this
	LocalSeq uint64 `json:"local_seq"` // per-process monotonic; idempotency key with Instance
	Seq      uint64 `json:"seq,omitempty"`

	StartNs int64 `json:"start_ns"`
	EndNs   int64 `json:"end_ns"`

	Req       PayloadRef        `json:"req,omitempty"`
	Resp      PayloadRef        `json:"resp,omitempty"`
	Mutations []Mutation        `json:"mutations,omitempty"`
	Reads     []ReadSummary     `json:"reads,omitempty"`
	TxID      string            `json:"tx_id,omitempty"`
	Attempt   int               `json:"attempt,omitempty"`
	Err       string            `json:"err,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
	Flags     []string          `json:"flags,omitempty"` // unattributed, truncated, redacted, ...
}
