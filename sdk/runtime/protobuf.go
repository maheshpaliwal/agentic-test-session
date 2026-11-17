package runtime

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// ProtobufEncoder is the default BatchEncoder: a stream of varint-length-
// delimited tsession.v1.Event messages (proto/tsession/v1/event.proto). It is
// a hand-written proto3 wire encoder because the root module must stay
// stdlib-only; proto/tsessionproto holds the generated-code reference and the
// conformance tests that keep the two byte-compatible.
//
// Normalisations match the reference: invalid UTF-8 becomes U+FFFD, NaN/±Inf
// become null, and `any` values of unusual Go types go through encoding/json.
// Output is deterministic (map keys sorted).
type ProtobufEncoder struct{}

// ContentType implements BatchEncoder.
func (ProtobufEncoder) ContentType() string { return "application/x-protobuf" }

// scratch buffers are pooled so steady-state encoding costs one allocation
// (the exact-size result the caller owns).
var scratchPool = sync.Pool{New: func() any { b := make([]byte, 0, 32<<10); return &b }}

// maxPooledScratch keeps one huge batch from pinning memory in the pool.
const maxPooledScratch = 4 << 20

// Encode implements BatchEncoder: batch as length-delimited Event messages.
func (ProtobufEncoder) Encode(batch []core.Event) ([]byte, error) {
	sp := scratchPool.Get().(*[]byte)
	b := (*sp)[:0]
	for i := range batch {
		start := len(b)
		b = append(b, 0) // reserve a 1-byte length prefix, widened by endMsg
		b = appendEvent(b, &batch[i])
		b = endMsg(b, start)
	}
	out := make([]byte, len(b))
	copy(out, b)
	if cap(b) <= maxPooledScratch {
		*sp = b[:0]
		scratchPool.Put(sp)
	}
	return out, nil
}

// Wire types.
const (
	wVarint  = 0
	wFixed64 = 1
	wBytes   = 2
)

// putTag appends a field key (field<<3 | wiretype); fields 16+ take two bytes.
func putTag(b []byte, field, wire int) []byte {
	if k := field<<3 | wire; k < 0x80 {
		return append(b, byte(k))
	} else {
		return append(b, byte(k)|0x80, byte(k>>7))
	}
}

// endMsg closes a length-delimited body that starts at start+1 behind a
// reserved prefix byte at start, widening the prefix in place when the body is
// 128 bytes or more. Writing then prefixing avoids a separate sizing pass and
// any per-message allocation.
func endMsg(b []byte, start int) []byte {
	n := len(b) - start - 1
	if n < 0x80 {
		b[start] = byte(n)
		return b
	}
	var tmp [binary.MaxVarintLen64]byte
	k := binary.PutUvarint(tmp[:], uint64(n))
	b = append(b, tmp[:k-1]...) // grow by the extra prefix bytes
	copy(b[start+k:], b[start+1:len(b)-(k-1)])
	copy(b[start:], tmp[:k])
	return b
}

// beginField writes the tag and reserves the length byte; returns its offset.
func beginField(b []byte, field int) ([]byte, int) {
	b = append(putTag(b, field, wBytes), 0)
	return b, len(b) - 1
}

func appendStr(b []byte, field int, s string) []byte {
	if s == "" {
		return b
	}
	return appendStrAlways(b, field, s)
}

func appendStrAlways(b []byte, field int, s string) []byte {
	b = putTag(b, field, wBytes)
	if utf8.ValidString(s) {
		b = binary.AppendUvarint(b, uint64(len(s)))
		return append(b, s...)
	}
	s = strings.ToValidUTF8(s, "�")
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendUint(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	return binary.AppendUvarint(putTag(b, field, wVarint), v)
}

func appendInt(b []byte, field int, v int64) []byte {
	return appendUint(b, field, uint64(v))
}

func appendBool(b []byte, field int, v bool) []byte {
	if !v {
		return b
	}
	return append(putTag(b, field, wVarint), 1)
}

func appendStrs(b []byte, field int, ss []string) []byte {
	for _, s := range ss {
		b = appendStrAlways(b, field, s) // repeated: empty elements are kept
	}
	return b
}

func appendEvent(b []byte, e *core.Event) []byte {
	b = appendStr(b, 1, e.SessionID)
	b = appendStr(b, 2, e.TraceID)
	b = appendStr(b, 3, e.SpanID)
	b = appendStr(b, 4, e.ParentID)
	b = appendStrs(b, 5, e.LinkIDs)
	b = appendStr(b, 6, e.Service)
	b = appendStr(b, 7, e.Instance)
	b = appendStr(b, 8, string(e.Kind))
	b = appendStr(b, 9, e.Name)
	b = appendUint(b, 10, e.HLC)
	b = appendUint(b, 11, e.EndHLC)
	b = appendUint(b, 12, e.LocalSeq)
	b = appendUint(b, 13, e.Seq)
	b = appendInt(b, 14, e.StartNs)
	b = appendInt(b, 15, e.EndNs)
	b = appendPayload(b, 16, &e.Req)
	b = appendPayload(b, 17, &e.Resp)
	for i := range e.Mutations {
		var p int
		b, p = beginField(b, 18)
		b = appendMutation(b, &e.Mutations[i])
		b = endMsg(b, p)
	}
	for i := range e.Reads {
		var p int
		b, p = beginField(b, 19)
		b = appendRead(b, &e.Reads[i])
		b = endMsg(b, p)
	}
	b = appendStr(b, 20, e.TxID)
	b = appendInt(b, 21, int64(e.Attempt))
	b = appendStr(b, 22, e.Err)
	b = appendStrMap(b, 23, e.Attrs)
	return appendStrs(b, 24, e.Flags)
}

func appendMutation(b []byte, m *core.Mutation) []byte {
	b = appendStr(b, 1, m.Store)
	b = appendStr(b, 2, m.Entity)
	b = appendAnyMap(b, 3, m.PK)
	b = appendStr(b, 4, m.Op)
	b = appendPayload(b, 5, &m.Before)
	b = appendPayload(b, 6, &m.After)
	for i := range m.Diff {
		d := &m.Diff[i]
		var p int
		b, p = beginField(b, 7)
		b = appendStr(b, 1, d.Field)
		if d.Old != nil {
			b = appendValueField(b, 2, d.Old)
		}
		if d.New != nil {
			b = appendValueField(b, 3, d.New)
		}
		b = endMsg(b, p)
	}
	b = appendStr(b, 8, m.Source)
	if m.Effective != nil { // optional: presence is the tri-state
		v := byte(0)
		if *m.Effective {
			v = 1
		}
		b = append(putTag(b, 9, wVarint), v)
	}
	return b
}

func appendRead(b []byte, r *core.ReadSummary) []byte {
	b = appendStr(b, 1, r.Store)
	b = appendStr(b, 2, r.Entity)
	b = appendStr(b, 3, r.Statement)
	b = appendPayload(b, 4, &r.Args)
	b = appendInt(b, 5, int64(r.RowCount))
	b = appendStrs(b, 6, r.PKs)
	return appendPayload(b, 7, &r.Sample)
}

// appendPayload omits an all-zero PayloadRef, as the reference does (nil msg).
func appendPayload(b []byte, field int, p *core.PayloadRef) []byte {
	if len(p.Inline) == 0 && p.BlobHash == "" && p.Size == 0 && !p.Truncated && !p.Redacted {
		return b
	}
	var pos int
	b, pos = beginField(b, field)
	if len(p.Inline) > 0 {
		b = putTag(b, 1, wBytes)
		b = binary.AppendUvarint(b, uint64(len(p.Inline)))
		b = append(b, p.Inline...)
	}
	b = appendStr(b, 2, p.BlobHash)
	b = appendInt(b, 3, int64(p.Size))
	b = appendBool(b, 4, p.Truncated)
	b = appendBool(b, 5, p.Redacted)
	return endMsg(b, pos)
}

// sortedKeys returns the map keys ordered by their sanitised form so output
// is deterministic. The slice is small and short-lived.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// map<string,string> entries always carry both key and value, like protobuf-go.
func appendStrMap(b []byte, field int, m map[string]string) []byte {
	for _, k := range sortedKeys(m) {
		var p int
		b, p = beginField(b, field)
		b = appendStrAlways(b, 1, k)
		b = appendStrAlways(b, 2, m[k])
		b = endMsg(b, p)
	}
	return b
}

// map<string,google.protobuf.Value> (Mutation.pk).
func appendAnyMap(b []byte, field int, m map[string]any) []byte {
	for _, k := range sortedKeys(m) {
		var p int
		b, p = beginField(b, field)
		b = appendStrAlways(b, 1, k)
		b = appendValueField(b, 2, m[k])
		b = endMsg(b, p)
	}
	return b
}

// appendValueField writes v as a google.protobuf.Value message at field.
func appendValueField(b []byte, field int, v any) []byte {
	var p int
	b, p = beginField(b, field)
	b = appendValue(b, v)
	return endMsg(b, p)
}

// Value oneof: null=1 number=2 string=3 bool=4 struct=5 list=6.
func appendValue(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return appendNull(b)
	case bool:
		return append(putTag(b, 4, wVarint), boolByte(x))
	case string:
		return appendStrAlways(b, 3, x)
	case float64:
		return appendNumber(b, x)
	case float32:
		return appendNumber(b, float64(x))
	case int:
		return appendNumber(b, float64(x))
	case int8:
		return appendNumber(b, float64(x))
	case int16:
		return appendNumber(b, float64(x))
	case int32:
		return appendNumber(b, float64(x))
	case int64:
		return appendNumber(b, float64(x))
	case uint:
		return appendNumber(b, float64(x))
	case uint8:
		return appendNumber(b, float64(x))
	case uint16:
		return appendNumber(b, float64(x))
	case uint32:
		return appendNumber(b, float64(x))
	case uint64:
		return appendNumber(b, float64(x))
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return appendNull(b)
		}
		return appendNumber(b, f)
	case []any:
		var p int
		b, p = beginField(b, 6)
		for _, e := range x {
			b = appendValueField(b, 1, e)
		}
		return endMsg(b, p)
	case map[string]any:
		var p int
		b, p = beginField(b, 5)
		for _, k := range sortedKeys(x) {
			var q int
			b, q = beginField(b, 1) // Struct.fields entry
			b = appendStrAlways(b, 1, k)
			b = appendValueField(b, 2, x[k])
			b = endMsg(b, q)
		}
		return endMsg(b, p)
	}
	// Any other type: normalise through JSON; unmarshalable values become null.
	raw, err := json.Marshal(v)
	if err != nil {
		return appendNull(b)
	}
	var n any
	if json.Unmarshal(raw, &n) != nil {
		return appendNull(b)
	}
	switch n.(type) {
	case nil, bool, string, float64, []any, map[string]any:
		return appendValue(b, n)
	}
	return appendNull(b)
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func appendNull(b []byte) []byte { return append(putTag(b, 1, wVarint), 0) }

func appendNumber(b []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return appendNull(b) // not representable in JSON either
	}
	b = putTag(b, 2, wFixed64)
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
}
