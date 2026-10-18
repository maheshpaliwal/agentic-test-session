// Package tsessionproto is the protobuf wire codec for tsession events:
// conversion between core.Event and tsessionpb.Event, varint-length-delimited
// framing, a streaming Decoder with a per-message size cap, and a
// runtime.BatchEncoder so SDKs can opt into protobuf ingest:
//
//	runtime.Start(runtime.Config{Service: "orders", Encoder: tsessionproto.Encoder{}})
//
// It lives in its own module so the root module stays stdlib-only.
package tsessionproto

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/maheshpaliwal/agentic-test-session/core"
	pb "github.com/maheshpaliwal/agentic-test-session/proto/tsessionpb"
)

// FromCore converts an event to its wire form. It never fails: invalid UTF-8
// is replaced with U+FFFD (as encoding/json does) and values in `any` fields
// that protobuf cannot represent natively are normalised through JSON.
func FromCore(e core.Event) *pb.Event {
	out := &pb.Event{
		SessionId: toUTF8(e.SessionID), TraceId: toUTF8(e.TraceID), SpanId: toUTF8(e.SpanID),
		ParentId: toUTF8(e.ParentID), LinkIds: strs(e.LinkIDs),
		Service: toUTF8(e.Service), Instance: toUTF8(e.Instance), Kind: toUTF8(string(e.Kind)), Name: toUTF8(e.Name),
		Hlc: e.HLC, EndHlc: e.EndHLC, LocalSeq: e.LocalSeq, Seq: e.Seq,
		StartNs: e.StartNs, EndNs: e.EndNs,
		Req: fromPayload(e.Req), Resp: fromPayload(e.Resp),
		TxId: toUTF8(e.TxID), Attempt: int64(e.Attempt), Err: toUTF8(e.Err), Flags: strs(e.Flags),
	}
	if len(e.Mutations) > 0 {
		out.Mutations = make([]*pb.Mutation, len(e.Mutations))
		for i, m := range e.Mutations {
			out.Mutations[i] = fromMutation(m)
		}
	}
	if len(e.Reads) > 0 {
		out.Reads = make([]*pb.ReadSummary, len(e.Reads))
		for i, r := range e.Reads {
			out.Reads[i] = &pb.ReadSummary{
				Store: toUTF8(r.Store), Entity: toUTF8(r.Entity), Statement: toUTF8(r.Statement),
				Args: fromPayload(r.Args), RowCount: int64(r.RowCount), Pks: strs(r.PKs), Sample: fromPayload(r.Sample),
			}
		}
	}
	if len(e.Attrs) > 0 {
		out.Attrs = make(map[string]string, len(e.Attrs))
		for k, v := range e.Attrs {
			out.Attrs[toUTF8(k)] = toUTF8(v)
		}
	}
	return out
}

func fromMutation(m core.Mutation) *pb.Mutation {
	out := &pb.Mutation{
		Store: toUTF8(m.Store), Entity: toUTF8(m.Entity), Op: toUTF8(m.Op), Source: toUTF8(m.Source),
		Before: fromPayload(m.Before), After: fromPayload(m.After), Effective: m.Effective,
	}
	if len(m.PK) > 0 {
		out.Pk = make(map[string]*structpb.Value, len(m.PK))
		for k, v := range m.PK {
			out.Pk[toUTF8(k)] = toValue(v)
		}
	}
	if len(m.Diff) > 0 {
		out.Diff = make([]*pb.FieldChange, len(m.Diff))
		for i, d := range m.Diff {
			fc := &pb.FieldChange{Field: toUTF8(d.Field)}
			if d.Old != nil {
				fc.Old = toValue(d.Old)
			}
			if d.New != nil {
				fc.New = toValue(d.New)
			}
			out.Diff[i] = fc
		}
	}
	return out
}

func fromPayload(p core.PayloadRef) *pb.PayloadRef {
	if len(p.Inline) == 0 && p.BlobHash == "" && p.Size == 0 && !p.Truncated && !p.Redacted {
		return nil
	}
	return &pb.PayloadRef{Inline: p.Inline, BlobHash: toUTF8(p.BlobHash), Size: int64(p.Size), Truncated: p.Truncated, Redacted: p.Redacted}
}

// ToCore converts a decoded message to the core model. NUL characters in
// strings (and \u0000 escapes in inline JSON) become U+FFFD because Postgres
// rejects them; this is the protobuf counterpart of store.StripNUL. It fails
// only when an inline payload is not valid JSON.
func ToCore(e *pb.Event) (core.Event, error) {
	if e == nil {
		return core.Event{}, nil
	}
	out := core.Event{
		SessionID: nonul(e.SessionId), TraceID: nonul(e.TraceId), SpanID: nonul(e.SpanId), ParentID: nonul(e.ParentId),
		LinkIDs: nonuls(e.LinkIds), Service: nonul(e.Service), Instance: nonul(e.Instance),
		Kind: core.Kind(nonul(e.Kind)), Name: nonul(e.Name),
		HLC: e.Hlc, EndHLC: e.EndHlc, LocalSeq: e.LocalSeq, Seq: e.Seq,
		StartNs: e.StartNs, EndNs: e.EndNs,
		TxID: nonul(e.TxId), Attempt: int(e.Attempt), Err: nonul(e.Err), Flags: nonuls(e.Flags),
	}
	var err error
	if out.Req, err = toPayload(e.Req); err != nil {
		return core.Event{}, fmt.Errorf("req: %w", err)
	}
	if out.Resp, err = toPayload(e.Resp); err != nil {
		return core.Event{}, fmt.Errorf("resp: %w", err)
	}
	if len(e.Mutations) > 0 {
		out.Mutations = make([]core.Mutation, len(e.Mutations))
		for i, m := range e.Mutations {
			if out.Mutations[i], err = toMutation(m); err != nil {
				return core.Event{}, fmt.Errorf("mutations[%d]: %w", i, err)
			}
		}
	}
	if len(e.Reads) > 0 {
		out.Reads = make([]core.ReadSummary, len(e.Reads))
		for i, r := range e.Reads {
			rs := core.ReadSummary{Store: nonul(r.GetStore()), Entity: nonul(r.GetEntity()), Statement: nonul(r.GetStatement()),
				RowCount: int(r.GetRowCount()), PKs: nonuls(r.GetPks())}
			if rs.Args, err = toPayload(r.GetArgs()); err != nil {
				return core.Event{}, fmt.Errorf("reads[%d].args: %w", i, err)
			}
			if rs.Sample, err = toPayload(r.GetSample()); err != nil {
				return core.Event{}, fmt.Errorf("reads[%d].sample: %w", i, err)
			}
			out.Reads[i] = rs
		}
	}
	if len(e.Attrs) > 0 {
		out.Attrs = make(map[string]string, len(e.Attrs))
		for k, v := range e.Attrs {
			out.Attrs[nonul(k)] = nonul(v)
		}
	}
	return out, nil
}

func toMutation(m *pb.Mutation) (core.Mutation, error) {
	out := core.Mutation{
		Store: nonul(m.GetStore()), Entity: nonul(m.GetEntity()), Op: nonul(m.GetOp()), Source: nonul(m.GetSource()),
		Effective: m.Effective,
	}
	var err error
	if out.Before, err = toPayload(m.GetBefore()); err != nil {
		return out, fmt.Errorf("before: %w", err)
	}
	if out.After, err = toPayload(m.GetAfter()); err != nil {
		return out, fmt.Errorf("after: %w", err)
	}
	if pk := m.GetPk(); len(pk) > 0 {
		out.PK = make(map[string]any, len(pk))
		for k, v := range pk {
			out.PK[nonul(k)] = fromValue(v)
		}
	}
	if d := m.GetDiff(); len(d) > 0 {
		out.Diff = make([]core.FieldChange, len(d))
		for i, fc := range d {
			out.Diff[i] = core.FieldChange{Field: nonul(fc.GetField()), Old: fromValue(fc.GetOld()), New: fromValue(fc.GetNew())}
		}
	}
	return out, nil
}

func toPayload(p *pb.PayloadRef) (core.PayloadRef, error) {
	if p == nil {
		return core.PayloadRef{}, nil
	}
	out := core.PayloadRef{BlobHash: nonul(p.BlobHash), Size: int(p.Size), Truncated: p.Truncated, Redacted: p.Redacted}
	if len(p.Inline) > 0 {
		// The NDJSON path validates inline payloads while decoding the line;
		// do the same so the store never sees malformed RawMessage.
		if !json.Valid(p.Inline) {
			return core.PayloadRef{}, fmt.Errorf("inline payload is not valid JSON")
		}
		out.Inline = stripNULEscapes(p.Inline)
	}
	return out, nil
}

// toValue converts a JSON-shaped Go value to a protobuf Value. Numbers become
// doubles, as they would after a JSON round trip.
func toValue(v any) *structpb.Value {
	switch x := v.(type) {
	case nil:
		return structpb.NewNullValue()
	case bool:
		return structpb.NewBoolValue(x)
	case string:
		return structpb.NewStringValue(toUTF8(x))
	case float64:
		return number(x)
	case float32:
		return number(float64(x))
	case int:
		return number(float64(x))
	case int8:
		return number(float64(x))
	case int16:
		return number(float64(x))
	case int32:
		return number(float64(x))
	case int64:
		return number(float64(x))
	case uint:
		return number(float64(x))
	case uint8:
		return number(float64(x))
	case uint16:
		return number(float64(x))
	case uint32:
		return number(float64(x))
	case uint64:
		return number(float64(x))
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return structpb.NewNullValue()
		}
		return number(f)
	case []any:
		l := make([]*structpb.Value, len(x))
		for i, e := range x {
			l[i] = toValue(e)
		}
		return structpb.NewListValue(&structpb.ListValue{Values: l})
	case map[string]any:
		f := make(map[string]*structpb.Value, len(x))
		for k, e := range x {
			f[toUTF8(k)] = toValue(e)
		}
		return structpb.NewStructValue(&structpb.Struct{Fields: f})
	}
	// Any other type: normalise through JSON, exactly what the NDJSON path
	// would have put on the wire. Unmarshalable values become null.
	b, err := json.Marshal(v)
	if err != nil {
		return structpb.NewNullValue()
	}
	var n any
	if json.Unmarshal(b, &n) != nil {
		return structpb.NewNullValue()
	}
	switch n.(type) {
	case nil, bool, string, float64, []any, map[string]any:
		return toValue(n)
	}
	return structpb.NewNullValue()
}

func number(f float64) *structpb.Value {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return structpb.NewNullValue() // not representable in JSON either
	}
	return structpb.NewNumberValue(f)
}

// fromValue is Value.AsInterface plus NUL replacement in strings and keys.
func fromValue(v *structpb.Value) any {
	switch k := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return nonul(k.StringValue)
	case *structpb.Value_ListValue:
		l := make([]any, len(k.ListValue.GetValues()))
		for i, e := range k.ListValue.GetValues() {
			l[i] = fromValue(e)
		}
		return l
	case *structpb.Value_StructValue:
		m := make(map[string]any, len(k.StructValue.GetFields()))
		for key, e := range k.StructValue.GetFields() {
			m[nonul(key)] = fromValue(e)
		}
		return m
	}
	return nil
}

func strs(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = toUTF8(s)
	}
	return out
}

func nonuls(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = nonul(s)
	}
	return out
}

func toUTF8(s string) string { return strings.ToValidUTF8(s, "�") }

func nonul(s string) string {
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(s, "\x00", "�")
}

// stripNULEscapes replaces JSON "\u0000" escapes with "�", leaving an
// escaped backslash followed by the text u0000 alone (same rule as
// store.StripNUL). It returns b itself when nothing needed replacing.
func stripNULEscapes(b []byte) []byte {
	var out []byte
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			if out != nil {
				out = append(out, b[i])
			}
			continue
		}
		if b[i+1] == 'u' && i+6 <= len(b) && string(b[i+2:i+6]) == "0000" {
			if out == nil {
				out = append(make([]byte, 0, len(b)), b[:i]...)
			}
			out = append(out, `�`...)
			i += 5
			continue
		}
		if out != nil {
			out = append(out, b[i], b[i+1])
		}
		i++ // skip the escaped byte
	}
	if out == nil {
		return b
	}
	return out
}
