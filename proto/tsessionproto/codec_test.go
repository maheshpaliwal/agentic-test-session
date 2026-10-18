package tsessionproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

var _ runtime.BatchEncoder = Encoder{}

func bptr(b bool) *bool { return &b }

func fullEvent() core.Event {
	return core.Event{
		SessionID: "ts_01", TraceID: "t1", SpanID: "s1", ParentID: "s0", LinkIDs: []string{"a", "b"},
		Service: "orders", Instance: "i-1", Kind: core.KindDBWrite, Name: "INSERT INTO t",
		HLC: 1<<63 + 5, EndHLC: 1<<63 + 9, LocalSeq: 7, Seq: 42, StartNs: 1700000000000000000, EndNs: 1700000000000001000,
		Req:  core.PayloadRef{Inline: json.RawMessage(`{"a":[1,2,{"b":null}]}`), Size: 22, Truncated: true, Redacted: true},
		Resp: core.PayloadRef{BlobHash: "abc", Size: 1 << 20},
		Mutations: []core.Mutation{{
			Store: "postgres", Entity: "orders", Op: "update", Source: "sdk", Effective: bptr(false),
			PK:     map[string]any{"id": 12.0, "k": "v", "n": nil, "ok": true, "l": []any{1.5, "x", nil}, "m": map[string]any{"z": 1.0}},
			Before: core.PayloadRef{Inline: json.RawMessage(`{"s":"a"}`)}, After: core.PayloadRef{Inline: json.RawMessage(`{"s":"b"}`), Redacted: true},
			Diff: []core.FieldChange{{Field: "s", Old: "a", New: "b"}, {Field: "gone", Old: 3.0}, {Field: "added", New: false}},
		}, {Store: "redis", Entity: "k:*", Op: "expire", Effective: bptr(true)}, {Store: "mongo", Op: "insert"}},
		Reads: []core.ReadSummary{{Store: "postgres", Entity: "orders", Statement: "SELECT 1", Args: core.PayloadRef{Inline: json.RawMessage(`[1]`)},
			RowCount: 3, PKs: []string{"1", "2"}, Sample: core.PayloadRef{Inline: json.RawMessage(`[{"id":1}]`), Truncated: true}}},
		TxID: "tx", Attempt: 3, Err: "boom", Attrs: map[string]string{"a": "b", "count": "3"}, Flags: []string{"unattributed"},
	}
}

func roundTrip(t *testing.T, in core.Event) core.Event {
	t.Helper()
	b, err := AppendFramed(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewDecoder(bytes.NewReader(b), 1<<20).Next()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRoundTripAllFields(t *testing.T) {
	in := fullEvent()
	if out := roundTrip(t, in); !reflect.DeepEqual(in, out) {
		t.Fatalf("mismatch\n in: %+v\nout: %+v", in, out)
	}
}

func TestRoundTripMatchesJSON(t *testing.T) {
	// ints in `any` become float64 exactly as after a JSON decode.
	in := fullEvent()
	in.Mutations[0].PK = map[string]any{"id": 12, "big": int64(1) << 40, "u": uint8(3), "f32": float32(0.5), "ss": []string{"a"}, "ms": map[string]int{"q": 1}}
	in.Mutations[0].Diff = []core.FieldChange{{Field: "n", Old: 1, New: int64(2)}}
	var want core.Event
	j, _ := json.Marshal(in)
	if err := json.Unmarshal(j, &want); err != nil {
		t.Fatal(err)
	}
	if got := roundTrip(t, in); !reflect.DeepEqual(want, got) {
		t.Fatalf("differs from JSON round trip\nwant: %+v\n got: %+v", want, got)
	}
}

func TestNilAndEmpty(t *testing.T) {
	for name, in := range map[string]core.Event{
		"zero":  {},
		"empty": {Mutations: []core.Mutation{}, Reads: []core.ReadSummary{}, Attrs: map[string]string{}, Flags: []string{}, LinkIDs: []string{}},
	} {
		out := roundTrip(t, in)
		if !reflect.DeepEqual(out, core.Event{}) {
			t.Errorf("%s: got %+v", name, out)
		}
	}
	// empty strings and zero sizes survive inside non-nil containers
	in := core.Event{Mutations: []core.Mutation{{}}, Reads: []core.ReadSummary{{}}}
	if out := roundTrip(t, in); !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v", out)
	}
}

func TestEffectiveTriState(t *testing.T) {
	in := core.Event{Mutations: []core.Mutation{{Op: "a"}, {Op: "b", Effective: bptr(true)}, {Op: "c", Effective: bptr(false)}}}
	out := roundTrip(t, in)
	if out.Mutations[0].Effective != nil || out.Mutations[1].Effective == nil || !*out.Mutations[1].Effective ||
		out.Mutations[2].Effective == nil || *out.Mutations[2].Effective {
		t.Fatalf("tri-state lost: %+v", out.Mutations)
	}
}

func TestUnicodeAndInvalidUTF8(t *testing.T) {
	in := core.Event{SessionID: "ts_日本語🙂", Name: "näme  ", Attrs: map[string]string{"ключ": "значение"},
		Mutations: []core.Mutation{{PK: map[string]any{"ключ": "💥"}}}}
	if out := roundTrip(t, in); !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v", out)
	}
	bad := core.Event{Name: "a\xffb", Attrs: map[string]string{"k\xfe": "v\xfd"}, Mutations: []core.Mutation{{PK: map[string]any{"k": "x\xff"}}}}
	out := roundTrip(t, bad) // must encode (as encoding/json would) rather than fail
	if out.Name != "a�b" || out.Attrs["k�"] != "v�" || out.Mutations[0].PK["k"] != "x�" {
		t.Fatalf("got %+v", out)
	}
}

func TestNonFiniteAndOddValues(t *testing.T) {
	type custom struct{ A int }
	in := core.Event{Mutations: []core.Mutation{{PK: map[string]any{"nan": math.NaN(), "inf": math.Inf(1), "c": custom{1}, "ch": make(chan int)}}}}
	pk := roundTrip(t, in).Mutations[0].PK
	if pk["nan"] != nil || pk["inf"] != nil || pk["ch"] != nil || !reflect.DeepEqual(pk["c"], map[string]any{"A": 1.0}) {
		t.Fatalf("got %#v", pk)
	}
}

func TestNULReplaced(t *testing.T) {
	in := core.Event{
		SessionID: "a\x00b", Name: "x\x00", Attrs: map[string]string{"k\x00": "v\x00"}, Flags: []string{"f\x00"},
		Req:       core.PayloadRef{Inline: json.RawMessage(`{"a":"x\u0000y","b":"\\u0000"}`), BlobHash: "h\x00"},
		Mutations: []core.Mutation{{Store: "s\x00", PK: map[string]any{"k\x00": "v\x00", "l": []any{"\x00"}}, Diff: []core.FieldChange{{Field: "f\x00", Old: "o\x00"}}}},
	}
	out := roundTrip(t, in)
	if out.SessionID != "a�b" || out.Name != "x�" || out.Attrs["k�"] != "v�" || out.Flags[0] != "f�" || out.Req.BlobHash != "h�" {
		t.Fatalf("got %+v", out)
	}
	if got, want := string(out.Req.Inline), `{"a":"x�y","b":"\\u0000"}`; got != want {
		t.Fatalf("inline: got %s want %s", got, want)
	}
	m := out.Mutations[0]
	if m.Store != "s�" || m.PK["k�"] != "v�" || m.PK["l"].([]any)[0] != "�" || m.Diff[0].Field != "f�" || m.Diff[0].Old != "o�" {
		t.Fatalf("got %+v", m)
	}
	if b, _ := json.Marshal(out); bytes.Contains(b, []byte(`"\u0000`)) {
		t.Fatalf("NUL survived: %s", b)
	}
}

func TestInvalidInlineJSONRejected(t *testing.T) {
	b, _ := AppendFramed(nil, core.Event{SessionID: "ok"})
	b2, _ := AppendFramed(nil, core.Event{Req: core.PayloadRef{Inline: json.RawMessage(`{not json`)}})
	b3, _ := AppendFramed(nil, core.Event{SessionID: "after"})
	d := NewDecoder(bytes.NewReader(append(append(b, b2...), b3...)), 1<<20)
	if e, err := d.Next(); err != nil || e.SessionID != "ok" {
		t.Fatal(e, err)
	}
	if _, err := d.Next(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
	if e, err := d.Next(); err != nil || e.SessionID != "after" {
		t.Fatal(e, err)
	}
}

func TestFramingStreamAndLimits(t *testing.T) {
	evs := []core.Event{{SessionID: "a"}, {SessionID: "big", Name: strings.Repeat("x", 5000)}, {SessionID: "c"}}
	b, err := Encode(evs)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDecoder(bytes.NewReader(b), 1000)
	var got []string
	var over int
	for {
		e, err := d.Next()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrOversize) {
			over++
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e.SessionID)
	}
	if over != 1 || !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("over=%d got=%v", over, got)
	}
	if b, err := Encode(nil); err != nil || len(b) != 0 {
		t.Fatalf("empty batch: %v %v", b, err)
	}
}

func TestUndecodableMessageKeepsFraming(t *testing.T) {
	good, _ := AppendFramed(nil, core.Event{SessionID: "g"})
	// a well-framed message whose body is not valid protobuf (bad wire type)
	bad := append([]byte{3}, 0x07, 0x07, 0x07)
	d := NewDecoder(bytes.NewReader(append(append(append([]byte{}, good...), bad...), good...)), 1<<20)
	var res []string
	for {
		e, err := d.Next()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrInvalid) {
			res = append(res, "bad")
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, e.SessionID)
	}
	if !reflect.DeepEqual(res, []string{"g", "bad", "g"}) {
		t.Fatalf("got %v", res)
	}
}

func TestTruncatedAndGarbageNeverPanic(t *testing.T) {
	b, err := Encode([]core.Event{fullEvent(), fullEvent()})
	if err != nil {
		t.Fatal(err)
	}
	drain := func(data []byte) {
		d := NewDecoder(bytes.NewReader(data), 1<<16)
		for i := 0; i < len(data)+2; i++ {
			if _, err := d.Next(); err != nil {
				if errors.Is(err, ErrOversize) || errors.Is(err, ErrInvalid) {
					continue
				}
				return // EOF / corrupt / read error ends the stream
			}
		}
		t.Fatalf("decoder did not terminate on %x", data)
	}
	for i := 0; i <= len(b); i++ { // every truncation point
		drain(b[:i])
	}
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ { // random garbage and bit flips
		g := make([]byte, r.Intn(300))
		r.Read(g)
		drain(g)
		m := append([]byte(nil), b...)
		for j := 0; j < 1+r.Intn(4); j++ {
			m[r.Intn(len(m))] ^= byte(1 << r.Intn(8))
		}
		drain(m)
	}
	drain(bytes.Repeat([]byte{0xff}, 20)) // varint overflow
	// truncation mid-stream is reported as corrupt framing
	_, err = NewDecoder(bytes.NewReader(b[:len(b)-3]), 1<<20).Next()
	if err != nil {
		t.Fatal(err) // first message intact
	}
	d := NewDecoder(bytes.NewReader(b[:len(b)-3]), 1<<20)
	d.Next()
	if _, err := d.Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v", err)
	}
}

func FuzzDecoder(f *testing.F) {
	b, _ := Encode([]core.Event{fullEvent()})
	f.Add(b)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder(bytes.NewReader(data), 1<<16)
		for i := 0; i <= len(data); i++ {
			e, err := d.Next()
			if err == nil {
				if b, _ := json.Marshal(e); bytes.Contains(b, []byte(`"\u0000`)) || bytes.Contains(b, []byte(`x\u0000`)) {
					t.Fatalf("NUL in decoded event: %s", b)
				}
				continue
			}
			if errors.Is(err, ErrOversize) || errors.Is(err, ErrInvalid) {
				continue
			}
			return
		}
	})
}

func benchBatch() []core.Event {
	var evs []core.Event
	for i := 0; i < 256; i++ {
		e := fullEvent()
		e.LocalSeq = uint64(i)
		evs = append(evs, e)
	}
	return evs
}

func BenchmarkEncodeNDJSON(b *testing.B) {
	evs := benchBatch()
	enc := runtime.NDJSONEncoder{}
	n, _ := enc.Encode(evs)
	b.SetBytes(int64(len(n)))
	b.ReportMetric(float64(len(n)), "wire-bytes")
	for b.Loop() {
		enc.Encode(evs)
	}
}

func BenchmarkEncodeProto(b *testing.B) {
	evs := benchBatch()
	n, _ := Encode(evs)
	b.ReportMetric(float64(len(n)), "wire-bytes")
	for b.Loop() {
		Encode(evs)
	}
}

func BenchmarkDecodeNDJSON(b *testing.B) {
	data, _ := runtime.NDJSONEncoder{}.Encode(benchBatch())
	for b.Loop() {
		for _, l := range bytes.Split(data, []byte("\n")) {
			if len(l) > 0 {
				var e core.Event
				_ = json.Unmarshal(l, &e)
			}
		}
	}
}

func BenchmarkDecodeProto(b *testing.B) {
	data, _ := Encode(benchBatch())
	for b.Loop() {
		d := NewDecoder(bytes.NewReader(data), 1<<20)
		for {
			if _, err := d.Next(); err != nil {
				break
			}
		}
	}
}

func TestWireSizeComparison(t *testing.T) {
	evs := benchBatch()
	n, _ := runtime.NDJSONEncoder{}.Encode(evs)
	p, _ := Encode(evs)
	t.Logf("256 events: ndjson=%d B protobuf=%d B (%.0f%%)", len(n), len(p), 100*float64(len(p))/float64(len(n)))
	if len(p) >= len(n) {
		t.Fatalf("protobuf not smaller")
	}
}
