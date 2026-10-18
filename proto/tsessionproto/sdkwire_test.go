package tsessionproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"

	"github.com/maheshpaliwal/agentic-test-session/core"
	pb "github.com/maheshpaliwal/agentic-test-session/proto/tsessionpb"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

// Conformance: the stdlib-only SDK encoder (runtime.ProtobufEncoder) must be
// indistinguishable, once decoded, from the generated-code reference.

var _ runtime.BatchEncoder = runtime.ProtobufEncoder{}

type weird struct {
	A int               `json:"a"`
	B string            `json:"b,omitempty"`
	C map[string]string `json:"c,omitempty"`
}

func decodeAll(t testing.TB, body []byte) []core.Event {
	t.Helper()
	d := NewDecoder(bytes.NewReader(body), 1<<30)
	var out []core.Event
	for {
		e, err := d.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		out = append(out, e)
	}
}

func parseFrames(t testing.TB, body []byte) []*pb.Event {
	t.Helper()
	r := bytes.NewReader(body)
	var out []*pb.Event
	for {
		m := &pb.Event{}
		err := protodelim.UnmarshalFrom(r, m)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("protodelim: %v", err)
		}
		out = append(out, m)
	}
}

// conform encodes batch with the SDK encoder and checks it against the reference.
func conform(t testing.TB, batch []core.Event) {
	t.Helper()
	enc := runtime.ProtobufEncoder{}
	got, err := enc.Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := enc.Encode(batch)
	if !bytes.Equal(got, again) {
		t.Fatal("SDK encoding is not deterministic")
	}
	ref, err := Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	gotEvs, wantEvs := decodeAll(t, got), decodeAll(t, ref)
	if len(gotEvs) != len(batch) || len(wantEvs) != len(batch) {
		t.Fatalf("event counts: sdk=%d ref=%d want=%d", len(gotEvs), len(wantEvs), len(batch))
	}
	for i := range batch {
		if !reflect.DeepEqual(gotEvs[i], wantEvs[i]) {
			t.Fatalf("event %d decodes differently\n sdk: %+v\n ref: %+v", i, gotEvs[i], wantEvs[i])
		}
		a, _ := json.Marshal(gotEvs[i])
		b, _ := json.Marshal(wantEvs[i])
		if !bytes.Equal(a, b) {
			t.Fatalf("event %d JSON differs\n sdk: %s\n ref: %s", i, a, b)
		}
	}
	msgs := parseFrames(t, got)
	if len(msgs) != len(batch) {
		t.Fatalf("frames: %d want %d", len(msgs), len(batch))
	}
	for i := range batch {
		if want := FromCore(batch[i]); !proto.Equal(msgs[i], want) {
			t.Fatalf("event %d: proto.Equal false\n sdk: %v\n ref: %v", i, msgs[i], want)
		}
	}
}

func conformTable() []core.Event {
	huge := strings.Repeat(`{"k":"vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv","n":[1,2,3]},`, 2500) // ~150 KB
	bigInline := json.RawMessage(`[` + huge + `null]`)
	deep := map[string]any{"a": []any{map[string]any{"b": []any{[]any{}, map[string]any{}, nil, true, "z", 1.25}}}}
	wide := map[string]any{}
	for i := 0; i < 300; i++ {
		wide[fmt.Sprintf("k%03d", i)] = float64(i) / 3
	}
	tbl := []core.Event{
		{},
		fullEvent(),
		{SessionID: "ünïcödé 日本語 🙂", Name: "tab\tnl\n\"quote\" <a&b> \\", Err: "e  "},
		{SessionID: "nul\x00in\x00side", Name: "\x00", Attrs: map[string]string{"k\x00": "v\x00", "": "", "empty": ""}, Flags: []string{"", "\x00"}},
		{SessionID: "bad\xff\xfeutf8", Name: "\xc3\x28", Err: "tail\xe2\x82", Attrs: map[string]string{"k\xffk": "v\xff"}, LinkIDs: []string{"\xff", ""}},
		{HLC: math.MaxUint64, EndHLC: math.MaxUint64, LocalSeq: math.MaxUint64, Seq: math.MaxUint64, StartNs: math.MinInt64, EndNs: math.MaxInt64, Attempt: math.MinInt32},
		{Attempt: -1, StartNs: -1, Req: core.PayloadRef{Size: -7}},
		{Req: core.PayloadRef{Truncated: true}, Resp: core.PayloadRef{Redacted: true}},
		{Req: core.PayloadRef{Inline: json.RawMessage(`"a\u0000b"`)}, Resp: core.PayloadRef{BlobHash: "h", Size: 1}},
		{Req: core.PayloadRef{Inline: bigInline, Size: len(bigInline)}, Name: strings.Repeat("n", 70<<10), Attrs: map[string]string{"big": strings.Repeat("v", 80<<10)}},
		{Mutations: []core.Mutation{{}, {Effective: bptr(true)}, {Effective: bptr(false)}, {Store: "s"}}},
		{Mutations: []core.Mutation{{
			Store: "pg", Entity: "t", Op: "update", Source: "cdc", Effective: bptr(false),
			PK: map[string]any{
				"nan": math.NaN(), "inf": math.Inf(1), "ninf": math.Inf(-1), "nil": nil, "t": true, "f": false,
				"i": 12, "i8": int8(-8), "i16": int16(16), "i32": int32(-32), "i64": int64(1) << 62, "u": uint(5), "u8": uint8(8),
				"u16": uint16(16), "u32": uint32(32), "u64": uint64(math.MaxUint64), "f32": float32(0.1), "f64": 1e308, "neg0": math.Copysign(0, -1),
				"num": json.Number("12.5"), "badnum": json.Number("x"), "str": "é\x00\xff", "deep": deep, "emptyl": []any{}, "emptym": map[string]any{},
				"strs": []string{"a", "\xff"}, "ms": map[string]string{"x": "y\xff"}, "st": weird{A: 1, C: map[string]string{"q": "r"}}, "pst": &weird{B: "p"},
				"nilptr": (*weird)(nil), "ch": make(chan int), "fn": func() {}, "bytes": []byte("hi"), "arr": [2]int{1, 2}, "cmplx": complex(1, 2),
				"nanlist": []any{math.NaN(), 1.0}, "k\xff": 1,
			},
			Diff: []core.FieldChange{
				{Field: "a", Old: deep, New: []any{}},
				{Field: "b"},
				{Field: "c", Old: math.NaN(), New: "x\xff"},
				{Field: "d", Old: map[string]any{}, New: weird{A: 2}},
				{Field: "\xff", Old: false, New: int64(-3)},
			},
			Before: core.PayloadRef{Inline: json.RawMessage(`{"a":1}`), Size: 7}, After: core.PayloadRef{Inline: json.RawMessage(`null`), Redacted: true},
		}}},
		{Mutations: []core.Mutation{{Store: "w", PK: wide}}},
		{Reads: []core.ReadSummary{{}, {Store: "r", PKs: []string{"", "a\xff", "b"}, RowCount: -1}, {Args: core.PayloadRef{Inline: json.RawMessage(`[]`)}, Sample: core.PayloadRef{Truncated: true, Size: 3}}}},
		{LinkIDs: []string{"a", "a", ""}, Flags: []string{"x", "x"}},
	}
	// Many events in one batch, with sizes straddling the 1/2/3-byte length prefix boundaries.
	for i := 0; i < 600; i++ {
		e := fullEvent()
		e.LocalSeq = uint64(i)
		e.Name = strings.Repeat("p", i*3%700)
		tbl = append(tbl, e)
	}
	return tbl
}

func TestSDKEncoderConformanceTable(t *testing.T) {
	tbl := conformTable()
	t.Run("batch", func(t *testing.T) { conform(t, tbl) })
	// Per-event too, so a failure names the culprit.
	for i := range tbl[:16] {
		t.Run(fmt.Sprint("event", i), func(t *testing.T) { conform(t, tbl[i:i+1]) })
	}
	t.Run("empty batch", func(t *testing.T) {
		b, err := runtime.ProtobufEncoder{}.Encode(nil)
		if err != nil || len(b) != 0 {
			t.Fatalf("b=%v err=%v", b, err)
		}
	})
	if ct := (runtime.ProtobufEncoder{}).ContentType(); ct != ContentType {
		t.Fatalf("content type %q", ct)
	}
}

// Encoding a batch must equal concatenating single-event encodings (pooled
// scratch space and prefix widening must not leak state between events).
func TestSDKEncoderBatchEqualsConcat(t *testing.T) {
	tbl := conformTable()[:40]
	var cat []byte
	for i := range tbl {
		b, _ := runtime.ProtobufEncoder{}.Encode(tbl[i : i+1])
		cat = append(cat, b...)
	}
	all, _ := runtime.ProtobufEncoder{}.Encode(tbl)
	if !bytes.Equal(cat, all) {
		t.Fatal("batch encoding differs from concatenated single-event encodings")
	}
}

func TestSDKEncoderConcurrent(t *testing.T) {
	tbl := conformTable()[:30]
	want, _ := runtime.ProtobufEncoder{}.Encode(tbl)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if got, _ := (runtime.ProtobufEncoder{}).Encode(tbl); !bytes.Equal(got, want) {
					t.Error("concurrent encode differs")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// ---- randomised ----

func rstr(r *rand.Rand) string {
	pieces := []string{"", "a", "id", "ü", "日本", "🙂", "\x00", "\xff", "\xc3", "\"", "\\", "\n", "x y", " "}
	var sb strings.Builder
	for n := r.Intn(4); n >= 0; n-- {
		sb.WriteString(pieces[r.Intn(len(pieces))])
	}
	return sb.String()
}

// ukey is unique per (prefix,i) so map keys cannot collide once sanitised.
func ukey(r *rand.Rand, i int) string { return rstr(r) + "#" + fmt.Sprint(i) }

func rnum(r *rand.Rand) float64 {
	switch r.Intn(8) {
	case 0:
		return math.NaN()
	case 1:
		return math.Inf(1 - 2*r.Intn(2))
	case 2:
		return float64(r.Int63()) * float64(1-2*r.Intn(2))
	case 3:
		return r.NormFloat64() * 1e300
	}
	return r.NormFloat64() * 100
}

func rany(r *rand.Rand, depth int) any {
	n := 14
	if depth <= 0 {
		n = 11
	}
	switch r.Intn(n) {
	case 0:
		return nil
	case 1:
		return r.Intn(2) == 0
	case 2:
		return rstr(r)
	case 3:
		return rnum(r)
	case 4:
		return r.Int()
	case 5:
		return int64(r.Uint64())
	case 6:
		return r.Uint64()
	case 7:
		return json.Number(fmt.Sprint(r.Intn(1000)) + ".5")
	case 8:
		return float32(r.Float32())
	case 9:
		return weird{A: r.Intn(9), B: rstr(r), C: map[string]string{rstr(r): rstr(r)}}
	case 10:
		return []string{rstr(r), rstr(r)}
	case 11:
		l := make([]any, r.Intn(4))
		for i := range l {
			l[i] = rany(r, depth-1)
		}
		return l
	default:
		m := map[string]any{}
		for i := r.Intn(4); i > 0; i-- {
			m[ukey(r, i)] = rany(r, depth-1)
		}
		return m
	}
}

func rpayload(r *rand.Rand) core.PayloadRef {
	var p core.PayloadRef
	if r.Intn(2) == 0 {
		v := rany(r, 2)
		if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			v = nil
		}
		if b, err := json.Marshal(v); err == nil {
			p.Inline = b
		}
	}
	if r.Intn(4) == 0 {
		p.BlobHash = rstr(r)
	}
	if r.Intn(3) == 0 {
		p.Size = r.Intn(1 << 20)
	}
	p.Truncated, p.Redacted = r.Intn(5) == 0, r.Intn(5) == 0
	return p
}

func rstrs(r *rand.Rand) []string {
	if r.Intn(2) == 0 {
		return nil
	}
	s := make([]string, r.Intn(4))
	for i := range s {
		s[i] = rstr(r)
	}
	return s
}

func genEvent(r *rand.Rand) core.Event {
	e := core.Event{
		SessionID: rstr(r), TraceID: rstr(r), SpanID: rstr(r), ParentID: rstr(r), LinkIDs: rstrs(r),
		Service: rstr(r), Instance: rstr(r), Kind: core.Kind(rstr(r)), Name: rstr(r),
		HLC: r.Uint64() >> r.Intn(64), EndHLC: r.Uint64() >> r.Intn(64), LocalSeq: r.Uint64() >> r.Intn(64), Seq: r.Uint64() >> r.Intn(64),
		StartNs: int64(r.Uint64()) >> r.Intn(64), EndNs: int64(r.Uint64()) >> r.Intn(64),
		Req: rpayload(r), Resp: rpayload(r), TxID: rstr(r), Attempt: r.Intn(10) - 2, Err: rstr(r), Flags: rstrs(r),
	}
	if r.Intn(3) == 0 {
		e.Attrs = map[string]string{}
		for i := r.Intn(4); i >= 0; i-- {
			e.Attrs[ukey(r, i)] = rstr(r)
		}
	}
	for i := r.Intn(3); i > 0; i-- {
		m := core.Mutation{Store: rstr(r), Entity: rstr(r), Op: rstr(r), Source: rstr(r), Before: rpayload(r), After: rpayload(r)}
		switch r.Intn(3) {
		case 0:
			m.Effective = bptr(r.Intn(2) == 0)
		}
		if r.Intn(2) == 0 {
			m.PK = map[string]any{}
			for j := r.Intn(4); j >= 0; j-- {
				m.PK[ukey(r, j)] = rany(r, 3)
			}
		}
		for j := r.Intn(3); j > 0; j-- {
			fc := core.FieldChange{Field: rstr(r)}
			if r.Intn(3) > 0 {
				fc.Old = rany(r, 3)
			}
			if r.Intn(3) > 0 {
				fc.New = rany(r, 3)
			}
			m.Diff = append(m.Diff, fc)
		}
		e.Mutations = append(e.Mutations, m)
	}
	for i := r.Intn(3); i > 0; i-- {
		e.Reads = append(e.Reads, core.ReadSummary{Store: rstr(r), Entity: rstr(r), Statement: rstr(r), Args: rpayload(r),
			RowCount: r.Intn(1000) - 1, PKs: rstrs(r), Sample: rpayload(r)})
	}
	return e
}

func TestSDKEncoderRandomRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(20260101))
	for chunk := 0; chunk < 40; chunk++ {
		batch := make([]core.Event, 50) // 2000 events
		for i := range batch {
			batch[i] = genEvent(r)
		}
		conform(t, batch)
	}
}

func FuzzProtobufEncoderVsReference(f *testing.F) {
	for i, e := range conformTable()[:16] {
		if b, err := json.Marshal(e); err == nil {
			f.Add(b, int64(i))
		}
	}
	f.Add([]byte("\xff\x00\xc3"), int64(1))
	f.Fuzz(func(t *testing.T, data []byte, seed int64) {
		h := fnv.New64a()
		h.Write(data)
		r := rand.New(rand.NewSource(seed ^ int64(h.Sum64())))
		var e core.Event
		if json.Unmarshal(data, &e) != nil {
			e = genEvent(r)
		}
		// Raw fuzz bytes into every string-bearing spot (invalid UTF-8, NUL, ...).
		raw := string(data)
		e.Name += raw
		e.Err += raw[:len(raw)/2]
		e.Flags = append(e.Flags, raw)
		if e.Attrs == nil {
			e.Attrs = map[string]string{}
		}
		e.Attrs[raw+"#k"] = raw
		e.Mutations = append(e.Mutations, core.Mutation{Store: raw, PK: map[string]any{raw + "#k": raw, "g": rany(r, 3)},
			Diff: []core.FieldChange{{Field: raw, Old: raw, New: rany(r, 3)}}})
		conform(t, []core.Event{e, genEvent(r)})
	})
}

// ---- exporter end to end ----

func TestExporterDefaultEncoderDecodesWithCollectorDecoder(t *testing.T) {
	var mu sync.Mutex
	var got []core.Event
	var ct string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		ct = r.Header.Get("Content-Type")
		got = append(got, decodeAll(t, body)...)
		mu.Unlock()
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer srv.Close()
	rec := runtime.NewRecorder(1024)
	x := runtime.NewExporter(rec, runtime.ExporterConfig{CollectorURL: srv.URL, BatchSize: 7, FlushInterval: time.Hour})
	want := conformTable()[:20]
	for i := range want {
		want[i].SessionID = "s"
		rec.Record(want[i])
	}
	if !x.Flush(5 * time.Second) {
		t.Fatal("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if ct != ContentType || len(got) != len(want) {
		t.Fatalf("ct=%q got %d events want %d", ct, len(got), len(want))
	}
	for i := range want {
		w, err := ToCore(FromCore(want[i]))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[i], w) {
			t.Fatalf("event %d differs\n got: %+v\nwant: %+v", i, got[i], w)
		}
	}
}

// A 413 on an oversize event must still bisect and deliver the stub with the
// protobuf default (the collector's decoder reports ErrOversize per message).
func TestExporterOversizeStubOverProtobuf(t *testing.T) {
	var mu sync.Mutex
	var got []core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		evs := decodeAll(t, body)
		for _, e := range evs {
			if len(e.Name) > 1000 {
				http.Error(w, "too big", http.StatusRequestEntityTooLarge)
				return
			}
		}
		mu.Lock()
		got = append(got, evs...)
		mu.Unlock()
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer srv.Close()
	rec := runtime.NewRecorder(64)
	x := runtime.NewExporter(rec, runtime.ExporterConfig{CollectorURL: srv.URL, FlushInterval: time.Hour, RetryBackoff: time.Millisecond})
	for i := 1; i <= 5; i++ {
		e := core.Event{SessionID: "s", SpanID: fmt.Sprint("sp", i), LocalSeq: uint64(i), Name: "n"}
		if i == 3 {
			e.Name = strings.Repeat("x", 2000)
		}
		rec.Record(e)
	}
	if !x.Flush(3 * time.Second) {
		t.Fatal("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("delivered %d want 5", len(got))
	}
	for _, e := range got {
		if e.LocalSeq == 3 && (len(e.Flags) != 1 || e.Flags[0] != "oversize_dropped") {
			t.Fatalf("bad stub %+v", e)
		}
	}
}

// ---- benchmarks ----

func benchEncode(b *testing.B, evs []core.Event, enc runtime.BatchEncoder) {
	out, _ := enc.Encode(evs)
	b.ReportMetric(float64(len(out)), "wire-bytes")
	b.ReportAllocs()
	b.SetBytes(int64(len(out)))
	for b.Loop() {
		enc.Encode(evs)
	}
}

func BenchmarkSDKEncode256(b *testing.B)       { benchEncode(b, benchBatch(), runtime.ProtobufEncoder{}) }
func BenchmarkReferenceEncode256(b *testing.B) { benchEncode(b, benchBatch(), Encoder{}) }

func BenchmarkSDKEncodeSmallEvents(b *testing.B) {
	evs := make([]core.Event, 256)
	for i := range evs {
		evs[i] = core.Event{SessionID: "ts_01", TraceID: "t", SpanID: fmt.Sprint(i), Service: "orders", Instance: "i-1", Kind: core.KindAPIIn,
			Name: "GET /orders", LocalSeq: uint64(i), StartNs: 1700000000000000000, EndNs: 1700000000000001000}
	}
	b.Run("sdk", func(b *testing.B) { benchEncode(b, evs, runtime.ProtobufEncoder{}) })
	b.Run("reference", func(b *testing.B) { benchEncode(b, evs, Encoder{}) })
}

func BenchmarkSDKEncodeLargePayload(b *testing.B) {
	e := conformTable()[9:10]
	b.Run("sdk", func(b *testing.B) { benchEncode(b, e, runtime.ProtobufEncoder{}) })
	b.Run("reference", func(b *testing.B) { benchEncode(b, e, Encoder{}) })
}
