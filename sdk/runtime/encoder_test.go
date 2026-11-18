package runtime

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestProtobufEncoderFraming(t *testing.T) {
	big := make([]byte, 300) // forces a multi-byte length prefix
	for i := range big {
		big[i] = 'x'
	}
	batch := []core.Event{
		{},
		{SessionID: "s", Kind: core.KindAPIIn, LocalSeq: 7, Attempt: 2, Flags: []string{"a", ""}, Attrs: map[string]string{"k": "v"}, Name: string(big)},
		{SessionID: "t", StartNs: -5, Err: "bad\xffutf8"},
	}
	enc := ProtobufEncoder{}
	if enc.ContentType() != "application/x-protobuf" {
		t.Fatal("content type")
	}
	body, err := enc.Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	// An empty event is a single zero length prefix.
	if body[0] != 0 {
		t.Fatalf("empty event frame = %x", body[0])
	}
	got, err := decodeFrames(body)
	if err != nil || len(got) != 3 {
		t.Fatalf("decode: n=%d err=%v", len(got), err)
	}
	if got[1].SessionID != "s" || got[1].LocalSeq != 7 || got[1].Attempt != 2 || got[1].Name != string(big) ||
		len(got[1].Flags) != 2 || got[1].Attrs["k"] != "v" || got[1].Kind != core.KindAPIIn {
		t.Fatalf("event 1 = %+v", got[1])
	}
	if got[2].StartNs != -5 || got[2].Err != "bad�utf8" {
		t.Fatalf("event 2 = %+v", got[2])
	}
	body2, _ := enc.Encode(batch)
	if !bytes.Equal(body, body2) {
		t.Fatal("encoding is not deterministic / pooled buffer leaked into result")
	}
	// Frame boundaries line up exactly.
	off := 0
	for off < len(body) {
		n, k := binary.Uvarint(body[off:])
		off += k + int(n)
	}
	if off != len(body) {
		t.Fatal("trailing bytes")
	}
}

type fixedEnc struct{}

func (fixedEnc) ContentType() string                   { return "application/x-test" }
func (fixedEnc) Encode(b []core.Event) ([]byte, error) { return []byte{byte(len(b))}, nil }

func TestExporterDefaultsToProtobuf(t *testing.T) {
	var mu sync.Mutex
	var ct string
	var evs []core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ct = r.Header.Get("Content-Type")
		evs = readFrames(r.Body)
		mu.Unlock()
		w.Write([]byte(`{"accepted":2}`))
	}))
	defer srv.Close()
	r := NewRecorder(16)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, FlushInterval: 10 * time.Millisecond})
	r.Record(core.Event{SessionID: "s", LocalSeq: 1})
	r.Record(core.Event{SessionID: "s", LocalSeq: 2})
	if !x.Flush(2 * time.Second) {
		t.Fatal("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if ct != "application/x-protobuf" || len(evs) != 2 || evs[1].LocalSeq != 2 {
		t.Fatalf("ct=%q evs=%+v", ct, evs)
	}
}

func TestExporterUsesConfiguredEncoder(t *testing.T) {
	var mu sync.Mutex
	var ct string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ct = r.Header.Get("Content-Type")
		body, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.Write([]byte(`{"accepted":3}`))
	}))
	defer srv.Close()
	r := NewRecorder(16)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, FlushInterval: 10 * time.Millisecond, Encoder: fixedEnc{}})
	for i := 0; i < 3; i++ {
		r.Record(core.Event{SessionID: "s", LocalSeq: uint64(i + 1)})
	}
	if !x.Flush(2 * time.Second) {
		t.Fatal("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if ct != "application/x-test" || !bytes.Equal(body, []byte{3}) {
		t.Fatalf("ct=%q body=%v", ct, body)
	}
}
