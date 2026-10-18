package runtime

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestNDJSONEncoderMatchesLegacyBytes(t *testing.T) {
	batch := []core.Event{
		{SessionID: "s", Name: "<a&b>", Req: core.PayloadRef{Inline: json.RawMessage(`{"x":1}`)}, Attrs: map[string]string{"k": "v"}},
		{SessionID: "s", LocalSeq: 2, Mutations: []core.Mutation{{Store: "pg", PK: map[string]any{"id": 1}}}},
	}
	var want bytes.Buffer
	enc := json.NewEncoder(&want)
	for i := range batch {
		_ = enc.Encode(&batch[i])
	}
	got, err := (NDJSONEncoder{}).Encode(batch)
	if err != nil || !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("bytes differ (err=%v)\nwant %q\n got %q", err, want.Bytes(), got)
	}
	if (NDJSONEncoder{}).ContentType() != "application/x-ndjson" {
		t.Fatal("content type")
	}
}

type fixedEnc struct{}

func (fixedEnc) ContentType() string                   { return "application/x-test" }
func (fixedEnc) Encode(b []core.Event) ([]byte, error) { return []byte{byte(len(b))}, nil }

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
