package collector

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

const ctProto = "application/x-protobuf"

func rawPost(t *testing.T, url, ct string, body []byte) (int, IngestResult) {
	t.Helper()
	resp, err := http.Post(url+"/v1/ingest", ct, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r IngestResult
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatalf("response %q: %v", b, err)
		}
	}
	return resp.StatusCode, r
}

func protoBody(t *testing.T, evs ...core.Event) []byte {
	t.Helper()
	b, err := tsessionproto.Encode(evs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestProtoIngestAcceptedAndStored(t *testing.T) {
	_, srv := newC(t, 0, "")
	in := ev("a", core.KindDBWrite, 1, 1)
	in.Mutations = []core.Mutation{{Store: "postgres", Entity: "t", Op: "insert", PK: map[string]any{"id": 1}}}
	code, r := rawPost(t, srv.URL, ctProto+"; charset=binary", protoBody(t, in, ev("b", core.KindAPIIn, 2, 2)))
	if code != 200 || r.Accepted != 2 || r.Duplicates != 0 || r.Rejected != 0 {
		t.Fatalf("%d %+v", code, r)
	}
	var out struct {
		Events []core.Event `json:"events"`
	}
	for i := 0; i < 100 && len(out.Events) < 2; i++ {
		time.Sleep(10 * time.Millisecond)
		get(t, srv.URL+"/v1/sessions/"+sid+"/events", &out)
	}
	if len(out.Events) != 2 || out.Events[0].SpanID != "a" || out.Events[0].Mutations[0].PK["id"] != 1.0 {
		t.Fatalf("%+v", out.Events)
	}
}

func TestProtoIdempotent(t *testing.T) {
	_, srv := newC(t, 0, "")
	body := protoBody(t, ev("a", core.KindAPIIn, 1, 1))
	_, r1 := rawPost(t, srv.URL, ctProto, body)
	_, r2 := rawPost(t, srv.URL, ctProto, body)
	if r1.Accepted != 1 || r2.Accepted != 0 || r2.Duplicates != 1 {
		t.Fatalf("first=%+v second=%+v", r1, r2)
	}
}

func TestProtoOversizeMessageRejectedAlone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.MaxMessageBytes = 2000
	_, srv := newC2(t, cfg)
	big := ev("big", core.KindAPIIn, 2, 2)
	big.Name = strings.Repeat("x", 5000)
	code, r := rawPost(t, srv.URL, ctProto, protoBody(t, ev("a", core.KindAPIIn, 1, 1), big, ev("c", core.KindAPIIn, 3, 3)))
	if code != 200 || r.Accepted != 2 || r.Rejected != 1 || r.RejectedReasons["oversize_message"] != 1 {
		t.Fatalf("%d %+v", code, r)
	}
}

func TestProtoUndecodableMessageAndBadSession(t *testing.T) {
	_, srv := newC(t, 0, "")
	bad := ev("x", core.KindAPIIn, 5, 5)
	bad.SessionID = "not a valid id!"
	badInline := ev("y", core.KindAPIIn, 6, 6)
	badInline.Req.Inline = json.RawMessage(`{broken`)
	body := protoBody(t, ev("a", core.KindAPIIn, 1, 1), bad, badInline)
	body = append(body, 3, 0x07, 0x07, 0x07) // well-framed, not protobuf
	body = append(body, protoBody(t, ev("b", core.KindAPIIn, 2, 2))...)
	code, r := rawPost(t, srv.URL, ctProto, body)
	if code != 200 || r.Accepted != 2 || r.Rejected != 3 || r.RejectedReasons["invalid_proto"] != 2 || r.RejectedReasons["invalid_session_id"] != 1 {
		t.Fatalf("%d %+v", code, r)
	}
}

func TestProtoGarbageTailKeepsDecodedEvents(t *testing.T) {
	_, srv := newC(t, 0, "")
	body := protoBody(t, ev("a", core.KindAPIIn, 1, 1), ev("b", core.KindAPIIn, 2, 2))
	body = append(body, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // varint overflow
	body = append(body, protoBody(t, ev("never", core.KindAPIIn, 3, 3))...)
	code, r := rawPost(t, srv.URL, ctProto, body)
	if code != 200 || r.Accepted != 2 || r.Rejected != 1 || r.RejectedReasons["invalid_proto"] != 1 {
		t.Fatalf("%d %+v", code, r)
	}
	// truncated final message
	whole := protoBody(t, ev("c", core.KindAPIIn, 4, 4), ev("d", core.KindAPIIn, 5, 5))
	code, r = rawPost(t, srv.URL, ctProto, whole[:len(whole)-4])
	if code != 200 || r.Accepted != 1 || r.RejectedReasons["invalid_proto"] != 1 {
		t.Fatalf("%d %+v", code, r)
	}
	// pure garbage and an empty body
	code, r = rawPost(t, srv.URL, ctProto, []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xffgarbage"))
	if code != 200 || r.Accepted != 0 || r.RejectedReasons["invalid_proto"] != 1 {
		t.Fatalf("%d %+v", code, r)
	}
	if code, r = rawPost(t, srv.URL, ctProto, nil); code != 200 || r.Accepted != 0 || r.Rejected != 0 {
		t.Fatalf("%d %+v", code, r)
	}
}

func TestProtoBodyLimit413(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.MaxBodyBytes = 1000
	_, srv := newC2(t, cfg)
	var evs []core.Event
	for i := 0; i < 50; i++ {
		evs = append(evs, ev("s", core.KindAPIIn, uint64(i+1), uint64(i+1)))
	}
	if code, _ := rawPost(t, srv.URL, ctProto, protoBody(t, evs...)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", code)
	}
	// a single oversize message that also exceeds the body cap while being skipped
	big := ev("big", core.KindAPIIn, 1, 1)
	big.Name = strings.Repeat("x", 5000)
	if code, _ := rawPost(t, srv.URL, ctProto, protoBody(t, big)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", code)
	}
}

// NDJSON ingest was removed: anything but application/x-protobuf is a 415
// with the JSON error shape naming the required type.
func TestIngestRequiresProtobufContentType(t *testing.T) {
	_, srv := newC(t, 0, "")
	good := protoBody(t, ev("a", core.KindAPIIn, 1, 1))
	nd, _ := json.Marshal(ev("a", core.KindAPIIn, 1, 1))
	nd = append(nd, '\n')
	for _, tc := range []struct {
		ct   string
		body []byte
	}{
		{"application/x-ndjson", nd},
		{"application/json", nd},
		{"text/plain", good},
		{"application/x-protobuf-evil", good},
		{"multipart/form-data", good},
		{"", nd}, // no Content-Type
		{"application/x-ndjson", nil},
		{"", nil},
	} {
		req, _ := http.NewRequest("POST", srv.URL+"/v1/ingest", bytes.NewReader(tc.body))
		if tc.ct != "" {
			req.Header.Set("Content-Type", tc.ct)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnsupportedMediaType || !strings.Contains(e.Error, ctProto) {
			t.Errorf("ct %q body %d bytes: status %d error %q", tc.ct, len(tc.body), resp.StatusCode, e.Error)
		}
	}
	// parameters on the right type are fine
	if code, r := rawPost(t, srv.URL, ctProto+"; charset=binary", good); code != 200 || r.Accepted != 1 {
		t.Fatalf("%d %+v", code, r)
	}
}

func TestProtoNULStripped(t *testing.T) {
	_, srv := newC(t, 0, "")
	e := ev("a", core.KindAPIIn, 1, 1)
	e.Name = "a\x00b"
	e.Req = core.PayloadRef{Inline: json.RawMessage(`{"k":"x\u0000y"}`)}
	if code, r := rawPost(t, srv.URL, ctProto, protoBody(t, e)); code != 200 || r.Accepted != 1 {
		t.Fatalf("%d %+v", code, r)
	}
	var out struct {
		Events []core.Event `json:"events"`
	}
	for i := 0; i < 100 && len(out.Events) < 1; i++ {
		time.Sleep(10 * time.Millisecond)
		get(t, srv.URL+"/v1/sessions/"+sid+"/events", &out)
	}
	if len(out.Events) != 1 || out.Events[0].Name != "a�b" || string(out.Events[0].Req.Inline) != `{"k":"x`+"�"+`y"}` {
		t.Fatalf("%+v", out.Events)
	}
}

// Exporter (protobuf encoder) -> HTTP -> collector, end to end.
func TestExporterProtobufEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	_, srv := newC2(t, cfg)
	core.Configure(core.Config{Service: "svc", Instance: "e2e", Enabled: true})
	defer core.Configure(core.Config{})

	rec := runtime.NewRecorder(1000)
	x := runtime.NewExporter(rec, runtime.ExporterConfig{CollectorURL: srv.URL, BatchSize: 20, FlushInterval: 10 * time.Millisecond,
		Encoder: tsessionproto.Encoder{}})
	const n = 50
	for i := 1; i <= n; i++ {
		e := ev("s", core.KindDBWrite, uint64(i), uint64(i))
		e.Mutations = []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", PK: map[string]any{"id": i}}}
		rec.Record(e)
	}
	if !x.Flush(5 * time.Second) {
		t.Fatal("flush timeout")
	}
	x.Close(time.Second)
	var out struct {
		Events []core.Event `json:"events"`
	}
	for i := 0; i < 200 && len(out.Events) < n; i++ {
		time.Sleep(10 * time.Millisecond)
		get(t, srv.URL+"/v1/sessions/"+sid+"/events?limit=1000", &out)
	}
	if len(out.Events) != n {
		t.Fatalf("stored %d of %d", len(out.Events), n)
	}
	if out.Events[0].Mutations[0].Store != "postgres" || out.Events[0].Instance != "i1" {
		t.Fatalf("%+v", out.Events[0])
	}
	if st := rec.Len(); st != 0 {
		t.Fatalf("recorder not drained: %d", st)
	}
}
