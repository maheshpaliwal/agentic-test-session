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

func ndjsonBody(evs ...core.Event) []byte {
	b, _ := runtime.NDJSONEncoder{}.Encode(evs)
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

func TestProtoIdempotentAcrossFormats(t *testing.T) {
	_, srv := newC(t, 0, "")
	e := ev("a", core.KindAPIIn, 1, 1)
	for _, order := range [][2]string{{"application/x-ndjson", ctProto}, {ctProto, "application/x-ndjson"}} {
		e.SessionID = sid + "_" + order[0][len(order[0])-4:]
		bodies := map[string][]byte{"application/x-ndjson": ndjsonBody(e), ctProto: protoBody(t, e)}
		_, r1 := rawPost(t, srv.URL, order[0], bodies[order[0]])
		_, r2 := rawPost(t, srv.URL, order[1], bodies[order[1]])
		if r1.Accepted != 1 || r2.Accepted != 0 || r2.Duplicates != 1 {
			t.Fatalf("%v: first=%+v second=%+v", order, r1, r2)
		}
	}
}

func TestProtoOversizeMessageRejectedAlone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.MaxLineBytes = 2000
	_, srv := newC2(t, cfg)
	big := ev("big", core.KindAPIIn, 2, 2)
	big.Name = strings.Repeat("x", 5000)
	code, r := rawPost(t, srv.URL, ctProto, protoBody(t, ev("a", core.KindAPIIn, 1, 1), big, ev("c", core.KindAPIIn, 3, 3)))
	if code != 200 || r.Accepted != 2 || r.Rejected != 1 || r.RejectedReasons["oversize_line"] != 1 {
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

func TestProtoWrongContentTypeKeepsExistingBehaviour(t *testing.T) {
	_, srv := newC(t, 0, "")
	for _, ct := range []string{"text/plain", "application/x-protobuf-evil", "multipart/form-data"} {
		if code, _ := rawPost(t, srv.URL, ct, protoBody(t, ev("a", core.KindAPIIn, 1, 1))); code != http.StatusUnsupportedMediaType {
			t.Errorf("%s: status %d", ct, code)
		}
	}
	// protobuf bytes sent as NDJSON are just invalid JSON lines
	code, r := rawPost(t, srv.URL, "application/x-ndjson", protoBody(t, ev("a", core.KindAPIIn, 1, 1)))
	if code != 200 || r.Accepted != 0 || r.RejectedReasons["invalid_json"] == 0 {
		t.Fatalf("%d %+v", code, r)
	}
	// NDJSON still works with and without parameters
	for i, ct := range []string{"application/x-ndjson", "application/json; charset=utf-8"} {
		e := ev("n", core.KindAPIIn, 1, 1)
		e.SessionID = "ts_ct" + string(rune('1'+i))
		if code, r := rawPost(t, srv.URL, ct, ndjsonBody(e)); code != 200 || r.Accepted != 1 {
			t.Fatalf("%s: %d %+v", ct, code, r)
		}
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
