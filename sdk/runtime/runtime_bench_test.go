package runtime

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func benchEvent() core.Event {
	return core.Event{SessionID: "s1", TraceID: "t1", SpanID: "p1", Service: "svc", Kind: core.KindAPIIn, Name: "POST /orders",
		Req: core.PayloadRef{Inline: []byte(`{"a":1}`), Size: 7}, Resp: core.PayloadRef{Inline: []byte(`{"ok":true}`), Size: 11}}
}

func BenchmarkRecorderRecord(b *testing.B) {
	r := NewRecorder(1 << 20)
	ev := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ev)
		if r.Len() > 1<<19 { // keep ring from filling (drop path measured separately)
			b.StopTimer()
			r.Drain(1 << 20)
			b.StartTimer()
		}
	}
}

func BenchmarkRecorderRecordFullDrop(b *testing.B) {
	r := NewRecorder(16)
	ev := benchEvent()
	for i := 0; i < 16; i++ {
		r.Record(ev)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ev)
	}
}

func BenchmarkRecordParallel(b *testing.B) {
	r := NewRecorder(1 << 12)
	ev := benchEvent()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			r.Record(ev)
			if n++; n%1024 == 0 {
				r.Drain(1 << 12)
			}
		}
	})
}

// Enqueue plus export: Record -> exporter goroutine batches -> httptest sink.
// Measures the end-to-end producer-side cost with a live exporter draining.
func BenchmarkRecordWithExporter(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer srv.Close()
	r := NewRecorder(8192)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, BatchSize: 256, FlushInterval: 20 * time.Millisecond})
	defer x.Close(time.Second)
	ev := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Record(ev)
	}
	b.StopTimer()
	x.Flush(5 * time.Second)
}

// Exporter drain+encode+POST cost per event (batch of 256 to httptest sink).
func BenchmarkExportBatch256(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Write([]byte(`{"accepted":256,"rejected":0}`))
	}))
	defer srv.Close()
	r := NewRecorder(1024)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, BatchSize: 256, FlushInterval: time.Hour})
	defer x.Close(time.Second)
	ev := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 256; j++ {
			r.Record(ev)
		}
		x.Flush(5 * time.Second)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/256, "ns/event")
}
