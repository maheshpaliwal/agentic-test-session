package runtime

import (
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestDisplayURLDropsCredentials(t *testing.T) {
	for in, want := range map[string]string{
		"http://ingest:s3cr3tpw@collector:7071/base?token=tok_abc#frag": "http://collector:7071/base",
		"http://127.0.0.1:7071": "http://127.0.0.1:7071",
		"::not a url":           "<unparseable URL>",
		"":                      "<unparseable URL>",
	} {
		if got := DisplayURL(in); got != want {
			t.Errorf("DisplayURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// The exporter's log lines go to the host application's log: they must not
// carry the credentials of the collector URL, and a collector that keeps
// flapping must not produce a line per flap.
func TestExporterLogIsSafeAndBounded(t *testing.T) {
	var out lockedBuf
	log.SetOutput(&out)
	defer log.SetOutput(log.Writer())

	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%2 == 1 { // every other request fails
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer srv.Close()
	withCreds := strings.Replace(srv.URL, "http://", "http://ingest:s3cr3tpw@", 1) + "/?token=tok_abc"

	rec := NewRecorder(256)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: withCreds, FlushInterval: 2 * time.Millisecond,
		RetryBackoff: time.Millisecond, MaxRetries: -1, MaxCooldown: 4 * time.Millisecond})
	defer exp.Close(time.Second)
	deadline := time.Now().Add(700 * time.Millisecond)
	for seq := uint64(1); time.Now().Before(deadline); seq++ {
		rec.Record(core.Event{SessionID: "s1", SpanID: "a", LocalSeq: seq})
		time.Sleep(3 * time.Millisecond)
	}
	logged := out.String()
	if n.Load() < 20 {
		t.Fatalf("test setup: only %d requests reached the collector", n.Load())
	}
	if strings.Contains(logged, "s3cr3tpw") || strings.Contains(logged, "tok_abc") {
		t.Fatalf("credentials in the log:\n%s", logged)
	}
	if lines := strings.Count(logged, "tsession:"); lines > 2 {
		t.Fatalf("%d log lines for a flapping collector within one minute, want at most one outage and one recovery:\n%s", lines, logged)
	}
}
