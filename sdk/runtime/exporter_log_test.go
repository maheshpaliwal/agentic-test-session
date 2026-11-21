package runtime

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// lockedBuf lets the export goroutine log while the test reads.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// A collector that cannot be reached must not be silent: the service's log
// says so once per outage, and once more when delivery resumes.
func TestExporterLogsOutageAndRecovery(t *testing.T) {
	var out lockedBuf
	log.SetOutput(&out)
	defer log.SetOutput(log.Writer())
	old := log.Flags()
	log.SetFlags(0)
	defer log.SetFlags(old)

	var down atomic.Bool
	down.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer srv.Close()

	rec := NewRecorder(64)
	exp := NewExporter(rec, ExporterConfig{CollectorURL: srv.URL, FlushInterval: 5 * time.Millisecond,
		RetryBackoff: time.Millisecond, MaxCooldown: 10 * time.Millisecond})
	defer exp.Close(time.Second)
	rec.Record(core.Event{SessionID: "s1", SpanID: "a", LocalSeq: 1})

	waitLog := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(out.String(), want) {
			if time.Now().After(deadline) {
				t.Fatalf("log never said %q:\n%s", want, out.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitLog("cannot deliver events to the collector at " + srv.URL)
	time.Sleep(60 * time.Millisecond) // several more failed passes
	if n := strings.Count(out.String(), "cannot deliver"); n != 1 {
		t.Fatalf("the outage was logged %d times, want once:\n%s", n, out.String())
	}
	down.Store(false)
	waitLog("delivering events to the collector at " + srv.URL + " again")
}
