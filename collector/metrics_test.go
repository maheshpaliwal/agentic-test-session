package collector

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("GET /metrics: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return string(b)
}

func TestMetricsEndpoint(t *testing.T) {
	c, srv := newC(t, 0, "")
	ctx := context.Background()
	bad := ev("x", core.KindAPIIn, 9, 9)
	bad.SessionID = "not a valid id"
	a := ev("a", core.KindQueuePub, 1, 1) // never consumed: the seal is incomplete
	post(t, srv.URL, a, a, ev("b", core.KindAPIIn, 2, 2), bad)
	if _, err := c.Seal(ctx, sid); err != nil {
		t.Fatal(err)
	}
	other := ev("c", core.KindAPIIn, 1, 1)
	other.SessionID = "ts_other"
	post(t, srv.URL, other)
	if _, err := c.Seal(ctx, "ts_other"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "ts_other"); err != nil {
		t.Fatal(err)
	}

	got := scrape(t, srv.URL)
	for _, want := range []string{
		`tsession_collector_ingested_events_total{result="accepted"} 3`,
		`tsession_collector_ingested_events_total{result="duplicate"} 1`,
		`tsession_collector_rejected_events_total{reason="invalid_session_id"} 1`,
		`tsession_collector_seals_total{result="incomplete"} 1`,
		`tsession_collector_seals_total{result="complete"} 1`,
		`tsession_collector_deleted_sessions_total{cause="api"} 1`,
		`tsession_collector_quarantined_events_total 0`,
		`tsession_collector_buffered_events 0`,
		`# TYPE tsession_collector_sessions_in_memory gauge`,
		`tsession_build_info{service="collector",`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	// Like every other endpoint, /metrics refuses a foreign Host (DNS rebinding).
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/metrics", nil)
	req.Host = "evil.example"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Host: %d", resp.StatusCode)
	}
}

func TestMetricsCountStoreTrouble(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	sp := &spyStore{poison: func(e core.Event) bool { return e.SpanID == "poison" }}
	c, srv := newSpyC(t, cfg, sp)
	if _, err := c.Ingest(context.Background(), "", []core.Event{ev("ok", core.KindAPIIn, 1, 1), ev("poison", core.KindAPIIn, 2, 2)}); err != nil {
		t.Fatal(err)
	}
	got := scrape(t, srv.URL)
	if !strings.Contains(got, "tsession_collector_quarantined_events_total 1") {
		t.Errorf("quarantine not counted:\n%s", got)
	}
	if strings.Contains(got, "tsession_collector_store_append_failures_total 0") {
		t.Errorf("append failures not counted:\n%s", got)
	}
	sp.down.Store(true)
	if _, err := c.Ingest(context.Background(), "", []core.Event{ev("late", core.KindAPIIn, 3, 3)}); err == nil {
		t.Fatal("expected a storage error")
	}
	if got := scrape(t, srv.URL); !strings.Contains(got, `tsession_collector_ingest_errors_total{cause="storage"} 1`) ||
		!strings.Contains(got, "tsession_collector_buffered_events 1") {
		t.Errorf("storage error / buffered gauge:\n%s", got)
	}
}
