package controlplane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpoint(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	scrape := func() string {
		t.Helper()
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /metrics: %d", resp.StatusCode)
		}
		return string(b)
	}

	if got := scrape(); !strings.Contains(got, `tsession_controlplane_sessions{state="ACTIVE"} 0`) ||
		!strings.Contains(got, `tsession_build_info{service="controlplane",`) {
		t.Fatalf("idle scrape:\n%s", got)
	}
	a, _ := m.Start(StartRequest{})
	b, _ := m.Start(StartRequest{QuiesceSeconds: 1})
	c, _ := m.Start(StartRequest{})
	_ = c
	m.Abort(a.ID)
	m.Stop(b.ID)
	waitState(t, m, b.ID, StateSealed)
	if err := m.Delete(context.Background(), a.ID); err != nil {
		t.Fatal(err)
	}
	m.SetKill(true)
	_, cancel := m.Subscribe()
	defer cancel()

	got := scrape()
	for _, want := range []string{
		`tsession_controlplane_sessions{state="ACTIVE"} 1`,
		`tsession_controlplane_sessions{state="DRAINING"} 0`,
		`tsession_controlplane_sessions_started_total 3`,
		`tsession_controlplane_sessions_finished_total{state="ABORTED"} 1`,
		`tsession_controlplane_sessions_finished_total{state="SEALED"} 1`,
		`tsession_controlplane_sessions_deleted_total 1`,
		`tsession_controlplane_seal_failures_total 0`,
		`tsession_controlplane_kill_switch 1`,
		`tsession_controlplane_subscribers 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
}
