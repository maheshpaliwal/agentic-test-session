package tsessiontest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

// The collector cannot seal: the control plane ends the session SEALED with
// incomplete=true, seal_error and seal_pending. RequireNoFailures is documented
// to fail on an incomplete session.
func TestRequireNoFailuresWithFailedSeal(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.Summary{}) // what an unsealed session's summary says
	})
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.DrainStatus{})
	})
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "storage error", http.StatusInternalServerError)
	})
	col := httptest.NewServer(mux)
	t.Cleanup(col.Close)
	m := controlplane.NewManager(controlplane.Config{Collector: &controlplane.HTTPCollector{BaseURL: col.URL},
		SealRetryBudget: 300 * time.Millisecond, PollInterval: 20 * time.Millisecond})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	opts := client.Options{BaseURL: cp.URL, QuiesceSeconds: 1, CollectorURL: col.URL}

	var s *client.Session
	failed, out := check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, opts)
		tsessiontest.RequireNoFailures(tb, s)
	})
	info := s.Info()
	t.Logf("control plane: state=%s incomplete=%v seal_error=%q", info.State, info.Incomplete, info.SealError)
	if !info.Incomplete || info.SealError == "" {
		t.Fatalf("test setup: expected an incomplete session, got %+v", info)
	}
	if !failed {
		t.Fatalf("RequireNoFailures passed for an incomplete, unsealed session (output %q)", out)
	}
}
