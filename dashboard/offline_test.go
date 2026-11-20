package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func writeExport(t *testing.T, name string, evs ...core.Event) string {
	t.Helper()
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for i := range evs {
		if err := enc.Encode(&evs[i]); err != nil {
			t.Fatal(err)
		}
	}
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOfflineServesAnExportReadOnly(t *testing.T) {
	no := false
	// Written out of order, as a concatenation of pages could be.
	file := writeExport(t, "checkout-failed.ndjson",
		core.Event{SessionID: "ts_run1", Seq: 2, SpanID: "b", ParentID: "a", Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders",
			StartNs: 2e9, EndNs: 3e9, TxID: "tx1",
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", PK: map[string]any{"id": 7}, Effective: &no}}},
		core.Event{SessionID: "ts_run1", Seq: 1, SpanID: "a", Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1e9, EndNs: 4e9, Err: "boom"},
		// The collector decided this commit failed; a naive re-resolution would call the insert effective.
		core.Event{SessionID: "ts_run1", Seq: 3, SpanID: "c", ParentID: "a", Service: "api", Kind: core.KindDBTxCommit, TxID: "tx1",
			StartNs: 3e9, EndNs: 3e9, Attrs: map[string]string{"commit_failed": "true"}},
	)
	h, err := New(Config{Open: []string{file}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	_, b := get(t, srv.URL, "/healthz")
	if !strings.Contains(string(b), `"offline":true`) || !strings.Contains(string(b), `"demo":false`) {
		t.Fatalf("healthz: %s", b)
	}
	var list []Session
	_, b = get(t, srv.URL, "/api/cp/v1/sessions")
	if err := json.Unmarshal(b, &list); err != nil || len(list) != 1 {
		t.Fatalf("sessions: %s %v", b, err)
	}
	if s := list[0]; s.ID != "ts_run1" || s.Name != "checkout-failed" || s.State != "SEALED" || s.InitiatedBy != "file" {
		t.Fatalf("session: %+v", s)
	}

	var page struct {
		Events []core.Event `json:"events"`
		Sealed bool         `json:"sealed"`
	}
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/events")
	if err := json.Unmarshal(b, &page); err != nil || len(page.Events) != 3 || !page.Sealed {
		t.Fatalf("events: %s %v", b, err)
	}
	for i, e := range page.Events {
		if e.Seq != uint64(i+1) {
			t.Fatalf("events not in seq order: %d at %d", e.Seq, i)
		}
	}
	if eff := page.Events[1].Mutations[0].Effective; eff == nil || *eff {
		t.Fatalf("the exported transaction outcome must be kept as recorded, got %v", eff)
	}
	var tr struct {
		Roots []*Node `json:"roots"`
	}
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/trace")
	if err := json.Unmarshal(b, &tr); err != nil || len(tr.Roots) != 1 || len(tr.Roots[0].Children) != 2 {
		t.Fatalf("trace: %s %v", b, err)
	}
	var sum map[string]any
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/summary")
	if err := json.Unmarshal(b, &sum); err != nil || sum["event_count"] != float64(3) {
		t.Fatalf("summary: %s %v", b, err)
	}

	// Nothing can be changed.
	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/api/cp/v1/sessions"},
		{http.MethodPost, "/api/cp/v1/sessions/ts_run1/stop"},
		{http.MethodPatch, "/api/cp/v1/sessions/ts_run1"},
		{http.MethodDelete, "/api/cp/v1/sessions/ts_run1"},
	} {
		req, _ := http.NewRequest(c.method, srv.URL+c.path, strings.NewReader(`{"name":"x"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", srv.URL)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: status %d, want 405", c.method, c.path, resp.StatusCode)
		}
	}
	if _, b := get(t, srv.URL, "/api/cp/v1/sessions"); !strings.Contains(string(b), "ts_run1") {
		t.Fatalf("session gone after refused changes: %s", b)
	}
}

func TestOfflineRejectsBadFiles(t *testing.T) {
	ev := core.Event{SessionID: "ts_a", Seq: 1, Kind: core.KindAPIIn}
	other := core.Event{SessionID: "ts_b", Seq: 2, Kind: core.KindAPIIn}
	good := writeExport(t, "a.ndjson", ev)
	notJSON := filepath.Join(t.TempDir(), "x.ndjson")
	os.WriteFile(notJSON, []byte("hello\n"), 0o600)
	for name, cfg := range map[string]Config{
		"missing file":       {Open: []string{filepath.Join(t.TempDir(), "nope.ndjson")}},
		"not json":           {Open: []string{notJSON}},
		"empty":              {Open: []string{writeExport(t, "empty.ndjson")}},
		"not an event":       {Open: []string{writeExport(t, "noseq.ndjson", core.Event{Name: "x"})}},
		"two sessions":       {Open: []string{writeExport(t, "mixed.ndjson", ev, other)}},
		"same session twice": {Open: []string{good, writeExport(t, "again.ndjson", ev)}},
		"with demo":          {Open: []string{good}, Demo: true},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := New(Config{Open: []string{good, writeExport(t, "b.ndjson", other)}}); err != nil {
		t.Fatalf("two different sessions: %v", err)
	}
}
