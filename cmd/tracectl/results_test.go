package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// resultsEnv is a control plane plus a canned collector read API.
type resultsEnv struct {
	cp, col string
	sum     client.Summary
	deleted []string
	// events overrides, per session name, the events the collector serves;
	// a session without an entry gets the default ones.
	events map[string][]core.Event
}

func newResultsEnv(t *testing.T) *resultsEnv {
	t.Helper()
	env := &resultsEnv{}
	yes, no := true, false
	evs := []core.Event{
		{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1_000_000_000, EndNs: 1_050_000_000},
		{Seq: 2, Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders", StartNs: 1_010_000_000, EndNs: 1_012_000_000,
			TxID: "tx1", Attrs: map[string]string{"rows": "1"}, Req: core.PayloadRef{Inline: json.RawMessage(`{"args":[42,"[REDACTED]"]}`), Redacted: true},
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", Effective: &yes, PK: map[string]any{"id": 42},
				Diff: []core.FieldChange{{Field: "status", New: "new"}}}}},
		{Seq: 3, Service: "worker", Kind: core.KindDBRead, Name: "SELECT 1", StartNs: 1_020_000_000},
		{Seq: 4, Service: "worker", Kind: core.KindWorkerEnd, Name: "ship", StartNs: 1_030_000_000, Err: "carrier\n  timeout"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(env.sum)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		from, _ := strconv.ParseUint(r.URL.Query().Get("from_seq"), 10, 64)
		all := evs
		if len(env.events) > 0 {
			s, err := client.Attach(r.Context(), client.Options{BaseURL: env.cp}, r.PathValue("id"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			if over, ok := env.events[s.Info().Name]; ok {
				all = over
			}
		}
		page := slicex.Filter(all, func(e core.Event) bool { return e.Seq >= from })
		if n, _ := strconv.Atoi(r.URL.Query().Get("limit")); n > 0 && len(page) > n {
			page = page[:n]
		}
		json.NewEncoder(w).Encode(client.EventPage{Events: page, NextSeq: 5, Sealed: true})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		node := func(i int, children, linked []*client.TraceNode) *client.TraceNode {
			return &client.TraceNode{Event: evs[i], Children: children, Linked: linked}
		}
		ship := node(3, nil, nil)
		json.NewEncoder(w).Encode(client.Trace{SessionID: r.PathValue("id"), Roots: []*client.TraceNode{
			node(0, []*client.TraceNode{node(1, nil, []*client.TraceNode{ship})}, nil),
			node(2, nil, nil),
		}})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]client.Entity{{Store: "postgres", Entity: "orders", PK: `{"id":42}`, MutationCount: 2}})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.EntityHistory{Store: r.PathValue("store"), Entity: r.PathValue("entity"), PK: `{"id":42}`,
			Mutations: []client.EntityMutation{
				{Seq: 2, Op: "insert", Service: "api", Kind: core.KindDBWrite, Effective: &yes,
					Diff: []core.FieldChange{{Field: "status", New: "new"}}},
				{Seq: 9, Op: "update", Service: "worker", Kind: core.KindDBWrite, Effective: &no,
					Diff: []core.FieldChange{{Field: "status", Old: "new", New: "shipped"}}},
			}})
	})
	// internal API used by the control plane
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.DrainStatus{})
	})
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.SealResult{})
	})
	mux.HandleFunc("DELETE /v1/internal/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		env.deleted = append(env.deleted, r.PathValue("id"))
		json.NewEncoder(w).Encode(map[string]bool{"deleted": true})
	})
	col := httptest.NewServer(mux)
	t.Cleanup(col.Close)
	m := controlplane.NewManager(controlplane.Config{Collector: &controlplane.HTTPCollector{BaseURL: col.URL}})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	env.cp, env.col = cp.URL, col.URL
	env.sum = client.Summary{EventCount: 4, DurationNs: 50_000_000, Sealed: true,
		ByKind: map[string]int{"api.in": 1, "db.write": 1, "db.read": 1, "worker.end": 1}, ByService: map[string]int{"api": 2, "worker": 2},
		Mutations: map[string]map[string]int{"postgres": {"orders": 1}}, Flags: map[string]int{}}
	return env
}

// tc runs tracectl against env and returns exit code, stdout and stderr.
func (env *resultsEnv) tc(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"--cp", env.cp, "--collector", env.col}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

func TestCLIResults(t *testing.T) {
	env := newResultsEnv(t)
	if code, _, e := env.tc("start", "--name", "run-1"); code != 0 {
		t.Fatalf("start: %d %s", code, e)
	}

	code, out, e := env.tc("summary", "run-1")
	if code != 0 || !strings.Contains(out, "events     4 over 50ms") || !strings.Contains(out, "by kind    api.in 1, db.read 1, db.write 1, worker.end 1") ||
		!strings.Contains(out, "mutations  postgres/orders 1") || !strings.Contains(out, "failures   0") {
		t.Fatalf("summary: %d\n%s\n%s", code, out, e)
	}
	if code, out, _ := env.tc("summary", "--json", "run-1"); code != 0 || !strings.Contains(out, `"event_count": 4`) {
		t.Fatalf("summary --json: %d %s", code, out)
	}
	// CI gates: a clean session passes, failures and incompleteness exit 3.
	if code, _, e := env.tc("summary", "run-1", "--fail-on-error", "--fail-on-incomplete"); code != 0 {
		t.Fatalf("clean gate: %d %s", code, e)
	}
	env.sum.Failures = []client.Failure{{Seq: 4, Kind: core.KindWorkerEnd, Service: "worker", Name: "ship", Err: "carrier timeout"}}
	env.sum.Incomplete = true
	if code, out, _ := env.tc("summary", "run-1"); code != 0 || !strings.Contains(out, "FAIL #4 worker.end") || !strings.Contains(out, "incomplete") {
		t.Fatalf("failures must be listed without failing the command: %d\n%s", code, out)
	}
	if code, _, e := env.tc("summary", "run-1", "--fail-on-error"); code != exitCheckFailed || !strings.Contains(e, "1 failed event") {
		t.Fatalf("--fail-on-error: %d %s", code, e)
	}
	if code, _, e := env.tc("summary", "run-1", "--fail-on-incomplete"); code != exitCheckFailed || !strings.Contains(e, "incomplete") {
		t.Fatalf("--fail-on-incomplete: %d %s", code, e)
	}

	code, out, _ = env.tc("events", "run-1")
	if code != 0 || !strings.Contains(out, "4 of 4 events") || !strings.Contains(out, "+10ms") ||
		!strings.Contains(out, "[1 mutation(s)]") || !strings.Contains(out, "ERR carrier timeout") {
		t.Fatalf("events: %d\n%s", code, out)
	}
	if _, out, _ := env.tc("events", "run-1", "--kind", "db.*"); !strings.Contains(out, "2 of 4 events") || strings.Contains(out, "api.in") {
		t.Fatalf("--kind prefix:\n%s", out)
	}
	if _, out, _ := env.tc("events", "run-1", "--kind", "api.in,worker.end", "--service", "worker"); !strings.Contains(out, "1 of 4 events") {
		t.Fatalf("--kind list + --service:\n%s", out)
	}
	if _, out, _ := env.tc("events", "--errors", "--json", "run-1"); strings.Count(out, "\n") != 1 || !strings.Contains(out, `"seq":4`) {
		t.Fatalf("--errors --json:\n%s", out)
	}
	if _, out, _ := env.tc("events", "run-1", "--limit", "2"); !strings.Contains(out, "2 of 4 events") {
		t.Fatalf("--limit:\n%s", out)
	}

	if code, out, _ := env.tc("entities", "run-1"); code != 0 || !strings.Contains(out, `{"id":42}`) || !strings.Contains(out, "1 entities") {
		t.Fatalf("entities: %d\n%s", code, out)
	}
	code, out, _ = env.tc("history", "run-1", "postgres", "orders", "42")
	if code != 0 || !strings.Contains(out, "effective") || !strings.Contains(out, "ROLLED BACK") ||
		!strings.Contains(out, `status: "new" -> "shipped"`) || !strings.Contains(out, `status: ∅ -> "new"`) {
		t.Fatalf("history: %d\n%s", code, out)
	}
	if code, _, e := env.tc("history", "run-1", "postgres"); code == 0 || !strings.Contains(e, "<store> <entity> <pk>") {
		t.Fatalf("history with missing args: %d %s", code, e)
	}

	file := filepath.Join(t.TempDir(), "run.ndjson")
	if code, out, e := env.tc("export", "run-1", "-o", file); code != 0 || out != "" || !strings.Contains(e, "wrote 4 events") {
		t.Fatalf("export: %d %q %s", code, out, e)
	}
	b, err := os.ReadFile(file)
	if err != nil || strings.Count(string(b), "\n") != 4 {
		t.Fatalf("exported file: %q %v", b, err)
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0o600 {
		t.Fatalf("export file mode %v, want 0600 (payloads can be sensitive)", fi.Mode().Perm())
	}
	if _, out, _ := env.tc("export", "run-1"); strings.Count(out, "\n") != 4 {
		t.Fatalf("export to stdout:\n%s", out)
	}
}

func TestCLIAbortAndDelete(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "doomed")
	if code, _, e := env.tc("delete", "doomed"); code != 1 || !strings.Contains(e, "ACTIVE") {
		t.Fatalf("delete of a running session: %d %s", code, e)
	}
	if code, out, e := env.tc("abort", "doomed"); code != 0 || !strings.Contains(out, `"state": "ABORTED"`) {
		t.Fatalf("abort: %d %s %s", code, out, e)
	}
	code, out, e := env.tc("delete", "doomed")
	if code != 0 || !strings.HasPrefix(out, "deleted ts_") {
		t.Fatalf("delete: %d %q %s", code, out, e)
	}
	if len(env.deleted) != 1 {
		t.Fatalf("collector deletes: %v", env.deleted)
	}
	if code, _, _ := env.tc("status", "doomed"); code == 0 {
		t.Fatal("deleted session still resolvable")
	}
}

func TestCLIUsageErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"bogus"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), `unknown command "bogus"`) {
		t.Fatalf("unknown command: %d %s", code, errb.String())
	}
	errb.Reset()
	if code := run(nil, &out, &errb); code != 2 || !strings.Contains(errb.String(), "usage: tracectl") {
		t.Fatalf("no command: %d", code)
	}
	if code := run([]string{"--help"}, &out, &errb); code != 0 {
		t.Fatalf("--help: %d", code)
	}
	if code := run([]string{"summary", "--help"}, &out, &errb); code != 0 {
		t.Fatalf("summary --help: %d", code)
	}
}

func TestKindFilter(t *testing.T) {
	for _, tc := range []struct {
		filter string
		kind   core.Kind
		want   bool
	}{
		{"", core.KindAPIIn, true},
		{"db.write", core.KindDBWrite, true},
		{"db.write", core.KindDBRead, false},
		{"db.*", core.KindDBTxCommit, true},
		{"db.*, queue.publish", core.KindQueuePub, true},
		{"db.*", core.KindAPIIn, false},
		{" , ", core.KindAPIIn, true},
	} {
		if got := newKindFilter(tc.filter).match(tc.kind); got != tc.want {
			t.Errorf("filter %q kind %q: got %v", tc.filter, tc.kind, got)
		}
	}
}
