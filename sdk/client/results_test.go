package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// fakeResults serves the collector read API for one session holding n events.
func fakeResults(t *testing.T, id string, n int) (*httptest.Server, *[]string) {
	t.Helper()
	var paths []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		from, _ := strconv.Atoi(r.URL.Query().Get("from_seq"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 1000
		}
		page := client.EventPage{Events: []core.Event{}, NextSeq: uint64(from), Sealed: true}
		for seq := max(from, 1); seq <= n && len(page.Events) < limit; seq++ {
			page.Events = append(page.Events, core.Event{SessionID: r.PathValue("id"), Seq: uint64(seq), Kind: core.KindDBWrite})
			page.NextSeq = uint64(seq + 1)
		}
		json.NewEncoder(w).Encode(page)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.Summary{SessionID: r.PathValue("id"), EventCount: n, Sealed: true,
			Failures:  []client.Failure{{Seq: 2, Kind: core.KindDBWrite, Err: "boom"}},
			Mutations: map[string]map[string]int{"postgres": {"orders": 3}}})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]client.Entity{{Store: "postgres", Entity: "orders", PK: `{"id":42}`, MutationCount: 3}})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}", func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.EscapedPath())
		no := false
		json.NewEncoder(w).Encode(client.EntityHistory{Store: r.PathValue("store"), Entity: r.PathValue("entity"), PK: r.PathValue("pk"),
			Mutations: []client.EntityMutation{{Seq: 1, Op: "update", Effective: &no}}})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.Trace{SessionID: r.PathValue("id"),
			Roots: []*client.TraceNode{{Event: core.Event{SpanID: "root"}, Children: []*client.TraceNode{{Event: core.Event{SpanID: "kid"}}}}}})
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "" && !strings.Contains(r.URL.Path, id) {
			http.Error(w, `{"error":"unexpected session"}`, http.StatusTeapot)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &paths
}

func TestResultsAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cp := setup(t)
	started, err := client.Start(ctx, client.Options{BaseURL: cp})
	if err != nil {
		t.Fatal(err)
	}
	const n = 10003 // one full page plus a short one
	col, paths := fakeResults(t, started.ID, n)
	s, err := client.Attach(ctx, client.Options{BaseURL: cp, CollectorURL: col.URL + "/"}, started.ID)
	if err != nil {
		t.Fatal(err)
	}

	evs, err := s.Events(ctx)
	if err != nil || len(evs) != n || evs[0].Seq != 1 || evs[n-1].Seq != n {
		t.Fatalf("events: %d %v", len(evs), err)
	}
	if len(*paths) != 2 || !strings.Contains((*paths)[1], "from_seq=10001") {
		t.Fatalf("paging requests: %v", *paths)
	}
	page, err := s.EventsPage(ctx, 5, 2)
	if err != nil || len(page.Events) != 2 || page.Events[0].Seq != 5 || page.NextSeq != 7 || !page.Sealed {
		t.Fatalf("page: %+v %v", page, err)
	}

	sum, err := s.Summary(ctx)
	if err != nil || sum.EventCount != n || len(sum.Failures) != 1 || sum.Failures[0].Err != "boom" || sum.Mutations["postgres"]["orders"] != 3 {
		t.Fatalf("summary: %+v %v", sum, err)
	}
	ents, err := s.Entities(ctx)
	if err != nil || len(ents) != 1 || ents[0].PK != `{"id":42}` {
		t.Fatalf("entities: %+v %v", ents, err)
	}
	// Keys with slashes, spaces and JSON punctuation must survive the path.
	*paths = nil
	h, err := s.History(ctx, "redis", "user:1/cart", `{"id":"a b/c"}`)
	if err != nil || h.Entity != "user:1/cart" || h.PK != `{"id":"a b/c"}` {
		t.Fatalf("history: %+v %v (%v)", h, err, *paths)
	}
	if len(h.Mutations) != 1 || h.Mutations[0].Effective == nil || *h.Mutations[0].Effective {
		t.Fatalf("effective flag lost: %+v", h.Mutations)
	}
	// A collector that predates the flags sends none: nothing is uncertain.
	if h.Mutations[0].Flags != nil || h.Mutations[0].Uncertain() || ents[0].Uncertain != 0 {
		t.Fatalf("old shape: %+v %+v", h.Mutations[0], ents[0])
	}
	// The unknown key is "{}" on the wire; an empty one is not a route of the collector.
	*paths = nil
	if h, err = s.History(ctx, "postgres", "orders", ""); err != nil || h.PK != "{}" || !strings.HasSuffix((*paths)[0], "/orders/%7B%7D") {
		t.Fatalf("history of the unknown key: %+v %v (%v)", h, err, *paths)
	}
	tr, err := s.Trace(ctx)
	if err != nil || len(tr.Roots) != 1 || tr.Roots[0].Children[0].Event.SpanID != "kid" {
		t.Fatalf("trace: %+v %v", tr, err)
	}
}

func TestResultsErrorsCarryServerMessage(t *testing.T) {
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":"store unavailable"}`)
	}))
	defer col.Close()
	ctx := context.Background()
	cp := setup(t)
	started, _ := client.Start(ctx, client.Options{BaseURL: cp})
	s, _ := client.Attach(ctx, client.Options{BaseURL: cp, CollectorURL: col.URL}, started.ID)
	if _, err := s.Summary(ctx); err == nil || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("want the server's message, got %v", err)
	}
	if _, err := s.Events(ctx); err == nil {
		t.Fatal("Events swallowed the error")
	}
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var deleted []string
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			deleted = append(deleted, r.URL.Path)
			json.NewEncoder(w).Encode(map[string]bool{"deleted": true})
		case strings.HasSuffix(r.URL.Path, "/seal"):
			json.NewEncoder(w).Encode(controlplane.SealResult{})
		default:
			json.NewEncoder(w).Encode(controlplane.DrainStatus{})
		}
	}))
	defer col.Close()
	m := controlplane.NewManager(controlplane.Config{Collector: &controlplane.HTTPCollector{BaseURL: col.URL},
		PollInterval: 5 * time.Millisecond, TimeUnit: 10 * time.Millisecond})
	defer m.Close()
	cp := httptest.NewServer(m.Handler())
	defer cp.Close()

	s, err := client.Start(ctx, client.Options{BaseURL: cp.URL, QuiesceSeconds: 1, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx); err == nil || !strings.Contains(err.Error(), "ACTIVE") {
		t.Fatalf("deleting a running session must fail with its state, got %v", err)
	}
	if _, err := s.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || !strings.HasSuffix(deleted[0], "/"+s.ID) {
		t.Fatalf("collector delete calls: %v", deleted)
	}
	if _, err := client.Attach(ctx, client.Options{BaseURL: cp.URL}, s.ID); err == nil {
		t.Fatal("deleted session still attachable")
	}
}

// The flags of a history entry and the uncertain count of an entity are
// optional on the wire: read when the collector sends them.
func TestEntityUncertaintyDecodes(t *testing.T) {
	var h client.EntityHistory
	if err := json.Unmarshal([]byte(`{"store":"postgres","entity":"stock","pk":"{\"sku\":\"w\"}","mutations":[
		{"seq":1,"op":"update","flags":["pk_not_unique","after_unavailable"]},
		{"seq":2,"op":"update","flags":["redacted"]},
		{"seq":3,"op":"update","some_later_field":true}]}`), &h); err != nil {
		t.Fatal(err)
	}
	if len(h.Mutations) != 3 || !h.Mutations[0].Uncertain() || h.Mutations[1].Uncertain() || h.Mutations[2].Uncertain() ||
		len(h.Mutations[0].Flags) != 2 || len(h.Mutations[1].Flags) != 1 {
		t.Fatalf("history: %+v", h)
	}
	var ents []client.Entity
	if err := json.Unmarshal([]byte(`[{"store":"postgres","entity":"stock","pk":"{}","mutation_count":3,"uncertain":2},
		{"store":"postgres","entity":"orders","pk":"{\"id\":1}","mutation_count":1}]`), &ents); err != nil {
		t.Fatal(err)
	}
	if ents[0].Uncertain != 2 || ents[1].Uncertain != 0 {
		t.Fatalf("entities: %+v", ents)
	}
}
