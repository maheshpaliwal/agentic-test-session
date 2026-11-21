package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

// The Go client's results API and session deletion against a real collector,
// control plane and instrumented app.
func TestE2E_ClientResultsAndDelete(t *testing.T) {
	pgDSN, redisAddr := itEnv(t)
	const appSchema = "app_it_results"
	schemaSetup(t, pgDSN, appSchema)
	in := newInfra(t, store.NewMem())
	in.startRuntime(t, "orders")
	ns := "itres:"
	delNS(t, redisAddr, ns)
	ae := newApp(t, withParam(pgDSN, "search_path", appSchema), redisAddr, ns)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := client.Start(ctx, client.Options{BaseURL: in.cpSrv.URL, CollectorURL: in.colSrv.URL, Name: "results",
		TTLSeconds: 600, QuiesceSeconds: 8, MaxDrainSeconds: 100, PollInterval: 100 * time.Millisecond})
	if err != nil {
		panic(err)
	}
	waitActive(t, s.ID)
	code, order := ae.createOrder(t, s.Header(), orderBody{SKU: "widget", Qty: 1, Customer: "results-it"})
	if code != 201 || order.ID == 0 {
		panic(fmt.Sprintf("POST /orders: %d %+v", code, order))
	}
	runtime.Flush(3 * time.Second)
	if _, err := s.StopAndWait(ctx); err != nil {
		panic(err)
	}

	sum, err := s.Summary(ctx)
	if err != nil || !sum.Sealed || sum.EventCount == 0 || sum.SessionID != s.ID {
		panic(fmt.Sprintf("summary: %+v %v", sum, err))
	}
	if sum.ByKind["api.in"] == 0 || sum.ByKind["db.write"] == 0 || sum.Mutations["postgres"]["orders"] == 0 {
		panic(fmt.Sprintf("summary misses the order write: %+v", sum))
	}
	evs, err := s.Events(ctx)
	if err != nil || len(evs) != sum.EventCount {
		panic(fmt.Sprintf("events: %d (summary says %d) %v", len(evs), sum.EventCount, err))
	}
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			panic(fmt.Sprintf("event %d has seq %d", i, e.Seq))
		}
	}
	page, err := s.EventsPage(ctx, 2, 3)
	if err != nil || len(page.Events) != min(3, len(evs)-1) || page.Events[0].Seq != 2 || !page.Sealed {
		panic(fmt.Sprintf("page: %+v %v", page, err))
	}

	ents, err := s.Entities(ctx)
	if err != nil {
		panic(err)
	}
	var orderPK string
	for _, e := range ents {
		if e.Store == "postgres" && e.Entity == "orders" {
			orderPK = e.PK
		}
	}
	if orderPK == "" {
		panic(fmt.Sprintf("orders row not among the entities: %+v", ents))
	}
	for _, pk := range []string{orderPK, strconv.FormatInt(order.ID, 10)} { // canonical JSON and bare value
		h, err := s.History(ctx, "postgres", "orders", pk)
		if err != nil || len(h.Mutations) == 0 {
			panic(fmt.Sprintf("history(%q): %+v %v", pk, h, err))
		}
		if m := h.Mutations[0]; m.Op != "insert" || m.Effective == nil || !*m.Effective {
			panic(fmt.Sprintf("first mutation of the order: %+v", m))
		}
	}
	tr, err := s.Trace(ctx)
	if err != nil || len(tr.Roots) == 0 || tr.SessionID != s.ID {
		panic(fmt.Sprintf("trace: %+v %v", tr, err))
	}
	found := false
	for _, r := range tr.Roots {
		if r.Event.Kind == "api.in" && strings.Contains(r.Event.Name, "/orders") && len(r.Children) > 0 {
			found = true
		}
	}
	if !found {
		panic("no api.in root with children in the trace")
	}

	// Delete: gone from the collector and from the control plane.
	if err := s.Delete(ctx); err != nil {
		panic(err)
	}
	col := colAPI{in.colSrv.URL}
	for _, si := range col.sessions(t) {
		if si.ID == s.ID {
			panic(fmt.Sprintf("collector still lists the deleted session: %+v", si))
		}
	}
	if evs := col.events(t, s.ID); len(evs) != 0 {
		panic(fmt.Sprintf("%d events survived the delete", len(evs)))
	}
	if _, err := s.Get(ctx); err == nil {
		panic("control plane still knows the deleted session")
	}
}

// The client mirrors the collector's response types by hand (the root module
// cannot import the collector). This fails when the collector grows a field
// the client does not know.
func TestClientTypesMatchCollector(t *testing.T) {
	yes := true
	for name, pair := range map[string][2]any{
		"summary": {collector.Summary{SessionID: "s", ByKind: map[string]int{"a": 1}, Failures: []collector.Failure{{Seq: 1, Err: "x"}},
			PendingIDs: []string{"p"}, Flags: map[string]int{"f": 1}, Mutations: map[string]map[string]int{"s": {"e": 1}},
			Quarantined: 1, Incomplete: true, Sealed: true, DroppedEvents: 2, DurationNs: 3, PendingAsync: 1, EventCount: 4,
			ByService: map[string]int{"svc": 1}}, &client.Summary{}},
		"entity": {collector.Entity{Store: "s", Entity: "e", PK: "p", MutationCount: 1, Uncertain: 1, Services: []string{"svc"}}, &client.Entity{}},
		"history": {collector.EntityHistory{Store: "s", Entity: "e", PK: "p", Mutations: []collector.EntityMutation{
			{Seq: 1, SpanID: "sp", Service: "svc", Kind: "db.write", TxID: "t", EndNs: 1, Op: "update", Source: "sdk", Effective: &yes}}},
			&client.EntityHistory{}},
		"trace": {collector.BuildTrace("s", nil), &client.Trace{}},
	} {
		b, err := json.Marshal(pair[0])
		if err != nil {
			panic(err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(pair[1]); err != nil {
			t.Errorf("%s: client type cannot hold the collector's response: %v\n%s", name, err, b)
			continue
		}
		back, _ := json.Marshal(pair[1])
		var want, got any
		_ = json.Unmarshal(b, &want)
		_ = json.Unmarshal(back, &got)
		wb, _ := json.Marshal(want)
		gb, _ := json.Marshal(got)
		if !bytes.Equal(wb, gb) {
			t.Errorf("%s: round trip changed the document\ncollector: %s\nclient:    %s", name, wb, gb)
		}
	}
}
