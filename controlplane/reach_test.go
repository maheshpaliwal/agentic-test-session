package controlplane

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// subscriberOf is the listed follower of a service.
func subscriberOf(m *Manager, service string) Subscriber {
	for _, s := range m.Subscribers() {
		if s.Service == service {
			return s
		}
	}
	panic("no follower of " + service)
}

// An SDK instance says whether it can reach its collector, with no session
// running: the control plane keeps that with the instance's connection and
// lists it. An instance that says nothing is not known, which is not the
// same as not reachable; and what an instance says is cleaned like every
// other word of it.
func TestSubscribersListWhetherAnInstanceReachesItsCollector(t *testing.T) {
	m := stoppedMgr(newFlaky(), &logBuffer{}, nil)
	defer m.Close()
	leaveOrders := follow(m, "orders-api", "orders-1")
	defer follow(m, "inventory", "inventory-1")()
	defer follow(m, "legacy", "legacy-1")()

	body := `{"service":"orders-api","instance":"orders-1","reports":[],"collector":{"reachable":false,
		"error":"Get \"http://ci:hunter2@10.0.0.9:9/healthz?token=tok123\": dial tcp 10.0.0.9:9: connect: connection refused \u001b[31m ` + strings.Repeat("x ", 400) + `"}}`
	began := time.Now().Add(-time.Second)
	sayStopped(t, m, body) // a batch like any other
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "inventory", Instance: "inventory-1", Collector: &CollectorReach{Reachable: true, Error: "ignored"}}); err != nil {
		panic(err)
	}
	hello(t, m, "legacy", "legacy-1") // an SDK from before the field

	orders := subscriberOf(m, "orders-api").Collector
	if orders == nil || orders.Reachable || !strings.Contains(orders.Error, "connection refused") || orders.At.Before(began) ||
		strings.Contains(orders.Error, "hunter2") || strings.Contains(orders.Error, "tok123") || strings.Contains(orders.Error, "\x1b") || len(orders.Error) > maxDeliveryError {
		panic(fmt.Sprintf("orders-api: %+v", orders))
	}
	if inv := subscriberOf(m, "inventory").Collector; inv == nil || !inv.Reachable || inv.Error != "" {
		panic(fmt.Sprintf("inventory: %+v", inv))
	}
	if legacy := subscriberOf(m, "legacy"); legacy.Collector != nil || legacy.LastDeliveryReport.IsZero() {
		panic(fmt.Sprintf("an instance that said nothing about its collector: %+v", legacy))
	}
	// As GET /v1/subscribers says it: omitted when not known.
	w := do(t, m.Handler(), "GET", "/v1/subscribers", "127.0.0.1:7070", "", nil)
	var listed []map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &listed); w.Code != http.StatusOK || err != nil || len(listed) != 3 {
		panic(fmt.Sprintf("%d %v %s", w.Code, err, w.Body.String()))
	}
	for _, s := range listed {
		_, has := s["collector"]
		if want := string(s["service"]) != `"legacy"`; has != want {
			panic(fmt.Sprintf("collector listed = %v for %s", has, s["service"]))
		}
	}
	if !strings.Contains(w.Body.String(), `"collector":{"reachable":false,"error":"Get `) || !strings.Contains(w.Body.String(), `"collector":{"reachable":true,"at":`) {
		panic(w.Body.String())
	}

	// A later word replaces the earlier one; one without the field leaves it.
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "orders-api", Instance: "orders-1"}); err != nil {
		panic(err)
	}
	if c := subscriberOf(m, "orders-api").Collector; c == nil || c.Reachable {
		panic(fmt.Sprintf("a heartbeat took the word back: %+v", c))
	}
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "orders-api", Instance: "orders-1", Collector: &CollectorReach{Reachable: true}}); err != nil {
		panic(err)
	}
	if c := subscriberOf(m, "orders-api").Collector; c == nil || !c.Reachable || c.Error != "" {
		panic(fmt.Sprintf("after it said that it reaches the collector: %+v", c))
	}
	// An instance that said that it cannot, without a reason, is still listed
	// as one that cannot.
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "orders-api", Instance: "orders-1", Collector: &CollectorReach{}}); err != nil {
		panic(err)
	}
	if c := subscriberOf(m, "orders-api").Collector; c == nil || c.Reachable || c.Error == "" {
		panic(fmt.Sprintf("unreachable without a reason: %+v", c))
	}

	// The word goes with the connection: a new one is not known until it says.
	leaveOrders()
	defer follow(m, "orders-api", "orders-1")()
	if c := subscriberOf(m, "orders-api").Collector; c != nil {
		panic(fmt.Sprintf("a new connection inherited %+v", c))
	}
}

// What an instance says about its collector decides nothing about a session:
// the session of an instance that cannot reach its collector and recorded
// nothing is complete, as before.
func TestAnUnreachableCollectorAloneDoesNotChangeAVerdict(t *testing.T) {
	col := newScriptCollector().counting()
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	defer follow(m, "orders-api", "orders-1")()
	s := mustStart(m, "p")
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "orders-api", Instance: "orders-1", Collector: &CollectorReach{Error: "connection refused"}}); err != nil {
		panic(err)
	}
	wantComplete(t, mustSeal(t, m, s.ID))
}
