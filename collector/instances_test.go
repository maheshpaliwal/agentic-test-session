package collector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func evOf(service, instance string, kind core.Kind, local uint64) core.Event {
	e := ev(fmt.Sprintf("%s-%d", instance, local), kind, 100+local, local)
	e.Service, e.Instance = service, instance
	return e
}

func drainStatus(t *testing.T, url string) DrainStatus {
	t.Helper()
	var ds DrainStatus
	get(t, url+"/v1/internal/sessions/"+sid+"/drain-status", &ds)
	return ds
}

// Drain-status says how many events of the session the collector has from
// every SDK instance: what the control plane holds an instance's own delivery
// report against. An event counts once however often it was sent, whether it
// is stored or still in the reorder window, and an sdk.dropped event (the
// SDK's report of a drop count, not an event of the session) does not count.
func TestDrainStatusCountsEventsPerInstance(t *testing.T) {
	for _, window := range []int{0, 1} {
		t.Run(fmt.Sprintf("window=%d", window), func(t *testing.T) {
			_, srv := newC(t, window, "")
			post(t, srv.URL,
				evOf("orders-api", "o-1", core.KindCacheRead, 1),
				evOf("orders-api", "o-1", core.KindCacheRead, 2),
				evOf("orders-api", "o-2", core.KindCacheRead, 1),
				evOf("inventory", "i-1", core.KindCacheRead, 1))
			// A resend of two of them, and a drop count.
			dropped := evOf("orders-api", "o-1", core.KindSDKDropped, 9)
			dropped.Attrs = map[string]string{"count": "4"}
			if r := post(t, srv.URL, evOf("orders-api", "o-1", core.KindCacheRead, 2), evOf("inventory", "i-1", core.KindCacheRead, 1), dropped); r.Duplicates != 2 {
				t.Fatalf("resend: %+v", r)
			}
			want := []InstanceCount{{"inventory", "i-1", 1}, {"orders-api", "o-1", 2}, {"orders-api", "o-2", 1}}
			if ds := drainStatus(t, srv.URL); !reflect.DeepEqual(ds.Instances, want) || ds.InstancesTruncated {
				t.Fatalf("instances %+v truncated=%v, want %+v", ds.Instances, ds.InstancesTruncated, want)
			}
		})
	}
}

// The counts survive the eviction of the session's actor and a restart: they
// are rebuilt from the stored events.
func TestDrainStatusInstancesFromTheStore(t *testing.T) {
	c, srv := newC(t, 0, "")
	post(t, srv.URL, evOf("a", "a-1", core.KindCacheRead, 1), evOf("a", "a-1", core.KindCacheRead, 2), evOf("b", "b-1", core.KindCacheRead, 1))
	c.seq.evictAll()
	want := []InstanceCount{{"a", "a-1", 2}, {"b", "b-1", 1}}
	for range 2 { // the second answer comes from the status memo
		if ds := drainStatus(t, srv.URL); !reflect.DeepEqual(ds.Instances, want) {
			t.Fatalf("instances %+v, want %+v", ds.Instances, want)
		}
	}
	if c.seq.has(sid) {
		t.Fatal("drain-status created an actor")
	}
}

// A session without events answers with an empty list, not without one: a
// control plane reads a missing field as "this collector does not count".
func TestDrainStatusInstancesIsAlwaysPresent(t *testing.T) {
	_, srv := newC(t, 0, "")
	resp, err := http.Get(srv.URL + "/v1/internal/sessions/ts_nothing/drain-status")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"instances":[]`) || strings.Contains(string(b), "instances_truncated") {
		t.Fatalf("drain-status of an unknown session: %s", b)
	}
}

// The list is bounded, and says so when it left instances out.
func TestDrainStatusInstancesAreBounded(t *testing.T) {
	c, _ := newC(t, 0, "")
	evs := make([]core.Event, 0, MaxStatusInstances+5)
	for i := range MaxStatusInstances + 5 {
		evs = append(evs, evOf("svc", fmt.Sprintf("i%04d", i), core.KindCacheRead, 1))
	}
	if _, err := c.Ingest(context.Background(), "", evs); err != nil {
		t.Fatal(err)
	}
	ds, err := c.DrainStatus(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds.Instances) != MaxStatusInstances || !ds.InstancesTruncated || ds.Instances[0].Instance != "i0000" ||
		ds.Instances[MaxStatusInstances-1].Instance != fmt.Sprintf("i%04d", MaxStatusInstances-1) {
		t.Fatalf("%d instances, truncated=%v, first %+v", len(ds.Instances), ds.InstancesTruncated, ds.Instances[0])
	}
}
