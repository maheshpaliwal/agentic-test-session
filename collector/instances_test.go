package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
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
				panic(fmt.Sprintf("resend: %+v", r))
			}
			want := []InstanceCount{{"inventory", "i-1", 1}, {"orders-api", "o-1", 2}, {"orders-api", "o-2", 1}}
			if ds := drainStatus(t, srv.URL); !reflect.DeepEqual(ds.Instances, want) || ds.InstancesTruncated {
				panic(fmt.Sprintf("instances %+v truncated=%v, want %+v", ds.Instances, ds.InstancesTruncated, want))
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
			panic(fmt.Sprintf("instances %+v, want %+v", ds.Instances, want))
		}
	}
	if c.seq.has(sid) {
		panic("drain-status created an actor")
	}
}

// A session without events answers with an empty list, not without one: a
// control plane reads a missing field as "this collector does not count".
func TestDrainStatusInstancesIsAlwaysPresent(t *testing.T) {
	_, srv := newC(t, 0, "")
	resp, err := http.Get(srv.URL + "/v1/internal/sessions/ts_nothing/drain-status")
	if err != nil {
		panic(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"instances":[]`) || strings.Contains(string(b), "instances_truncated") {
		panic(fmt.Sprintf("drain-status of an unknown session: %s", b))
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
		panic(err)
	}
	ds, err := c.DrainStatus(context.Background(), sid)
	if err != nil {
		panic(err)
	}
	if len(ds.Instances) != MaxStatusInstances || !ds.InstancesTruncated || ds.Instances[0].Instance != "i0000" ||
		ds.Instances[MaxStatusInstances-1].Instance != fmt.Sprintf("i%04d", MaxStatusInstances-1) {
		panic(fmt.Sprintf("%d instances, truncated=%v, first %+v", len(ds.Instances), ds.InstancesTruncated, ds.Instances[0]))
	}
}

func instanceCounts(t *testing.T, url, id string, ask ...InstanceCount) (int, []InstanceCount) {
	t.Helper()
	body, _ := json.Marshal(map[string][]InstanceCount{"instances": ask})
	resp, err := http.Post(url+"/v1/internal/sessions/"+id+"/instance-counts", "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var out struct {
		Instances []InstanceCount `json:"instances"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out.Instances
}

// The collector answers for the instances it is asked about by name, however
// many others sent events: the list of drain-status can be crowded out (it
// holds the first 512 in order), an answer to a question cannot. That is what
// the control plane holds a delivery report against when the list is cut.
func TestInstanceCountsAnswerForNamedInstances(t *testing.T) {
	for _, window := range []int{0, 1} {
		t.Run(fmt.Sprintf("window=%d", window), func(t *testing.T) {
			c, srv := newC(t, window, "")
			evs := make([]core.Event, 0, MaxStatusInstances+8)
			for i := range MaxStatusInstances + 1 { // all sort before "real"
				evs = append(evs, evOf("a", fmt.Sprintf("x%04d", i), core.KindCacheRead, 1))
			}
			evs = append(evs, evOf("real", "real-1", core.KindCacheRead, 1), evOf("real", "real-1", core.KindCacheRead, 2),
				evOf("real ", "real-1", core.KindCacheRead, 3), // the same instance to the control plane: names are cleaned
				evOf("real", "real-1", core.KindSDKDropped, 4))
			if _, err := c.Ingest(context.Background(), "", evs); err != nil {
				panic(err)
			}
			if ds := drainStatus(t, srv.URL); !ds.InstancesTruncated || slices.ContainsFunc(ds.Instances, func(i InstanceCount) bool { return i.Service == "real" }) {
				panic(fmt.Sprintf("test setup: the list should be cut before the instance asked about (truncated=%v)", ds.InstancesTruncated))
			}
			ask := []InstanceCount{{Service: "real", Instance: "real-1"}, {Service: "nobody", Instance: "n-1"}, {Service: "a", Instance: "x0003"}}
			want := []InstanceCount{{"real", "real-1", 3}, {"nobody", "n-1", 0}, {"a", "x0003", 1}}
			for _, evicted := range []bool{false, true, true} { // from the actor, from the store, from the memo
				if evicted {
					c.seq.evictAll()
				}
				if code, got := instanceCounts(t, srv.URL, sid, ask...); code != http.StatusOK || !reflect.DeepEqual(got, want) {
					panic(fmt.Sprintf("evicted=%v: HTTP %d %+v, want %+v", evicted, code, got, want))
				}
			}
			if c.seq.has(sid) {
				panic("instance-counts created an actor")
			}
		})
	}
}

// Without a cut list the answer is the same, an unknown session has nothing
// from anybody, and a request is bounded.
func TestInstanceCountsBounds(t *testing.T) {
	_, srv := newC(t, 0, "")
	post(t, srv.URL, evOf("a", "a-1", core.KindCacheRead, 1), evOf("a", "a-1", core.KindCacheRead, 2))
	if code, got := instanceCounts(t, srv.URL, sid, InstanceCount{Service: "a", Instance: "a-1"}); code != http.StatusOK || len(got) != 1 || got[0].Events != 2 {
		panic(fmt.Sprintf("HTTP %d %+v", code, got))
	}
	if code, got := instanceCounts(t, srv.URL, "ts_nothing", InstanceCount{Service: "a", Instance: "a-1"}); code != http.StatusOK || len(got) != 1 || got[0].Events != 0 {
		panic(fmt.Sprintf("unknown session: HTTP %d %+v", code, got))
	}
	if code, got := instanceCounts(t, srv.URL, sid); code != http.StatusOK || len(got) != 0 {
		panic(fmt.Sprintf("nothing asked: HTTP %d %+v", code, got))
	}
	if code, _ := instanceCounts(t, srv.URL, sid, make([]InstanceCount, MaxInstanceCountsAsk+1)...); code != http.StatusBadRequest {
		panic(fmt.Sprintf("too many instances: HTTP %d", code))
	}
	resp, err := http.Post(srv.URL+"/v1/internal/sessions/"+sid+"/instance-counts", "application/json", strings.NewReader("{"))
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		panic(fmt.Sprintf("bad JSON: HTTP %d", resp.StatusCode))
	}
}
