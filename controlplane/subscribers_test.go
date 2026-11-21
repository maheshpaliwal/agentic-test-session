package controlplane

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestSubscribersListsConnectedServices(t *testing.T) {
	m := NewManager(Config{})
	defer m.Close()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	list := func() []Subscriber {
		t.Helper()
		resp, err := http.Get(srv.URL + "/v1/subscribers")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out []Subscriber
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != 200 {
			t.Fatalf("status %d: %v", resp.StatusCode, err)
		}
		return out
	}
	if got := list(); got == nil || len(got) != 0 {
		t.Fatalf("no followers must list as [], got %#v", got)
	}

	follow := func(service, instance string) (stop func()) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/active/stream", nil)
		if service != "" {
			req.Header.Set(core.HeaderService, service)
			req.Header.Set(core.HeaderInstance, instance)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return func() { resp.Body.Close() }
	}
	stopB := follow("worker", "worker-2")
	defer stopB()
	stopA := follow("api\t\u00e9"+strings.Repeat("x", 300), "api-1")
	defer stopA()
	stopAnon := follow("", "")
	defer stopAnon()

	waitFor := func(n int) []Subscriber {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			got := list()
			if len(got) == n {
				return got
			}
			if time.Now().After(deadline) {
				t.Fatalf("subscribers: %+v, want %d", got, n)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	got := waitFor(3)
	// Sorted by service, then instance; the unnamed follower sorts first.
	if got[0].Service != "" || got[2].Service != "worker" || got[2].Instance != "worker-2" || got[2].ConnectedAt.IsZero() {
		t.Fatalf("subscribers: %+v", got)
	}
	if s := got[1].Service; len(s) != 128 || !strings.HasPrefix(s, "apixxx") {
		t.Fatalf("reported name not cleaned and capped: %q", s)
	}
	stopB()
	if got := waitFor(2); got[1].Instance != "api-1" {
		t.Fatalf("after one follower left: %+v", got)
	}
}
