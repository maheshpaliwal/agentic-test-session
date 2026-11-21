package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
)

type panicHeadStore struct{ store.Store }

func (panicHeadStore) Head(context.Context, string) (uint64, error) { panic("store bug") }

// hangHeadStore blocks until its context ends, as a query on a dead connection does.
type hangHeadStore struct{ store.Store }

func (hangHeadStore) Head(ctx context.Context, _ string) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// The by-id lookups run on goroutines of their own: a panic in the store must
// cost the one request its answer, not the collector its life.
func TestSessionCountsSurvivesAPanickingStore(t *testing.T) {
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: panicHeadStore{store.NewMem()}})
	resp, err := http.Get(srv.URL + "/v1/sessions?ids=ts_a,ts_b")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out []sessionCount
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK || len(out) != 0 {
		t.Fatalf("status %d, body %v, err %v", resp.StatusCode, out, err)
	}
	if got := resp.Header.Get(headerUnanswered); got != "2" {
		t.Fatalf("%s = %q, want 2", headerUnanswered, got)
	}
}

// A store that hangs must not hold the lookups until the client gives up.
func TestSessionCountsLookupsAreBounded(t *testing.T) {
	c := New(hangHeadStore{store.NewMem()}, DefaultConfig())
	t.Cleanup(c.Close)
	start := time.Now()
	done := make(chan struct{})
	var unanswered int
	go func() {
		defer close(done)
		_, unanswered = c.sessionCounts(context.Background(), []string{"ts_a", "ts_b", "ts_c"})
	}()
	select {
	case <-done:
	case <-time.After(listLookupTimeout + 10*time.Second):
		t.Fatal("lookups against a hanging store never returned")
	}
	if unanswered != 3 || time.Since(start) < listLookupTimeout {
		t.Fatalf("unanswered %d after %v, want 3 after about %v", unanswered, time.Since(start), listLookupTimeout)
	}
}

// A query that does not parse is refused; answering it with every session
// (as an absent ids parameter is) would hide the client's mistake.
func TestSessionListRefusesAMalformedQuery(t *testing.T) {
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: store.NewMem()})
	for _, q := range []string{"?ids=%zz", "?ids=a;x=1"} {
		resp, err := http.Get(srv.URL + "/v1/sessions" + q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
	}
}
