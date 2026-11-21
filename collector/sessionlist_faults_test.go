package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
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
		panic(err)
	}
	defer resp.Body.Close()
	var out []sessionCount
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK || len(out) != 0 {
		panic(fmt.Sprintf("status %d, body %v, err %v", resp.StatusCode, out, err))
	}
	if got := resp.Header.Get(headerUnanswered); got != "2" {
		panic(fmt.Sprintf("%s = %q, want 2", headerUnanswered, got))
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
		panic("lookups against a hanging store never returned")
	}
	if unanswered != 3 || time.Since(start) < listLookupTimeout {
		panic(fmt.Sprintf("unanswered %d after %v, want 3 after about %v", unanswered, time.Since(start), listLookupTimeout))
	}
}

// busyHeadStore fails for one session and records how many lookups ran at
// the same time.
type busyHeadStore struct {
	store.Store
	bad         string
	running, hi atomic.Int64
}

func (s *busyHeadStore) Head(_ context.Context, id string) (uint64, error) {
	n := s.running.Add(1)
	defer s.running.Add(-1)
	for hi := s.hi.Load(); n > hi && !s.hi.CompareAndSwap(hi, n); hi = s.hi.Load() {
	}
	time.Sleep(2 * time.Millisecond) // long enough for the lookups to overlap
	if id == s.bad {
		return 0, fmt.Errorf("connection reset")
	}
	return 3, nil
}

// One lookup that fails costs its own id, not the rest of the request, and
// no more than listLookups run at a time.
func TestSessionCountsOneFailureAndTheBound(t *testing.T) {
	st := &busyHeadStore{Store: store.NewMem(), bad: "ts_7"}
	c := New(st, DefaultConfig())
	t.Cleanup(c.Close)
	var ids []string
	for i := range 5 * listLookups {
		ids = append(ids, fmt.Sprintf("ts_%d", i))
	}
	out, unanswered := c.sessionCounts(context.Background(), ids)
	if unanswered != 1 || len(out) != len(ids)-1 {
		panic(fmt.Sprintf("%d answered, %d unanswered; want %d and 1", len(out), unanswered, len(ids)-1))
	}
	for i, sc := range out { // in the order asked, the failed id left out
		want := ids[i]
		if i >= 7 {
			want = ids[i+1]
		}
		if sc.ID != want || sc.EventCount != 3 {
			t.Errorf("answer %d: %+v, want %s with 3 events", i, sc, want)
		}
	}
	if hi := st.hi.Load(); hi > listLookups || hi < 2 {
		t.Errorf("%d lookups ran at the same time, want more than one and at most %d", hi, listLookups)
	}

	// A request that has ended asks the store nothing more: every id is unanswered.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, unanswered := c.sessionCounts(ctx, ids); len(out) != 0 || unanswered != len(ids) {
		t.Errorf("after the request ended: %d answered, %d unanswered", len(out), unanswered)
	}
}

// A query that does not parse is refused; answering it with every session
// (as an absent ids parameter is) would hide the client's mistake.
func TestSessionListRefusesAMalformedQuery(t *testing.T) {
	_, srv := newSpyC(t, DefaultConfig(), &spyStore{Store: store.NewMem()})
	for _, q := range []string{"?ids=%zz", "?ids=a;x=1"} {
		resp, err := http.Get(srv.URL + "/v1/sessions" + q)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
	}
}
