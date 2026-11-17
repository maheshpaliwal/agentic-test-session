package collector

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func del(t *testing.T, url string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestDeleteSession(t *testing.T) {
	dir := t.TempDir()
	c, srv := newC(t, 0, dir)
	ctx := context.Background()
	post(t, srv.URL, ev("a", core.KindQueuePub, 1, 1), ev("b", core.KindDBWrite, 2, 2))
	if _, err := c.Seal(ctx, sid); err != nil {
		t.Fatal(err)
	}
	var sum Summary
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &sum) // warms the view cache
	if sum.EventCount != 2 || !sum.Sealed {
		t.Fatalf("summary before delete: %+v", sum)
	}
	if _, err := os.Stat(filepath.Join(dir, sid, "manifest.json")); err != nil {
		t.Fatalf("bundle missing before delete: %v", err)
	}

	if code := del(t, srv.URL+"/v1/internal/sessions/"+sid); code != http.StatusOK {
		t.Fatalf("delete: %d", code)
	}
	if code := del(t, srv.URL+"/v1/internal/sessions/"+sid); code != http.StatusOK {
		t.Fatalf("second delete must be idempotent: %d", code)
	}
	if code := del(t, srv.URL+"/v1/internal/sessions/bad%20id"); code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", code)
	}
	if _, err := os.Stat(filepath.Join(dir, sid)); !os.IsNotExist(err) {
		t.Fatalf("bundle dir survived delete: %v", err)
	}
	get(t, srv.URL+"/v1/sessions/"+sid+"/summary", &sum)
	if sum.EventCount != 0 || sum.Sealed || sum.PendingAsync != 0 {
		t.Fatalf("summary after delete: %+v", sum)
	}
	var list []store.SessionInfo
	get(t, srv.URL+"/v1/sessions", &list)
	if len(list) != 0 {
		t.Fatalf("still listed: %+v", list)
	}

	// The id is reusable: replays are no longer duplicates and Seq restarts.
	if r := post(t, srv.URL, ev("a", core.KindAPIIn, 1, 1)); r.Accepted != 1 || r.Duplicates != 0 {
		t.Fatalf("ingest after delete: %+v", r)
	}
	if got := events(t, c); len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("after re-ingest: %+v", got)
	}
}

// Deleting must also drop what is still inside the reorder window.
func TestDeleteDropsBufferedEvents(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = time.Hour
	st := store.NewMem()
	c := New(st, cfg)
	ctx := context.Background()
	if _, err := c.Ingest(ctx, "", []core.Event{ev("a", core.KindAPIIn, 1, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, sid); err != nil {
		t.Fatal(err)
	}
	c.Close() // would flush the window if anything were left in it
	if h, _ := st.Head(ctx, sid); h != 0 {
		t.Fatalf("buffered event resurrected the deleted session (head %d)", h)
	}
}

// A session removed from the store behind the collector's back (another
// process, manual cleanup) must not wedge its actor on seq conflicts.
func TestResyncAfterExternalDelete(t *testing.T) {
	c, srv := newC(t, 0, "")
	post(t, srv.URL, ev("a", core.KindAPIIn, 1, 1), ev("b", core.KindAPIIn, 2, 2))
	if err := c.st.DeleteSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	if r := post(t, srv.URL, ev("c", core.KindAPIIn, 3, 3)); r.Accepted != 1 {
		t.Fatalf("ingest: %+v", r)
	}
	got := events(t, c)
	if len(got) != 1 || got[0].SpanID != "c" || got[0].Seq != 1 {
		t.Fatalf("after resync: %+v", got)
	}
}

func TestRetentionPurgesExpiredSessions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.Retention = time.Hour
	cfg.RetentionInterval = time.Hour // driven by hand below
	cfg.IdleEvict = -1                // keep actors: the unsealed rule depends on it
	c := New(store.NewMem(), cfg)
	defer c.Close()
	ctx := context.Background()
	now := time.Now()
	mk := func(id string, endNs int64) {
		e := ev("a", core.KindAPIIn, 1, 1)
		e.SessionID, e.EndNs = id, endNs
		if _, err := c.Ingest(ctx, "", []core.Event{e}); err != nil {
			t.Fatal(err)
		}
	}
	mk("sealed_old", now.UnixNano())
	mk("sealed_new", now.UnixNano())
	mk("open_live", now.Add(-48*time.Hour).UnixNano())  // stale SDK clock, but its actor is alive
	mk("open_stale", now.Add(-48*time.Hour).UnixNano()) // abandoned: actor evicted below
	for _, id := range []string{"sealed_old", "sealed_new"} {
		if _, err := c.Seal(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	// Backdate one seal past the retention.
	si, _ := c.st.GetSeal(ctx, "sealed_old")
	si.SealedAt = now.Add(-2 * time.Hour)
	if err := c.st.PutSeal(ctx, "sealed_old", *si); err != nil {
		t.Fatal(err)
	}
	c.seq.mu.Lock()
	stale := c.seq.actors["open_stale"]
	delete(c.seq.actors, "open_stale")
	c.seq.mu.Unlock()
	stale.stop()
	<-stale.done

	n, err := c.PurgeExpired(ctx, now)
	if err != nil || n != 2 {
		t.Fatalf("purged %d, err %v", n, err)
	}
	left := map[string]bool{}
	l, _ := c.st.ListSessions(ctx)
	for _, s := range l {
		left[s.ID] = true
	}
	if len(left) != 2 || !left["sealed_new"] || !left["open_live"] {
		t.Fatalf("wrong sessions survived: %v", left)
	}
}

// The background loop must run on its own and stop with Close.
func TestRetentionLoopRuns(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.Retention = time.Millisecond
	cfg.RetentionInterval = 5 * time.Millisecond
	c := New(store.NewMem(), cfg)
	defer c.Close()
	ctx := context.Background()
	if _, err := c.Ingest(ctx, "", []core.Event{ev("a", core.KindAPIIn, 1, 1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Seal(ctx, sid); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retention sweep", func() bool { h, _ := c.st.Head(ctx, sid); return h == 0 })
}
