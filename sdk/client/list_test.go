package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// listServer is a control plane holding n finished sessions (every third one
// named "nightly") and one that is running. With old set it ignores the
// paging parameters, like a control plane from before them.
func listServer(t *testing.T, n int, old bool) string {
	t.Helper()
	st := controlplane.NewMemStore()
	base := time.Date(2026, 7, 8, 9, 0, 0, 0, time.UTC)
	for i := range n + 1 {
		at := base.Add(time.Duration(i) * time.Second)
		s := controlplane.Session{ID: fmt.Sprintf("ts_list%04d", i), Name: fmt.Sprintf("s%d", i), Mode: controlplane.ModeHeader,
			State: controlplane.StateSealed, CreatedAt: at, StartedAt: at, EndedAt: at, TTLSeconds: 3600}
		if i%3 == 0 {
			s.Name = "nightly"
		}
		if i == n {
			s.State, s.EndedAt = controlplane.StateActive, time.Time{}
		}
		if err := st.Put(s); err != nil {
			t.Fatal(err)
		}
	}
	m := controlplane.NewManager(controlplane.Config{Store: st})
	t.Cleanup(m.Close)
	h := m.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if old {
			q := r.URL.Query()
			q.Del("name")
			q.Del("limit")
			q.Del("before")
			r.URL.RawQuery = q.Encode()
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestListPage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const n = 25
	o := client.Options{BaseURL: listServer(t, n, false)}

	all, err := client.List(ctx, o, "")
	if err != nil || len(all) != n+1 || all[0].ID != "ts_list0000" || all[n].ID != "ts_list0025" {
		t.Fatalf("List: %d sessions, err %v", len(all), err)
	}
	// Paging returns the same sessions, newest first, each once.
	walk := func(lo client.ListOptions) []string {
		t.Helper()
		var ids []string
		for {
			page, next, err := client.ListPage(ctx, o, lo)
			if err != nil {
				t.Fatalf("ListPage %+v: %v", lo, err)
			}
			if len(page) > lo.Limit || (next != "" && len(page) != lo.Limit) {
				t.Fatalf("ListPage %+v: %d sessions, next %q", lo, len(page), next)
			}
			for _, i := range page {
				ids = append(ids, i.ID)
			}
			if lo.Before = next; next == "" {
				return ids
			}
		}
	}
	want := func(keep func(client.Info) bool) []string {
		var ids []string
		for _, i := range slices.Backward(all) {
			if keep(i) {
				ids = append(ids, i.ID)
			}
		}
		return ids
	}
	for _, limit := range []int{1, 4, 26, 1000} {
		if got := walk(client.ListOptions{Limit: limit}); !slices.Equal(got, want(func(client.Info) bool { return true })) {
			t.Errorf("limit %d: got %v", limit, got)
		}
	}
	if got := walk(client.ListOptions{Limit: 2, Name: "nightly"}); len(got) != 9 || !slices.Equal(got, want(func(i client.Info) bool { return i.Name == "nightly" })) {
		t.Errorf("by name: got %v", got)
	}
	if got := walk(client.ListOptions{Limit: 5, State: "ACTIVE"}); !slices.Equal(got, []string{"ts_list0025"}) {
		t.Errorf("by state: got %v", got)
	}
	if got := walk(client.ListOptions{Limit: 5, State: "SEALED", Name: "nightly"}); len(got) != 9 {
		t.Errorf("by state and name: got %v", got)
	}
	// Without a limit: every match, oldest first, like List.
	page, next, err := client.ListPage(ctx, o, client.ListOptions{Name: "nightly"})
	if err != nil || next != "" || len(page) != 9 || page[0].ID != "ts_list0000" {
		t.Errorf("by name without a limit: %d sessions, next %q, err %v", len(page), next, err)
	}
	if page, _, err := client.ListPage(ctx, o, client.ListOptions{Name: "no such"}); err != nil || len(page) != 0 {
		t.Errorf("unknown name: %v, err %v", page, err)
	}
	// What the control plane rejects comes back as an error.
	for _, lo := range []client.ListOptions{{Limit: 1001}, {Limit: -1}, {Limit: 5, Before: "not a cursor"}, {Before: "x"}} {
		if _, _, err := client.ListPage(ctx, o, lo); err == nil || !strings.Contains(err.Error(), "400") {
			t.Errorf("ListPage %+v: err %v, want a 400", lo, err)
		}
	}
}

// A control plane that does not know the paging parameters answers with
// everything, oldest first, and no cursor.
func TestListPageAgainstAnOlderControlPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	o := client.Options{BaseURL: listServer(t, 25, true)}
	page, next, err := client.ListPage(ctx, o, client.ListOptions{Name: "nightly", Limit: 5})
	if err != nil || next != "" || len(page) != 26 || page[0].ID != "ts_list0000" || page[1].Name == "nightly" {
		t.Fatalf("%d sessions, next %q, err %v", len(page), next, err)
	}
	page, next, err = client.ListPage(ctx, o, client.ListOptions{State: "ACTIVE", Limit: 5})
	if err != nil || next != "" || len(page) != 1 {
		t.Fatalf("by state: %d sessions, next %q, err %v", len(page), next, err)
	}
}
