package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

func waitSubs(t *testing.T, m *controlplane.Manager, n int) {
	d := time.Now().Add(5 * time.Second)
	for len(m.Subscribers()) != n {
		if time.Now().After(d) {
			t.Fatalf("subs=%d want %d", len(m.Subscribers()), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// The control plane restarts (its generation counter starts again from zero)
// between the two requests of a wait: the wait must still finish.
func TestWaitPropagatedSurvivesControlPlaneRestart(t *testing.T) {
	var cur atomic.Pointer[http.Handler]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*cur.Load()).ServeHTTP(w, r) }))
	defer srv.Close()
	m1 := controlplane.NewManager(controlplane.Config{})
	h1 := m1.Handler()
	cur.Store(&h1)
	opts := client.Options{BaseURL: srv.URL}
	ctx := context.Background()
	var s *client.Session
	for i := 0; i < 5; i++ { // generation 5
		var err error
		if s, err = client.Start(ctx, opts); err != nil {
			t.Fatal(err)
		}
	}
	m2 := controlplane.NewManager(controlplane.Config{})
	defer m2.Close()
	h2 := m2.Handler()
	srv2 := httptest.NewServer(h2)
	defer srv2.Close()
	sdk := runtime.NewSubscriberFor(srv2.URL, "orders-api", "orders-api-1") // reconnected to the restarted plane
	defer sdk.Close()
	waitSubs(t, m2, 1)
	// swap to the "restarted" control plane right after /v1/active was answered
	var swapped atomic.Bool
	inner := h1
	h1w := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.ServeHTTP(w, r)
		if r.URL.Path == "/v1/active" && swapped.CompareAndSwap(false, true) {
			m1.Close()
			cur.Store(&h2)
		}
	}))
	cur.Store(&h1w)
	done := make(chan struct{})
	var n int
	var werr error
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go func() { n, werr = s.WaitPropagated(wctx); close(done) }()
	for !swapped.Load() {
		time.Sleep(time.Millisecond)
	}
	if _, err := client.Start(ctx, opts); err != nil { // new session on the restarted plane
		t.Fatal(err)
	}
	<-done
	t.Logf("subscribers after restart: %+v, active gen=%d", m2.Subscribers(), m2.Active().Generation)
	if werr != nil {
		t.Fatalf("WaitPropagated never finished after a control-plane restart: n=%d err=%v", n, werr)
	}
}
