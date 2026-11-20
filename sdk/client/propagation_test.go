package client_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

func TestWaitPropagated(t *testing.T) {
	m := controlplane.NewManager(controlplane.Config{})
	defer m.Close()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	opts := client.Options{BaseURL: srv.URL}
	ctx := context.Background()

	// No service connected: nothing to wait for.
	s, err := client.Start(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.WaitPropagated(ctx); err != nil || n != 0 {
		t.Fatalf("no followers: %d %v", n, err)
	}

	// A connected SDK is sent the session; once it was, the SDK has it shortly after.
	sdk := runtime.NewSubscriberFor(srv.URL, "orders-api", "orders-api-1")
	defer sdk.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Subscribers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the SDK never connected")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s2, err := client.Start(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	n, err := s2.WaitPropagated(wctx)
	cancel()
	if err != nil || n != 1 {
		t.Fatalf("one follower: %d %v", n, err)
	}
	for !sdk.IsActive(s2.ID) {
		if time.Now().After(deadline) {
			t.Fatal("the SDK was reported as sent the session but never applied it")
		}
		time.Sleep(time.Millisecond)
	}

	// An in-process follower (Manager.Subscribe) receives every version through its channel and
	// must not hold the wait open.
	_, leave := m.SubscribeAs("embedded", "embedded-1")
	defer leave()
	s3, err := client.Start(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	n, err = s3.WaitPropagated(wctx)
	cancel()
	if err != nil || n != 2 {
		t.Fatalf("with an in-process follower: %d %v, want 2 and no error", n, err)
	}
}
