package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// AttachEnv hands out the session a surrounding tracectl run named in
// TSESSION_ID, and only one that can record.
func TestAttachEnv(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := client.Options{BaseURL: setup(t), QuiesceSeconds: 1, PollInterval: 5 * time.Millisecond}

	t.Setenv(client.SessionEnv, "")
	if _, err := client.AttachEnv(ctx, o); !errors.Is(err, client.ErrNoSessionEnv) {
		t.Fatalf("variable not set: %v", err)
	}
	t.Setenv(client.SessionEnv, "ts_nobody")
	if _, err := client.AttachEnv(ctx, o); err == nil || errors.Is(err, client.ErrNoSessionEnv) ||
		!strings.Contains(err.Error(), `"ts_nobody" named by TSESSION_ID`) || !strings.Contains(err.Error(), "404") {
		t.Fatalf("unknown session: %v", err)
	}

	outer, err := client.Start(ctx, client.Options{BaseURL: o.BaseURL, Name: "outer", QuiesceSeconds: 1, PollInterval: o.PollInterval})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(client.SessionEnv, outer.ID)
	// Where the services are comes from the environment too, as tracectl run
	// exports it; the session options of o are not used.
	t.Setenv("TSESSION_CONTROLPLANE_URL", o.BaseURL)
	s, err := client.AttachEnv(ctx, client.Options{Name: "ignored", Mode: "exclusive"})
	if err != nil || s.ID != outer.ID || s.Info().Name != "outer" || s.Info().Mode != "header" {
		t.Fatalf("attached: %+v %v", s, err)
	}

	// The handle tags requests with the ambient session.
	headers := make(chan http.Header, 1)
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { headers <- r.Header.Clone() }))
	defer svc.Close()
	resp, err := (&http.Client{Transport: s.Transport(nil)}).Get(svc.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := <-headers; got.Get(core.KeySession) != outer.ID || got.Get(core.KeyTSessionTraceparent) == "" {
		t.Fatalf("request not tagged: %v", got)
	}

	// Attaching changes nothing about the session, and a session that has
	// ended is refused: it would record nothing.
	if info, err := outer.Get(ctx); err != nil || info.State != "ACTIVE" {
		t.Fatalf("after AttachEnv: %+v %v", info, err)
	}
	if _, err := outer.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AttachEnv(ctx, o); err == nil || !strings.Contains(err.Error(), "is SEALED, not ACTIVE") {
		t.Fatalf("sealed session: %v", err)
	}
}
