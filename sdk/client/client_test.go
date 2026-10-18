package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func setup(t *testing.T) string {
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/seal") {
			json.NewEncoder(w).Encode(controlplane.SealResult{BundleURI: "mem://b"})
			return
		}
		json.NewEncoder(w).Encode(controlplane.DrainStatus{})
	}))
	t.Cleanup(col.Close)
	m := controlplane.NewManager(controlplane.Config{
		Collector:    &controlplane.HTTPCollector{BaseURL: col.URL},
		PollInterval: 5 * time.Millisecond,
		TimeUnit:     10 * time.Millisecond,
	})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	return cp.URL
}

func TestLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := client.Options{BaseURL: setup(t), Name: "t1", Mode: "exclusive", QuiesceSeconds: 1, PollInterval: 5 * time.Millisecond}
	s, err := client.Start(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if s.Info().State != "ACTIVE" || s.Info().InitiatedBy != "client" {
		t.Fatalf("%+v", s.Info())
	}
	if _, err := client.Start(ctx, o); !errors.Is(err, client.ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}

	h := s.Header()
	if h.Get(core.KeySession) != s.ID {
		t.Fatal("missing session header")
	}
	if sc, ok := core.ExtractSpanContext(core.HTTPHeaderCarrier(h)); !ok || len(sc.TraceID) != 32 || len(sc.SpanID) != 16 || sc.HLC == 0 {
		t.Fatalf("bad propagation headers: %v", h)
	}

	info, err := s.StopAndWait(ctx)
	if err != nil || info.State != "SEALED" || info.BundleURI != "mem://b" {
		t.Fatalf("%+v %v", info, err)
	}
}

func TestTransportAndAbort(t *testing.T) {
	ctx := context.Background()
	s, err := client.Start(ctx, client.Options{BaseURL: setup(t)})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-tsession-id")
	}))
	defer srv.Close()
	hc := &http.Client{Transport: s.Transport(nil)}
	resp, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != s.ID {
		t.Fatalf("got %q", got)
	}
	if _, err := s.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Wait(ctx); err == nil {
		t.Fatal("Wait on aborted session should error")
	}
	if _, err := client.Attach(ctx, client.Options{BaseURL: setup(t)}, "ts_missing"); err == nil {
		t.Fatal("attach to missing should fail")
	}
}

func TestStartSendsBindingsAndConflict(t *testing.T) {
	var got struct {
		Bindings []client.Binding `json:"bindings"`
	}
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 2 {
			w.WriteHeader(409)
			w.Write([]byte(`{"error":"binding header:x-user-id already held by ts_1","session_id":"ts_1"}`))
			return
		}
		json.NewDecoder(r.Body).Decode(&got)
		json.NewEncoder(w).Encode(map[string]any{"id": "ts_1", "state": "ACTIVE", "bindings": got.Bindings})
	}))
	defer srv.Close()
	o := client.Options{BaseURL: srv.URL, Bindings: []client.Binding{client.BindHeader("x-user-id", "u1"), client.BindJWT("sub", "u1")}}
	s, err := client.Start(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Bindings) != 2 || got.Bindings[1].Source != "jwt_claim" || len(s.Info().Bindings) != 2 || s.Info().Bindings[0].Value != "u1" {
		t.Fatalf("%+v %+v", got, s.Info())
	}
	if _, err := client.Start(context.Background(), o); !errors.Is(err, client.ErrConflict) || !strings.Contains(err.Error(), "ts_1") {
		t.Fatalf("%v", err)
	}
}

func TestParseBinding(t *testing.T) {
	b, err := client.ParseBinding("jwt:sub=u=1")
	if err != nil || b != (client.Binding{Source: "jwt_claim", Name: "sub", Value: "u=1"}) {
		t.Fatal(b, err)
	}
	for _, bad := range []string{"", "header", "header:x", "bogus:a=b", "header:=v", "header:a="} {
		if _, err := client.ParseBinding(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestRenameAndAutoName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := setup(t)
	o := client.Options{BaseURL: base, QuiesceSeconds: 1, PollInterval: 5 * time.Millisecond,
		Bindings: []client.Binding{client.BindHeader("X-USER-ID", "act_bind_9")}}
	s, err := client.Start(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if i := s.Info(); !i.NameAuto || i.Name != "x-user-id=act_bind_9" {
		t.Fatalf("%+v", i)
	}
	info, err := s.Rename(ctx, "run {bind}  ")
	if err != nil || info.Name != "run act_bind_9" || info.NameAuto || s.Info().Name != info.Name {
		t.Fatalf("%+v %v", info, err)
	}
	if _, err := s.Rename(ctx, "   "); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("want 400, got %v", err)
	}
	if _, err := s.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	// rename a SEALED session
	if info, err := s.Rename(ctx, "sealed label"); err != nil || info.State != "SEALED" || info.Name != "sealed label" {
		t.Fatalf("%+v %v", info, err)
	}
	ghost, _ := client.Attach(ctx, client.Options{BaseURL: base}, s.ID)
	ghost.ID = "ts_missing"
	if _, err := ghost.Rename(ctx, "x"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("want 404, got %v", err)
	}
	// no bindings: initiated_by + time
	s2, err := client.Start(ctx, client.Options{BaseURL: base, InitiatedBy: "tester"})
	if err != nil || !s2.Info().NameAuto || !strings.HasPrefix(s2.Info().Name, "tester ") {
		t.Fatalf("%+v %v", s2.Info(), err)
	}
}
