package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		panic(err)
	}
	if s.Info().State != "ACTIVE" || s.Info().InitiatedBy != "client" {
		panic(fmt.Sprintf("%+v", s.Info()))
	}
	if _, err := client.Start(ctx, o); !errors.Is(err, client.ErrConflict) {
		panic(fmt.Sprintf("want conflict, got %v", err))
	}

	h := s.Header()
	if h.Get(core.KeySession) != s.ID {
		panic("missing session header")
	}
	if sc, ok := core.ExtractSpanContext(core.HTTPHeaderCarrier(h)); !ok || len(sc.TraceID) != 32 || len(sc.SpanID) != 16 || sc.HLC == 0 {
		panic(fmt.Sprintf("bad propagation headers: %v", h))
	}
	if tp := h.Get(core.KeyTraceparent); tp == "" || h.Get(core.KeyTSessionTraceparent) != tp+";"+h.Get(core.KeyHLC) {
		panic(fmt.Sprintf("want traceparent, and %s holding it and the clock: %v", core.KeyTSessionTraceparent, h))
	}
	// A traceparent from the test's own tracing is kept; injecting again
	// replaces only what the client wrote.
	const appTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	h2 := http.Header{}
	h2.Set(core.KeyTraceparent, appTP)
	s.Inject(h2)
	if h2.Get(core.KeyTraceparent) != appTP || h2.Get(core.KeyTSessionTraceparent) == "" {
		panic(fmt.Sprintf("caller's traceparent not kept: %v", h2))
	}
	s.Inject(h)
	if tp := h.Get(core.KeyTraceparent); h.Get(core.KeyTSessionTraceparent) != tp+";"+h.Get(core.KeyHLC) {
		panic(fmt.Sprintf("second Inject left a stale traceparent: %v", h))
	}

	info, err := s.StopAndWait(ctx)
	if err != nil || info.State != "SEALED" || info.BundleURI != "mem://b" {
		panic(fmt.Sprintf("%+v %v", info, err))
	}
}

func TestTransportAndAbort(t *testing.T) {
	ctx := context.Background()
	s, err := client.Start(ctx, client.Options{BaseURL: setup(t)})
	if err != nil {
		panic(err)
	}
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-tsession-id")
	}))
	defer srv.Close()
	hc := &http.Client{Transport: s.Transport(nil)}
	resp, err := hc.Get(srv.URL)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if got != s.ID {
		panic(fmt.Sprintf("got %q", got))
	}
	if _, err := s.Abort(ctx); err != nil {
		panic(err)
	}
	if _, err := s.Wait(ctx); err == nil {
		panic("Wait on aborted session should error")
	}
	if _, err := client.Attach(ctx, client.Options{BaseURL: setup(t)}, "ts_missing"); err == nil {
		panic("attach to missing should fail")
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
		panic(err)
	}
	if len(got.Bindings) != 2 || got.Bindings[1].Source != "jwt_claim" || len(s.Info().Bindings) != 2 || s.Info().Bindings[0].Value != "u1" {
		panic(fmt.Sprintf("%+v %+v", got, s.Info()))
	}
	if _, err := client.Start(context.Background(), o); !errors.Is(err, client.ErrConflict) || !strings.Contains(err.Error(), "ts_1") {
		panic(fmt.Sprintf("%v", err))
	}
}

func TestParseBinding(t *testing.T) {
	b, err := client.ParseBinding("jwt:sub=u=1")
	if err != nil || b != (client.Binding{Source: "jwt_claim", Name: "sub", Value: "u=1"}) {
		panic(fmt.Sprint(b, err))
	}
	for _, bad := range []string{"", "header", "header:x", "bogus:a=b", "header:=v", "header:a="} {
		if _, err := client.ParseBinding(bad); err == nil {
			panic(fmt.Sprintf("%q accepted", bad))
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
		panic(err)
	}
	if i := s.Info(); !i.NameAuto || i.Name != "x-user-id=act_bind_9" {
		panic(fmt.Sprintf("%+v", i))
	}
	info, err := s.Rename(ctx, "run {bind}  ")
	if err != nil || info.Name != "run act_bind_9" || info.NameAuto || s.Info().Name != info.Name {
		panic(fmt.Sprintf("%+v %v", info, err))
	}
	if _, err := s.Rename(ctx, "   "); err == nil || !strings.Contains(err.Error(), "400") {
		panic(fmt.Sprintf("want 400, got %v", err))
	}
	if _, err := s.StopAndWait(ctx); err != nil {
		panic(err)
	}
	// rename a SEALED session
	if info, err := s.Rename(ctx, "sealed label"); err != nil || info.State != "SEALED" || info.Name != "sealed label" {
		panic(fmt.Sprintf("%+v %v", info, err))
	}
	ghost, _ := client.Attach(ctx, client.Options{BaseURL: base}, s.ID)
	ghost.ID = "ts_missing"
	if _, err := ghost.Rename(ctx, "x"); err == nil || !strings.Contains(err.Error(), "404") {
		panic(fmt.Sprintf("want 404, got %v", err))
	}
	// no bindings: initiated_by + time
	s2, err := client.Start(ctx, client.Options{BaseURL: base, InitiatedBy: "tester"})
	if err != nil || !s2.Info().NameAuto || !strings.HasPrefix(s2.Info().Name, "tester ") {
		panic(fmt.Sprintf("%+v %v", s2.Info(), err))
	}
}

func TestKillSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := client.Options{BaseURL: setup(t)}
	s, err := client.Start(ctx, o)
	if err != nil {
		panic(err)
	}
	if on, err := client.KillSwitch(ctx, o); err != nil || on {
		panic(fmt.Sprintf("a new control plane: on=%v %v", on, err))
	}
	for _, want := range []bool{true, true, false} { // setting it twice is not an error
		if err := client.SetKill(ctx, o, want); err != nil {
			panic(err)
		}
		if on, err := client.KillSwitch(ctx, o); err != nil || on != want {
			panic(fmt.Sprintf("after SetKill(%v): on=%v %v", want, on, err))
		}
	}
	// The switch stops recording, not the session.
	if info, err := s.Get(ctx); err != nil || info.State != "ACTIVE" {
		panic(fmt.Sprintf("%+v %v", info, err))
	}

	// Errors carry the control plane's status.
	down := httptest.NewServer(http.NotFoundHandler())
	defer down.Close()
	if err := client.SetKill(ctx, client.Options{BaseURL: down.URL}, true); err == nil || !strings.Contains(err.Error(), "404") {
		panic(fmt.Sprintf("SetKill against a server without the endpoint: %v", err))
	}
	if on, err := client.KillSwitch(ctx, client.Options{BaseURL: down.URL}); err == nil || on {
		panic(fmt.Sprintf("KillSwitch against a server without the endpoint: on=%v %v", on, err))
	}
}
