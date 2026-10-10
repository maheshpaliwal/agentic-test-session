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

	"tsession/controlplane"
	"tsession/core"
	"tsession/sdk/client"
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
