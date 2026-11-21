package thttp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A transport wrapped before the SDK starts stays unwrapped, so Transport and
// a Client with a transport of its own report it. A client without one uses
// http.DefaultTransport at request time, which the SDK can still patch when
// it starts: that one must not be reported.
func TestClientWrappersBeforeStart(t *testing.T) {
	before := core.WrappersWhileDisabled()
	plain := Client(nil)
	if after := core.WrappersWhileDisabled(); !slices.Equal(after, before) {
		t.Errorf("a client without a transport is noted: %q -> %q", before, after)
	}
	rt := Transport(&http.Transport{})
	own := Client(&http.Client{Transport: &http.Transport{}})
	noted := core.WrappersWhileDisabled()
	for _, w := range []string{"thttp.Transport", "thttp.Client"} {
		if !slices.Contains(noted, w) {
			t.Errorf("%s is not noted: %q", w, noted)
		}
	}

	rec := setup(t, active("s1"))
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	get := func(c *http.Client) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := c.Do(req)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
	}
	get(own)
	get(&http.Client{Transport: rt})
	if n := len(kind(rec.Events(), core.KindAPIOut)); n != 0 {
		panic(fmt.Sprintf("%d api.out events from wrappers made before the start: the note would be wrong", n))
	}

	// What the SDK does at start with PatchDefaultTransport.
	orig := http.DefaultTransport
	http.DefaultTransport = Transport(orig)
	defer func() { http.DefaultTransport = orig }()
	get(plain)
	if n := len(kind(rec.Events(), core.KindAPIOut)); n != 1 {
		panic(fmt.Sprintf("%d api.out events from a client without a transport, want 1", n))
	}
}
