package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

// helperSvcEnv tells TestGoTestHelperProcess where the service under test is.
const helperSvcEnv = "TRACECTL_TEST_SVC"

// TestGoTestHelperProcess is not a test of tracectl: it is the `go test` that
// TestCLIRunGoTests lets `tracectl run` start, written the way a user's tests
// are. helperEnv selects which tests it has:
//
//	go-own      one test with a session of its own (tsessiontest.Run)
//	go-ambient  that test, two parallel tests in the ambient session
//	            (tsessiontest.Ambient) and code using client.AttachEnv
func TestGoTestHelperProcess(t *testing.T) {
	mode := os.Getenv(helperEnv)
	if mode != "go-own" && mode != "go-ambient" {
		return
	}
	get := func(t *testing.T, s *client.Session, path string) {
		t.Helper()
		resp, err := (&http.Client{Transport: s.Transport(nil)}).Get(os.Getenv(helperSvcEnv) + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	t.Run("own", func(t *testing.T) {
		s := tsessiontest.Run(t, client.Options{QuiesceSeconds: 1})
		get(t, s, "/own")
		tsessiontest.RequireNoFailures(t, s) // stops and seals the test's session, not the ambient one
		fmt.Printf("own=%s\n", s.ID)
	})
	if mode == "go-own" {
		return
	}
	t.Run("ambient", func(t *testing.T) {
		for _, name := range []string{"a", "b"} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				get(t, tsessiontest.Ambient(t, client.Options{}), "/ambient-"+name)
			})
		}
	})
	// The tests above have ended, one of them after stopping its own session:
	// the ambient one must still be recording, or AttachEnv fails.
	s, err := client.AttachEnv(context.Background(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	get(t, s, "/plain")
}

// goTestEnv is a real control plane, a service that notes which session each
// request it gets belongs to, and a collector that answers from those notes.
type goTestEnv struct {
	cp, col, svc string

	mu   sync.Mutex
	hits map[string][]string // session id -> request paths
}

func newGoTestEnv(t *testing.T) *goTestEnv {
	t.Helper()
	env := &goTestEnv{hits: map[string][]string{}}
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()
		id := r.Header.Get(core.KeySession)
		env.hits[id] = append(env.hits[id], r.URL.Path)
	}))
	t.Cleanup(svc.Close)
	events := func(id string) []core.Event {
		env.mu.Lock()
		defer env.mu.Unlock()
		var evs []core.Event
		for i, p := range env.hits[id] {
			evs = append(evs, core.Event{Seq: uint64(i + 1), Service: "svc", Kind: core.KindAPIIn, Name: "GET " + p})
		}
		return evs
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.Summary{SessionID: r.PathValue("id"), EventCount: len(events(r.PathValue("id"))), Sealed: true})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		evs := events(r.PathValue("id"))
		json.NewEncoder(w).Encode(client.EventPage{Events: evs, NextSeq: uint64(len(evs)) + 1, Sealed: true})
	})
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.DrainStatus{})
	})
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.SealResult{})
	})
	col := httptest.NewServer(mux)
	t.Cleanup(col.Close)
	m := controlplane.NewManager(controlplane.Config{
		Collector:    &controlplane.HTTPCollector{BaseURL: col.URL},
		PollInterval: 5 * time.Millisecond,
		TimeUnit:     10 * time.Millisecond, // a "second" of quiesce
	})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	env.cp, env.col, env.svc = cp.URL, col.URL, svc.URL
	return env
}

// run runs "tracectl run <flags> -- <this test binary as go test>" and
// returns the exit status, the ids of the session run started and of the one
// the helper's own test started, and stderr.
func (env *goTestEnv) run(t *testing.T, mode string, flags ...string) (code int, outer, own, stderr string) {
	t.Helper()
	t.Setenv(helperEnv, mode)
	t.Setenv(helperSvcEnv, env.svc)
	// Where the services are reaches the helper only through tracectl run.
	t.Setenv("TSESSION_CONTROLPLANE_URL", "http://127.0.0.1:1")
	t.Setenv("TSESSION_COLLECTOR_URL", "http://127.0.0.1:1")
	argv := append([]string{"--cp", env.cp, "--collector", env.col, "run", "--name", "outer", "--quiesce", "1"}, flags...)
	argv = append(argv, "--", os.Args[0], "-test.run=^TestGoTestHelperProcess$")
	var out, errb bytes.Buffer
	code = run(argv, &out, &errb)
	stderr = errb.String()
	find := func(re, in string) string {
		m := regexp.MustCompile(re).FindStringSubmatch(in)
		if m == nil {
			t.Fatalf("no %s in:\n%s\nstderr:\n%s", re, in, stderr)
		}
		return m[1]
	}
	return code, find(`tracectl: session (\S+) \(outer\) started`, stderr), find(`(?m)^own=(\S+)$`, out.String()), stderr
}

// wantHits checks which requests the service saw for the session, and that
// the session is sealed.
func (env *goTestEnv) wantHits(t *testing.T, id string, want ...string) {
	t.Helper()
	env.mu.Lock()
	got := slices.Sorted(slices.Values(env.hits[id]))
	env.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Errorf("session %s recorded %q, want %q", id, got, want)
	}
	s, err := client.Attach(context.Background(), client.Options{BaseURL: env.cp, CollectorURL: env.col}, id)
	if err != nil || s.Info().State != "SEALED" {
		t.Errorf("session %s: %+v %v, want SEALED", id, s, err)
	}
}

// `tracectl run -- go test`: tests that ask for the ambient session record in
// the session run started, which outlives them and is sealed by run; a test
// with a session of its own records there and nowhere else.
func TestCLIRunGoTests(t *testing.T) {
	env := newGoTestEnv(t)
	code, outer, own, e := env.run(t, "go-ambient", "--fail-on-error", "--fail-on-incomplete", "--fail-on-empty")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, e)
	}
	if outer == own {
		t.Fatalf("tsessiontest.Run used the ambient session %s", outer)
	}
	env.wantHits(t, outer, "/ambient-a", "/ambient-b", "/plain")
	env.wantHits(t, own, "/own")
	if !strings.Contains(e, "session    "+outer+"  outer  (SEALED)\n") || !strings.Contains(e, "events     3 over") ||
		strings.Contains(e, "nothing was recorded") {
		t.Errorf("summary of the ambient session:\n%s", e)
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	if n := len(env.hits); n != 2 {
		t.Errorf("%d sessions recorded something, want 2: %v", n, env.hits)
	}
}

// When every test has a session of its own, the session of tracectl run is
// empty: --fail-on-empty fails, and run says why.
func TestCLIRunGoTestsWithTheirOwnSessions(t *testing.T) {
	env := newGoTestEnv(t)
	code, outer, own, e := env.run(t, "go-own", "--fail-on-empty")
	if code != exitCheckFailed {
		t.Fatalf("exit %d, want %d\n%s", code, exitCheckFailed, e)
	}
	env.wantHits(t, outer)
	env.wantHits(t, own, "/own")
	for _, want := range []string{
		"tracectl: check failed: " + emptySession + "\n",
		"tracectl: nothing was recorded: the command has to send $TSESSION_ID as the x-tsession-id header itself",
		"such as those of tsessiontest.Run, are separate from this one\n",
	} {
		if !strings.Contains(e, want) {
			t.Errorf("stderr lacks %q:\n%s", want, e)
		}
	}
}

// The hint is for sessions that record by id only: with a binding or in
// exclusive mode an empty session has another cause.
func TestCLIRunHintsOnlyWhenTheIDIsNeeded(t *testing.T) {
	for _, flags := range [][]string{{"--bind", "header:x-user-id=u_1"}, {"--mode", "exclusive"}} {
		env := newRunEnv(t)
		if code, _, e := env.run("ok", flags); code != 0 || strings.Contains(e, "nothing was recorded") {
			t.Errorf("run %q: exit %d\n%s", flags, code, e)
		}
	}
	env := newRunEnv(t)
	dir := t.TempDir()
	if code, _, e := env.run("ok", []string{"--report", dir + "/r.md"}); code != 0 || !strings.Contains(e, "nothing was recorded") {
		t.Errorf("run with a report: exit %d\n%s", code, e)
	}
}
