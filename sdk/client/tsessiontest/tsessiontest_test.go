package tsessiontest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

// env is a real control plane in front of a canned collector.
type env struct {
	opts client.Options
	sum  client.Summary
	// counts is what the collector says it has per SDK instance; nil makes it
	// one that does not count (an older collector).
	counts atomic.Pointer[[]controlplane.InstanceCount]
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{}
	yes, no := true, false
	evs := []core.Event{
		{Seq: 1, Service: "api", Kind: core.KindDBWrite, TxID: "tx1",
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", Effective: &yes}}},
		{Seq: 2, Service: "api", Kind: core.KindDBWrite, TxID: "tx2",
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "update", Effective: &no}}},
		{Seq: 3, Service: "worker", Kind: core.KindCacheWrite,
			Mutations: []core.Mutation{{Store: "redis", Entity: "cart:*", Op: "set"}}},
		{Seq: 4, Service: "api", Kind: core.KindAPIOut, Name: "POST http://billing.test/charge", Err: "HTTP 503",
			Attrs: map[string]string{"http.status": "503"}},
		{Seq: 5, Service: "api", Kind: core.KindAPIOut, Name: "GET http://billing.test/rates"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.EventPage{Events: evs, NextSeq: 6, Sealed: true})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(e.sum)
	})
	mux.HandleFunc("GET /v1/internal/sessions/{id}/drain-status", func(w http.ResponseWriter, r *http.Request) {
		st := controlplane.DrainStatus{}
		if c := e.counts.Load(); c != nil {
			st.Instances = *c
		}
		json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("POST /v1/internal/sessions/{id}/seal", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(controlplane.SealResult{})
	})
	col := httptest.NewServer(mux)
	t.Cleanup(col.Close)
	m := controlplane.NewManager(controlplane.Config{Collector: &controlplane.HTTPCollector{BaseURL: col.URL}})
	t.Cleanup(m.Close)
	cp := httptest.NewServer(m.Handler())
	t.Cleanup(cp.Close)
	e.opts = client.Options{BaseURL: cp.URL, QuiesceSeconds: 1, CollectorURL: col.URL}
	return e
}

// recorder stands in for the test's testing.TB so a failing assertion can be observed.
type recorder struct {
	testing.TB
	failed bool
	msgs   []string
}

type failNow struct{}

func (r *recorder) Errorf(format string, a ...any) {
	r.failed = true
	r.msgs = append(r.msgs, fmt.Sprintf(format, a...))
}
func (r *recorder) Fatalf(format string, a ...any) { r.Errorf(format, a...); panic(failNow{}) }
func (r *recorder) FailNow()                       { r.failed = true; panic(failNow{}) }
func (r *recorder) Failed() bool                   { return r.failed }
func (r *recorder) Logf(format string, a ...any)   { r.msgs = append(r.msgs, fmt.Sprintf(format, a...)) }

// check runs fn against a recorder and returns what it reported.
func check(t *testing.T, fn func(tb testing.TB)) (failed bool, out string) {
	t.Helper()
	r := &recorder{TB: t}
	func() {
		defer func() {
			// The helpers stop a test by panicking with what is wrong, or
			// through FailNow of the test they were given.
			if v := recover(); v != nil {
				if _, ok := v.(failNow); !ok {
					r.failed = true
					r.msgs = append(r.msgs, fmt.Sprint(v))
				}
			}
		}()
		fn(r)
	}()
	return r.failed, strings.Join(r.msgs, "\n")
}

func TestRunStopsTheSessionWithTheTest(t *testing.T) {
	e := newEnv(t)
	var id string
	t.Run("inner", func(t *testing.T) {
		s := tsessiontest.Run(t, e.opts)
		id = s.ID
		if s.Info().Name != t.Name() || s.Info().State != "ACTIVE" {
			panic(fmt.Sprintf("session: %+v", s.Info()))
		}
	})
	s, err := client.Attach(context.Background(), e.opts, id)
	if err != nil || s.Info().State != "SEALED" {
		panic(fmt.Sprintf("after the test: %+v %v", s.Info(), err))
	}
}

func TestRequireNoFailures(t *testing.T) {
	e := newEnv(t)
	s := tsessiontest.Run(t, e.opts)
	if failed, out := check(t, func(tb testing.TB) { tsessiontest.RequireNoFailures(tb, s) }); failed {
		panic(fmt.Sprintf("clean session reported: %s", out))
	}
	if !s.Info().Done() {
		panic(fmt.Sprintf("the assertion must stop the session first: %+v", s.Info()))
	}
	e.sum.Failures = []client.Failure{{Seq: 7, Service: "api", Kind: core.KindDBWrite, Name: "INSERT", Err: "duplicate key"}}
	failed, out := check(t, func(tb testing.TB) { tsessiontest.RequireNoFailures(tb, s) })
	if !failed || !strings.Contains(out, "#7 api db.write INSERT: duplicate key") {
		panic(fmt.Sprintf("failure not reported: %v %s", failed, out))
	}
	e.sum = client.Summary{Incomplete: true, PendingAsync: 2}
	failed, out = check(t, func(tb testing.TB) { tsessiontest.RequireNoFailures(tb, s) })
	if !failed || !strings.Contains(out, "incomplete") || !strings.Contains(out, "2 async job(s)") {
		panic(fmt.Sprintf("incomplete session not reported: %v %s", failed, out))
	}
}

func TestRequireMutation(t *testing.T) {
	e := newEnv(t)
	s := tsessiontest.Run(t, e.opts)

	ms := tsessiontest.RequireMutation(t, s, client.MutationFilter{Store: "postgres", Entity: "orders", Op: "INSERT"})
	if len(ms) != 1 || ms[0].Seq != 1 || ms[0].Service != "api" || ms[0].TxID != "tx1" {
		panic(fmt.Sprintf("matches: %+v", ms))
	}
	// A mutation without a transaction outcome counts; a rolled-back one does not.
	tsessiontest.RequireMutation(t, s, client.MutationFilter{Store: "redis"})
	failed, out := check(t, func(tb testing.TB) {
		tsessiontest.RequireMutation(tb, s, client.MutationFilter{Entity: "orders", Op: "update"})
	})
	if !failed || !strings.Contains(out, "no effective mutation") {
		panic(fmt.Sprintf("rolled-back update must not satisfy the assertion: %v %s", failed, out))
	}
	// The message names what was asked for in the caller's words, not the filter's fields.
	if !strings.Contains(out, `matching entity "orders", op "update"`) || strings.Contains(out, "ExcludeRolledBack") {
		panic("the message of a failed RequireMutation: " + out)
	}

	tsessiontest.RequireNoMutation(t, s, client.MutationFilter{Entity: "orders", Op: "update"})
	tsessiontest.RequireNoMutation(t, s, client.MutationFilter{Store: "mongo"})
	failed, out = check(t, func(tb testing.TB) { tsessiontest.RequireNoMutation(tb, s, client.MutationFilter{Store: "postgres"}) })
	if !failed || !strings.Contains(out, "event #1 insert postgres/orders") {
		panic(fmt.Sprintf("unexpected mutation not reported: %v %s", failed, out))
	}

	all, err := s.Mutations(context.Background(), client.MutationFilter{})
	if err != nil || len(all) != 3 {
		panic(fmt.Sprintf("unfiltered: %+v %v", all, err))
	}
}

func TestFailedTestLogsTheSession(t *testing.T) {
	e := newEnv(t)
	e.sum.Failures = []client.Failure{{Seq: 3, Service: "worker", Kind: core.KindWorkerEnd, Name: "ship", Err: "timeout"}}
	r := &recorder{TB: t}
	var cleanups []func()
	rc := &cleanupRecorder{recorder: r, fns: &cleanups}
	s := tsessiontest.Run(rc, e.opts)
	r.failed = true // the test failed for its own reasons
	for _, fn := range cleanups {
		fn()
	}
	out := strings.Join(r.msgs, "\n")
	if !strings.Contains(out, "tracectl digest "+s.ID) || !strings.Contains(out, "#3 worker worker.end ship: timeout") {
		panic(fmt.Sprintf("log of a failed test:\n%s", out))
	}
}

func TestFailedTestSavesAnArtifact(t *testing.T) {
	e := newEnv(t)
	dir := filepath.Join(t.TempDir(), "artifacts")
	t.Setenv(tsessiontest.ArtifactDirEnv, dir)
	run := func(failed bool) string {
		r := &recorder{TB: t}
		var cleanups []func()
		tsessiontest.Run(&cleanupRecorder{recorder: r, fns: &cleanups}, e.opts)
		r.failed = failed
		for _, fn := range cleanups {
			fn()
		}
		return strings.Join(r.msgs, "\n")
	}
	if out := run(false); strings.Contains(out, "saved") {
		panic(fmt.Sprintf("a passing test must not save anything:\n%s", out))
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		panic(fmt.Sprintf("artifact directory created for a passing test: %v", err))
	}
	out := run(true)
	// The recorder reports the outer test's name, which contains no slash here;
	// the sanitising is exercised through the file name below.
	matches, _ := filepath.Glob(filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".ts_*.ndjson"))
	if len(matches) != 1 {
		panic(fmt.Sprintf("artifacts in %s: %v", dir, matches))
	}
	path := matches[0]
	if !strings.Contains(out, "dashboard -open "+path) {
		panic(fmt.Sprintf("log does not point at the artifact:\n%s", out))
	}
	b, err := os.ReadFile(path)
	if err != nil || strings.Count(string(b), "\n") != 5 || !strings.Contains(string(b), `"seq":1`) {
		panic(fmt.Sprintf("artifact: %q %v", b, err))
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		panic(fmt.Sprintf("artifact mode %v, want 0600", fi.Mode().Perm()))
	}
	// A second failing test with the same name gets its own file.
	run(true)
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.ndjson")); len(matches) != 2 {
		panic(fmt.Sprintf("two failed runs must leave two files, got %v", matches))
	}
}

// cleanupRecorder captures Cleanup functions so the test can run them itself.
type cleanupRecorder struct {
	*recorder
	fns *[]func()
}

func (c *cleanupRecorder) Cleanup(fn func()) { *c.fns = append(*c.fns, fn) }

// A failed event that RequireNoFailures reported is not printed again when
// the test ends.
func TestFailureIsListedOnce(t *testing.T) {
	e := newEnv(t)
	e.sum.Failures = []client.Failure{{Seq: 3, Service: "worker", Kind: core.KindWorkerEnd, Name: "ship", Err: "timeout"}}
	r := &recorder{TB: t}
	var cleanups []func()
	rc := &cleanupRecorder{recorder: r, fns: &cleanups}
	s := tsessiontest.Run(rc, e.opts)
	func() {
		defer func() {
			if recover() == nil {
				panic("RequireNoFailures must stop the test")
			}
		}()
		tsessiontest.RequireNoFailures(rc, s)
	}()
	for _, fn := range cleanups {
		fn()
	}
	out := strings.Join(r.msgs, "\n")
	if n := strings.Count(out, "#3 worker worker.end ship: timeout"); n != 1 {
		panic(fmt.Sprintf("the failed event is listed %d times, want once:\n%s", n, out))
	}
	if !strings.Contains(out, "tracectl digest "+s.ID) {
		panic(fmt.Sprintf("the pointer to the session is still printed:\n%s", out))
	}
}

func TestRequireEventAndFailure(t *testing.T) {
	e := newEnv(t)
	s := tsessiontest.Run(t, e.opts)
	// Did the api call billing? Twice, and the filter's name is a part of the event's.
	if got := tsessiontest.RequireEvent(t, s, tsessiontest.EventFilter{Service: "api", Kind: core.KindAPIOut, Name: "billing.test"}); len(got) != 2 || got[0].Seq != 4 {
		panic(fmt.Sprintf("calls to billing: %+v", got))
	}
	if got := tsessiontest.RequireEvent(t, s, tsessiontest.EventFilter{}); len(got) != 5 {
		panic(fmt.Sprintf("the empty filter matched %d of 5 events", len(got)))
	}
	failed, out := check(t, func(tb testing.TB) {
		tsessiontest.RequireEvent(tb, s, tsessiontest.EventFilter{Service: "worker", Kind: core.KindAPIOut})
	})
	if !failed || !strings.Contains(out, `recorded no event matching service "worker", kind "api.out"`) {
		panic(fmt.Sprintf("an event that was not recorded: %v %s", failed, out))
	}

	// A failure the test provoked: found, with its error; one that did not happen is a failed test.
	got := tsessiontest.RequireFailure(t, s, tsessiontest.EventFilter{Name: "/charge"})
	if len(got) != 1 || got[0].Err != "HTTP 503" || got[0].Attrs["http.status"] != "503" {
		panic(fmt.Sprintf("the failed charge: %+v", got))
	}
	failed, out = check(t, func(tb testing.TB) {
		tsessiontest.RequireFailure(tb, s, tsessiontest.EventFilter{Name: "/rates"}) // recorded, and it went well
	})
	if !failed || !strings.Contains(out, `recorded no failed event matching name containing "/rates"`) {
		panic(fmt.Sprintf("a call that went well taken for a failure: %v %s", failed, out))
	}
}
