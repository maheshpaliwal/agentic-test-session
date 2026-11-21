package tsessiontest_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

// startAmbient does what tracectl run does before it starts its command: it
// starts a session and names it in TSESSION_ID.
func startAmbient(t *testing.T, e *env) *client.Session {
	t.Helper()
	o := e.opts
	o.Name = "outer"
	outer, err := client.Start(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { outer.Abort(context.Background()) })
	t.Setenv(client.SessionEnv, outer.ID)
	return outer
}

func wantState(t *testing.T, s *client.Session, state string) {
	t.Helper()
	if info, err := s.Get(context.Background()); err != nil || info.State != state {
		t.Fatalf("session %s: state %q (%v), want %s", s.ID, info.State, err, state)
	}
}

// Tests that ask for the ambient session all get the same one, also in
// parallel; their requests carry its id, and it is still running when they
// have ended.
func TestAmbientIsSharedAndLeftRunning(t *testing.T) {
	e := newEnv(t)
	outer := startAmbient(t, e)
	var mu sync.Mutex
	seen := map[string]string{} // request path -> session id it carried
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get(core.KeySession)
		mu.Unlock()
	}))
	defer svc.Close()

	t.Run("tests", func(t *testing.T) {
		for _, name := range []string{"a", "b", "c"} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				s := tsessiontest.Ambient(t, e.opts)
				if s.ID != outer.ID || s.Info().Name != "outer" {
					t.Fatalf("got session %+v, want %s", s.Info(), outer.ID)
				}
				resp, err := (&http.Client{Transport: s.Transport(nil)}).Get(svc.URL + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
			})
		}
	})
	mu.Lock()
	defer mu.Unlock()
	for _, name := range []string{"/a", "/b", "/c"} {
		if seen[name] != outer.ID {
			t.Errorf("request %s carried session %q, want %s", name, seen[name], outer.ID)
		}
	}
	wantState(t, outer, "ACTIVE")
}

// The ambient session is not the test's to stop: Stop and every assertion
// (they all stop the session first) fail the test and leave it running,
// however the handle was obtained.
func TestAmbientSessionIsNotStopped(t *testing.T) {
	e := newEnv(t)
	outer := startAmbient(t, e)
	attached, err := client.Attach(context.Background(), e.opts, outer.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*client.Session{tsessiontest.Ambient(t, e.opts), attached} {
		for name, fn := range map[string]func(tb testing.TB){
			"Stop":              func(tb testing.TB) { tsessiontest.Stop(tb, s) },
			"RequireNoFailures": func(tb testing.TB) { tsessiontest.RequireNoFailures(tb, s) },
			"RequireMutation":   func(tb testing.TB) { tsessiontest.RequireMutation(tb, s, client.MutationFilter{Store: "postgres"}) },
			"RequireNoMutation": func(tb testing.TB) { tsessiontest.RequireNoMutation(tb, s, client.MutationFilter{Store: "mongo"}) },
		} {
			failed, out := check(t, fn)
			if !failed || !strings.Contains(out, "session "+outer.ID+" is the one in $TSESSION_ID") || !strings.Contains(out, "tsessiontest.Run") {
				t.Errorf("%s on the ambient session: failed=%v, output %q", name, failed, out)
			}
		}
	}
	wantState(t, outer, "ACTIVE")
}

// A test that asks for the ambient session fails when there is none it can
// record in, instead of passing unrecorded.
func TestAmbientNeedsARunningSession(t *testing.T) {
	e := newEnv(t)
	ambient := func() (bool, string) {
		return check(t, func(tb testing.TB) { tsessiontest.Ambient(tb, e.opts) })
	}
	t.Setenv(client.SessionEnv, "")
	if failed, out := ambient(); !failed || !strings.Contains(out, "TSESSION_ID is not set") || !strings.Contains(out, "Run starts a session") {
		t.Fatalf("variable not set: failed=%v %q", failed, out)
	}
	t.Setenv(client.SessionEnv, "ts_nobody")
	if failed, out := ambient(); !failed || !strings.Contains(out, "ts_nobody") {
		t.Fatalf("unknown session: failed=%v %q", failed, out)
	}
	outer := startAmbient(t, e)
	if _, err := outer.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if failed, out := ambient(); !failed || !strings.Contains(out, "is ABORTED, not ACTIVE") {
		t.Fatalf("ended session: failed=%v %q", failed, out)
	}
}

// A failed test points at the ambient session. Nothing is saved: the session
// is still recording, and the log says how to get the file.
func TestFailedAmbientTestPointsAtTheSession(t *testing.T) {
	e := newEnv(t)
	outer := startAmbient(t, e)
	dir := filepath.Join(t.TempDir(), "artifacts")
	t.Setenv(tsessiontest.ArtifactDirEnv, dir)
	run := func(failed bool) string {
		r := &recorder{TB: t}
		var cleanups []func()
		tsessiontest.Ambient(&cleanupRecorder{recorder: r, fns: &cleanups}, e.opts)
		r.failed = failed
		for _, fn := range cleanups {
			fn()
		}
		return strings.Join(r.msgs, "\n")
	}
	// No service is connected in this environment, which Ambient reports as Run does.
	if out := run(false); strings.Contains(out, "recorded this test") || !strings.Contains(out, "no service is connected") {
		t.Fatalf("log of a passing test:\n%s", out)
	}
	out := run(true)
	for _, want := range []string{
		"session " + outer.ID + " (outer) recorded this test",
		"tracectl summary " + outer.ID,
		"is not saved to $TSESSION_ARTIFACT_DIR",
		"tracectl export " + outer.ID + " -o FILE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log of a failed test lacks %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("an artifact directory was created for a session that is still recording: %v", err)
	}
	wantState(t, outer, "ACTIVE")
}

// Run does not look at TSESSION_ID: the test gets a session of its own, its
// assertions stop that one, and the ambient session is left alone.
func TestRunIgnoresTheAmbientSession(t *testing.T) {
	e := newEnv(t)
	outer := startAmbient(t, e)
	s := tsessiontest.Run(t, e.opts)
	if s.ID == outer.ID || s.Info().Name != t.Name() {
		t.Fatalf("Run returned %+v; the ambient session is %s", s.Info(), outer.ID)
	}
	tsessiontest.RequireNoFailures(t, s)
	tsessiontest.RequireMutation(t, s, client.MutationFilter{Store: "postgres", Op: "insert"})
	wantState(t, s, "SEALED")
	wantState(t, outer, "ACTIVE")
}
