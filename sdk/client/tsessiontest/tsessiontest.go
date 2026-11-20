// Package tsessiontest wraps sdk/client for Go tests: it ties a session to the test's lifetime and turns
// what the session recorded into assertions.
//
//	func TestCheckout(t *testing.T) {
//		sess := tsessiontest.Run(t, client.Options{Name: t.Name()})
//		httpClient := &http.Client{Transport: sess.Transport(nil)}
//		// ... drive the system under test with httpClient ...
//
//		tsessiontest.RequireNoFailures(t, sess)
//		tsessiontest.RequireMutation(t, sess, client.MutationFilter{Store: "postgres", Entity: "orders", Op: "insert"})
//	}
//
// The assertions stop the session and wait for its seal first, so asynchronous work (queue consumers,
// retries) and transaction outcomes are included.
//
// Instrumented services learn about a new session over a stream. Run waits (up to PropagationTimeout) until
// the control plane has sent the session to every connected service, so the first request of a test is
// normally recorded; a service that connects later, or one still applying the update, can miss it.
package tsessiontest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// ArtifactDirEnv names the environment variable that makes Run save the
// session of a failed test as a file in that directory (created if missing),
// so the run can still be opened after the CI job and its collector are gone:
//
//	TSESSION_ARTIFACT_DIR=artifacts go test ./...   # then upload artifacts/ from CI
//	dashboard -open artifacts/TestCheckout.ndjson
//
// The files hold recorded payloads (redacted, but possibly still sensitive)
// and are written with mode 0600. Whether they may be uploaded to CI storage
// is a question for your own data-handling policy.
const ArtifactDirEnv = "TSESSION_ARTIFACT_DIR"

// saveArtifact exports s to dir/<test name>.ndjson and returns the path.
func saveArtifact(ctx context.Context, s *client.Session, dir, test string) (path string, err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	// Subtest names contain slashes; keep the file inside dir.
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		}
		return '_'
	}, test)
	path = filepath.Join(dir, strings.TrimLeft(name, ".")+".ndjson")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	_, err = s.Export(ctx, f)
	return path, err
}

// PropagationTimeout bounds how long Run waits for the connected services to
// be sent the new session.
var PropagationTimeout = 2 * time.Second

// StopTimeout bounds how long stopping a session may take (drain and seal included).
var StopTimeout = 60 * time.Second

// Run starts a session for the test and stops it when the test ends. If the test failed, the session id and
// the failures it recorded are logged, so the run can be opened in the dashboard or with tracectl.
// An empty o.Name is set to the test's name.
func Run(t testing.TB, o client.Options) *client.Session {
	t.Helper()
	if o.Name == "" {
		o.Name = t.Name()
	}
	ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
	defer cancel()
	s, err := client.Start(ctx, o)
	if err != nil {
		t.Fatalf("tsession: start session: %v", err)
		return nil
	}
	// Give the connected services the session before the test's first request.
	pctx, pcancel := context.WithTimeout(ctx, PropagationTimeout)
	if _, err := s.WaitPropagated(pctx); err != nil {
		t.Logf("tsession: not every connected service was sent session %s within %v (%v); its first requests may go unrecorded",
			s.ID, PropagationTimeout, err)
	}
	pcancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
		defer cancel()
		if err := stop(ctx, s); err != nil {
			t.Logf("tsession: stop session %s: %v", s.ID, err)
			return
		}
		if !t.Failed() {
			return
		}
		t.Logf("tsession: session %s (%s) recorded this test; inspect it with: tracectl summary %s", s.ID, s.Info().Name, s.ID)
		if sum, err := s.Summary(ctx); err == nil {
			for _, f := range sum.Failures {
				t.Logf("tsession: failed event #%d %s %s %s: %s", f.Seq, f.Service, f.Kind, f.Name, f.Err)
			}
		}
		if dir := os.Getenv(ArtifactDirEnv); dir != "" {
			if path, err := saveArtifact(ctx, s, dir, t.Name()); err != nil {
				t.Logf("tsession: saving the session to %s: %v", dir, err)
			} else {
				t.Logf("tsession: session saved; open it with: dashboard -open %s", path)
			}
		}
	})
	return s
}

// stop stops s and waits for its seal unless it already ended.
func stop(ctx context.Context, s *client.Session) error {
	if s.Info().Done() {
		return nil
	}
	_, err := s.StopAndWait(ctx)
	return err
}

// Stop stops the session, waits for its seal and returns the summary. The Require functions do this
// themselves; call Stop to inspect the results directly.
func Stop(t testing.TB, s *client.Session) client.Summary {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
	defer cancel()
	if err := stop(ctx, s); err != nil {
		t.Fatalf("tsession: stop session %s: %v", s.ID, err)
		return client.Summary{}
	}
	sum, err := s.Summary(ctx)
	if err != nil {
		t.Fatalf("tsession: summary of session %s: %v", s.ID, err)
	}
	return sum
}

// RequireNoFailures fails the test if the session recorded a failed event or is incomplete (dropped or
// quarantined events, asynchronous work that never finished).
func RequireNoFailures(t testing.TB, s *client.Session) {
	t.Helper()
	sum := Stop(t, s)
	for _, f := range sum.Failures {
		t.Errorf("tsession: failed event #%d %s %s %s: %s", f.Seq, f.Service, f.Kind, f.Name, f.Err)
	}
	if sum.Incomplete {
		t.Errorf("tsession: session %s is incomplete: %d dropped, %d quarantined, %d async job(s) unfinished",
			s.ID, sum.DroppedEvents, sum.Quarantined, sum.PendingAsync)
	}
	// The control plane knows things the collector's summary does not: a drain
	// that ran out of time and a seal that failed.
	info := s.Info()
	switch {
	case info.SealError != "":
		t.Errorf("tsession: session %s was not sealed: %s", s.ID, info.SealError)
	case info.DrainTimedOut:
		t.Errorf("tsession: session %s stopped draining at its time limit with asynchronous work still running", s.ID)
	case info.Incomplete && !sum.Incomplete:
		t.Errorf("tsession: the control plane marked session %s incomplete", s.ID)
	}
	if len(sum.Failures) > 0 || sum.Incomplete || info.SealError != "" || info.DrainTimedOut || info.Incomplete {
		t.FailNow()
	}
}

// RequireMutation fails the test unless the session recorded at least one mutation matching f that was not
// rolled back. It returns the matches for further checks (count, diff, before/after images).
func RequireMutation(t testing.TB, s *client.Session, f client.MutationFilter) []client.Mutation {
	t.Helper()
	f.ExcludeRolledBack = true
	ms := mutations(t, s, f)
	if len(ms) == 0 {
		t.Fatalf("tsession: session %s recorded no effective mutation matching %+v", s.ID, f)
	}
	return ms
}

// RequireNoMutation fails the test if the session recorded a mutation matching f that was not rolled back.
func RequireNoMutation(t testing.TB, s *client.Session, f client.MutationFilter) {
	t.Helper()
	f.ExcludeRolledBack = true
	if ms := mutations(t, s, f); len(ms) > 0 {
		m := ms[0]
		t.Fatalf("tsession: session %s recorded %d mutation(s) matching %+v, first: event #%d %s %s/%s",
			s.ID, len(ms), f, m.Seq, m.Op, m.Store, m.Entity)
	}
}

func mutations(t testing.TB, s *client.Session, f client.MutationFilter) []client.Mutation {
	t.Helper()
	Stop(t, s)
	if e := s.Info().SealError; e != "" {
		// Without the seal, open transactions are unresolved: an assertion
		// about what took effect would be a guess.
		t.Fatalf("tsession: session %s was not sealed, so transaction outcomes are not final: %s", s.ID, e)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
	defer cancel()
	ms, err := s.Mutations(ctx, f)
	if err != nil {
		t.Fatalf("tsession: mutations of session %s: %v", s.ID, err)
	}
	return ms
}
