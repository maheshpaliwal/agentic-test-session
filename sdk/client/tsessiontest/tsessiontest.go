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
//
// # Under tracectl run
//
// `tracectl run -- go test ./...` starts one session, the ambient session, and gives the tests its id in
// TSESSION_ID. The rules:
//
//   - Run does not look at TSESSION_ID. A test that calls Run records in a session of its own, with or
//     without tracectl run, and the ambient session sees nothing of it. If every test uses Run, the ambient
//     session stays empty and tracectl run's --fail-on-empty fails; run such tests with plain `go test`.
//   - Ambient is the opt-in: it returns the ambient session instead of starting one, so that everything the
//     command does lands in one session, which tracectl run stops, seals, reports and gates
//     (--fail-on-error, --fail-on-incomplete, --fail-on-empty, --report).
//   - The ambient session is tracectl run's. Ambient never stops it, and Stop and the Require functions
//     refuse a session whose id is the one in TSESSION_ID (they fail the test) rather than stop it under
//     the other tests; this also holds for a handle from client.AttachEnv or client.Attach.
//   - There are no per-test assertions on the ambient session, on purpose. They need two things a session
//     that is still recording and shared does not give. Completeness: services send events in batches and
//     workers run later, and until the seal nothing says that everything a test caused has arrived or
//     whether its transactions committed, so "no failures" and RequireNoMutation could pass on events
//     still in flight. Attribution: the session holds the events of every test of the command, numbered
//     in the order they reached the collector, so neither "events after a mark" nor a count belongs to one
//     test, least of all with t.Parallel. The verdict on the ambient session is tracectl run's, made once,
//     after the seal, for the command as a whole. A test that needs its own verdict uses Run.
//   - Both can be used in one command, also in one test: they are different sessions.
//
// Ambient is safe with t.Parallel: each call makes its own handle, and tagging requests keeps no state.
// When a test that used Ambient fails, the log names the ambient session; it is not saved to
// TSESSION_ARTIFACT_DIR, because it is still recording (use tracectl run --report, or tracectl export
// once the command has ended).
package tsessiontest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// ArtifactDirEnv names the environment variable that makes Run save the
// session of a failed test as a file in that directory (created if missing),
// so the run can still be opened after the CI job and its collector are gone:
//
//	TSESSION_ARTIFACT_DIR=$PWD/artifacts go test ./...   # then upload artifacts/ from CI
//	dashboard -open artifacts/TestCheckout.ts_....ndjson
//
// Use an absolute path: go test runs each package in its own directory, so a
// relative one would create a directory per package.
//
// The files hold recorded payloads (redacted, but possibly still sensitive)
// and are written with mode 0600. Whether they may be uploaded to CI storage
// is a question for your own data-handling policy.
const ArtifactDirEnv = "TSESSION_ARTIFACT_DIR"

// saveArtifact exports s to dir/<test name>.<session id>.ndjson and returns
// the absolute path. The session id keeps tests whose names sanitise to the
// same text, or that share a name across packages, from overwriting each
// other; the file is created exclusively, so nothing existing (a symlink
// included) is written through.
func saveArtifact(ctx context.Context, s *client.Session, dir, test string) (path string, err error) {
	if dir, err = filepath.Abs(dir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	// Subtest names contain slashes, and the session id is whatever the
	// control plane answered: neither may move the file out of dir.
	safe := func(s string) string {
		return strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
				return r
			}
			return '_'
		}, s)
	}
	path = filepath.Join(dir, strings.TrimLeft(safe(test), ".")+"."+strings.Trim(safe(s.ID), ".")+".ndjson")
	if filepath.Dir(path) != dir {
		return "", fmt.Errorf("artifact path %q leaves %q", path, dir)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
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
	waitPropagated(ctx, t, s)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
		defer cancel()
		stopErr := stop(ctx, s)
		if stopErr != nil {
			t.Logf("tsession: stop session %s: %v", s.ID, stopErr)
		}
		if !t.Failed() {
			return
		}
		t.Logf("tsession: session %s (%s) recorded this test; inspect it with: tracectl summary %s", s.ID, s.Info().Name, s.ID)
		if _, listed := failuresListed.Load(s.ID); listed {
			failuresListed.Delete(s.ID)
		} else if sum, err := s.Summary(ctx); err == nil {
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

// waitPropagated gives the connected services the session before the test's
// first request.
func waitPropagated(ctx context.Context, t testing.TB, s *client.Session) {
	t.Helper()
	pctx, pcancel := context.WithTimeout(ctx, PropagationTimeout)
	defer pcancel()
	switch n, err := s.WaitPropagated(pctx); {
	case err != nil:
		t.Logf("tsession: not every connected service was sent session %s within %v (%v); its first requests may go unrecorded",
			s.ID, PropagationTimeout, err)
	case n == 0:
		t.Logf("tsession: no service is connected to the control plane, so nothing will record session %s (see tracectl doctor)", s.ID)
	}
}

// Ambient returns the ambient session: the one a surrounding `tracectl run -- go test ...` started and
// named in TSESSION_ID (see client.AttachEnv). Use it instead of Run when the whole command is to be
// recorded as one session:
//
//	func TestCheckout(t *testing.T) {
//		sess := tsessiontest.Ambient(t, client.Options{})
//		httpClient := &http.Client{Transport: sess.Transport(nil)}
//		// ... drive the system under test with httpClient ...
//	}
//
// The test fails at once if TSESSION_ID is not set, or names a session that the control plane does not know
// or that is not ACTIVE: a test that asks for the ambient session must not pass unrecorded. A test that
// should also run without tracectl run can choose with os.Getenv(client.SessionEnv) between Ambient and Run.
//
// The session is not the test's: it is not stopped when the test ends, and Stop and the Require functions
// refuse it (see the package documentation for why there are no assertions on it). Like Run, Ambient waits
// (up to PropagationTimeout) until the connected services have been sent the session; tracectl run starts
// its command without waiting for that. If the test fails, the session id is logged.
func Ambient(t testing.TB, o client.Options) *client.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
	defer cancel()
	s, err := client.AttachEnv(ctx, o)
	if err != nil {
		t.Fatalf("tsession: the ambient session of tracectl run: %v (Run starts a session for the test alone)", err)
		return nil
	}
	waitPropagated(ctx, t, s)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		t.Logf("tsession: session %s (%s) recorded this test and everything else the command of tracectl run does; "+
			"it is still recording. tracectl run prints its summary when the command ends; then: tracectl summary %s",
			s.ID, s.Info().Name, s.ID)
		if os.Getenv(ArtifactDirEnv) != "" {
			t.Logf("tsession: session %s is not saved to $%s, since it is not sealed yet; "+
				"give tracectl run --report FILE, or save it afterwards with: tracectl export %s -o FILE", s.ID, ArtifactDirEnv, s.ID)
		}
	})
	return s
}

// ambient reports whether s is the session named in TSESSION_ID, which
// belongs to tracectl run and is shared with the rest of its command.
func ambient(s *client.Session) bool {
	return s.ID != "" && s.ID == os.Getenv(client.SessionEnv)
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
//
// Stop, and with it every Require function, fails the test instead when s is the ambient session (the one
// named in TSESSION_ID): that session is tracectl run's to stop.
func Stop(t testing.TB, s *client.Session) client.Summary {
	t.Helper()
	if ambient(s) {
		t.Fatalf("tsession: session %s is the one in $%s: it belongs to tracectl run and is shared with the rest of its command, "+
			"so a test must not stop it, and the assertions of tsessiontest need a stopped session. "+
			"Assert on a session of the test's own (tsessiontest.Run), or let tracectl run check this one (--fail-on-error)",
			s.ID, client.SessionEnv)
		return client.Summary{}
	}
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

// failuresListed holds the ids of sessions whose failed events
// RequireNoFailures has already printed, so Run's cleanup does not print them
// a second time.
var failuresListed sync.Map

// RequireNoFailures fails the test if the session recorded a failed event or is incomplete (dropped or
// quarantined events, asynchronous work that never finished, events a service recorded that the collector
// does not have). Each reason the control plane gives is printed.
//
// A delivery the control plane could not confirm (Info.DeliveryUnconfirmed) is logged and does not fail
// the test; `go test` shows a passing test's log only with -v. Use RequireDeliveryConfirmed to fail on it.
func RequireNoFailures(t testing.TB, s *client.Session) {
	t.Helper()
	sum := Stop(t, s)
	if len(sum.Failures) > 0 {
		failuresListed.Store(s.ID, struct{}{})
	}
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
	for _, r := range info.IncompleteReasons {
		t.Errorf("tsession: session %s is incomplete: %s", s.ID, r)
	}
	for _, w := range info.DeliveryUnconfirmed {
		t.Logf("tsession: session %s: delivery unconfirmed: %s", s.ID, w)
	}
	switch {
	case info.SealError != "":
		t.Errorf("tsession: session %s was not sealed: %s", s.ID, info.SealError)
	case info.DrainTimedOut:
		t.Errorf("tsession: session %s stopped draining at its time limit with asynchronous work still running", s.ID)
	case info.Incomplete && !sum.Incomplete && len(info.IncompleteReasons) == 0:
		t.Errorf("tsession: the control plane marked session %s incomplete", s.ID)
	}
	if len(sum.Failures) > 0 || sum.Incomplete || info.SealError != "" || info.DrainTimedOut || info.Incomplete {
		t.FailNow()
	}
}

// RequireDeliveryConfirmed fails the test if the control plane could not tell, for some service instance,
// whether all its events reached the collector: an instance that recorded more after its last delivery
// report and then stopped reporting (it was killed, say). The session is not incomplete for that, so
// RequireNoFailures passes. It stops the session first, like RequireNoFailures, and can be called after it.
func RequireDeliveryConfirmed(t testing.TB, s *client.Session) {
	t.Helper()
	Stop(t, s)
	info := s.Info()
	for _, w := range info.DeliveryUnconfirmed {
		t.Errorf("tsession: session %s: delivery unconfirmed: %s", s.ID, w)
	}
	if len(info.DeliveryUnconfirmed) > 0 {
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
