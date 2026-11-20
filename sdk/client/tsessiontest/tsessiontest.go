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
// Instrumented services learn about a new session over a stream, shortly after Run returns. A request sent
// in the same instant can go unrecorded; when the first request of a test must be captured, give the services
// a moment or retry it.
package tsessiontest

import (
	"context"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

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
	if len(sum.Failures) > 0 || sum.Incomplete {
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
	ctx, cancel := context.WithTimeout(context.Background(), StopTimeout)
	defer cancel()
	ms, err := s.Mutations(ctx, f)
	if err != nil {
		t.Fatalf("tsession: mutations of session %s: %v", s.ID, err)
	}
	return ms
}
