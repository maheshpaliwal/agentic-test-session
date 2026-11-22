package controlplane

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/faultpoint"
)

const degradedFor = "degraded: a background goroutine panicked and waits to be restarted: %s; the log has the panic\n"

// waitHealthz asks /healthz until it answers code.
func waitHealthz(t *testing.T, m *Manager, code int, what string) string {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(time.Millisecond) {
		got, body := healthz(t, m)
		if got == code {
			return body
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("/healthz never answered %d (%s): %d %q", code, what, got, body))
		}
	}
}

// A seal attempt is one goroutine per attempt: nothing waits for its
// restart, so /healthz answered ok over a control plane whose seal retries
// panicked every time. It is degraded now while the attempt after a panic is
// not due yet, like a loop that waits for its restart, and healthy again
// once an attempt has gone through.
func TestHealthzIsDegradedWhileASealRetryThatPanickedWaitsForItsNextAttempt(t *testing.T) {
	col := newFlaky()
	m := newRetryMgr(t, col, func(c *Config) {
		c.Logger, c.SealRetryMaxInterval = (&logBuffer{}).logger(), 200*time.Millisecond
	})
	s := waitPending(t, m, StartRequest{Name: "p"})
	// A seal that fails is not a panic.
	if code, body := healthz(t, m); code != http.StatusOK {
		panic(fmt.Sprintf("/healthz over a seal that only fails: %d %q", code, body))
	}
	restore, hits := faultpoint.PanicAt("sealretry.top", -1)
	defer restore()
	// The backoff doubles with every panic, up to a fifth of a second.
	if body := waitHealthz(t, m, http.StatusServiceUnavailable, "a seal retry panicked"); body != fmt.Sprintf(degradedFor, "seal_retry") {
		panic(fmt.Sprintf("/healthz: %q", body))
	}
	if hits() == 0 {
		panic("degraded without a panic")
	}
	restore()
	col.failing.Store(false)
	waitSession(t, m, s.ID, "sealed on the attempt after the panics", func(s Session) bool { return !s.SealPending })
	if body := waitHealthz(t, m, http.StatusOK, "the seal went through"); body != "ok\n" {
		panic(fmt.Sprintf("/healthz: %q", body))
	}
}

// A seal retry that is given up at its panic is not tried again, so nothing
// waits: the control plane is not degraded for it.
func TestHealthzIsNotDegradedForASealRetryThatIsGivenUp(t *testing.T) {
	col := newFlaky()
	restore, hits := faultpoint.PanicAt("sealretry.top", -1)
	defer restore()
	m := newRetryMgr(t, col, func(c *Config) {
		c.Logger, c.SealPendingBudget = (&logBuffer{}).logger(), time.Nanosecond
	})
	s, err := m.Start(StartRequest{Name: "p"})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	waitSession(t, m, s.ID, "given up", func(s Session) bool { return s.State == StateSealed && !s.SealPending && s.SealError != "" })
	if code, body := healthz(t, m); hits() == 0 || code != http.StatusOK {
		panic(fmt.Sprintf("/healthz after %d panics of a retry that was given up: %d %q", hits(), code, body))
	}
}

// A compaction is one goroutine per compaction, too. After one panicked the
// control plane is degraded until a compaction goes through.
func TestHealthzIsDegradedAfterACompactionPanickedUntilOneGoesThrough(t *testing.T) {
	restore, hits := faultpoint.PanicAt("compaction.write", 1)
	defer restore()
	logs := &logBuffer{}
	f, err := NewFileStore(filepath.Join(t.TempDir(), "sessions.json"), WithStoreLogger(logs.logger()))
	if err != nil {
		panic(err)
	}
	m := NewManager(Config{Store: f, Logger: logs.logger()})
	defer func() {
		m.Close()
		if err := f.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	// One compaction, at the next write.
	compactNext := func() {
		f.lockIdle()
		f.compactMin, f.compactAt = 1<<40, f.journalSize+1
		f.wmu.Unlock()
	}
	put := func(i int) {
		if err := f.Put(Session{ID: fmt.Sprintf("ts_%02d", i), State: StateSealed}); err != nil {
			panic(err)
		}
	}
	compactNext()
	put(0)
	if body := waitHealthz(t, m, http.StatusServiceUnavailable, "a compaction panicked"); body != fmt.Sprintf(degradedFor, "compaction") {
		panic(fmt.Sprintf("/healthz: %q", body))
	}
	// No compaction has run since: it stays degraded, however often it is asked.
	put(1)
	if code, _ := healthz(t, m); code != http.StatusServiceUnavailable {
		panic("/healthz is ok again although no compaction went through")
	}
	compactNext()
	put(2)
	waitHealthz(t, m, http.StatusOK, "a compaction went through")
	if hits() != 1 {
		panic(fmt.Sprintf("%d panics", hits()))
	}
}
