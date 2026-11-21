//go:build unix

package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

// readyEnv names the file the "term" helper creates once it is ready to be
// signalled.
const readyEnv = "TRACECTL_TEST_READY"

func init() {
	helpers["killed"] = func([]string) int {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Minute)
		return 0
	}
	// term waits for a SIGTERM and then exits cleanly.
	helpers["term"] = func([]string) int {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM)
		if err := os.WriteFile(os.Getenv(readyEnv), nil, 0o600); err != nil {
			return 8
		}
		select {
		case <-sigs:
			fmt.Println("terminated")
			return 0
		case <-time.After(time.Minute):
			return 9
		}
	}
}

// signalSelf sends this process, which is tracectl in these tests, a SIGTERM.
// tracectl handles the signal while a command runs, so it does not end the test.
func signalSelf(t *testing.T) {
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Error(err)
	}
}

// signalWhenReady sends tracectl a SIGTERM once the "term" helper is waiting
// for one.
func signalWhenReady(t *testing.T) {
	ready := t.TempDir() + "/ready"
	t.Setenv(readyEnv, ready)
	go func() {
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if _, err := os.Stat(ready); err == nil {
				signalSelf(t)
				return
			}
		}
		t.Error("the command never became ready")
	}()
}

func TestCLIRunCommandKilled(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	code, _, e := env.run("killed", []string{"--fail-on-error"})
	if want := 128 + int(syscall.SIGKILL); code != want || !strings.Contains(e, "the command was killed by a signal") ||
		!strings.Contains(e, "exit status 137") || !strings.Contains(e, "events     2 over") {
		t.Fatalf("exit %d, want %d\n%s", code, want, e)
	}
	env.wantSession(1, "SEALED")
}

// A SIGTERM sent to tracectl reaches the command, and the session is stopped
// and sealed after it.
func TestCLIRunForwardsSignal(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	signalWhenReady(t)
	code, out, e := env.run("term", []string{"--fail-on-error"})
	if code != 0 || out != "terminated\n" || !strings.Contains(e, "(SEALED)") {
		t.Fatalf("exit %d, stdout %q\n%s", code, out, e)
	}
	env.wantSession(1, "SEALED")
}

// A second signal, once the command is gone, ends the wait for the seal.
func TestCLIRunSecondSignalAborts(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	env.neverSeal = true
	env.onStop = func() { signalSelf(t) }
	signalWhenReady(t)
	start := time.Now()
	code, out, e := env.run("term", []string{"--timeout", "5m"})
	if code != 1 || out != "terminated\n" || !strings.Contains(e, "was not stopped and sealed: interrupted") {
		t.Fatalf("exit %d, stdout %q\n%s", code, out, e)
	}
	if d := time.Since(start); d > time.Minute {
		t.Fatalf("the wait was not aborted: took %s", d)
	}
	env.wantSession(1, "DRAINING")
}
