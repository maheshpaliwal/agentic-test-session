package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

const (
	runArgs = "[session flags] [--fail-on-error] [--fail-on-incomplete] [--fail-on-empty] [--fail-on-unconfirmed] " +
		"[--report FILE [--format markdown|junit]] [--digest FILE] [--timeout D] -- <command> [args...]"

	// sessionEnv is the variable in which the command finds the session id.
	sessionEnv = client.SessionEnv

	// What env(1) and timeout(1) exit with when the command cannot be run.
	exitCannotRun = 126
	exitNotFound  = 127

	// runResultsTimeout bounds reading the summary and the report once the
	// session has ended.
	runResultsTimeout = 30 * time.Second
)

// exitError ends tracectl with a status of its own: a usage error of run
// (2) or the status of the command run ran. msg, if any, is printed first.
type exitError struct {
	code int
	msg  string
}

func (e exitError) Error() string { return e.msg }

func runUsage(format string, a ...any) error {
	return exitError{code: 2, msg: "usage: tracectl run " + runArgs + ": " + fmt.Sprintf(format, a...)}
}

// runCommand records one command in a session of its own: it starts the
// session, runs the command with the session id in TSESSION_ID, stops the
// session, waits for the seal and prints the summary.
//
// The session is stopped and sealed whatever the command does, so that a
// failed run can be read afterwards. The exit status is the command's own if
// it failed (128+n if signal n killed it); otherwise 1 if the session could
// not be stopped, sealed or read, since nothing can then be said about the
// run; otherwise 3 if a --fail-on-* check failed; otherwise 0.
//
// Standard output belongs to the command. Everything tracectl prints goes to
// standard error, and the report only to a file.
func (c *cli) runCommand(ctx context.Context, args []string) error {
	fl := c.flags("run")
	opts := c.opts
	binds := sessionFlags(fl, &opts)
	var g gates
	g.register(fl)
	reportPath := fl.String("report", "", "write a report of the session to this file")
	format := fl.String("format", "", "format of --report: markdown (default) or junit")
	digestPath := fl.String("digest", "", "write a digest of the session to this file: Markdown, or JSON if the name ends in .json")
	timeout := fl.Duration("timeout", 2*time.Minute, "how long to wait for the session to stop and seal")
	if err := fl.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return exitError{code: 2} // the flag package has said what is wrong
	}
	argv := fl.Args()
	var render func(*sessionReport) ([]byte, error)
	switch {
	case len(argv) == 0:
		return runUsage("no command to run")
	case *timeout <= 0:
		return runUsage("--timeout must be more than zero: the session has to be stopped and sealed")
	case *reportPath == "-":
		return runUsage("--report needs a file: standard output belongs to the command")
	case *digestPath == "-":
		return runUsage("--digest needs a file: standard output belongs to the command")
	case *reportPath == "" && *format != "":
		return runUsage("--format needs --report FILE")
	}
	if *digestPath != "" {
		if st, err := os.Stat(filepath.Dir(*digestPath)); err != nil || !st.IsDir() {
			return runUsage("--digest %s: its directory does not exist", *digestPath)
		}
	}
	if *reportPath != "" {
		if *format == "" {
			*format = "markdown"
		}
		var err error
		if render, err = reportRenderer(*format); err != nil {
			return runUsage("%v", err)
		}
		// Now, not after the command has run for an hour.
		if st, err := os.Stat(filepath.Dir(*reportPath)); err != nil || !st.IsDir() {
			return runUsage("--report %s: its directory does not exist", *reportPath)
		}
	}
	// The command is never run through a shell. It is looked up before the
	// session starts, so a mistyped name leaves no session behind.
	if _, err := exec.LookPath(argv[0]); err != nil {
		code := exitCannotRun
		if errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist) {
			code = exitNotFound
		}
		return exitError{code: code, msg: "cannot run the command: " + err.Error()}
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	ownGroup(cmd)

	// From here on a signal is run's to handle: ctx ends at the first one,
	// but the session must still be stopped.
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, runSignals...)
	defer signal.Stop(sigs)
	defer surviveBrokenPipe()()

	opts.Bindings = binds.list
	warnJWT(c.stderr, opts.Bindings)
	s, err := client.Start(ctx, opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.stderr, "tracectl: session %s (%s) started\n", s.ID, displayName(s.Info()))

	// waitCtx is what the work after the command runs under: it outlives
	// ctx and ends when a signal aborts the wait.
	waitCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()
	relay := &signalRelay{abort: abort}
	relayDone := make(chan struct{})
	defer close(relayDone)
	go relay.run(sigs, relayDone)

	// The id, and where tracectl found the services if it was told on the
	// command line: the command then talks to the same ones.
	cmd.Env = append(os.Environ(), sessionEnv+"="+s.ID)
	if c.opts.BaseURL != "" {
		cmd.Env = append(cmd.Env, "TSESSION_CONTROLPLANE_URL="+c.opts.BaseURL)
	}
	if c.opts.CollectorURL != "" {
		cmd.Env = append(cmd.Env, "TSESSION_COLLECTOR_URL="+c.opts.CollectorURL)
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = c.stdin, c.stdout, c.stderr
	code, outcome := exitCannotRun, ""
	if err := cmd.Start(); err != nil {
		outcome = "cannot run the command: " + err.Error()
	} else {
		relay.attach(cmd.Process)
		err := cmd.Wait()
		relay.attach(nil)
		code, outcome = exitStatus(err)
	}

	// Stop and seal, whatever the command did.
	failed := false // the session could not be stopped, sealed or read
	stopCtx, cancel := context.WithTimeout(waitCtx, *timeout)
	for {
		// A control plane that cannot be reached right now (a restart) is
		// asked again until the time is up: giving up leaves the session
		// active until its TTL.
		if _, err = s.StopAndWait(stopCtx); err == nil || stopCtx.Err() != nil || !unreachable(err) {
			break
		}
		select {
		case <-stopCtx.Done():
		case <-time.After(stopRetryEvery):
		}
	}
	timedOut := stopCtx.Err() != nil && waitCtx.Err() == nil
	cancel()
	if err != nil {
		failed = true
		switch {
		case waitCtx.Err() != nil:
			err = errors.New("interrupted")
		case timedOut && unreachable(err) && !errors.Is(err, context.DeadlineExceeded):
			err = fmt.Errorf("not done within %s (state %s): %w", *timeout, s.Info().State, err)
		case timedOut:
			err = fmt.Errorf("not done within %s (state %s)", *timeout, s.Info().State)
		case s.Info().State == "EXPIRED":
			err = fmt.Errorf("%w: it ran past its TTL; give run a --ttl longer than the command takes", err)
		}
		fmt.Fprintf(c.stderr, "tracectl: session %s was not stopped and sealed: %v; what it recorded cannot be relied on\n", s.ID, err)
	}

	// Summary, report and gates: also for a session that did not seal, as
	// far as it can be read.
	var gateErr error
	if waitCtx.Err() == nil {
		readCtx, cancel := context.WithTimeout(waitCtx, runResultsTimeout)
		// In header mode without bindings only what carries the id is recorded.
		byID := opts.Mode != "exclusive" && len(opts.Bindings) == 0
		gateErr, err = c.runResults(readCtx, s, &g, byID, *reportPath, *format, render)
		if err == nil && *digestPath != "" {
			err = c.runDigest(readCtx, s, *digestPath)
		}
		cancel()
		if err != nil {
			failed = true
			fmt.Fprintf(c.stderr, "tracectl: reading session %s: %v\n", s.ID, err)
		}
	}
	if gateErr != nil && !failed { // the checks of a session that was not sealed or read say nothing
		fmt.Fprintln(c.stderr, "tracectl:", gateErr)
	}
	if outcome != "" {
		fmt.Fprintln(c.stderr, "tracectl:", outcome)
	}
	switch {
	case code != 0:
		return exitError{code: code}
	case failed:
		return exitError{code: 1}
	case gateErr != nil:
		return exitError{code: exitCheckFailed}
	}
	return nil
}

// runResults prints the summary of the session run recorded, writes its
// report and returns the verdict of the gates. byID says that the session
// records only requests that carry its id; an empty one then gets a hint.
func (c *cli) runResults(ctx context.Context, s *client.Session, g *gates, byID bool, reportPath, format string,
	render func(*sessionReport) ([]byte, error)) (gateErr, err error) {
	if render == nil {
		sum, err := s.Summary(ctx)
		if err != nil {
			return nil, err
		}
		printSummary(c.stderr, s.Info(), sum)
		c.hintUntagged(byID, sum)
		return g.check(s.Info(), sum, len(sum.Failures)), nil
	}
	rep, err := loadReport(ctx, s)
	if err != nil {
		return nil, err
	}
	printSummary(c.stderr, rep.info, rep.sum)
	c.hintUntagged(byID, rep.sum)
	gateErr = g.check(rep.info, rep.sum, len(rep.failures))
	out, err := render(rep)
	if err != nil {
		return gateErr, err
	}
	return gateErr, c.writeReportFile(reportPath, format, out)
}

// runDigest writes the digest of the session run recorded to path.
func (c *cli) runDigest(ctx context.Context, s *client.Session, path string) error {
	format := "markdown"
	if strings.HasSuffix(path, ".json") {
		format = "json"
	}
	render, err := digestRenderer(format)
	if err != nil {
		return err
	}
	src, err := loadDigest(ctx, s)
	if err != nil {
		return err
	}
	out, err := src.fit(render, digestDefaultBytes)
	if err != nil {
		return err
	}
	return c.writeDigestFile(path, out)
}

// hintUntagged says, for an empty session that records by id, what most
// often went wrong: the command did not send the id. Go tests written with
// tsessiontest.Run are the common case: each records in a session of its own.
func (c *cli) hintUntagged(byID bool, sum client.Summary) {
	if !byID || sum.EventCount != 0 {
		return
	}
	fmt.Fprintln(c.stderr, "tracectl: nothing was recorded: the command has to send $"+sessionEnv+
		" as the x-tsession-id header itself (Go: client.AttachEnv, tsessiontest.Ambient)")
	fmt.Fprintln(c.stderr, "tracectl: sessions the command starts on its own, such as those of tsessiontest.Run, are separate from this one")
}

// stopRetryEvery is the pause between attempts to stop the session while
// the control plane cannot be reached.
var stopRetryEvery = time.Second

// unreachable reports whether err is a failure to reach the service at all,
// as opposed to an answer from it.
func unreachable(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}

// exitStatus turns what exec.Cmd.Wait returned into the exit status a shell
// would report and a line for the user; both are zero for a command that
// succeeded.
func exitStatus(err error) (int, string) {
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, ""
	case !errors.As(err, &ee):
		return 1, "the command failed: " + err.Error()
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		code := 128 + int(ws.Signal())
		return code, fmt.Sprintf("the command was killed by a signal (%v): exit status %d", ws.Signal(), code)
	}
	return ee.ExitCode(), fmt.Sprintf("the command failed: exit status %d", ee.ExitCode())
}

// signalRelay passes the signals run receives on to the command while it
// runs; from the third one on the command is killed, since it has ignored
// two. Once the command is gone the session is still stopped and sealed; a
// signal then aborts that wait if it is not the first one run received, so
// one signal (a CI job being cancelled) ends the command and still leaves a
// sealed session, and a second one gets the user out.
type signalRelay struct {
	abort context.CancelFunc

	mu      sync.Mutex
	child   *os.Process
	pending os.Signal // received before the command started
	seen    int
}

func (r *signalRelay) run(sigs <-chan os.Signal, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case sig := <-sigs:
			r.mu.Lock()
			r.seen++
			switch {
			case r.child != nil && r.seen > 2:
				killCommand(r.child)
			case r.child != nil:
				forward(r.child, sig)
			case r.seen == 1:
				r.pending = sig
			default:
				r.abort()
			}
			r.mu.Unlock()
		}
	}
}

// attach names the running command, or nil once it has exited. A signal that
// arrived before the command started is delivered to it now.
func (r *signalRelay) attach(p *os.Process) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.child = p
	if p != nil && r.pending != nil {
		forward(p, r.pending)
	}
	r.pending = nil
}
