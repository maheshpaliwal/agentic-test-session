package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

const (
	runID = "ts_run1"

	// helperEnv selects what the test binary does when run starts it as the
	// command; see TestHelperProcess.
	helperEnv = "TRACECTL_TEST_HELPER"
)

// helpers are the commands the tests let run start, by name. Each returns
// its exit status.
var helpers = map[string]func(args []string) int{
	"ok": func(args []string) int {
		fmt.Printf("id=%s args=%q\n", os.Getenv(sessionEnv), args)
		fmt.Fprintln(os.Stderr, "helper stderr")
		return 0
	},
	"exit7": func([]string) int { return 7 },
	"env": func([]string) int {
		fmt.Printf("cp=%s col=%s\n", os.Getenv("TSESSION_CONTROLPLANE_URL"), os.Getenv("TSESSION_COLLECTOR_URL"))
		return 0
	},
}

// TestHelperProcess is not a test: it is the command of the run tests, which
// start this test binary with -test.run=TestHelperProcess and helperEnv set.
func TestHelperProcess(t *testing.T) {
	h, ok := helpers[os.Getenv(helperEnv)]
	if !ok {
		return
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			args = args[i+1:]
			break
		}
	}
	os.Exit(h(args))
}

// runEnv is a control plane and collector for one session, which seals as
// soon as it is stopped.
type runEnv struct {
	t   *testing.T
	url string
	evs []core.Event // what the session recorded

	failStart bool // POST /v1/sessions fails
	dropStops int  // this many stop requests get no answer: the connection is closed
	neverSeal bool // the session stays DRAINING
	onStop    func()
	// subs answers GET /v1/subscribers; the argument counts the requests
	// from 1. Nil: the control plane does not know the endpoint.
	subs     func(request int) []client.Subscriber
	subsReqs int

	mu      sync.Mutex
	started []client.Info // the sessions asked for, as sent
	stops   int
	state   string
}

func newRunEnv(t *testing.T, evs ...core.Event) *runEnv {
	t.Helper()
	env := &runEnv{t: t, evs: evs}
	info := func() client.Info {
		in := client.Info{ID: runID, State: env.state}
		if len(env.started) > 0 {
			in.Name = env.started[0].Name
		}
		return in
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/subscribers", func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if env.subs == nil {
			http.NotFound(w, r)
			return
		}
		env.subsReqs++
		json.NewEncoder(w).Encode(env.subs(env.subsReqs))
	})
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()
		var o client.Info
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil || env.failStart {
			http.Error(w, "no sessions today", http.StatusInternalServerError)
			return
		}
		env.started, env.state = append(env.started, o), "ACTIVE"
		json.NewEncoder(w).Encode(info())
	})
	mux.HandleFunc("GET /v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if env.state == "DRAINING" && !env.neverSeal {
			env.state = "SEALED"
		}
		json.NewEncoder(w).Encode(info())
	})
	mux.HandleFunc("POST /v1/sessions/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if env.dropStops > 0 {
			env.dropStops--
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
			return
		}
		env.stops++
		env.state = "DRAINING"
		if env.onStop != nil && env.stops == 1 {
			env.onStop()
		}
		json.NewEncoder(w).Encode(info())
	})
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		sum := client.Summary{SessionID: runID, EventCount: len(env.evs), Sealed: true}
		for _, e := range env.evs {
			if e.Err != "" {
				sum.Failures = append(sum.Failures, client.Failure{Seq: e.Seq, Service: e.Service, Kind: e.Kind, Name: e.Name, Err: e.Err})
			}
		}
		json.NewEncoder(w).Encode(sum)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.EventPage{Events: env.evs, NextSeq: uint64(len(env.evs)) + 1, Sealed: true})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		tr := client.Trace{SessionID: runID}
		for _, e := range env.evs {
			tr.Roots = append(tr.Roots, &client.TraceNode{Event: e})
		}
		json.NewEncoder(w).Encode(tr)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	env.url = srv.URL
	return env
}

// run runs "tracectl run <flags> -- <helper> <args>" with the test binary as
// the command and returns exit code, stdout and stderr.
func (env *runEnv) run(helper string, flags []string, args ...string) (int, string, string) {
	env.t.Helper()
	env.t.Setenv(helperEnv, helper)
	argv := append([]string{"run"}, flags...)
	argv = append(argv, "--", os.Args[0], "-test.run=^TestHelperProcess$", "--")
	return env.tc(append(argv, args...)...)
}

func (env *runEnv) tc(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"--cp", env.url, "--collector", env.url}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

// wantSession checks how many sessions were started and that every one of
// them was stopped and ended in state.
func (env *runEnv) wantSession(started int, state string) {
	env.t.Helper()
	env.mu.Lock()
	defer env.mu.Unlock()
	if len(env.started) != started || env.stops != started || env.state != state {
		panic(fmt.Sprintf("sessions: %d started, %d stop(s), state %q; want %d started and stopped, state %q",
			len(env.started), env.stops, env.state, started, state))
	}
}

func okEvents() []core.Event {
	return []core.Event{
		{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1_000_000_000, EndNs: 1_050_000_000},
		{Seq: 2, Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders", StartNs: 1_010_000_000, EndNs: 1_012_000_000},
	}
}

func failedEvents() []core.Event {
	return append(okEvents(), core.Event{Seq: 3, Service: "api", Kind: core.KindAPIIn, Name: "GET /orders/42", Err: "HTTP 500"})
}

func TestCLIRun(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	code, out, e := env.run("ok", []string{"--name", "ci", "--ttl", "600", "--bind", "header:x-user-id=u_1", "--fail-on-error", "--fail-on-incomplete", "--fail-on-empty"},
		"-x", "a b", "--name")
	if code != 0 {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	// Standard output is the command's alone; it saw the session id and got
	// its arguments as they were given.
	if want := "id=" + runID + ` args=["-x" "a b" "--name"]` + "\n"; out != want {
		panic(fmt.Sprintf("stdout %q, want %q", out, want))
	}
	for _, want := range []string{
		"tracectl: session " + runID + " (ci) started\n",
		"helper stderr\n",
		"session    " + runID + "  ci  (SEALED)\n",
		"events     2 over",
		"failures   0\n",
	} {
		if !strings.Contains(e, want) {
			t.Errorf("stderr lacks %q:\n%s", want, e)
		}
	}
	env.wantSession(1, "SEALED")
	if o := env.started[0]; o.Name != "ci" || o.Mode != "header" || o.TTLSeconds != 600 || formatBindings(o.Bindings) != "header:x-user-id=u_1" {
		panic(fmt.Sprintf("session options: %+v", o))
	}
}

func TestCLIRunExitStatus(t *testing.T) {
	gates := []string{"--fail-on-error", "--fail-on-empty"}
	for _, tc := range []struct {
		name   string
		evs    []core.Event
		helper string
		flags  []string
		code   int
		stderr []string
	}{
		{"command fails", okEvents(), "exit7", gates, 7, []string{"tracectl: the command failed: exit status 7\n", "events     2 over"}},
		{"gate fails", failedEvents(), "ok", gates, exitCheckFailed,
			[]string{"FAIL #3", "tracectl: check failed: 1 failed event(s)\n"}},
		{"gate not asked for", failedEvents(), "ok", nil, 0, []string{"failures   1\n"}},
		{"nothing recorded", nil, "ok", gates, exitCheckFailed, []string{"tracectl: check failed: " + emptySession + "\n"}},
		// The command's status wins over a failed gate; both are reported.
		{"command and gate fail", failedEvents(), "exit7", gates, 7,
			[]string{"tracectl: check failed: 1 failed event(s)\n", "tracectl: the command failed: exit status 7\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newRunEnv(t, tc.evs...)
			code, out, e := env.run(tc.helper, tc.flags)
			if code != tc.code {
				panic(fmt.Sprintf("exit %d, want %d\n%s", code, tc.code, e))
			}
			for _, want := range tc.stderr {
				if !strings.Contains(e, want) {
					t.Errorf("stderr lacks %q:\n%s", want, e)
				}
			}
			if strings.Contains(out, "tracectl") || strings.Contains(out, "session") {
				t.Errorf("tracectl wrote to stdout: %q", out)
			}
			env.wantSession(1, "SEALED")
		})
	}
}

func TestCLIRunReport(t *testing.T) {
	env := newRunEnv(t, failedEvents()...)
	dir := t.TempDir()
	md, junit := filepath.Join(dir, "report.md"), filepath.Join(dir, "junit.xml")

	code, out, e := env.run("ok", []string{"--name", "ci", "--report", md})
	if code != 0 || !strings.HasPrefix(out, "id=") || strings.Count(out, "\n") != 1 || !strings.Contains(e, "wrote markdown report to "+md) {
		panic(fmt.Sprintf("markdown: %d\n%s\n%s", code, out, e))
	}
	if b, err := os.ReadFile(md); err != nil || !strings.Contains(string(b), "GET /orders/42") || !strings.Contains(string(b), "HTTP 500") {
		panic(fmt.Sprintf("markdown report: %v\n%s", err, b))
	}

	// The report is written for a command that failed as well, and the gate
	// counts what the report shows.
	code, _, e = env.run("exit7", []string{"--report", junit, "--format", "junit", "--fail-on-error"})
	if code != 7 || !strings.Contains(e, "check failed: 1 failed event(s)") {
		panic(fmt.Sprintf("junit: %d\n%s", code, e))
	}
	b, err := os.ReadFile(junit)
	if err != nil {
		panic(err)
	}
	if s := parseJUnit(t, string(b)); s.Tests != 3 || s.Failures != 1 {
		panic(fmt.Sprintf("junit report: %d tests, %d failures\n%s", s.Tests, s.Failures, b))
	}
	env.wantSession(2, "SEALED")
}

// A session that does not seal makes the run fail, even if the command and
// the gates passed: what it recorded cannot be relied on.
func TestCLIRunNotSealed(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	env.neverSeal = true
	code, _, e := env.run("ok", []string{"--timeout", "250ms", "--fail-on-error"})
	if code != 1 || !strings.Contains(e, "tracectl: session "+runID+" was not stopped and sealed: not done within 250ms (state DRAINING)") {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	// The summary is still printed, as far as the session can be read.
	if !strings.Contains(e, "session    "+runID) || !strings.Contains(e, "(DRAINING)") {
		panic(fmt.Sprintf("no summary:\n%s", e))
	}
	env.wantSession(1, "DRAINING")

	// The command's own failure still wins.
	if code, _, e := env.run("exit7", []string{"--timeout", "250ms"}); code != 7 || !strings.Contains(e, "was not stopped and sealed") {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
}

func TestCLIRunCannotStart(t *testing.T) {
	// Without a session the command is not run.
	env := newRunEnv(t)
	env.failStart = true
	if code, out, e := env.run("ok", nil); code != 1 || out != "" || !strings.Contains(e, "no sessions today") {
		panic(fmt.Sprintf("start fails: %d %q\n%s", code, out, e))
	}

	// Without a command no session is started.
	env = newRunEnv(t)
	missing := filepath.Join(t.TempDir(), "no-such-command")
	if code, _, e := env.tc("run", "--", missing, "x"); code != exitNotFound || !strings.Contains(e, "tracectl: cannot run the command: ") {
		panic(fmt.Sprintf("missing command: %d\n%s", code, e))
	}
	notExecutable := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(notExecutable, []byte("x"), 0o600); err != nil {
		panic(err)
	}
	if code, _, e := env.tc("run", "--", notExecutable); code != exitCannotRun && code != exitNotFound {
		panic(fmt.Sprintf("not executable: %d\n%s", code, e))
	}
	env.wantSession(0, "")
}

func TestCLIRunUsage(t *testing.T) {
	env := newRunEnv(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "no command to run"},
		{[]string{"--"}, "no command to run"},
		{[]string{"--name", "ci", "--fail-on-error", "--"}, "no command to run"},
		{[]string{"--no-such-flag", "--", "true"}, "flag provided but not defined: -no-such-flag"},
		{[]string{"--id-only", "--", "true"}, "flag provided but not defined: -id-only"},
		{[]string{"--timeout", "soon", "--", "true"}, "invalid value"},
		{[]string{"--bind", "nonsense", "--", "true"}, "invalid value"},
		{[]string{"--report", "-", "--", "true"}, "--report needs a file"},
		{[]string{"--timeout", "0", "--", "true"}, "--timeout must be more than zero"},
		{[]string{"--timeout", "-1s", "--", "true"}, "--timeout must be more than zero"},
		{[]string{"--report", "no/such/dir/r.md", "--", "true"}, "its directory does not exist"},
		{[]string{"--format", "junit", "--", "true"}, "--format needs --report FILE"},
		{[]string{"--report", "r.html", "--format", "html", "--", "true"}, `unknown --format "html"`},
	} {
		code, out, e := env.tc(append([]string{"run"}, tc.args...)...)
		if code != 2 || out != "" || !strings.Contains(e, tc.want) {
			t.Errorf("run %q: exit %d, want 2 and %q\n%s", tc.args, code, tc.want, e)
		}
	}
	if code, _, e := env.tc("run", "-h"); code != 0 || !strings.Contains(e, "-fail-on-error") {
		t.Errorf("run -h: %d\n%s", code, e)
	}
	env.wantSession(0, "")
	if !strings.Contains(usage, "run [start's session flags") {
		t.Error("the usage text does not list run")
	}
}

// The command is told where the services are, when tracectl was told on the
// command line, so a test it runs reaches the same ones.
func TestCLIRunExportsTheServiceURLs(t *testing.T) {
	env := newRunEnv(t, okEvents()...)
	t.Setenv("TSESSION_CONTROLPLANE_URL", "http://127.0.0.1:1")
	code, out, e := env.run("env", nil)
	if want := "cp=" + env.url + " col=" + env.url + "\n"; code != 0 || out != want {
		panic(fmt.Sprintf("exit %d, stdout %q, want %q\n%s", code, out, want, e))
	}
}

// A control plane that drops the connection when asked to stop (a restart)
// is asked again: giving up would leave the session active until its TTL.
func TestCLIRunRetriesTheStop(t *testing.T) {
	old := stopRetryEvery
	stopRetryEvery = 10 * time.Millisecond
	defer func() { stopRetryEvery = old }()
	env := newRunEnv(t, okEvents()...)
	env.dropStops = 3
	code, _, e := env.run("ok", []string{"--fail-on-error"})
	if code != 0 || !strings.Contains(e, "(SEALED)") {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	env.wantSession(1, "SEALED")
}

// run --require-services: enough services are connected or the command is
// not run, and a service that was connected and is gone when the command
// ends fails the run, which may otherwise read as complete.
func TestCLIRunRequireServices(t *testing.T) {
	defer func(w, p time.Duration) { servicesGoneWait, servicesGonePoll = w, p }(servicesGoneWait, servicesGonePoll)
	servicesGoneWait, servicesGonePoll = 60*time.Millisecond, 10*time.Millisecond
	api := client.Subscriber{Service: "api", Instance: "api-1"}
	worker := client.Subscriber{Service: "worker", Instance: "worker-1"}

	// Too few: nothing is started and nothing is run.
	env := newRunEnv(t)
	env.subs = func(int) []client.Subscriber { return []client.Subscriber{api} }
	code, _, e := env.run("ok", []string{"--require-services", "2"})
	if code != exitCheckFailed || !strings.Contains(e, "1 service(s) connected, --require-services wants 2; the command was not run") {
		panic(fmt.Sprintf("too few services: exit %d\n%s", code, e))
	}
	env.wantSession(0, "")

	// Both stay: the run is what it is without the flag.
	env = newRunEnv(t)
	env.subs = func(int) []client.Subscriber { return []client.Subscriber{api, worker} }
	if code, _, e := env.run("ok", []string{"--require-services", "2"}); code != 0 {
		panic(fmt.Sprintf("both services stay: exit %d\n%s", code, e))
	}
	env.wantSession(1, "SEALED")

	// The worker is gone when the command ends: the run fails and names it; the session is still stopped.
	env = newRunEnv(t)
	env.subs = func(request int) []client.Subscriber {
		if request == 1 {
			return []client.Subscriber{api, worker}
		}
		return []client.Subscriber{api}
	}
	code, _, e = env.run("ok", []string{"--require-services", "2"})
	if code != exitCheckFailed || !strings.Contains(e, "1 service(s) connected when the run began are gone: worker (worker-1)") {
		panic(fmt.Sprintf("a service that went away: exit %d\n%s", code, e))
	}
	env.wantSession(1, "SEALED")

	// A command that failed keeps its own status.
	env = newRunEnv(t)
	env.subs = func(request int) []client.Subscriber {
		if request == 1 {
			return []client.Subscriber{api, worker}
		}
		return nil
	}
	if code, _, e := env.run("exit7", []string{"--require-services", "1"}); code != 7 || !strings.Contains(e, "are gone: api (api-1), worker (worker-1)") {
		panic(fmt.Sprintf("a failed command and services gone: exit %d\n%s", code, e))
	}

	// A control plane that cannot list its services: said before anything is run.
	env = newRunEnv(t)
	if code, _, e := env.run("ok", []string{"--require-services", "1"}); code != 1 || !strings.Contains(e, "--require-services: listing the connected services") {
		panic(fmt.Sprintf("no subscribers endpoint: exit %d\n%s", code, e))
	}
	env.wantSession(0, "")
	if code, _, e := env.tc("run", "--require-services", "-1", "--", "true"); code != 2 || !strings.Contains(e, "must not be negative") {
		panic(fmt.Sprintf("a negative number: exit %d\n%s", code, e))
	}
}

// What --require-services tells apart beyond names: one of two replicas
// that are called the same is gone; a service that is away for a moment
// (its stream was cut, the control plane restarted) and back is not.
func TestCLIRunRequireServicesCountsAndWaits(t *testing.T) {
	defer func(w, p time.Duration) { servicesGoneWait, servicesGonePoll = w, p }(servicesGoneWait, servicesGonePoll)
	servicesGoneWait, servicesGonePoll = 2*time.Second, 10*time.Millisecond
	replica := client.Subscriber{Service: "api"} // no instance name: two of them read the same

	env := newRunEnv(t)
	env.subs = func(request int) []client.Subscriber {
		if request == 1 {
			return []client.Subscriber{replica, replica}
		}
		return []client.Subscriber{replica}
	}
	servicesGoneWait = 60 * time.Millisecond
	if code, _, e := env.run("ok", []string{"--require-services", "2"}); code != exitCheckFailed || !strings.Contains(e, "1 service(s) connected when the run began are gone") {
		panic(fmt.Sprintf("one of two replicas gone: exit %d\n%s", code, e))
	}

	// Away at the first look after the command, back at the third: not gone.
	servicesGoneWait = 2 * time.Second
	env = newRunEnv(t)
	env.subs = func(request int) []client.Subscriber {
		if request == 2 || request == 3 {
			return nil
		}
		return []client.Subscriber{replica, replica}
	}
	if code, _, e := env.run("ok", []string{"--require-services", "2"}); code != 0 {
		panic(fmt.Sprintf("services that came back: exit %d\n%s", code, e))
	}
}

// The files of a run say what the run was: the status of a command that
// failed without a failed event, and a service that went away. A job that
// has only the files fails as the run did.
func TestCLIRunExportCarriesWhatFailedTheRun(t *testing.T) {
	defer func(w, p time.Duration) { servicesGoneWait, servicesGonePoll = w, p }(servicesGoneWait, servicesGonePoll)
	servicesGoneWait, servicesGonePoll = 60*time.Millisecond, 10*time.Millisecond
	t.Chdir(t.TempDir())
	firstLine := func(file string) string {
		b, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		return strings.SplitN(string(b), "\n", 2)[0]
	}

	// A command that exits 7, and no failed event.
	env := newRunEnv(t, core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9})
	code, _, e := env.run("exit7", []string{"--export", "seven.ndjson", "--digest", "seven.md", "--report", "seven-report.md"})
	if code != 7 {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	if l := firstLine("seven.ndjson"); !strings.Contains(l, `"command_status":7`) {
		panic("the export of a run whose command exited 7: " + l)
	}
	for file, want := range map[string]string{
		"seven.md":        "**Failed**: the command of the run failed (exit status 7); no failed events were recorded.",
		"seven-report.md": "**Failed**: the command of the run failed (exit status 7); no failed events were recorded.",
	} {
		if b, err := os.ReadFile(file); err != nil || !strings.Contains(string(b), want) || strings.Contains(string(b), "**Passed**") {
			panic(fmt.Sprintf("%s: %v\n%s", file, err, b))
		}
	}

	// A service that went away: the export is incomplete and says why.
	env = newRunEnv(t, core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9})
	env.subs = func(request int) []client.Subscriber {
		if request == 1 {
			return []client.Subscriber{{Service: "worker", Instance: "worker-1"}}
		}
		return nil
	}
	if code, _, e := env.run("ok", []string{"--require-services", "1", "--export", "gone.ndjson"}); code != exitCheckFailed {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	if l := firstLine("gone.ndjson"); !strings.Contains(l, `"incomplete":true`) || !strings.Contains(l, "connected when the run began are gone: worker (worker-1)") ||
		!strings.Contains(l, `"command_status":0`) {
		panic("the export of a run in which a service went away: " + l)
	}
}
