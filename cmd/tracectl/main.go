// Command tracectl drives the tsession control plane from the command line
// and reads what a session recorded from the collector.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

const usage = `usage: tracectl [--cp URL] [--collector URL] <command> [flags]

session lifecycle (control plane):
  (--name templates: {bind} {bind.<name>} {initiated_by} {time} {date}; with no --name the server auto-names)
  start [--name N] [--mode header|exclusive] [--ttl S] [--quiesce S] [--max-drain S] [--id-only]
        [--bind source:name=value]...   source header|jwt|jwt_claim|cookie|query (repeatable;
        not with --mode exclusive). jwt reads an UNVERIFIED token: spoofable, test envs only.
  rename [--short] <id|name> <new name>   (works on sealed sessions; templates allowed)
  stop <id|name> [--wait] [--timeout D] [--short]
  abort <id|name> [--short]     end now, without draining or sealing
  delete <id|name>              remove a finished session and everything recorded for it
  status <id|name> [--short]
  list [--state STATE] [--limit N]   all sessions, oldest first; with --limit the newest N
  wait <id|name> [--timeout D] [--short]
  (rename, stop, abort and wait print the session as JSON, status adds "name:" and "bindings:" lines
   after it; with --short all five print one line for scripts: "<id> <STATE>", followed by
   " incomplete" when the session is)
  watch-docker [--docker BIN]   start/stop sessions from containers labelled tsession.id=<name>
                                (optional label tsession.bind=header:x-user-id=u_1,jwt:sub=u_1)

record a command (control plane and collector):
  run [start's session flags, except --id-only] [--fail-on-error] [--fail-on-incomplete] [--fail-on-empty]
      [--fail-on-unconfirmed] [--report FILE [--format markdown|junit]] [--digest FILE] [--timeout D]
      -- <command> [args...]
                                start a session, run the command with the session id in $TSESSION_ID, stop
                                the session, wait for the seal (at most --timeout, default 2m) and print the
                                summary. The command is not run through a shell. Its stdin, stdout and stderr
                                pass through; tracectl prints to stderr only and writes the report to FILE.
                                --digest FILE writes what "digest" prints (JSON if FILE ends in .json).
                                The session is stopped and sealed also when the command fails.
                                A Go test joins the session with tsessiontest.Ambient or client.AttachEnv;
                                tsessiontest.Run still starts a session of its own per test.
                                The command runs in a process group of its own (it cannot read from the
                                terminal) and needs a --ttl longer than its runtime (default 1h).
                                SIGINT, SIGTERM, SIGHUP and SIGQUIT are passed on to the command's group, a
                                third one kills it; a second signal, once the
                                command has exited, stops waiting for the seal.
                                exit status, first that applies: the command's own if it failed (128+n if
                                signal n killed it; 127 not found and 126 not runnable, no session started);
                                1 the session could not be stopped, sealed or read; 3 a --fail-on-* check
                                failed; 0. Bad usage of run exits 2.

session results (collector):
  summary <id|name> [--json] [--fail-on-error] [--fail-on-incomplete] [--fail-on-empty] [--fail-on-unconfirmed]
                                counts, failures and flags; the --fail-on flags exit 3 for CI gates.
                                --fail-on-incomplete fails on events known to be lost; --fail-on-unconfirmed
                                also when the control plane could not tell (a connected service whose
                                delivery reports never arrive, say): use it where every connected service
                                is yours, not in a large shared environment
  events <id|name> [--kind K[,K]] [--service S] [--errors] [--limit N] [--json]
                                K is a kind (db.write) or a prefix ending in * (db.*)
  entities <id|name> [--json]   rows/documents/keys the session changed; "N uncertain" counts the
                                changes whose key or row image is incomplete (history shows the flags)
  history <id|name> <store> <entity> <pk> [--json]
  history <id|name> <store> <entity> --unknown-key [--json]
                                every change to one entity, with diffs, tx outcome and the flags of
                                the event (pk_not_unique, after_unavailable, ...: the recorded key or
                                row image is incomplete or uncertain).
                                <pk> is a key that entities lists, typed as the JSON it prints
                                ('{"order_id":7,"sku":"a"}', keys in any order), as order_id=7,sku=a,
                                or for a one-column key as the bare value (42). A value holding a
                                comma or = needs the JSON form; put -- before a pk that starts with -.
                                --unknown-key (or '' or '{}' as <pk>) reads the changes recorded
                                without a key.
  trace <id|name> [--errors] [--depth N] [--json]
                                the call tree; ~> marks async work a span triggered
  show <id|name> <seq> [--json] one event in full: request, response, mutations with diffs
  export <id|name> [-o FILE]    all events as NDJSON (stdout by default), the session's verdict on the
                                first line (alone, when there are no events): "dashboard -open FILE"
                                and tracectl against it then say what they say of the live session,
                                --fail-on flags included
  report <id|name> [--format markdown|junit] [-o FILE] [--fail-on-error] [--fail-on-incomplete] [--fail-on-empty]
         [--fail-on-unconfirmed]
                                a CI report: Markdown for a job summary (default) or JUnit XML, with
                                each failure traced to its root request; --fail-on flags as in summary
  digest <id|name> [--format markdown|json] [--max-bytes N] [-o FILE] [--fail-on-error] [--fail-on-incomplete]
         [--fail-on-empty] [--fail-on-unconfirmed]
                                the whole session in one bounded answer, for an AI agent or a paste into a
                                model: verdict and counts, every failure with its root request and
                                ancestors, mutations per entity with changed fields, tx outcome and
                                flags, and the call tree with repeated siblings collapsed ("40x": an
                                N+1). Never more than --max-bytes (default 16384, at least 4096): the
                                tree is cut first, then mutation detail, then failure detail, and
                                "Omitted" names the command that prints what is missing. Recorded text
                                is shown as quoted data. --format json (or --json) prints one line,
                                schema tsession.digest/v1 (docs/DIGEST.md). Works on a live session
                                and says so; --fail-on flags as in summary
  diff <id|name a> <id|name b> [--json] [--fail-on-diff] [--ignore-counts]
                                what differs in b (candidate) from a (baseline): failures, mutations,
                                events, first divergence; --fail-on-diff exits 3 when anything differs

  kill on|off|status            the global kill switch: while on, no service records anything, for
                                any session (kept in memory: a control plane restart turns it off)
  doctor [--json] [--require-services N] [--require-delivery]
                                check that the control plane and collector answer and list the
                                services connected to the control plane; one that reports events
                                it cannot deliver is marked. --require-delivery exits 3 on such a
                                service, or one whose reports do not arrive: a check for DURING a
                                run (it sees only services that report, and only running sessions)
  version                       print the tracectl version

exit status: 0 ok, 1 failed (including bad arguments to a command), 2 unknown command or global flag,
             3 a --fail-on-* check failed; run: see above

--cp defaults to $TSESSION_CONTROLPLANE_URL or http://127.0.0.1:7070
--collector defaults to $TSESSION_COLLECTOR_URL or http://127.0.0.1:7071`

// exitCheckFailed is the exit status of a failed --fail-on-* gate, distinct
// from 1 (the command itself failed) and 2 (bad usage).
const exitCheckFailed = 3

// checkError reports a failed --fail-on-* gate.
type checkError struct{ msg string }

func (e checkError) Error() string { return e.msg }

// cli carries what every command needs.
type cli struct {
	opts   client.Options
	stdin  io.Reader // read by the command of run only
	stdout io.Writer
	stderr io.Writer
}

// commands maps a command name to its implementation. Each receives the
// arguments after the command name.
var commands = map[string]func(*cli, context.Context, []string) error{
	"start":        (*cli).start,
	"run":          (*cli).runCommand,
	"rename":       (*cli).rename,
	"stop":         (*cli).stop,
	"abort":        (*cli).abort,
	"delete":       (*cli).delete,
	"status":       (*cli).status,
	"list":         (*cli).list,
	"wait":         (*cli).wait,
	"watch-docker": (*cli).watchDocker,
	"summary":      (*cli).summary,
	"events":       (*cli).events,
	"entities":     (*cli).entities,
	"history":      (*cli).history,
	"trace":        (*cli).trace,
	"show":         (*cli).show,
	"export":       (*cli).export,
	"diff":         (*cli).diff,
	"digest":       (*cli).digest,
	"report":       (*cli).report,
	"kill":         (*cli).kill,
	"doctor":       (*cli).doctor,
	"version":      (*cli).version,
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes one tracectl invocation and returns its exit status.
func run(args []string, stdout, stderr io.Writer) int {
	root := flag.NewFlagSet("tracectl", flag.ContinueOnError)
	root.SetOutput(stderr)
	cp := root.String("cp", "", "control plane URL")
	col := root.String("collector", "", "collector URL")
	root.Usage = func() { fmt.Fprintln(stderr, usage) }
	if err := root.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if root.NArg() == 0 {
		root.Usage()
		return 2
	}
	for flagName, v := range map[string]string{"--cp": *cp, "--collector": *col} {
		if v != "" && !strings.Contains(v, "://") {
			fmt.Fprintf(stderr, "tracectl: %s %q is not a URL: it needs a scheme, as in http://%s\n", flagName, v, v)
			return 2
		}
	}
	name, rest := root.Arg(0), root.Args()[1:]
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintf(stderr, "tracectl: unknown command %q; run 'tracectl --help' for the list of commands\n", name)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &cli{opts: client.Options{BaseURL: *cp, CollectorURL: *col, InitiatedBy: "api"}, stdin: os.Stdin, stdout: stdout, stderr: stderr}
	err := cmd(c, ctx, rest)
	var ce checkError
	var ee exitError
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, &ee):
		if ee.msg != "" {
			fmt.Fprintln(stderr, "tracectl:", ee.msg)
		}
		return ee.code
	case errors.As(err, &ce):
		fmt.Fprintln(stderr, "tracectl:", err)
		return exitCheckFailed
	default:
		fmt.Fprintln(stderr, "tracectl:", err)
		return 1
	}
}

func (c *cli) version(context.Context, []string) error {
	v, g := svc.Version()
	fmt.Fprintf(c.stdout, "tracectl %s (%s)\n", v, g)
	return nil
}

// flags returns a FlagSet for one command that reports errors to stderr.
func (c *cli) flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	return fs
}

// session parses fs with exactly one positional argument (a session id or
// name) and resolves it.
func (c *cli) session(ctx context.Context, fs *flag.FlagSet, args []string) (*client.Session, error) {
	pos, err := parseArgs(fs, args, 1, "<id|name>")
	if err != nil {
		return nil, err
	}
	return resolve(ctx, c.opts, pos[0])
}
