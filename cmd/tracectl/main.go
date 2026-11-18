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
	"syscall"

	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

const usage = `usage: tracectl [--cp URL] [--collector URL] <command> [flags]

session lifecycle (control plane):
  (--name templates: {bind} {bind.<name>} {initiated_by} {time} {date}; with no --name the server auto-names)
  start [--name N] [--mode header|exclusive] [--ttl S] [--quiesce S] [--max-drain S]
        [--bind source:name=value]...   source header|jwt|jwt_claim|cookie|query (repeatable;
        not with --mode exclusive). jwt reads an UNVERIFIED token: spoofable, test envs only.
  rename <id|name> <new name>   (works on sealed sessions; templates allowed)
  stop <id|name> [--wait] [--timeout D]
  abort <id|name>               end now, without draining or sealing
  delete <id|name>              remove a finished session and everything recorded for it
  status <id|name>
  list [--state STATE]
  wait <id|name> [--timeout D]
  watch-docker [--docker BIN]   start/stop sessions from containers labelled tsession.id=<name>
                                (optional label tsession.bind=header:x-user-id=u_1,jwt:sub=u_1)

session results (collector):
  summary <id|name> [--json] [--fail-on-error] [--fail-on-incomplete]
                                counts, failures and flags; the --fail-on flags exit 3 for CI gates
  events <id|name> [--kind K[,K]] [--service S] [--errors] [--limit N] [--json]
                                K is a kind (db.write) or a prefix ending in * (db.*)
  entities <id|name> [--json]   rows/documents/keys the session changed
  history <id|name> <store> <entity> <pk> [--json]
                                every change to one entity, with diffs and tx outcome
  export <id|name> [-o FILE]    all events as NDJSON (stdout by default)

  version                       print the tracectl version

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
	stdout io.Writer
	stderr io.Writer
}

// commands maps a command name to its implementation. Each receives the
// arguments after the command name.
var commands = map[string]func(*cli, context.Context, []string) error{
	"start":        (*cli).start,
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
	"export":       (*cli).export,
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
	name, rest := root.Arg(0), root.Args()[1:]
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintln(stderr, usage)
		fmt.Fprintf(stderr, "tracectl: unknown command %q\n", name)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &cli{opts: client.Options{BaseURL: *cp, CollectorURL: *col, InitiatedBy: "api"}, stdout: stdout, stderr: stderr}
	err := cmd(c, ctx, rest)
	var ce checkError
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
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
