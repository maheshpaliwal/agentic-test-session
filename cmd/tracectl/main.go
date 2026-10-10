// Command tracectl drives the tsession control plane from the command line.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tsession/sdk/client"
)

const usage = `usage: tracectl [--cp URL] <command> [flags]

commands:
  start [--name N] [--mode header|exclusive] [--ttl S] [--quiesce S] [--max-drain S]
  stop <id|name> [--wait] [--timeout D]
  status <id|name>
  list [--state STATE]
  wait <id|name> [--timeout D]
  watch-docker [--docker BIN]   start/stop sessions from containers labelled tsession.id=<name>

--cp defaults to $TSESSION_CONTROLPLANE_URL or http://127.0.0.1:7070`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	root := flag.NewFlagSet("tracectl", flag.ContinueOnError)
	root.SetOutput(stderr)
	cp := root.String("cp", "", "control plane URL")
	root.Usage = func() { fmt.Fprintln(stderr, usage) }
	if err := root.Parse(args); err != nil {
		return 2
	}
	if root.NArg() == 0 {
		root.Usage()
		return 2
	}
	cmd, rest := root.Arg(0), root.Args()[1:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := dispatch(ctx, *cp, cmd, rest, stdout, stderr); err != nil {
		fmt.Fprintln(stderr, "tracectl:", err)
		return 1
	}
	return 0
}

func printJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func dispatch(ctx context.Context, cp, cmd string, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	opts := client.Options{BaseURL: cp, InitiatedBy: "api"}

	switch cmd {
	case "start":
		fs.StringVar(&opts.Name, "name", "", "session name")
		fs.StringVar(&opts.Mode, "mode", "header", "header|exclusive")
		fs.IntVar(&opts.TTLSeconds, "ttl", 0, "ttl seconds")
		fs.IntVar(&opts.QuiesceSeconds, "quiesce", 0, "quiesce seconds")
		fs.IntVar(&opts.MaxDrainSeconds, "max-drain", 0, "max drain seconds")
		if err := fs.Parse(args); err != nil {
			return err
		}
		s, err := client.Start(ctx, opts)
		if err != nil {
			return err
		}
		printJSON(stdout, s.Info())

	case "stop":
		wait := fs.Bool("wait", false, "wait until sealed")
		timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
		ref, err := parseWithArg(fs, args)
		if err != nil {
			return err
		}
		s, err := resolve(ctx, opts, ref)
		if err != nil {
			return err
		}
		info, err := s.Stop(ctx)
		if err != nil {
			return err
		}
		if *wait {
			wctx, cancel := context.WithTimeout(ctx, *timeout)
			defer cancel()
			if info, err = s.Wait(wctx); err != nil {
				return err
			}
		}
		printJSON(stdout, info)

	case "status":
		ref, err := parseWithArg(fs, args)
		if err != nil {
			return err
		}
		s, err := resolve(ctx, opts, ref)
		if err != nil {
			return err
		}
		printJSON(stdout, s.Info())

	case "list":
		state := fs.String("state", "", "filter by state")
		if err := fs.Parse(args); err != nil {
			return err
		}
		list, err := listSessions(ctx, opts, *state)
		if err != nil {
			return err
		}
		for _, i := range list {
			fmt.Fprintf(stdout, "%-30s %-9s %-9s %s\n", i.ID, i.State, i.Mode, i.Name)
		}

	case "wait":
		timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
		ref, err := parseWithArg(fs, args)
		if err != nil {
			return err
		}
		s, err := resolve(ctx, opts, ref)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		info, err := s.Wait(wctx)
		if err != nil {
			return err
		}
		printJSON(stdout, info)

	case "watch-docker":
		bin := fs.String("docker", "docker", "docker binary")
		if err := fs.Parse(args); err != nil {
			return err
		}
		opts.InitiatedBy = "docker"
		w := &Watcher{
			Logf: log.Printf,
			Start: func(ctx context.Context, name string) (string, error) {
				o := opts
				o.Name = name
				s, err := client.Start(ctx, o)
				if err != nil {
					return "", err
				}
				return s.ID, nil
			},
			Stop: func(ctx context.Context, id string) error {
				// The container is gone; use a fresh context so shutdown does not skip the stop.
				sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				s, err := client.Attach(sctx, opts, id)
				if err != nil {
					return err
				}
				_, err = s.Stop(sctx)
				return err
			},
		}
		return w.RunDocker(ctx, *bin)

	default:
		fmt.Fprintln(stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

// parseWithArg parses flags that may appear before or after the single positional arg.
func parseWithArg(fs *flag.FlagSet, args []string) (string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return "", err
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	if len(pos) != 1 {
		return "", fmt.Errorf("expected exactly one session id or name")
	}
	return pos[0], nil
}

func listSessions(ctx context.Context, o client.Options, state string) ([]client.Info, error) {
	return client.List(ctx, o, state)
}

// resolve accepts a session id (ts_...) or a name (latest non-terminal
// session with that name, falling back to the latest with that name).
func resolve(ctx context.Context, o client.Options, ref string) (*client.Session, error) {
	if strings.HasPrefix(ref, "ts_") {
		return client.Attach(ctx, o, ref)
	}
	list, err := client.List(ctx, o, "")
	if err != nil {
		return nil, err
	}
	var best *client.Info
	for i := range list { // ascending by creation
		if list[i].Name != ref {
			continue
		}
		if best == nil || !list[i].Done() || best.Done() {
			best = &list[i]
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no session named %q", ref)
	}
	return client.Attach(ctx, o, best.ID)
}
