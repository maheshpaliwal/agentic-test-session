package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func (c *cli) start(ctx context.Context, args []string) error {
	fs := c.flags("start")
	opts := c.opts
	binds := sessionFlags(fs, &opts)
	idOnly := fs.Bool("id-only", false, "print only the session id, for scripts: ID=$(tracectl start --id-only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.Bindings = binds.list
	warnJWT(c.stderr, opts.Bindings)
	s, err := client.Start(ctx, opts)
	if err != nil {
		return err
	}
	if *idOnly {
		_, err = fmt.Fprintln(c.stdout, s.ID)
		return err
	}
	printJSON(c.stdout, s.Info())
	return nil
}

// sessionFlags registers the flags that describe a new session, shared by
// start and run. The bindings are complete once fs is parsed.
func sessionFlags(fs *flag.FlagSet, opts *client.Options) *bindFlags {
	fs.StringVar(&opts.Name, "name", "", "session name")
	fs.StringVar(&opts.Mode, "mode", "header", "header|exclusive")
	fs.IntVar(&opts.TTLSeconds, "ttl", 0, "ttl seconds")
	fs.IntVar(&opts.QuiesceSeconds, "quiesce", 0, "quiesce seconds")
	fs.IntVar(&opts.MaxDrainSeconds, "max-drain", 0, "max drain seconds")
	binds := &bindFlags{}
	fs.Var(binds, "bind", "source:name=value (repeatable; header|jwt|jwt_claim|cookie|query)")
	return binds
}

// shortFlag registers --short, the same on every command that prints a
// session: one line "<id> <STATE>" instead of the session as JSON.
func shortFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("short", false, `print one line "<id> <STATE>" (then " incomplete" if so) instead of the session as JSON`)
}

// printInfo prints a session as JSON or, when short, as one line for scripts:
// the id and the state, followed by "incomplete" when the session is.
func printInfo(w io.Writer, info client.Info, short bool) {
	if !short {
		printJSON(w, info)
		return
	}
	line := info.ID + " " + info.State
	if info.Incomplete {
		line += " incomplete"
	}
	fmt.Fprintln(w, line)
}

func (c *cli) rename(ctx context.Context, args []string) error {
	fs := c.flags("rename")
	short := shortFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: tracectl rename [--short] <id|name> <new name>")
	}
	s, err := resolve(ctx, c.opts, fs.Arg(0))
	if err != nil {
		return err
	}
	info, err := s.Rename(ctx, strings.Join(fs.Args()[1:], " "))
	if err != nil {
		return err
	}
	printInfo(c.stdout, info, *short)
	return nil
}

func (c *cli) stop(ctx context.Context, args []string) error {
	fs := c.flags("stop")
	wait := fs.Bool("wait", false, "wait until sealed")
	timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
	short := shortFlag(fs)
	s, err := c.session(ctx, fs, args)
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
	printInfo(c.stdout, info, *short)
	return nil
}

func (c *cli) abort(ctx context.Context, args []string) error {
	fs := c.flags("abort")
	short := shortFlag(fs)
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	info, err := s.Abort(ctx)
	if err != nil {
		return err
	}
	printInfo(c.stdout, info, *short)
	return nil
}

func (c *cli) delete(ctx context.Context, args []string) error {
	s, err := c.session(ctx, c.flags("delete"), args)
	if err != nil {
		return err
	}
	if err := s.Delete(ctx); err != nil {
		return err
	}
	fmt.Fprintln(c.stdout, "deleted", s.ID)
	return nil
}

func (c *cli) status(ctx context.Context, args []string) error {
	fs := c.flags("status")
	short := shortFlag(fs)
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	info := s.Info()
	if *short {
		printInfo(c.stdout, info, true)
		return nil
	}
	printJSON(c.stdout, info)
	fmt.Fprintln(c.stdout, "name:", displayName(info))
	if b := formatBindings(info.Bindings); b != "" {
		fmt.Fprintln(c.stdout, "bindings:", b)
	}
	return nil
}

func (c *cli) list(ctx context.Context, args []string) error {
	fs := c.flags("list")
	state := fs.String("state", "", "filter by state")
	limit := fs.Int("limit", 0, "list only the newest N sessions (default: all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit < 0 {
		return errors.New("--limit must be a positive number")
	}
	*state = strings.ToUpper(strings.TrimSpace(*state))
	if *state != "" && !slices.Contains(sessionStates, *state) {
		return fmt.Errorf("--state %q is not a session state; the states are %s", *state, strings.Join(sessionStates, ", "))
	}
	list, more, err := listNewest(ctx, c.opts, *state, *limit)
	if err != nil {
		return err
	}
	for _, i := range list {
		line := fmt.Sprintf("%-30s %-9s %-9s %s", i.ID, i.State, i.Mode, displayName(i))
		if b := formatBindings(i.Bindings); b != "" {
			line += "  [" + b + "]"
		}
		fmt.Fprintln(c.stdout, line+listMarks(i))
	}
	if more {
		fmt.Fprintf(c.stderr, "tracectl: these are the newest %d sessions; there are older ones (raise --limit, or leave it out to list all)\n", len(list))
	}
	return nil
}

// sessionStates are the states a session can be in, as the control plane names them.
var sessionStates = []string{"CREATED", "ACTIVE", "DRAINING", "SEALED", "ABORTED", "EXPIRED"}

// listMarks says, at the end of a list line, that a session's record is known to be incomplete or that the
// delivery of some instance could not be confirmed; empty for a session with neither.
func listMarks(i client.Info) string {
	var marks string
	if i.Incomplete {
		marks += "  INCOMPLETE"
	}
	if len(i.DeliveryUnconfirmed) > 0 {
		marks += "  DELIVERY-UNCONFIRMED"
	}
	return marks
}

// listNewest returns the newest limit sessions (in state, if set), oldest
// first, and whether there are older ones; with limit 0, all sessions.
func listNewest(ctx context.Context, o client.Options, state string, limit int) (list []client.Info, more bool, err error) {
	if limit == 0 {
		list, err = client.List(ctx, o, state)
		return list, false, err
	}
	const maxPage = 1000 // the most the control plane returns per request
	for before := ""; len(list) < limit; {
		page, next, err := client.ListPage(ctx, o, client.ListOptions{State: state, Limit: min(limit-len(list), maxPage), Before: before})
		if err != nil {
			return nil, false, err
		}
		list = append(list, page...)
		if before, more = next, next != ""; !more {
			break
		}
	}
	// Oldest first, like the list without a limit. Sorting here, not relying
	// on the order of the reply, also serves a control plane that does not
	// know the limit and answers with all sessions.
	slices.SortFunc(list, byCreation)
	if len(list) > limit {
		list, more = list[len(list)-limit:], true
	}
	return list, more, nil
}

func (c *cli) wait(ctx context.Context, args []string) error {
	fs := c.flags("wait")
	timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
	short := shortFlag(fs)
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	info, err := s.Wait(wctx)
	if err != nil {
		return err
	}
	printInfo(c.stdout, info, *short)
	return nil
}

func (c *cli) watchDocker(ctx context.Context, args []string) error {
	fs := c.flags("watch-docker")
	bin := fs.String("docker", "docker", "docker binary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := c.opts
	opts.InitiatedBy = "docker"
	w := &Watcher{
		Logf: log.Printf,
		Start: func(ctx context.Context, name string, binds []client.Binding) (string, error) {
			o := opts
			o.Name = name
			o.Bindings = binds
			warnJWT(c.stderr, binds)
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
}
