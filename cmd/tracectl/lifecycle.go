package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func (c *cli) start(ctx context.Context, args []string) error {
	fs := c.flags("start")
	opts := c.opts
	fs.StringVar(&opts.Name, "name", "", "session name")
	fs.StringVar(&opts.Mode, "mode", "header", "header|exclusive")
	fs.IntVar(&opts.TTLSeconds, "ttl", 0, "ttl seconds")
	fs.IntVar(&opts.QuiesceSeconds, "quiesce", 0, "quiesce seconds")
	fs.IntVar(&opts.MaxDrainSeconds, "max-drain", 0, "max drain seconds")
	var binds bindFlags
	fs.Var(&binds, "bind", "source:name=value (repeatable; header|jwt|jwt_claim|cookie|query)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.Bindings = binds.list
	warnJWT(c.stderr, opts.Bindings)
	s, err := client.Start(ctx, opts)
	if err != nil {
		return err
	}
	printJSON(c.stdout, s.Info())
	return nil
}

func (c *cli) rename(ctx context.Context, args []string) error {
	fs := c.flags("rename")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: tracectl rename <id|name> <new name>")
	}
	s, err := resolve(ctx, c.opts, fs.Arg(0))
	if err != nil {
		return err
	}
	info, err := s.Rename(ctx, strings.Join(fs.Args()[1:], " "))
	if err != nil {
		return err
	}
	printJSON(c.stdout, info)
	return nil
}

func (c *cli) stop(ctx context.Context, args []string) error {
	fs := c.flags("stop")
	wait := fs.Bool("wait", false, "wait until sealed")
	timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
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
	printJSON(c.stdout, info)
	return nil
}

func (c *cli) abort(ctx context.Context, args []string) error {
	s, err := c.session(ctx, c.flags("abort"), args)
	if err != nil {
		return err
	}
	info, err := s.Abort(ctx)
	if err != nil {
		return err
	}
	printJSON(c.stdout, info)
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
	s, err := c.session(ctx, c.flags("status"), args)
	if err != nil {
		return err
	}
	info := s.Info()
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	list, err := client.List(ctx, c.opts, *state)
	if err != nil {
		return err
	}
	for _, i := range list {
		line := fmt.Sprintf("%-30s %-9s %-9s %s", i.ID, i.State, i.Mode, displayName(i))
		if b := formatBindings(i.Bindings); b != "" {
			line += "  [" + b + "]"
		}
		fmt.Fprintln(c.stdout, line)
	}
	return nil
}

func (c *cli) wait(ctx context.Context, args []string) error {
	fs := c.flags("wait")
	timeout := fs.Duration("timeout", 2*time.Minute, "wait timeout")
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
	printJSON(c.stdout, info)
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
