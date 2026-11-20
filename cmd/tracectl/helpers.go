package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func printJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// parseArgs parses fs and collects exactly n positional arguments; flags may
// appear before, between or after them. what names the positionals in the
// error message.
func parseArgs(fs *flag.FlagSet, args []string, n int, what string) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	if len(pos) != n {
		return nil, fmt.Errorf("usage: tracectl %s %s", fs.Name(), what)
	}
	return pos, nil
}

// resolve accepts a session id (ts_...) or a name (latest non-terminal
// session with that name, falling back to the latest with that name).
func resolve(ctx context.Context, o client.Options, ref string) (*client.Session, error) {
	if strings.HasPrefix(ref, "ts_") {
		s, err := client.Attach(ctx, o, ref)
		if err != nil && strings.Contains(err.Error(), ": 404 ") {
			// The control plane forgets finished sessions (retention, restart)
			// while the collector may still hold their events: read those.
			// The collector answers for any id, with zero events for one it
			// has never seen: fall back only when it really holds events, or
			// a mistyped id would read as an empty, passing session.
			d := client.Detached(o, ref)
			if sum, serr := d.Summary(ctx); serr == nil && sum.EventCount > 0 {
				fmt.Fprintf(os.Stderr, "tracectl: the control plane no longer knows %s; reading what the collector holds\n", ref)
				return d, nil
			}
		}
		return s, err
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

// displayName renders the name with an "(auto)" marker for generated names.
func displayName(i client.Info) string {
	if i.NameAuto {
		return i.Name + " (auto)"
	}
	return i.Name
}

// bindFlags collects repeatable --bind values.
type bindFlags struct{ list []client.Binding }

func (b *bindFlags) String() string { return formatBindings(b.list) }

func (b *bindFlags) Set(v string) error {
	bd, err := client.ParseBinding(v)
	if err != nil {
		return err
	}
	b.list = append(b.list, bd)
	return nil
}

// formatBindings renders "header:x-user-id=u_1,jwt_claim:sub=u_1".
func formatBindings(bs []client.Binding) string {
	parts := make([]string, len(bs))
	for i, b := range bs {
		parts[i] = b.Source + ":" + b.Name + "=" + b.Value
	}
	return strings.Join(parts, ",")
}

// warnJWT prints the spoofing warning when any binding reads a JWT.
func warnJWT(w io.Writer, bs []client.Binding) {
	for _, b := range bs {
		if b.Source == "jwt_claim" {
			fmt.Fprintln(w, "tracectl: WARNING: jwt bindings decode the token payload WITHOUT verifying its signature; "+
				"anyone can spoof the claim and land traffic in this session. Attribution only, never a security boundary.")
			return
		}
	}
}

// parseBindLabel parses the tsession.bind label value: comma-separated
// source:name=value items (values therefore cannot contain commas).
func parseBindLabel(v string) ([]client.Binding, error) {
	var out []client.Binding
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		b, err := client.ParseBinding(item)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}
