package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func (c *cli) summary(ctx context.Context, args []string) error {
	fs := c.flags("summary")
	asJSON := fs.Bool("json", false, "print the raw summary as JSON")
	failErr := fs.Bool("fail-on-error", false, "exit 3 when the session recorded failed events")
	failInc := fs.Bool("fail-on-incomplete", false, "exit 3 when the session is incomplete (dropped/quarantined events, orphaned async work)")
	failEmpty := fs.Bool("fail-on-empty", false, "exit 3 when the session recorded no events (nothing was instrumented, connected or attributed)")
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	sum, err := s.Summary(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(c.stdout, sum)
	} else {
		printSummary(c.stdout, s.Info(), sum)
	}
	var failed []string
	if *failErr && len(sum.Failures) > 0 {
		failed = append(failed, fmt.Sprintf("%d failed event(s)", len(sum.Failures)))
	}
	// The control plane knows about a failed seal and a drain that timed out;
	// the collector's summary does not.
	switch info := s.Info(); {
	case !*failInc:
	case sum.Incomplete || info.Incomplete || info.SealError != "":
		failed = append(failed, "session is incomplete")
	case info.State == "":
		// Read through the collector only: whether the seal failed or the
		// drain timed out is not known, so the gate cannot pass.
		failed = append(failed, "the control plane no longer knows this session, so whether it is complete is unknown")
	}
	if *failEmpty && sum.EventCount == 0 {
		failed = append(failed, emptySession)
	}
	if len(failed) > 0 {
		return checkError{msg: "check failed: " + strings.Join(failed, "; ")}
	}
	return nil
}

// emptySession is the --fail-on-empty verdict. A session with no events
// passes every other check, although it usually means that nothing recorded:
// the service was not connected, could not reach the collector, or the
// requests did not carry the session.
const emptySession = "session recorded no events (see tracectl doctor)"

func printSummary(w io.Writer, info client.Info, sum client.Summary) {
	state := info.State
	if sum.Incomplete {
		state += ", incomplete"
	}
	fmt.Fprintf(w, "session    %s  %s  (%s)\n", info.ID, displayName(info), state)
	fmt.Fprintf(w, "events     %d over %s\n", sum.EventCount, time.Duration(sum.DurationNs).Round(time.Millisecond))
	fmt.Fprintf(w, "failures   %d\n", len(sum.Failures))
	if sum.PendingAsync > 0 {
		label := "pending"
		if sum.Sealed {
			label = "orphaned"
		}
		fmt.Fprintf(w, "async      %d %s\n", sum.PendingAsync, label)
	}
	if sum.DroppedEvents > 0 || sum.Quarantined > 0 {
		fmt.Fprintf(w, "lost       %d dropped by SDKs, %d quarantined by the collector\n", sum.DroppedEvents, sum.Quarantined)
	}
	if len(sum.ByKind) > 0 {
		fmt.Fprintf(w, "by kind    %s\n", joinCounts(sum.ByKind))
	}
	if len(sum.ByService) > 0 {
		fmt.Fprintf(w, "by service %s\n", joinCounts(sum.ByService))
	}
	muts := map[string]int{}
	for store, ents := range sum.Mutations {
		for ent, n := range ents {
			muts[store+"/"+ent] = n
		}
	}
	if len(muts) > 0 {
		fmt.Fprintf(w, "mutations  %s\n", joinCounts(muts))
	}
	if len(sum.Flags) > 0 {
		fmt.Fprintf(w, "flags      %s\n", joinCounts(sum.Flags))
	}
	for _, f := range sum.Failures {
		fmt.Fprintf(w, "  FAIL #%d %-13s %-12s %s: %s\n", f.Seq, f.Kind, f.Service, clip(f.Name, 60), clip(f.Err, 160))
	}
}

// joinCounts renders "a 3, b 1" with keys sorted so output is stable.
func joinCounts(m map[string]int) string {
	parts := slicex.Map(mapx.SortedKeys(m), func(k string) string { return fmt.Sprintf("%s %d", k, m[k]) })
	return strings.Join(parts, ", ")
}

// clip shortens s to at most n runes on one line.
func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// kindFilter matches event kinds against a comma-separated list of exact
// kinds ("db.write") and prefixes ending in * ("db.*"). An empty list
// matches everything.
type kindFilter []string

func newKindFilter(s string) kindFilter {
	return slicex.SplitTrim(s, ",")
}

func (f kindFilter) match(k core.Kind) bool {
	if len(f) == 0 {
		return true
	}
	for _, p := range f {
		if pre, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(string(k), pre) {
				return true
			}
		} else if string(k) == p {
			return true
		}
	}
	return false
}

func (c *cli) events(ctx context.Context, args []string) error {
	fs := c.flags("events")
	kinds := fs.String("kind", "", "only these kinds: comma-separated, a trailing * matches a prefix (db.*)")
	service := fs.String("service", "", "only this service")
	errsOnly := fs.Bool("errors", false, "only failed events")
	limit := fs.Int("limit", 0, "stop after N matching events (0 = all)")
	asJSON := fs.Bool("json", false, "print matching events as NDJSON")
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	evs, err := s.Events(ctx)
	if err != nil {
		return err
	}
	kf := newKindFilter(*kinds)
	out := bufio.NewWriter(c.stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	var t0 int64
	if len(evs) > 0 {
		t0 = evs[0].StartNs
		for i := range evs {
			if evs[i].StartNs > 0 && evs[i].StartNs < t0 {
				t0 = evs[i].StartNs
			}
		}
	}
	shown := 0
	for i := range evs {
		e := &evs[i]
		if !kf.match(e.Kind) || (*service != "" && e.Service != *service) || (*errsOnly && e.Err == "") {
			continue
		}
		if *limit > 0 && shown >= *limit {
			break
		}
		shown++
		if *asJSON {
			if err := enc.Encode(e); err != nil {
				return err
			}
			continue
		}
		line := fmt.Sprintf("%6d  %10s  %-14s %-15s %s", e.Seq,
			"+"+time.Duration(e.StartNs-t0).Round(time.Millisecond).String(), clip(e.Service, 14), e.Kind, clip(e.Name, 70))
		if n := len(e.Mutations); n > 0 {
			line += fmt.Sprintf("  [%d mutation(s)]", n)
		}
		if e.Err != "" {
			line += "  ERR " + clip(e.Err, 100)
		}
		fmt.Fprintln(out, line)
	}
	if !*asJSON {
		fmt.Fprintf(out, "%d of %d events\n", shown, len(evs))
	}
	return nil
}

func (c *cli) entities(ctx context.Context, args []string) error {
	fs := c.flags("entities")
	asJSON := fs.Bool("json", false, "print as JSON")
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	ents, err := s.Entities(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(c.stdout, ents)
		return nil
	}
	for _, e := range ents {
		fmt.Fprintf(c.stdout, "%-10s %-28s %-30s %d mutation(s)\n", e.Store, clip(e.Entity, 28), clip(e.PK, 30), e.MutationCount)
	}
	fmt.Fprintf(c.stdout, "%d entities\n", len(ents))
	return nil
}

func (c *cli) history(ctx context.Context, args []string) error {
	fs := c.flags("history")
	asJSON := fs.Bool("json", false, "print as JSON")
	pos, err := parseArgs(fs, args, 4, "<id|name> <store> <entity> <pk>")
	if err != nil {
		return err
	}
	s, err := resolve(ctx, c.opts, pos[0])
	if err != nil {
		return err
	}
	h, err := s.History(ctx, pos[1], pos[2], pos[3])
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(c.stdout, h)
		return nil
	}
	fmt.Fprintf(c.stdout, "%s/%s %s: %d mutation(s)\n", h.Store, h.Entity, h.PK, len(h.Mutations))
	for _, m := range h.Mutations {
		outcome := "tx open"
		switch {
		case m.Effective == nil:
		case *m.Effective:
			outcome = "effective"
		default:
			outcome = "ROLLED BACK"
		}
		fmt.Fprintf(c.stdout, "#%-5d %-8s %-12s %-12s %s\n", m.Seq, m.Op, outcome, clip(m.Service, 12), m.Kind)
		for _, d := range m.Diff {
			fmt.Fprintf(c.stdout, "       %s: %s -> %s\n", d.Field, compact(d.Old), compact(d.New))
		}
	}
	return nil
}

// compact renders a diff value on one line.
func compact(v any) string {
	if v == nil {
		return "∅"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return clip(string(b), 80)
}

func (c *cli) export(ctx context.Context, args []string) (err error) {
	fs := c.flags("export")
	path := fs.String("o", "", "write to this file instead of stdout")
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	w := c.stdout
	if *path != "" {
		f, ferr := os.OpenFile(*path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // events may hold sensitive payloads
		if ferr != nil {
			return ferr
		}
		defer func() {
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}()
		w = f
	}
	n, err := s.Export(ctx, w)
	if err != nil {
		return err
	}
	if *path != "" {
		fmt.Fprintf(c.stderr, "wrote %d events to %s\n", n, *path)
	}
	return nil
}
