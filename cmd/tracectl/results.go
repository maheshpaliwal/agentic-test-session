package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func (c *cli) summary(ctx context.Context, args []string) error {
	fs := c.flags("summary")
	asJSON := fs.Bool("json", false, "print the raw summary as JSON")
	var g gates
	g.register(fs)
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
	return g.of(c).check(c.stderr, s.Info(), sum, len(sum.Failures))
}

// gates are the --fail-on-* checks of summary, report, digest and run.
type gates struct {
	onError, onIncomplete, onEmpty, onUnconfirmed bool
	// file: the session is read from a file of --from. What such a session
	// lacks will not arrive: where a running session gets a note, the file
	// of one fails the gate.
	file bool
}

// of returns g for the sessions c reads: from the services, or from files.
func (g *gates) of(c *cli) *gates {
	g.file = len(c.files) > 0
	return g
}

func (g *gates) register(fs *flag.FlagSet) {
	fs.BoolVar(&g.onError, "fail-on-error", false, "exit 3 when the session recorded failed events")
	fs.BoolVar(&g.onIncomplete, "fail-on-incomplete", false, "exit 3 when the session is incomplete (dropped/quarantined/undelivered events, orphaned async work, a failed seal)")
	fs.BoolVar(&g.onEmpty, "fail-on-empty", false, "exit 3 when the session recorded no events (nothing was instrumented, connected or attributed)")
	fs.BoolVar(&g.onUnconfirmed, "fail-on-unconfirmed", false, "exit 3 when the control plane could not tell whether every event of every service instance reached the collector (delivery_unconfirmed); for environments where every connected service is yours")
}

// check returns a checkError naming every gate the session fails; failures
// is the number of failed events the caller found.
func (g *gates) check(stderr io.Writer, info client.Info, sum client.Summary, failures int) error {
	var failed []string
	if g.onError && failures > 0 {
		failed = append(failed, fmt.Sprintf("%d failed event(s)", failures))
	}
	// The control plane knows about a failed seal and a drain that timed out;
	// the collector's summary does not.
	switch {
	case !g.onIncomplete:
	case sum.Incomplete || info.Incomplete || info.SealError != "":
		msg := "session is incomplete"
		if len(info.IncompleteReasons) > 0 {
			msg += " (" + strings.Join(info.IncompleteReasons, "; ") + ")"
		}
		failed = append(failed, msg)
	case info.State == "":
		// Read through the collector only: whether the seal failed or the
		// drain timed out is not known, so the gate cannot pass.
		if g.file {
			failed = append(failed, "the file does not say how its session ended or how many events it holds, so whether it is complete is unknown")
		} else {
			failed = append(failed, "the control plane no longer knows this session, so whether it is complete is unknown")
		}
	case !info.Done() && g.file:
		failed = append(failed, fmt.Sprintf("the session had not ended when the file was written (%s), so the file does not hold all of it and whether it is complete is not known; export the session once it has ended",
			dataText(info.State, 20)))
	case !info.Done():
		// Still running: what its services hold has not all arrived, and the
		// verdict on it is given when it ends. The gate judges what there is
		// and says so; the exit status stays what it was for scripts that
		// look at a session while it runs.
		fmt.Fprintf(stderr, "tracectl: note: the session is still running (%s): --fail-on-incomplete can only judge what has arrived so far; stop it first for a verdict (tracectl stop %s --wait)\n",
			dataText(info.State, 20), dataText(info.ID, 64))
	}
	if g.onUnconfirmed && !g.onIncomplete && (sum.Incomplete || info.Incomplete || info.SealError != "") {
		// The stricter gate on its own: a session known to have lost events
		// must not pass where one that is merely unconfirmed fails.
		msg := "session is incomplete"
		if len(info.IncompleteReasons) > 0 {
			msg += " (" + strings.Join(info.IncompleteReasons, "; ") + ")"
		}
		failed = append(failed, msg)
	}
	switch {
	case !g.onUnconfirmed:
	case len(info.DeliveryUnconfirmed) > 0:
		failed = append(failed, "delivery is unconfirmed ("+strings.Join(info.DeliveryUnconfirmed, "; ")+")")
	case info.State == "" && !g.onIncomplete:
		if g.file {
			failed = append(failed, "the file does not say how its session ended, so whether its delivery was confirmed is unknown")
		} else {
			failed = append(failed, "the control plane no longer knows this session, so whether its delivery was confirmed is unknown")
		}
	}
	if g.onEmpty && sum.EventCount == 0 {
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
	// The control plane's verdict counts as the collector's does: a drain that
	// timed out, or a seal that failed, leaves a session the collector may
	// find nothing wrong with. (The header of such a session said "(SEALED)",
	// and --fail-on-incomplete exited 3 on it.)
	if sum.Incomplete || info.Incomplete || len(info.IncompleteReasons) > 0 {
		state += ", incomplete"
	}
	if len(info.DeliveryUnconfirmed) > 0 {
		state += ", delivery unconfirmed"
	}
	fmt.Fprintf(w, "session    %s  %s  (%s)\n", info.ID, displayName(info), state)
	events := fmt.Sprintf("events     %d over %s", sum.EventCount, time.Duration(sum.DurationNs).Round(time.Millisecond))
	if sum.EventCount == 0 {
		events = "events     0: the session recorded nothing, so nothing was checked (see tracectl doctor)"
	}
	fmt.Fprintln(w, events)
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
	// Events that never reached the collector are in none of the counts
	// above: only the service that recorded them knows, and it told the
	// control plane.
	for _, r := range info.IncompleteReasons {
		fmt.Fprintf(w, "delivery   incomplete: %s\n", clip(r, 400))
	}
	for _, r := range info.DeliveryUnconfirmed {
		fmt.Fprintf(w, "delivery   unconfirmed: %s\n", clip(r, 400))
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
	printEntities(c.stdout, ents)
	return nil
}

func printEntities(w io.Writer, ents []client.Entity) {
	for _, e := range ents {
		pk := e.PK
		if pk == "{}" || pk == "" { // "{}" is still the value to pass to "history"
			pk = "{} " + unknownKey
		}
		// The key is printed whole: it is what "history" takes, and a masked
		// Redis key ("session:#1f2e3d4c5b6a") is useless when cut.
		line := fmt.Sprintf("%-10s %-28s %-30s %d mutation(s)", e.Store, clip(e.Entity, 28), pk, e.MutationCount)
		if e.Uncertain > 0 { // the key or row image of that many is incomplete: "history" shows the flags
			line += fmt.Sprintf(", %d uncertain", e.Uncertain)
		}
		if len(e.Services) > 0 { // absent from an older collector
			line += "  by " + clip(strings.Join(e.Services, ", "), 60)
		}
		fmt.Fprintln(w, line)
	}
	fmt.Fprintf(w, "%d entities\n", len(ents))
}

const historyUsage = "usage: tracectl history <id|name> <store> <entity> <pk>   (or --unknown-key in place of <pk>)\n" +
	"  <pk> as `tracectl entities` lists it ('{\"id\":42}'), as id=42 (a composite key: a=1,b=2) or, for a one-column key, 42"

func (c *cli) history(ctx context.Context, args []string) error {
	fs := c.flags("history")
	asJSON := fs.Bool("json", false, "print as JSON")
	unknown := fs.Bool("unknown-key", false, "the changes recorded without a key, in place of <pk> (the same as '' or '{}')")
	pos, err := positionals(fs, args)
	if err == nil {
		// With --from the session need not be named: one argument fewer.
		n := 4
		if *unknown {
			n = 3
		}
		pos, err = c.fromSessions("history", pos, n)
	}
	if err != nil {
		return err
	}
	switch {
	case len(pos) == 3 && *unknown:
		pos = append(pos, noKey)
	case len(pos) == 4 && *unknown && strings.TrimSpace(pos[3]) != "" && pos[3] != noKey:
		return errors.New("--unknown-key stands in place of <pk>: pass one or the other\n" + historyUsage)
	case len(pos) != 4:
		return errors.New(historyUsage)
	}
	s, err := c.resolve(ctx, pos[0])
	if err != nil {
		return err
	}
	// The key is looked up among the session's entities: what was typed may be
	// a looser spelling than the collector matches, and an entity that is not
	// there is an error, not an empty history.
	ents, err := s.Entities(ctx)
	if err != nil {
		return err
	}
	key, err := resolveKey(ents, pos[1], pos[2], pos[3])
	if err != nil {
		return fmt.Errorf("%w\n  `%s` lists what the session changed", err, drillCommand(c.fromWord(s.ID), pos[0], "entities ID"))
	}
	h, err := s.History(ctx, pos[1], pos[2], key)
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(c.stdout, h)
		return nil
	}
	printHistory(c.stdout, h)
	return nil
}

func printHistory(w io.Writer, h client.EntityHistory) {
	head := fmt.Sprintf("%s/%s %s: %d mutation(s)", h.Store, h.Entity, h.PK, len(h.Mutations))
	if n := uncertainCount(h.Mutations); n > 0 {
		head += fmt.Sprintf(", %d uncertain (see the flags)", n)
	}
	// Who wrote it, as "entities" says it; each line below names its own.
	services := set.Set[string]{}
	for _, m := range h.Mutations {
		if m.Service != "" {
			services.Add(m.Service)
		}
	}
	if len(services) > 0 {
		head += "  by " + clip(strings.Join(set.Sorted(services), ", "), 60)
	}
	fmt.Fprintln(w, head)
	for _, m := range h.Mutations {
		outcome := "tx open"
		switch {
		case m.Effective == nil:
		case *m.Effective:
			outcome = "effective"
		default:
			outcome = "ROLLED BACK"
		}
		fmt.Fprintf(w, "#%-5d %-8s %-12s %-12s %s\n", m.Seq, m.Op, outcome, clip(m.Service, 12), m.Kind)
		if len(m.Flags) > 0 { // of the event, as "show" prints them
			fmt.Fprintf(w, "       flags: %s\n", clip(strings.Join(m.Flags, ", "), 200))
		}
		if m.AfterUnavailable() {
			// No diff, even one an older SDK sent: it lists every field as removed.
			fmt.Fprintf(w, "       %s\n", afterUnavailable)
			continue
		}
		for _, d := range m.Diff {
			fmt.Fprintf(w, "       %s\n", diffLine(d))
		}
	}
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
		if n == 0 {
			fmt.Fprintln(c.stderr, "the session has no events: the file holds its verdict only (one line), or nothing if the control plane does not know the session")
		}
	}
	return nil
}
