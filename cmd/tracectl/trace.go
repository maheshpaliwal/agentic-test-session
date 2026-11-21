package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func (c *cli) trace(ctx context.Context, args []string) error {
	fs := c.flags("trace")
	errsOnly := fs.Bool("errors", false, "only branches that contain a failed event")
	depth := fs.Int("depth", 0, "print at most N levels (0 = all)")
	asJSON := fs.Bool("json", false, "print the raw call tree as JSON")
	s, err := c.session(ctx, fs, args)
	if err != nil {
		return err
	}
	tr, err := s.Trace(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		printJSON(c.stdout, tr)
		return nil
	}
	out := bufio.NewWriter(c.stdout)
	defer out.Flush()
	p := tracePrinter{w: out, errsOnly: *errsOnly, maxDepth: *depth, failed: map[*client.TraceNode]bool{}}
	for _, n := range tr.Roots {
		p.print(n, 0, false)
	}
	fmt.Fprintf(out, "%d span(s)", p.shown)
	if p.hidden > 0 {
		fmt.Fprintf(out, ", %d hidden", p.hidden)
	}
	fmt.Fprintln(out)
	return nil
}

// tracePrinter renders a call tree as indented lines. Asynchronous work a
// span triggered is marked "~>".
type tracePrinter struct {
	w             io.Writer
	errsOnly      bool
	maxDepth      int
	shown, hidden int
	failed        map[*client.TraceNode]bool // memo of hasError
}

// hasError reports whether n or anything below it failed.
func (p *tracePrinter) hasError(n *client.TraceNode) bool {
	if v, ok := p.failed[n]; ok {
		return v
	}
	p.failed[n] = false // guards against a cycle in malformed input
	v := n.Event.Err != ""
	for _, ch := range append(n.Children[:len(n.Children):len(n.Children)], n.Linked...) {
		v = p.hasError(ch) || v
	}
	p.failed[n] = v
	return v
}

// size counts n and everything below it.
func size(n *client.TraceNode, seen map[*client.TraceNode]bool) int {
	if seen[n] {
		return 0
	}
	seen[n] = true
	total := 1
	for _, ch := range n.Children {
		total += size(ch, seen)
	}
	for _, ch := range n.Linked {
		total += size(ch, seen)
	}
	return total
}

func (p *tracePrinter) print(n *client.TraceNode, level int, linked bool) {
	if p.errsOnly && !p.hasError(n) {
		p.hidden += size(n, map[*client.TraceNode]bool{})
		return
	}
	if p.maxDepth > 0 && level >= p.maxDepth {
		p.hidden += size(n, map[*client.TraceNode]bool{})
		return
	}
	p.shown++
	e := &n.Event
	line := strings.Repeat("  ", level)
	if linked {
		line += "~> "
	}
	line += fmt.Sprintf("#%d %s %s %s", e.Seq, e.Service, e.Kind, clip(e.Name, 70))
	if d := e.EndNs - e.StartNs; d > 0 {
		line += "  " + time.Duration(d).Round(10*time.Microsecond).String()
	}
	if m := len(e.Mutations); m > 0 {
		line += fmt.Sprintf("  [%d mutation(s)]", m)
	}
	if e.Err != "" {
		line += "  ERR " + clip(e.Err, 100)
	}
	fmt.Fprintln(p.w, line)
	for _, ch := range n.Children {
		p.print(ch, level+1, false)
	}
	for _, ch := range n.Linked {
		p.print(ch, level+1, true)
	}
}

func (c *cli) show(ctx context.Context, args []string) error {
	fs := c.flags("show")
	asJSON := fs.Bool("json", false, "print the raw event as JSON")
	pos, err := parseArgs(fs, args, 2, "<id|name> <seq>")
	if err != nil {
		return err
	}
	seq, err := strconv.ParseUint(strings.TrimPrefix(pos[1], "#"), 10, 64)
	if err != nil || seq == 0 {
		return fmt.Errorf("show: %q is not an event sequence number", pos[1])
	}
	s, err := resolve(ctx, c.opts, pos[0])
	if err != nil {
		return err
	}
	page, err := s.EventsPage(ctx, seq, 1)
	if err != nil {
		return err
	}
	if len(page.Events) == 0 || page.Events[0].Seq != seq {
		return fmt.Errorf("show: session %s has no event #%d", s.ID, seq)
	}
	e := &page.Events[0]
	if *asJSON {
		printJSON(c.stdout, e)
		return nil
	}
	printEvent(c.stdout, e)
	return nil
}

func printEvent(w io.Writer, e *core.Event) {
	fmt.Fprintf(w, "#%d  %s  %s  %s\n", e.Seq, e.Service, e.Kind, e.Name)
	fmt.Fprintf(w, "span      %s (parent %s, trace %s)\n", e.SpanID, orDash(e.ParentID), e.TraceID)
	if d := e.EndNs - e.StartNs; d > 0 {
		fmt.Fprintf(w, "duration  %s\n", time.Duration(d).Round(10*time.Microsecond))
	}
	if e.TxID != "" {
		fmt.Fprintf(w, "tx        %s\n", e.TxID)
	}
	if e.Attempt > 0 {
		fmt.Fprintf(w, "attempt   %d\n", e.Attempt)
	}
	if e.Err != "" {
		fmt.Fprintf(w, "error     %s\n", e.Err)
	}
	if len(e.Flags) > 0 {
		fmt.Fprintf(w, "flags     %s\n", strings.Join(e.Flags, ", "))
	}
	for _, k := range mapx.SortedKeys(e.Attrs) {
		fmt.Fprintf(w, "attr      %s=%s\n", k, e.Attrs[k])
	}
	printPayload(w, "request", e.Req)
	printPayload(w, "response", e.Resp)
	for i := range e.Mutations {
		m := &e.Mutations[i]
		outcome := ""
		if m.Effective != nil {
			outcome = "  effective"
			if !*m.Effective {
				outcome = "  ROLLED BACK"
			}
		}
		fmt.Fprintf(w, "mutation  %s %s/%s %s%s\n", m.Op, m.Store, m.Entity, pkText(m.PK), outcome)
		if m.AfterUnavailable() {
			// No diff, even one an older SDK sent: it lists every field as removed.
			fmt.Fprintf(w, "            %s\n", afterUnavailable)
			printPayload(w, "  before", m.Before)
			continue
		}
		for _, d := range m.Diff {
			fmt.Fprintf(w, "            %s: %s -> %s\n", d.Field, compact(d.Old), compact(d.New))
		}
		if len(m.Diff) == 0 {
			printPayload(w, "  before", m.Before)
			printPayload(w, "  after", m.After)
		}
	}
}

// afterUnavailable is printed in place of the diff of an update whose row
// could not be read back (core.Mutation.AfterUnavailable).
const afterUnavailable = "after image unavailable: the new values were not captured"

// unknownKey stands for the key of a mutation recorded without one.
const unknownKey = "(key unknown)"

// pkText renders a mutation's primary key on one line.
func pkText(pk map[string]any) string {
	if len(pk) == 0 {
		return unknownKey
	}
	return compact(pk)
}

// printPayload prints a payload indented under label; an absent one prints nothing.
func printPayload(w io.Writer, label string, p core.PayloadRef) {
	if p.IsZero() {
		return
	}
	var notes []string
	if p.Truncated {
		notes = append(notes, "truncated")
	}
	if p.Redacted {
		notes = append(notes, "redacted")
	}
	head := label
	if len(notes) > 0 {
		head += " (" + strings.Join(notes, ", ") + ")"
	}
	if len(p.Inline) == 0 {
		fmt.Fprintf(w, "%s: <stored as blob %s, %d bytes>\n", head, p.BlobHash, p.Size)
		return
	}
	body := string(p.Inline)
	var v any
	if json.Unmarshal(p.Inline, &v) == nil {
		if b, err := json.MarshalIndent(v, "  ", "  "); err == nil {
			body = string(b)
		}
	}
	fmt.Fprintf(w, "%s:\n  %s\n", head, body)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
