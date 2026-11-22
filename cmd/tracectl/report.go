package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

const (
	reportArgs = "<id|name> [--format markdown|junit] [-o FILE] [--fail-on-error] [--fail-on-incomplete] [--fail-on-empty] [--fail-on-unconfirmed]"

	reportMaxRows     = 50   // rows of a Markdown table
	junitMaxCases     = 1000 // test cases made from trace roots
	junitMaxBodyLines = 200  // failed events listed in one <failure>
	reportNameMax     = 120  // runes kept of a recorded name
	reportErrMax      = 300  // runes kept of a recorded error
)

// report writes one session as a CI report: Markdown for a job summary, or
// JUnit XML for the test-report ingestion of a CI system.
func (c *cli) report(ctx context.Context, args []string) error {
	fs := c.flags("report")
	format := fs.String("format", "markdown", "markdown or junit")
	path := fs.String("o", "", "write to this file instead of stdout")
	var g gates
	g.register(fs)
	pos, err := c.parseArgs(fs, args, 1, reportArgs)
	if err != nil {
		return err
	}
	render, err := reportRenderer(*format)
	if err != nil {
		return fmt.Errorf("usage: tracectl report %s: %w", reportArgs, err)
	}
	s, err := c.resolve(ctx, pos[0])
	if err != nil {
		return err
	}
	rep, err := loadReport(ctx, s)
	if err != nil {
		return err
	}
	out, err := render(rep)
	if err != nil {
		return err
	}
	if *path != "" {
		if err := c.writeReportFile(*path, *format, out); err != nil {
			return err
		}
	} else if _, err := c.stdout.Write(out); err != nil {
		return err
	}
	return g.check(c.stderr, rep.info, rep.sum, len(rep.failures))
}

// reportRenderer returns the renderer of a --format value.
func reportRenderer(format string) (func(*sessionReport) ([]byte, error), error) {
	switch format {
	case "markdown":
		return renderMarkdown, nil
	case "junit":
		return renderJUnit, nil
	}
	return nil, fmt.Errorf("unknown --format %q", format)
}

// loadReport reads what a report is made of.
func loadReport(ctx context.Context, s *client.Session) (*sessionReport, error) {
	sum, err := s.Summary(ctx)
	if err != nil {
		return nil, err
	}
	tr, err := s.Trace(ctx)
	if err != nil {
		return nil, err
	}
	muts, err := s.Mutations(ctx, client.MutationFilter{})
	if err != nil {
		return nil, err
	}
	return buildReport(s.Info(), sum, tr, muts), nil
}

// writeReportFile writes a rendered report to path, replacing the file.
func (c *cli) writeReportFile(path, format string, out []byte) error {
	// 0644, unlike export's 0600: the report holds names and error text,
	// not request or row payloads, and CI steps that publish it often run
	// as another user.
	if err := os.WriteFile(path, out, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(c.stderr, "wrote %s report to %s\n", format, path)
	return nil
}

// sessionReport is what both formats render.
type sessionReport struct {
	info       client.Info
	sum        client.Summary
	roots      []reportRoot    // the trace roots, in trace order
	failures   []reportFailure // every failed event, in Seq order
	incomplete bool
	reasons    []string // why it is incomplete; plain text, not yet escaped
	mutations  []mutationRow
}

// reportRoot is one trace root with the failed events of its tree, async
// work it triggered included.
type reportRoot struct {
	event  *core.Event
	failed []reportFailure
}

// reportFailure is a failed event; root indexes sessionReport.roots and is
// -1 when the trace does not hold the event.
type reportFailure struct {
	client.Failure
	root int
}

type mutationRow struct {
	store, entity, op                string
	effective, rolledBack, undecided int
}

// buildReport attributes every failed event to the trace root it came from.
// The collector hangs a worker run under the span that published its job
// (TraceNode.Linked), so walking a root's children and linked spans reaches
// the async work the request caused.
// deliveryReasonLabel says what one of the control plane's incomplete_reasons
// is about. Most name an instance whose events did not all reach the
// collector. Two do not say that anything is missing: an instance whose
// events the collector has and that did not report how many it recorded, and
// reports beyond the ones a session keeps.
func deliveryReasonLabel(reason string) string {
	if strings.HasPrefix(reason, "the collector has ") || strings.HasPrefix(reason, "more than ") {
		return "Delivery is not accounted for: "
	}
	return "Events did not reach the collector: "
}

func buildReport(info client.Info, sum client.Summary, tr client.Trace, muts []client.Mutation) *sessionReport {
	rep := &sessionReport{info: info, sum: sum, incomplete: sum.Incomplete || info.Incomplete}
	rootOf := map[uint64]int{}
	failed := map[uint64]bool{}
	for _, n := range tr.Roots {
		if n == nil {
			continue
		}
		idx := len(rep.roots)
		rep.roots = append(rep.roots, reportRoot{event: &n.Event})
		walkTrace(n, func(e *core.Event) {
			rootOf[e.Seq] = idx
			if e.Err != "" && !failed[e.Seq] {
				failed[e.Seq] = true
				rep.failures = append(rep.failures, reportFailure{root: idx,
					Failure: client.Failure{Seq: e.Seq, SpanID: e.SpanID, Service: e.Service, Kind: e.Kind, Name: e.Name, Err: e.Err}})
			}
		})
	}
	// The summary is read separately from the trace; keep a failure only it has.
	for _, f := range sum.Failures {
		if failed[f.Seq] {
			continue
		}
		failed[f.Seq] = true
		idx, ok := rootOf[f.Seq]
		if !ok {
			idx = -1
		}
		rep.failures = append(rep.failures, reportFailure{Failure: f, root: idx})
	}
	slices.SortStableFunc(rep.failures, func(a, b reportFailure) int { return cmp.Compare(a.Seq, b.Seq) })
	for _, f := range rep.failures {
		if f.root >= 0 {
			rep.roots[f.root].failed = append(rep.roots[f.root].failed, f)
		}
	}
	if rep.incomplete {
		rep.reasons = incompleteReasons(info, sum)
	}
	rep.mutations = mutationRows(muts)
	return rep
}

// walkTrace visits n and everything below it, linked async spans included.
func walkTrace(n *client.TraceNode, visit func(*core.Event)) {
	if n == nil {
		return
	}
	visit(&n.Event)
	for _, ch := range n.Children {
		walkTrace(ch, visit)
	}
	for _, ch := range n.Linked {
		walkTrace(ch, visit)
	}
}

// incompleteReasons lists what is known about why a session is incomplete.
func incompleteReasons(info client.Info, sum client.Summary) []string {
	var out []string
	if n := max(sum.DroppedEvents, info.DroppedEvents); n > 0 {
		out = append(out, fmt.Sprintf("%d event(s) were dropped by the SDKs before they reached the collector.", n))
	}
	if sum.Quarantined > 0 {
		out = append(out, fmt.Sprintf("%d event(s) were quarantined by the collector because it could not process them.", sum.Quarantined))
	}
	if sum.PendingAsync > 0 {
		state := "has not finished yet"
		if sum.Sealed {
			state = "never finished (orphaned)"
		}
		out = append(out, fmt.Sprintf("Unfinished async work: %d published job(s) that a worker %s.", sum.PendingAsync, state))
	}
	for _, r := range info.IncompleteReasons {
		out = append(out, deliveryReasonLabel(r)+clip(r, reportErrMax))
	}
	if info.SealError != "" {
		r := "Seal error: the collector could not seal the session: " + clip(info.SealError, reportErrMax)
		if info.SealPending {
			r += " (the seal is still being retried)"
		}
		out = append(out, r)
	}
	if info.DrainTimedOut {
		out = append(out, fmt.Sprintf("Drain timed out: asynchronous work was still outstanding after the %ds drain limit.", info.MaxDrainSeconds))
	}
	if len(out) == 0 {
		out = append(out, "The session is marked incomplete; no further detail was recorded.")
	}
	return out
}

// mutationRows counts mutations per store, entity and operation, sorted.
func mutationRows(muts []client.Mutation) []mutationRow {
	type key struct{ store, entity, op string }
	counts := map[key]*mutationRow{}
	for i := range muts {
		m := &muts[i]
		k := key{m.Store, m.Entity, strings.ToLower(m.Op)}
		row := counts[k]
		if row == nil {
			row = &mutationRow{store: k.store, entity: k.entity, op: k.op}
			counts[k] = row
		}
		switch {
		case m.Effective == nil: // transaction still open when the events were read
			row.undecided++
		case *m.Effective:
			row.effective++
		default:
			row.rolledBack++
		}
	}
	rows := make([]mutationRow, 0, len(counts))
	for _, r := range counts {
		rows = append(rows, *r)
	}
	slices.SortFunc(rows, func(a, b mutationRow) int {
		return cmp.Or(cmp.Compare(a.store, b.store), cmp.Compare(a.entity, b.entity), cmp.Compare(a.op, b.op))
	})
	return rows
}

// rootLabel names a trace root: its event name, or its kind when unnamed.
func rootLabel(e *core.Event) string {
	return cmp.Or(strings.TrimSpace(e.Name), string(e.Kind))
}

func plural(n int, one string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + one + "s"
}

// mdEscape makes recorded text safe to place in Markdown, table cells
// included: it becomes one line of at most limit runes in which nothing is
// markup. Whitespace runs (newlines too) collapse to a space, control and
// bidi-override characters are dropped, HTML-significant characters become
// entities and Markdown-significant ones are backslash-escaped. The cap is
// applied before escaping so that it cannot cut an escape sequence in half.
// It does not stop a site from linking a bare URL or an @mention.
func mdEscape(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r):
			return ' '
		case unicode.IsControl(r), r >= 0x202A && r <= 0x202E, r >= 0x2066 && r <= 0x2069:
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, "�"))
	s = clip(s, limit)
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '&':
			b.WriteString("&amp;")
		case '\\', '`', '|', '*', '_', '[', ']', '~', '#', '$':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// mdCode renders s as a code span, or as escaped text when a code span
// could not hold it. Not for table cells.
func mdCode(s string) string {
	if s == "" || strings.ContainsFunc(s, func(r rune) bool { return r == '`' || unicode.IsControl(r) }) {
		return mdEscape(s, reportNameMax)
	}
	return "`" + s + "`"
}

// mdTable writes a table; cells must already be escaped.
func mdTable(b *bytes.Buffer, head []string, rows [][]string) {
	line := func(cells []string) { b.WriteString("| " + strings.Join(cells, " | ") + " |\n") }
	line(head)
	line(slices.Repeat([]string{"---"}, len(head)))
	for _, r := range rows {
		line(r)
	}
}

// mdCell is mdEscape for a table cell, with "-" for nothing.
func mdCell(s string, limit int) string {
	return cmp.Or(mdEscape(s, limit), "-")
}

func renderMarkdown(rep *sessionReport) ([]byte, error) {
	var b bytes.Buffer
	info, sum := rep.info, rep.sum
	fmt.Fprintf(&b, "## tsession report: %s (%s)\n\n", mdCell(displayName(info), reportNameMax), mdCode(info.ID))

	var verdict string
	switch n := len(rep.failures); {
	case n > 0 && rep.incomplete:
		verdict = "**Failed**: " + plural(n, "failed event") + "; the recording is also incomplete."
	case n > 0:
		verdict = "**Failed**: " + plural(n, "failed event") + "."
	case rep.incomplete:
		verdict = "**Incomplete**: no failed events were recorded, but the recording is missing data."
	case sum.EventCount == 0:
		// Not "Passed": nothing was checked. This text is what a CI job summary shows.
		verdict = "**Empty**: the session recorded no events, so nothing was checked (see `tracectl doctor`)."
	default:
		verdict = "**Passed**: no failed events."
	}
	if !info.Done() {
		verdict += " The session is still " + mdEscape(info.State, 40) + ", so this report can change."
	}
	if len(info.DeliveryUnconfirmed) > 0 {
		// A warning, not a failure: but "Passed" alone would hide it.
		verdict += " Whether every event arrived could not be confirmed (see below)."
	}
	b.WriteString(verdict + "\n\n")
	mdTable(&b, []string{"State", "Events", "Duration", "Services"}, [][]string{{
		mdCell(info.State, 40), strconv.Itoa(sum.EventCount),
		time.Duration(sum.DurationNs).Round(time.Millisecond).String(),
		mdCell(strings.Join(mapx.SortedKeys(sum.ByService), ", "), reportErrMax),
	}})

	if len(rep.failures) > 0 {
		b.WriteString("\n### Failures\n\n")
		shown := rep.failures[:min(len(rep.failures), reportMaxRows)]
		rows := make([][]string, len(shown))
		for i, f := range shown {
			root := "-"
			if f.root >= 0 {
				e := rep.roots[f.root].event
				root = fmt.Sprintf("%s (seq %d)", mdCell(rootLabel(e), reportNameMax), e.Seq)
			}
			rows[i] = []string{strconv.FormatUint(f.Seq, 10), mdCell(f.Service, 60), mdCell(string(f.Kind), 40),
				mdCell(f.Name, reportNameMax), mdCell(f.Err, reportErrMax), root}
		}
		mdTable(&b, []string{"#seq", "Service", "Kind", "Name", "Error", "Root request"}, rows)
		if n := len(rep.failures) - len(shown); n > 0 {
			fmt.Fprintf(&b, "\n%d more failed event(s) omitted.\n", n)
		}
	}

	if rep.incomplete {
		b.WriteString("\n### Why it is incomplete\n\n")
		for _, r := range rep.reasons {
			b.WriteString("- " + mdEscape(r, 2*reportErrMax) + "\n")
		}
	}

	if len(info.DeliveryUnconfirmed) > 0 {
		b.WriteString("\n### Delivery not confirmed\n\n")
		b.WriteString("The control plane could not tell whether all events of these service instances reached the collector. The session is not marked incomplete for it.\n\n")
		for _, r := range info.DeliveryUnconfirmed {
			b.WriteString("- " + mdEscape(r, 2*reportErrMax) + "\n")
		}
	}

	b.WriteString("\n### Mutations\n\n")
	if len(rep.mutations) == 0 {
		b.WriteString("No mutations were recorded.\n")
	} else {
		shown := rep.mutations[:min(len(rep.mutations), reportMaxRows)]
		rows := make([][]string, len(shown))
		for i, m := range shown {
			rows[i] = []string{mdCell(m.store, 60), mdCell(m.entity, reportNameMax), mdCell(m.op, 40),
				strconv.Itoa(m.effective), strconv.Itoa(m.rolledBack), strconv.Itoa(m.undecided)}
		}
		mdTable(&b, []string{"Store", "Entity", "Op", "Took effect", "Rolled back", "Outcome unknown"}, rows)
		if n := len(rep.mutations) - len(shown); n > 0 {
			fmt.Fprintf(&b, "\n%d more row(s) omitted.\n", n)
		}
	}

	fmt.Fprintf(&b, "\nDig further: %s, %s\n", mdCode("tracectl trace "+info.ID+" --errors"), mdCode("tracectl show "+info.ID+" <seq>"))
	return b.Bytes(), nil
}

// The JUnit document, in the shape CI systems ingest. encoding/xml does the
// escaping and replaces characters XML cannot hold.
type junitSuites struct {
	XMLName  xml.Name     `xml:"testsuites"`
	Name     string       `xml:"name,attr"`
	Tests    int          `xml:"tests,attr"`
	Failures int          `xml:"failures,attr"`
	Time     string       `xml:"time,attr"`
	Suites   []junitSuite `xml:"testsuite"`
}

type junitSuite struct {
	Name       string          `xml:"name,attr"`
	Tests      int             `xml:"tests,attr"`
	Failures   int             `xml:"failures,attr"`
	Errors     int             `xml:"errors,attr"`
	Skipped    int             `xml:"skipped,attr"`
	Time       string          `xml:"time,attr"`
	Properties []junitProperty `xml:"properties>property"`
	Cases      []junitCase     `xml:"testcase"`
	SystemOut  string          `xml:"system-out,omitempty"`
}

type junitProperty struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type junitCase struct {
	Name      string        `xml:"name,attr"`
	Classname string        `xml:"classname,attr"`
	Time      string        `xml:"time,attr"`
	Failure   *junitFailure `xml:"failure"`
}

type junitFailure struct {
	Message string `xml:"message,attr"`
	Type    string `xml:"type,attr"`
	Body    string `xml:",chardata"`
}

// junitClass is the classname of the cases that are about the session
// itself, not one request.
const junitClass = "tsession"

func junitSeconds(ns int64) string {
	return strconv.FormatFloat(time.Duration(max(ns, 0)).Seconds(), 'f', 3, 64)
}

// junitEventFailure is the <failure> for a set of failed events: the first
// error as the message, one line per event as the body.
func junitEventFailure(failed []reportFailure) *junitFailure {
	var body strings.Builder
	for i, f := range failed {
		if i == junitMaxBodyLines {
			fmt.Fprintf(&body, "... %d more failed event(s) omitted\n", len(failed)-i)
			break
		}
		fmt.Fprintf(&body, "#%d %s %s %s: %s\n", f.Seq, clip(f.Service, 60), f.Kind, clip(f.Name, reportNameMax), clip(f.Err, 1000))
	}
	return &junitFailure{Message: clip(failed[0].Err, reportErrMax), Type: "failed", Body: body.String()}
}

func renderJUnit(rep *sessionReport) ([]byte, error) {
	info := rep.info
	suite := junitSuite{Name: cmp.Or(info.Name, info.ID), Time: junitSeconds(rep.sum.DurationNs), Properties: []junitProperty{
		{Name: "session_id", Value: info.ID}, {Name: "state", Value: info.State},
		{Name: "incomplete", Value: strconv.FormatBool(rep.incomplete)},
	}}
	// Why the session is incomplete, and what could not be confirmed, as
	// properties: a CI system shows them also when no test case failed.
	for _, r := range rep.reasons {
		suite.Properties = append(suite.Properties, junitProperty{Name: "incomplete_reason", Value: clip(r, 2*reportErrMax)})
	}
	for _, r := range info.DeliveryUnconfirmed {
		suite.Properties = append(suite.Properties, junitProperty{Name: "delivery_unconfirmed", Value: clip(r, 2*reportErrMax)})
	}
	roots := rep.roots
	if len(roots) > junitMaxCases {
		// Keep the failed ones: a stable sort moves them to the front.
		roots = slices.Clone(roots)
		slices.SortStableFunc(roots, func(a, b reportRoot) int {
			return cmp.Compare(min(len(b.failed), 1), min(len(a.failed), 1))
		})
		suite.SystemOut = fmt.Sprintf("%d of %d trace roots are reported as test cases (the cap), failed ones first; %d omitted.",
			junitMaxCases, len(roots), len(roots)-junitMaxCases)
		roots = roots[:junitMaxCases]
	}
	for _, r := range roots {
		e := r.event
		tc := junitCase{Name: clip(rootLabel(e), 2*reportNameMax), Classname: e.Service, Time: junitSeconds(e.EndNs - e.StartNs)}
		if len(r.failed) > 0 {
			tc.Failure = junitEventFailure(r.failed)
		}
		suite.Cases = append(suite.Cases, tc)
	}
	// Failed events the trace does not hold must not vanish from the report.
	loose := slicex.Filter(rep.failures, func(f reportFailure) bool { return f.root < 0 })
	if len(loose) > 0 {
		suite.Cases = append(suite.Cases, junitCase{Name: "failed events outside the trace", Classname: junitClass,
			Time: junitSeconds(0), Failure: junitEventFailure(loose)})
	}
	if rep.incomplete {
		suite.Cases = append(suite.Cases, junitCase{Name: "session complete", Classname: junitClass, Time: junitSeconds(0),
			Failure: &junitFailure{Message: "the session is incomplete: " + rep.reasons[0], Type: "incomplete",
				Body: strings.Join(rep.reasons, "\n") + "\n"}})
	}
	suite.Tests = len(suite.Cases)
	suite.Failures = slicex.Count(suite.Cases, func(tc junitCase) bool { return tc.Failure != nil })
	doc := junitSuites{Name: "tsession", Tests: suite.Tests, Failures: suite.Failures, Time: suite.Time, Suites: []junitSuite{suite}}
	out, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), append(out, '\n')...), nil
}
