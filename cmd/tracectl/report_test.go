package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// reportEnv serves one canned session, control plane and collector on the
// same address, so a test controls the session state as well as its events.
type reportEnv struct {
	url   string
	info  client.Info
	sum   client.Summary
	evs   []core.Event
	roots []*client.TraceNode
}

const reportID = "ts_r1"

// newReportEnv serves evs as a SEALED session. The summary is derived from
// the events; the trace is whatever the test sets in roots.
func newReportEnv(t *testing.T, evs ...core.Event) *reportEnv {
	t.Helper()
	env := &reportEnv{evs: evs, info: client.Info{ID: reportID, Name: "checkout", State: "SEALED"}}
	env.sum = client.Summary{SessionID: reportID, EventCount: len(evs), Sealed: true, ByService: map[string]int{}}
	for _, e := range evs {
		env.sum.ByService[e.Service]++
		if e.Err != "" {
			env.sum.Failures = append(env.sum.Failures, client.Failure{Seq: e.Seq, Service: e.Service, Kind: e.Kind, Name: e.Name, Err: e.Err})
		}
		env.sum.DurationNs = max(env.sum.DurationNs, e.EndNs-evs[0].StartNs)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sessions/{id}", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(env.info) })
	mux.HandleFunc("GET /v1/sessions/{id}/summary", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(env.sum) })
	mux.HandleFunc("GET /v1/sessions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.EventPage{Events: env.evs, NextSeq: uint64(len(env.evs)) + 1, Sealed: true})
	})
	mux.HandleFunc("GET /v1/sessions/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(client.Trace{SessionID: reportID, Roots: env.roots})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	env.url = srv.URL
	return env
}

func (env *reportEnv) tc(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"--cp", env.url, "--collector", env.url}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

// node builds a trace node for the event with this seq.
func (env *reportEnv) node(seq int, children, linked []*client.TraceNode) *client.TraceNode {
	return &client.TraceNode{Event: env.evs[seq-1], Children: children, Linked: linked}
}

func nodes(n ...*client.TraceNode) []*client.TraceNode { return n }

// parseJUnit parses a report back; a report that is not well-formed fails the test.
func parseJUnit(t *testing.T, out string) junitSuite {
	t.Helper()
	if !strings.HasPrefix(out, xml.Header) {
		panic(fmt.Sprintf("no XML header:\n%s", out))
	}
	var doc junitSuites
	if err := xml.Unmarshal([]byte(out), &doc); err != nil {
		panic(fmt.Sprintf("JUnit output is not well-formed: %v\n%s", err, out))
	}
	if len(doc.Suites) != 1 {
		panic(fmt.Sprintf("want one test suite, got %d", len(doc.Suites)))
	}
	s := doc.Suites[0]
	if doc.Tests != s.Tests || doc.Failures != s.Failures || s.Tests != len(s.Cases) {
		panic(fmt.Sprintf("counts disagree: suites %d/%d, suite %d/%d, %d cases", doc.Tests, doc.Failures, s.Tests, s.Failures, len(s.Cases)))
	}
	return s
}

// mdCells splits a table row on its unescaped pipes.
func mdCells(line string) []string {
	var cells []string
	var cur strings.Builder
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\\' && i+1 < len(line):
			cur.WriteString(line[i : i+2])
			i++
		case line[i] == '|':
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(line[i])
		}
	}
	return cells[1:] // the text before the leading pipe
}

// mdRow returns the cells of the table row whose first cell is first.
func mdRow(t *testing.T, out, first string) []string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "| "+first+" |") {
			return mdCells(line)
		}
	}
	panic(fmt.Sprintf("no table row starting with %q in:\n%s", first, out))

}

// failingEnv is two requests and a health check: POST /orders publishes a
// job whose worker fails and rolls back, GET /orders/42 fails on a query.
func failingEnv(t *testing.T) *reportEnv {
	yes, no := true, false
	env := newReportEnv(t,
		core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1_000_000_000, EndNs: 1_050_000_000},
		core.Event{Seq: 2, Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders", StartNs: 1_010_000_000, EndNs: 1_012_000_000,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", Effective: &yes}}},
		core.Event{Seq: 3, Service: "api", Kind: core.KindQueuePub, Name: "ship", StartNs: 1_020_000_000, EndNs: 1_021_000_000},
		core.Event{Seq: 4, Service: "worker", Kind: core.KindDBWrite, Name: "UPDATE orders", StartNs: 1_100_000_000, EndNs: 1_101_000_000,
			Mutations: []core.Mutation{
				{Store: "postgres", Entity: "orders", Op: "UPDATE", Effective: &no},
				{Store: "postgres", Entity: "orders", Op: "update", Effective: &no},
				{Store: "redis", Entity: "cart:*", Op: "delete"},
			}},
		core.Event{Seq: 5, Service: "worker", Kind: core.KindWorkerEnd, Name: "ship", StartNs: 1_090_000_000, EndNs: 1_200_000_000, Err: "carrier\n  timeout"},
		core.Event{Seq: 6, Service: "api", Kind: core.KindDBRead, Name: "SELECT * FROM orders", StartNs: 1_300_000_000, EndNs: 1_301_000_000, Err: "connection reset"},
		core.Event{Seq: 7, Service: "api", Kind: core.KindAPIIn, Name: "GET /orders/42", StartNs: 1_290_000_000, EndNs: 1_310_000_000, Err: "500 Internal Server Error"},
		core.Event{Seq: 8, Service: "api", Kind: core.KindAPIIn, Name: "GET /healthz", StartNs: 1_400_000_000, EndNs: 1_401_500_000},
	)
	worker := env.node(5, nodes(env.node(4, nil, nil)), nil)
	env.roots = nodes(
		env.node(1, nodes(env.node(2, nil, nil), env.node(3, nil, nodes(worker))), nil),
		env.node(7, nodes(env.node(6, nil, nil)), nil),
		env.node(8, nil, nil),
	)
	return env
}

func TestReportClean(t *testing.T) {
	yes := true
	env := newReportEnv(t,
		core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1_000_000_000, EndNs: 1_050_000_000},
		core.Event{Seq: 2, Service: "db", Kind: core.KindDBWrite, Name: "INSERT INTO orders", StartNs: 1_010_000_000, EndNs: 1_012_000_000,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", Effective: &yes}}},
	)
	env.roots = nodes(env.node(1, nodes(env.node(2, nil, nil)), nil))

	code, out, e := env.tc("report", reportID, "--fail-on-error", "--fail-on-incomplete")
	if code != 0 || e != "" {
		panic(fmt.Sprintf("clean markdown report: %d %s", code, e))
	}
	for _, want := range []string{
		"## tsession report: checkout (`ts_r1`)\n",
		"**Passed**: no failed events.\n",
		"| State | Events | Duration | Services |\n| --- | --- | --- | --- |\n| SEALED | 2 | 50ms | api, db |\n",
		"| postgres | orders | insert | 1 | 0 | 0 |\n",
		"Dig further: `tracectl trace ts_r1 --errors`, `tracectl show ts_r1 <seq>`\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	for _, not := range []string{"### Failures", "### Why it is incomplete"} {
		if strings.Contains(out, not) {
			t.Errorf("a clean report has no %q:\n%s", not, out)
		}
	}

	code, out, e = env.tc("report", "--format", "junit", reportID, "--fail-on-error", "--fail-on-incomplete")
	if code != 0 || e != "" {
		panic(fmt.Sprintf("clean junit report: %d %s", code, e))
	}
	s := parseJUnit(t, out)
	if s.Name != "checkout" || s.Tests != 1 || s.Failures != 0 || s.Time != "0.050" || s.SystemOut != "" {
		panic(fmt.Sprintf("suite: %+v", s))
	}
	if tc := s.Cases[0]; tc.Name != "POST /orders" || tc.Classname != "api" || tc.Time != "0.050" || tc.Failure != nil {
		panic(fmt.Sprintf("case: %+v", tc))
	}
	if len(s.Properties) == 0 || s.Properties[0] != (junitProperty{Name: "session_id", Value: reportID}) {
		panic(fmt.Sprintf("properties: %+v", s.Properties))
	}
}

func TestReportFailuresMarkdown(t *testing.T) {
	env := failingEnv(t)
	code, out, e := env.tc("report", reportID)
	if code != 0 || e != "" {
		panic(fmt.Sprintf("a report without gates lists failures and exits 0: %d %s", code, e))
	}
	if !strings.Contains(out, "**Failed**: 3 failed events.\n") {
		t.Errorf("verdict:\n%s", out)
	}
	if !strings.Contains(out, "| #seq | Service | Kind | Name | Error | Root request |\n| --- | --- | --- | --- | --- | --- |\n| 5 |") {
		t.Errorf("failures table:\n%s", out)
	}
	for first, want := range map[string][]string{
		// the worker ran asynchronously: its failure belongs to the request that published the job
		"5": {"5", "worker", "worker.end", "ship", "carrier timeout", "POST /orders (seq 1)"},
		"6": {"6", "api", "db.read", "SELECT \\* FROM orders", "connection reset", "GET /orders/42 (seq 7)"},
		"7": {"7", "api", "api.in", "GET /orders/42", "500 Internal Server Error", "GET /orders/42 (seq 7)"},
	} {
		if got := mdRow(t, out, first); strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			t.Errorf("row %s:\n got %q\nwant %q", first, got, want)
		}
	}
	// Mutations are counted per store, entity and op (case-insensitively), by outcome.
	for _, want := range []string{
		"| Store | Entity | Op | Took effect | Rolled back | Outcome unknown |\n",
		"| postgres | orders | insert | 1 | 0 | 0 |\n| postgres | orders | update | 0 | 2 | 0 |\n| redis | cart:\\* | delete | 0 | 0 | 1 |\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}

	code, out, e = env.tc("report", reportID, "--fail-on-error", "--fail-on-incomplete")
	if code != exitCheckFailed || e != "tracectl: check failed: 3 failed event(s)\n" || !strings.Contains(out, "### Failures") {
		panic(fmt.Sprintf("--fail-on-error: %d %q\n%s", code, e, out))
	}
	if code, _, e := env.tc("report", reportID, "--fail-on-incomplete"); code != 0 {
		panic(fmt.Sprintf("a complete session passes --fail-on-incomplete: %d %s", code, e))
	}
}

func TestReportFailuresJUnit(t *testing.T) {
	env := failingEnv(t)
	code, out, e := env.tc("report", reportID, "--format", "junit", "--fail-on-error")
	if code != exitCheckFailed || !strings.Contains(e, "3 failed event(s)") {
		panic(fmt.Sprintf("junit --fail-on-error: %d %s", code, e))
	}
	s := parseJUnit(t, out)
	if s.Tests != 3 || s.Failures != 2 {
		panic(fmt.Sprintf("suite: %+v", s))
	}
	post, get, health := s.Cases[0], s.Cases[1], s.Cases[2]
	if post.Name != "POST /orders" || post.Classname != "api" || post.Failure == nil ||
		post.Failure.Message != "carrier timeout" || post.Failure.Body != "#5 worker worker.end ship: carrier timeout\n" {
		t.Errorf("the publishing request must fail with the worker's error: %+v %+v", post, post.Failure)
	}
	if get.Name != "GET /orders/42" || get.Time != "0.020" || get.Failure == nil || get.Failure.Message != "connection reset" ||
		get.Failure.Body != "#6 api db.read SELECT * FROM orders: connection reset\n#7 api api.in GET /orders/42: 500 Internal Server Error\n" {
		t.Errorf("failed request: %+v %+v", get, get.Failure)
	}
	if health.Name != "GET /healthz" || health.Time != "0.002" || health.Failure != nil {
		t.Errorf("passing request: %+v", health)
	}
}

// The canned session of newResultsEnv, behind a real control plane: resolved
// by name, with its failed worker linked to a write of POST /orders.
func TestReportByName(t *testing.T) {
	env := newResultsEnv(t)
	env.tc("start", "--name", "run-1")
	env.sum.Failures = []client.Failure{{Seq: 4, Kind: core.KindWorkerEnd, Service: "worker", Name: "ship", Err: "carrier timeout"}}

	code, out, e := env.tc("report", "run-1", "--fail-on-error")
	if code != exitCheckFailed || !strings.Contains(e, "1 failed event(s)") {
		panic(fmt.Sprintf("report by name: %d %s", code, e))
	}
	if !strings.Contains(out, "**Failed**: 1 failed event. The session is still ACTIVE, so this report can change.") {
		t.Errorf("verdict:\n%s", out)
	}
	if got := mdRow(t, out, "4"); got[5] != "POST /orders (seq 1)" {
		t.Errorf("root request: %q", got)
	}
	if !strings.Contains(out, "| ACTIVE | 4 | 50ms | api, worker |") || !strings.Contains(out, "| postgres | orders | insert | 1 | 0 | 0 |") {
		t.Errorf("tables:\n%s", out)
	}
}

func TestReportIncomplete(t *testing.T) {
	env := newReportEnv(t, core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1, EndNs: 2})
	env.roots = nodes(env.node(1, nil, nil))
	env.sum.Incomplete, env.sum.DroppedEvents, env.sum.Quarantined, env.sum.PendingAsync = true, 3, 2, 1
	env.info.Incomplete, env.info.DrainTimedOut, env.info.MaxDrainSeconds = true, true, 30
	env.info.SealError, env.info.SealPending = "seal: 503 <unavailable>", true

	code, out, e := env.tc("report", reportID, "--fail-on-error", "--fail-on-incomplete")
	if code != exitCheckFailed || e != "tracectl: check failed: session is incomplete\n" {
		panic(fmt.Sprintf("--fail-on-incomplete: %d %q", code, e))
	}
	bullets := []string{
		"- 3 event(s) were dropped by the SDKs before they reached the collector.",
		"- 2 event(s) were quarantined by the collector because it could not process them.",
		"- Unfinished async work: 1 published job(s) that a worker never finished (orphaned).",
		"- Seal error: the collector could not seal the session: seal: 503 &lt;unavailable&gt; (the seal is still being retried)",
		"- Drain timed out: asynchronous work was still outstanding after the 30s drain limit.",
	}
	if !strings.Contains(out, "**Incomplete**: no failed events were recorded, but the recording is missing data.\n") ||
		!strings.Contains(out, "### Why it is incomplete\n\n"+strings.Join(bullets, "\n")+"\n\n### Mutations\n\nNo mutations were recorded.\n") {
		t.Errorf("incomplete report:\n%s", out)
	}
	if code, _, e := env.tc("report", reportID, "--fail-on-error"); code != 0 {
		panic(fmt.Sprintf("no failed events, so --fail-on-error passes: %d %s", code, e))
	}

	_, out, _ = env.tc("report", reportID, "--format", "junit")
	s := parseJUnit(t, out)
	if s.Tests != 2 || s.Failures != 1 || s.Cases[0].Failure != nil {
		panic(fmt.Sprintf("suite: %+v", s))
	}
	last := s.Cases[1]
	if last.Name != "session complete" || last.Classname != "tsession" || last.Failure == nil || last.Failure.Type != "incomplete" ||
		!strings.HasPrefix(last.Failure.Message, "the session is incomplete: 3 event(s) were dropped") ||
		strings.Count(last.Failure.Body, "\n") != 5 || !strings.Contains(last.Failure.Body, "seal: 503 <unavailable>") {
		t.Errorf("session complete case: %+v %+v", last, last.Failure)
	}

	// Only the reasons that apply are listed; a bare flag still gets a line.
	env.sum = client.Summary{Incomplete: true, Sealed: true, Quarantined: 1}
	env.info = client.Info{ID: reportID, Name: "checkout", State: "SEALED"}
	if _, out, _ := env.tc("report", reportID); strings.Count(out, "\n- ") != 1 || !strings.Contains(out, "- 1 event(s) were quarantined") {
		t.Errorf("one reason:\n%s", out)
	}
	env.sum.Quarantined = 0
	if _, out, _ := env.tc("report", reportID); !strings.Contains(out, "- The session is marked incomplete; no further detail was recorded.\n") {
		t.Errorf("no known reason:\n%s", out)
	}
}

func TestMDEscape(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"POST /orders", "POST /orders"},
		{"a|b", `a\|b`},
		{"line one\r\n\n  line\ttwo ", "line one line two"},
		{"`rm -rf` ``x``", "\\`rm -rf\\` \\`\\`x\\`\\`"},
		{"<script>alert(1)</script>", "&lt;script&gt;alert(1)&lt;/script&gt;"},
		{"<!-- hidden", "&lt;!-- hidden"},
		{"&lt;b&gt; &#124;", "&amp;lt;b&amp;gt; &amp;\\#124;"},
		{"[click](http://evil.example) ![i](x)", `\[click\](http://evil.example) !\[i\](x)`},
		{"**bold** _it_ ~~gone~~ # h $x$", `\*\*bold\*\* \_it\_ \~\~gone\~\~ \# h \$x\$`},
		{`trailing \`, `trailing \\`},
		{`\| already escaped`, `\\\| already escaped`},
		{"nul\x00 esc\x1b[31m bidi\u202Eevil", `nul esc\[31m bidievil`},
		{"bad utf8 \xff\xfe", "bad utf8 \uFFFD"},
	} {
		if got := mdEscape(tc.in, 100); got != tc.want {
			t.Errorf("mdEscape(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}

	// The cap counts runes of the text, and never leaves half an escape.
	got := mdEscape(strings.Repeat("é|", 100), 10)
	if want := strings.Repeat(`é\|`, 4) + "é…"; got != want {
		t.Errorf("capped: %q, want %q", got, want)
	}
	if got := mdEscape(strings.Repeat("|", 500), 20); got != strings.Repeat(`\|`, 19)+"…" {
		t.Errorf("capped pipes: %q", got)
	}

	// Whatever goes in, nothing that Markdown or HTML acts on comes out bare.
	for _, in := range []string{
		"x | y |\n| --- |\n| z", "```\ncode\n```", "</td></tr></table><img src=x onerror=alert(1)>", `\`, `\\|`, "|\\", "a\\\n|b",
		strings.Repeat("`|<>*_[]\\\n", 80),
	} {
		out := mdEscape(in, 200)
		if !utf8.ValidString(out) {
			t.Errorf("mdEscape(%q) is not valid UTF-8", in)
		}
		for i := 0; i < len(out); i++ {
			switch c := out[i]; {
			case c == '\\':
				if i++; i == len(out) {
					t.Errorf("mdEscape(%q) ends in a bare backslash: %q", in, out)
				}
			case strings.ContainsRune("|`<>*_[]~#$\n\r", rune(c)):
				t.Errorf("mdEscape(%q) leaves %q bare at %d: %q", in, c, i, out)
			}
		}
	}

	// A code span cannot hold a backtick or a newline; such text is escaped instead.
	if got := mdCode("tracectl show ts_1 <seq>"); got != "`tracectl show ts_1 <seq>`" {
		t.Errorf("mdCode: %q", got)
	}
	if got := mdCode("ts_`x`\n# h"); got != "ts\\_\\`x\\` \\# h" {
		t.Errorf("mdCode with a backtick: %q", got)
	}
}

func TestReportHostileText(t *testing.T) {
	const hostile = "x | y |\n| --- |\n`code` <script>alert(1)</script> [l](http://evil.example) \x00\x1b]0;t\x07 ]]> &amp; \"q\" 'a' \\"
	long := strings.Repeat("A|", 2000)
	env := newReportEnv(t,
		core.Event{Seq: 1, Service: hostile, Kind: core.Kind(hostile), Name: hostile, StartNs: 1, EndNs: 2, Err: hostile,
			Mutations: []core.Mutation{{Store: hostile, Entity: hostile, Op: hostile}}},
		core.Event{Seq: 2, Service: "api", Kind: core.KindAPIIn, Name: long, StartNs: 3, EndNs: 4, Err: long},
	)
	env.roots = nodes(env.node(1, nil, nil), env.node(2, nil, nil))
	env.info.Name, env.info.State, env.info.SealError = hostile, hostile, hostile
	env.sum.Incomplete = true

	code, out, e := env.tc("report", reportID)
	if code != 0 {
		panic(fmt.Sprintf("report: %d %s", code, e))
	}
	// Every line is one the report wrote: a heading, a table row of the right
	// width, a bullet or a plain sentence. Hostile text opened none of them.
	width := 0
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "| "):
			cells := mdCells(line)
			if cells[0] == "State" || cells[0] == "#seq" || cells[0] == "Store" {
				width = len(cells)
			}
			if len(cells) != width || !strings.HasSuffix(line, " |") {
				t.Errorf("table row has %d cells, want %d: %s", len(cells), width, line)
			}
		case line == "", strings.HasPrefix(line, "## tsession report: "), strings.HasPrefix(line, "### "), strings.HasPrefix(line, "**"),
			strings.HasPrefix(line, "- Seal error: "), strings.HasPrefix(line, "Dig further: "):
		default:
			t.Errorf("unexpected line: %q", line)
		}
		if len(line) > 4000 {
			t.Errorf("line of %d bytes is not length-capped", len(line))
		}
	}
	for _, bare := range []string{"<script", "`code`", "[l](", "\x00", "\x1b", "\x07"} {
		if strings.Contains(out, bare) {
			t.Errorf("%q reached the report:\n%s", bare, out)
		}
	}
	if !strings.Contains(out, `\`+"`code\\` &lt;script&gt;alert(1)&lt;/script&gt; \\[l\\](http://evil.example)") {
		t.Errorf("escaped text missing:\n%s", out)
	}
	if got := mdRow(t, out, "2"); utf8.RuneCountInString(strings.ReplaceAll(got[3], `\`, "")) != reportNameMax ||
		utf8.RuneCountInString(strings.ReplaceAll(got[4], `\`, "")) != reportErrMax {
		t.Errorf("long name and error are not capped: %d and %d bytes", len(got[3]), len(got[4]))
	}

	code, out, e = env.tc("report", reportID, "--format", "junit")
	if code != 0 {
		panic(fmt.Sprintf("junit: %d %s", code, e))
	}
	s := parseJUnit(t, out)
	if s.Tests != 3 || s.Failures != 3 {
		panic(fmt.Sprintf("suite: %+v", s))
	}
	// Text survives the round trip, on one line, minus what XML cannot hold.
	flat := "x | y | | --- | `code` <script>alert(1)</script> [l](http://evil.example) \uFFFD\uFFFD]0;t\uFFFD ]]> &amp; \"q\" 'a' \\"
	first := s.Cases[0]
	if first.Name != flat || first.Classname != strings.ReplaceAll(hostile, "\x00\x1b]0;t\x07", "\uFFFD\uFFFD]0;t\uFFFD") ||
		first.Failure == nil || first.Failure.Message != flat || !strings.Contains(first.Failure.Body, flat+": "+flat+"\n") {
		t.Errorf("hostile case: %+v %+v", first, first.Failure)
	}
	if s.Name != strings.ReplaceAll(hostile, "\x00\x1b]0;t\x07", "\uFFFD\uFFFD]0;t\uFFFD") {
		t.Errorf("suite name: %q", s.Name)
	}
	if second := s.Cases[1]; utf8.RuneCountInString(second.Name) != 2*reportNameMax || utf8.RuneCountInString(second.Failure.Message) != reportErrMax {
		t.Errorf("long text is not capped: name %d runes, message %d runes", utf8.RuneCountInString(second.Name), utf8.RuneCountInString(second.Failure.Message))
	}
	if strings.Contains(out, "<script>") {
		t.Errorf("unescaped markup in the XML:\n%s", out)
	}
}

func TestReportCaps(t *testing.T) {
	const n = junitMaxCases + 5
	evs := make([]core.Event, n)
	for i := range evs {
		evs[i] = core.Event{Seq: uint64(i + 1), Service: "api", Kind: core.KindAPIIn, Name: fmt.Sprintf("GET /items/%d", i+1), StartNs: 1, EndNs: 2,
			Mutations: []core.Mutation{{Store: "postgres", Entity: fmt.Sprintf("t%04d", i%60), Op: "insert"}}}
		if i < 60 || i == n-1 {
			evs[i].Err = "boom"
		}
	}
	env := newReportEnv(t, evs...)
	for i := range evs {
		env.roots = append(env.roots, env.node(i+1, nil, nil))
	}

	_, out, _ := env.tc("report", reportID)
	if got := strings.Count(out, "| boom |"); got != reportMaxRows || !strings.Contains(out, "\n11 more failed event(s) omitted.\n") {
		t.Errorf("failures: %d rows", got)
	}
	if got := strings.Count(out, "| postgres |"); got != reportMaxRows || !strings.Contains(out, "\n10 more row(s) omitted.\n") {
		t.Errorf("mutations: %d rows", got)
	}

	_, out, _ = env.tc("report", reportID, "--format", "junit")
	s := parseJUnit(t, out)
	if s.Tests != junitMaxCases || s.Failures != 61 {
		panic(fmt.Sprintf("suite: %d tests, %d failures", s.Tests, s.Failures))
	}
	// The failed root past the cap is kept ahead of passing ones.
	if s.Cases[60].Name != fmt.Sprintf("GET /items/%d", n) || s.Cases[60].Failure == nil || s.Cases[61].Name != "GET /items/61" {
		t.Errorf("cases around the cap: %q, %q", s.Cases[60].Name, s.Cases[61].Name)
	}
	if want := "1000 of 1005 trace roots are reported as test cases (the cap), failed ones first; 5 omitted."; s.SystemOut != want {
		t.Errorf("system-out: %q", s.SystemOut)
	}
}

// A failure the summary has but the trace does not is still reported.
func TestReportFailureOutsideTrace(t *testing.T) {
	env := newReportEnv(t, core.Event{Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /a", StartNs: 1, EndNs: 2})
	env.roots = nodes(env.node(1, nil, nil))
	env.sum.Failures = []client.Failure{{Seq: 9, Service: "worker", Kind: core.KindWorkerEnd, Name: "late", Err: "boom"}}

	code, out, _ := env.tc("report", reportID, "--fail-on-error")
	if got := mdRow(t, out, "9"); code != exitCheckFailed || got[5] != "-" {
		t.Errorf("markdown: %d %q", code, got)
	}
	_, out, _ = env.tc("report", reportID, "--format", "junit")
	s := parseJUnit(t, out)
	if s.Tests != 2 || s.Failures != 1 || s.Cases[1].Name != "failed events outside the trace" ||
		s.Cases[1].Failure.Body != "#9 worker worker.end late: boom\n" {
		t.Errorf("suite: %+v", s)
	}
}

func TestReportOutputFile(t *testing.T) {
	env := failingEnv(t)
	file := filepath.Join(t.TempDir(), "report.xml")
	if err := os.WriteFile(file, []byte(strings.Repeat("stale ", 10000)), 0o644); err != nil {
		panic(err)
	}
	code, out, e := env.tc("report", reportID, "-o", file, "--format", "junit", "--fail-on-error")
	if code != exitCheckFailed || out != "" || !strings.Contains(e, "wrote junit report to "+file) || !strings.Contains(e, "check failed") {
		panic(fmt.Sprintf("the report is written before the gate fails: %d %q %s", code, out, e))
	}
	b, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}
	if s := parseJUnit(t, string(b)); s.Failures != 2 || strings.Contains(string(b), "stale") {
		panic(fmt.Sprintf("file was not replaced by the report: %+v", s))
	}

	file = filepath.Join(t.TempDir(), "report.md")
	if code, out, e := env.tc("report", "-o", file, reportID); code != 0 || out != "" || !strings.Contains(e, "wrote markdown report") {
		panic(fmt.Sprintf("-o: %d %q %s", code, out, e))
	}
	if b, _ := os.ReadFile(file); !strings.HasPrefix(string(b), "## tsession report: checkout") {
		panic(fmt.Sprintf("markdown file: %q", b))
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0o644 {
		panic(fmt.Sprintf("report file mode %v, want 0644", fi.Mode().Perm()))
	}
	if code, _, e := env.tc("report", reportID, "-o", filepath.Join(t.TempDir(), "missing", "r.md")); code != 1 || e == "" {
		panic(fmt.Sprintf("unwritable -o: %d %s", code, e))
	}
}

func TestReportUsageErrors(t *testing.T) {
	// No server: a bad invocation fails before anything is fetched.
	for _, args := range [][]string{
		{"report", "ts_1", "--format", "html"},
		{"report"},
		{"report", "ts_1", "ts_2"},
	} {
		var out, errb bytes.Buffer
		code := run(append([]string{"--cp", "http://127.0.0.1:1", "--collector", "http://127.0.0.1:1"}, args...), &out, &errb)
		if code != 1 || out.Len() != 0 || !strings.Contains(errb.String(), "usage: tracectl report <id|name> [--format markdown|junit]") {
			t.Errorf("%v: %d %s", args, code, errb.String())
		}
	}
	var out, errb bytes.Buffer
	run([]string{"report", "ts_1", "--format", "html"}, &out, &errb)
	if !strings.Contains(errb.String(), `unknown --format "html"`) {
		t.Errorf("unknown format: %s", errb.String())
	}
	if !strings.Contains(usage, "report <id|name> [--format markdown|junit] [-o FILE] [--fail-on-error] [--fail-on-incomplete]") {
		t.Error("report is missing from the usage text")
	}
}

// A session with no events is not reported as passed: with the documented CI
// recipe this text becomes the job summary, and nothing was checked.
func TestReportEmptySession(t *testing.T) {
	env := newReportEnv(t)
	code, out, e := env.tc("report", reportID, "--fail-on-error", "--fail-on-incomplete")
	if code != 0 || e != "" {
		panic(fmt.Sprintf("an empty session fails no gate unless --fail-on-empty is given: %d %s", code, e))
	}
	if strings.Contains(out, "**Passed**") || !strings.Contains(out, "**Empty**: the session recorded no events") {
		panic(fmt.Sprintf("headline of an empty session:\n%s", out))
	}
	code, out, e = env.tc("report", reportID, "--fail-on-empty")
	if code != 3 || !strings.Contains(e, "recorded no events") || strings.Contains(out, "**Passed**") {
		panic(fmt.Sprintf("--fail-on-empty: %d %q\n%s", code, e, out))
	}
}
