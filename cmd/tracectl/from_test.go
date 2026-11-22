package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/dashboard"
)

// exportFile writes evs the way `tracectl export` does: one event per line.
// It returns the file's name as it was given: the tests run in dir, and name
// the file as a person does.
func exportFile(t *testing.T, dir, name string, evs ...core.Event) string {
	t.Helper()
	// As tracectl export writes it: the first line carries the verdict and the number of events.
	var b bytes.Buffer
	for i := range evs {
		var meta *core.ExportSession
		if i == 0 {
			meta = &core.ExportSession{State: "SEALED", Events: count(len(evs))}
		}
		line, err := core.ExportLine(&evs[i], meta)
		if err != nil {
			panic(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, name), b.Bytes(), 0o600); err != nil {
		panic(err)
	}
	return name
}

// fromEvents is a small session with a failure, an effective and a rolled
// back write, a write without a key and an async edge.
func fromEvents(id string) []core.Event {
	yes := true
	return []core.Event{
		{SessionID: id, Seq: 1, TraceID: "t1", SpanID: "a", Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1e9, EndNs: 5e9, Err: "HTTP 500"},
		{SessionID: id, Seq: 2, TraceID: "t1", SpanID: "b", ParentID: "a", Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders", StartNs: 2e9, EndNs: 3e9,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", PK: map[string]any{"id": 7}, Effective: &yes,
				Diff: []core.FieldChange{{Field: "status", New: "new"}}}}},
		{SessionID: id, Seq: 3, TraceID: "t1", SpanID: "c", ParentID: "a", Service: "api", Kind: core.KindDBWrite, Name: "UPDATE stock", StartNs: 3e9, EndNs: 4e9,
			Mutations: []core.Mutation{{Store: "postgres", Entity: "stock", Op: "update", Effective: &yes}}},
		{SessionID: id, Seq: 4, TraceID: "t1", SpanID: "d", ParentID: "a", Service: "api", Kind: core.KindDBRead, Name: "SELECT 1", StartNs: 4e9, EndNs: 4.5e9, Err: "connection reset"},
	}
}

// fromTC runs tracectl with the files of --from and no service: the
// addresses of the environment are ones nothing may be asked at.
func fromTC(t *testing.T, files []string, args ...string) (int, string, string) {
	t.Helper()
	t.Setenv("TSESSION_CONTROLPLANE_URL", "not-a-url") // tracectl without --from exits 2 on these
	t.Setenv("TSESSION_COLLECTOR_URL", "not-a-url")
	var argv []string
	for _, f := range files {
		argv = append(argv, "--from", f)
	}
	var out, errb bytes.Buffer
	code := run(append(argv, args...), &out, &errb)
	return code, out.String(), errb.String()
}

// Every read command prints for --from FILE what it prints against the
// dashboard that opened FILE (which the integration tests hold to the live
// session), whether the session is named or left out.
func TestFromReadsLikeTheDashboardThatOpenedTheFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	a := exportFile(t, dir, "checkout.ndjson", fromEvents("ts_a")...)
	b := exportFile(t, dir, "retry.ndjson", fromEvents("ts_b")[:3]...)
	h, err := dashboard.New(dashboard.Config{Open: []string{a, b}})
	if err != nil {
		panic(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	served := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := run(append([]string{"--cp", srv.URL + "/api/cp", "--collector", srv.URL + "/api/col"}, args...), &out, &errb)
		return code, out.String(), errb.String()
	}
	// The command with the session named (id marks where), and what it prints.
	for _, args := range [][]string{
		{"summary", "id"}, {"summary", "id", "--json"}, {"summary", "id", "--fail-on-error"}, {"summary", "id", "--fail-on-empty", "--fail-on-incomplete"},
		{"report", "id"}, {"report", "id", "--format", "junit", "--fail-on-error"},
		{"events", "id"}, {"events", "id", "--errors", "--json"}, {"events", "--kind", "db.*", "id"},
		{"trace", "id"}, {"trace", "id", "--json"}, {"trace", "id", "--errors"},
		{"show", "id", "2"}, {"show", "id", "2", "--json"},
		{"entities", "id"}, {"entities", "id", "--json"},
		{"history", "id", "postgres", "orders", "7"}, {"history", "id", "postgres", "orders", "id=7", "--json"},
		{"history", "id", "postgres", "stock", "--unknown-key"},
		{"status", "id"}, {"status", "id", "--short"},
		{"digest", "id", "--format", "json", "--fail-on-error"},
	} {
		named := func(id string) []string {
			return strings.Split(strings.Replace(strings.Join(args, "\x00"), "\x00id", "\x00"+id, 1), "\x00")
		}
		omitted := append([]string{args[0]}, args[2:]...)
		if args[0] == "events" && args[1] != "id" {
			omitted = args[:3]
		}
		wantCode, want, wantErr := served(named("ts_a")...)
		if wantCode != 0 && wantCode != exitCheckFailed || want == "" {
			panic(fmt.Sprintf("tracectl %v on the dashboard: exit %d\n%s%s", args, wantCode, want, wantErr))
		}
		if args[0] == "digest" || args[0] == "report" {
			// The one difference: the commands a digest or a report names. See TestFromDigestNamesTheFile.
			want = strings.ReplaceAll(want, `,"drill_down_note":`+string(jsonBytes(drillDownNote)[:len(jsonBytes(drillDownNote))-1]), "")
			for _, c := range []string{"show", "history", "trace", "events"} {
				want = strings.ReplaceAll(want, "tracectl "+c+" ts_a", "tracectl --from "+a+" "+c)
			}
		}
		for _, tt := range []struct {
			what  string
			files []string
			args  []string
		}{
			{"the session left out", []string{a}, omitted},
			{"the session by id", []string{a}, named("ts_a")},
			{"the session by the file's name", []string{a}, named("checkout")},
			{"two files, the session by id", []string{b, a}, named("ts_a")},
		} {
			code, got, gotErr := fromTC(t, tt.files, tt.args...)
			if code != wantCode || got != want || gotErr != wantErr {
				t.Errorf("tracectl --from %v (%s): exit %d, the dashboard %d\n--- file\n%s%s\n--- dashboard\n%s%s", tt.args, tt.what, code, wantCode, got, gotErr, want, wantErr)
			}
		}
	}

	// list: the files, as the dashboard lists them.
	_, want, _ := served("list")
	if code, got, e := fromTC(t, []string{a, b}, "list"); code != 0 || got != want || e != "" || strings.Count(got, "\n") != 2 {
		t.Errorf("list: exit %d\n%s%s\nthe dashboard:\n%s", code, got, e, want)
	}
	// diff: two files are baseline and candidate, in the order given; or two of the sessions by name.
	for _, flags := range [][]string{nil, {"--json"}, {"--fail-on-diff"}} {
		wantCode, want, wantErr := served(append([]string{"diff", "ts_a", "ts_b"}, flags...)...)
		if !strings.Contains(want+wantErr, "ts_b") {
			panic(fmt.Sprintf("diff on the dashboard: exit %d\n%s%s", wantCode, want, wantErr))
		}
		for _, tt := range []struct {
			files []string
			args  []string
		}{{[]string{a, b}, []string{"diff"}}, {[]string{b, a}, []string{"diff", "ts_a", "ts_b"}}, {[]string{b, a}, []string{"diff", "checkout", "retry"}}} {
			if code, got, gotErr := fromTC(t, tt.files, append(tt.args, flags...)...); code != wantCode || got != want || gotErr != wantErr {
				t.Errorf("--from %v %v %v: exit %d, the dashboard %d\n--- file\n%s%s\n--- dashboard\n%s%s", tt.files, tt.args, flags, code, wantCode, got, gotErr, want, wantErr)
			}
		}
	}
	// The other order is the other comparison.
	_, want, _ = served("diff", "ts_b", "ts_a")
	if _, got, _ := fromTC(t, []string{b, a}, "diff"); got != want {
		t.Errorf("--from retry --from checkout diff:\n%s\nwant\n%s", got, want)
	}
}

// A digest read from a file names the file in its commands: they work in no
// other way. It has no note about --cp and --collector, and is the same
// bytes each time.
func TestFromDigestNamesTheFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	plain := exportFile(t, dir, "run.ndjson", fromEvents("ts_a")...)
	code, md, e := fromTC(t, []string{plain}, "digest")
	if code != 0 || e != "" {
		panic(fmt.Sprintf("exit %d\n%s", code, e))
	}
	sec := checkDigestMarkdown(t, md)
	wantAll(t, sec["### Drill down"],
		"- `tracectl --from "+plain+" show <seq>`: one event in full",
		"- `tracectl --from "+plain+" history <store> <entity> <key>`: ",
		"- `tracectl --from "+plain+" trace --errors`: ",
		"- `tracectl --from "+plain+" events --errors`: ")
	if strings.Contains(md, "--cp") || strings.Contains(md, "TSESSION_") || strings.Contains(sec["### Drill down"], "ts_a") {
		t.Errorf("the digest of a file sends its reader to the services:\n%s", md)
	}
	_, js, _ := fromTC(t, []string{plain}, "digest", "--json")
	if d := checkDigestJSON(t, js); d.DrillDownNote != "" || d.DrillDown[0].Command != "tracectl --from "+plain+" show <seq>" {
		t.Errorf("the JSON digest: note %q, first command %q", d.DrillDownNote, d.DrillDown[0].Command)
	}
	if _, again, _ := fromTC(t, []string{plain}, "digest"); again != md {
		t.Error("the digest of a file differs between two runs")
	}
	// The commands the digest names work as they are printed.
	for _, c := range []string{"show 2", "history postgres orders 7", "trace --errors", "events --errors"} {
		if code, out, e := fromTC(t, []string{plain}, strings.Fields(c)...); code != 0 || out == "" || e != "" {
			t.Errorf("tracectl --from FILE %s: exit %d\n%s%s", c, code, out, e)
		}
	}

	// What is cut names the file too. Many failures, the smallest budget and the longest name that is repeated.
	var evs []core.Event
	for i := range 300 {
		evs = append(evs, core.Event{SessionID: "ts_big", Seq: uint64(i + 1), Service: "api", Kind: core.KindAPIIn,
			Name: fmt.Sprintf("GET /orders/%c%c%c?%s", 'a'+i/26, 'a'+i%26, 'a'+i%7, strings.Repeat("q", 40)), Err: "HTTP 500 " + strings.Repeat("e", 80), StartNs: 1e9, EndNs: 2e9})
	}
	short := exportFile(t, dir, strings.Repeat("s", fromArgTightMax-len(".ndjson"))+".ndjson", evs...)
	long := exportFile(t, dir, strings.Repeat("l", fromArgMax-len(".ndjson"))+".ndjson", evs...)
	for _, format := range []string{"markdown", "json"} {
		// Short of room: a name of up to fromArgTightMax bytes is repeated, a longer one is the word FILE.
		for file, want := range map[string]string{short: short, long: "FILE"} {
			code, out, e := fromTC(t, []string{file}, "digest", "--max-bytes", "4096", "--format", format)
			if code != 0 || len(out) > 4096 || !strings.Contains(out, "tracectl --from "+want+" events --errors --limit 50") ||
				!strings.Contains(out, "tracectl --from "+want+" show <seq>") || want == "FILE" && strings.Contains(out, long) {
				t.Errorf("--max-bytes 4096 --format %s with a name of %d bytes: exit %d, %d bytes\n%s%s", format, len(file), code, len(out), out, e)
			}
		}
		// With room, the long name is repeated too.
		code, out, e := fromTC(t, []string{long}, "digest", "--max-bytes", "400000", "--format", format)
		if code != 0 || !strings.Contains(out, "tracectl --from "+long+" show <seq>") {
			t.Errorf("--max-bytes 400000 --format %s with a name of %d bytes: exit %d\n%s", format, len(long), code, e)
		}
	}

	// A name is one word of a command, or it is not repeated.
	for name, want := range map[string]string{
		"my run.ndjson":   "'my run.ndjson'",
		"a$b;c.ndjson":    "'a$b;c.ndjson'",
		"it's.ndjson":     "FILE",
		"tick`s.ndjson":   "FILE",
		"line\nfeed.json": "FILE",
		"üml.ndjson":      "FILE",
		strings.Repeat("x", fromArgMax) + ".ndjson": "FILE",
	} {
		f := exportFile(t, dir, name, fromEvents("ts_a")...)
		code, out, e := fromTC(t, []string{f}, "digest")
		if code != 0 || !strings.Contains(out, "- `tracectl --from "+want+" show <seq>`: ") {
			t.Errorf("%q: exit %d, want --from %s\n%s%s", name, code, want, checkDigestMarkdown(t, out)["### Drill down"], e)
		}
	}
	if shellWord("-o", false) != "FILE" || shellWord("a/b-c_d.e+f=g:h,i@j%k", false) != "a/b-c_d.e+f=g:h,i@j%k" {
		t.Error("shellWord: a name that starts with - or a plain name")
	}

	// Of two files, the digest names the one that holds its session.
	other := exportFile(t, dir, "other.ndjson", fromEvents("ts_b")...)
	if _, out, _ := fromTC(t, []string{other, plain}, "digest", "ts_a"); !strings.Contains(out, "`tracectl --from "+plain+" show <seq>`") || strings.Contains(out, other) {
		t.Errorf("the digest of the second of two files:\n%s", checkDigestMarkdown(t, out)["### Drill down"])
	}
}

// What cannot be read says so in one line and exits 1; what cannot work with
// a file exits 2 before the file is opened.
func TestFromErrors(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	good := exportFile(t, dir, "run.ndjson", fromEvents("ts_a")...)
	other := exportFile(t, dir, "other.ndjson", fromEvents("ts_b")...)
	write := func(name, content string) string {
		if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
			panic(err)
		}
		return name
	}
	whole, err := os.ReadFile(good)
	if err != nil {
		panic(err)
	}
	missing := "missing.ndjson"
	empty := write("empty.ndjson", "")
	blank := write("blank.ndjson", "\n\n")
	notExport := write("notes.json", `{"hello":"world"}`+"\n")
	text := write("README.md", "# not JSON at all\n")
	cut := write("cut.ndjson", string(whole[:len(whole)-40])) // the last line stops in the middle
	cutOnly := write("cut1.ndjson", string(whole[:60]))       // and so does the only one
	mixed := exportFile(t, dir, "mixed.ndjson", append(fromEvents("ts_a"), core.Event{SessionID: "ts_z", Seq: 5, Kind: core.KindAPIIn})...)
	for _, tt := range []struct {
		files []string
		args  []string
		code  int
		want  string
	}{
		{[]string{missing}, []string{"digest"}, 1, "open " + missing + ": no such file or directory"},
		{[]string{empty}, []string{"digest"}, 1, empty + ": no events, and no session"},
		{[]string{blank}, []string{"summary"}, 1, blank + ": no events, and no session"},
		{[]string{notExport}, []string{"digest"}, 1, notExport + ":1: not an exported event (no session_id or seq)"},
		{[]string{text}, []string{"digest"}, 1, text + ":1: not an exported event"},
		{[]string{cut}, []string{"digest"}, 1, cut + ":4: not an exported event: unexpected end of JSON input: the file is cut short"},
		{[]string{cut}, []string{"summary", "--fail-on-error"}, 1, "export the session again"},
		{[]string{cutOnly}, []string{"digest"}, 1, cutOnly + ":1: not an exported event"},
		{[]string{mixed}, []string{"digest"}, 1, "mixed.ndjson:5: events of two sessions (ts_a and ts_z) in one file"},
		{[]string{good, good}, []string{"list"}, 1, "run.ndjson was given twice"},
		{[]string{good, missing}, []string{"digest", "ts_a"}, 1, "no such file or directory"},
		// The session, when it is named, is the file's.
		{[]string{good}, []string{"digest", "ts_other"}, 1, `no session with the id or the name "ts_other" in the files of --from: ` + good + " holds ts_a"},
		{[]string{good}, []string{"summary", "nightly"}, 1, `no session with the id or the name "nightly" in the files of --from: ` + good + " holds ts_a"},
		{[]string{good}, []string{"show", "ts_other", "2"}, 1, `no session with the id or the name "ts_other" in the files of --from`},
		{[]string{good, other}, []string{"diff", "ts_a", "ts_c"}, 1, `no session with the id or the name "ts_c" in the files of --from: ` + good + " holds ts_a, " + other + " holds ts_b"},
		// Which session, of several files; and what diff needs.
		{[]string{good, other}, []string{"digest"}, 1, "--from was given 2 files: name the session to read (ts_a, ts_b)"},
		{[]string{good, other}, []string{"show", "2"}, 1, "--from was given 2 files: name the session to read"},
		{[]string{good}, []string{"diff"}, 1, "diff compares two sessions and --from was given 1: give two files (--from BASELINE --from CANDIDATE)"},
		{[]string{good}, []string{"show"}, 1, "usage: tracectl show <id|name> <seq>"},
		{[]string{good}, []string{"show", "99"}, 1, ""},
		// Nothing of a file can be started, stopped or changed, and no service is there to ask.
		{[]string{good}, []string{"start"}, 2, "start does not work with --from"},
		{[]string{good}, []string{"stop", "ts_a"}, 2, "stop does not work with --from"},
		{[]string{good}, []string{"run", "--", "true"}, 2, "run does not work with --from"},
		{[]string{good}, []string{"abort", "ts_a"}, 2, "abort does not work with --from"},
		{[]string{good}, []string{"delete", "ts_a"}, 2, "delete does not work with --from"},
		{[]string{good}, []string{"rename", "ts_a", "x"}, 2, "rename does not work with --from"},
		{[]string{good}, []string{"wait", "ts_a"}, 2, "wait does not work with --from"},
		{[]string{good}, []string{"doctor"}, 2, "doctor does not work with --from"},
		{[]string{good}, []string{"kill", "status"}, 2, "kill does not work with --from"},
		{[]string{good}, []string{"watch-docker"}, 2, "watch-docker does not work with --from"},
		{[]string{good}, []string{"export", "ts_a"}, 2, "export does not work with --from"},
		{[]string{missing}, []string{"stop", "ts_a"}, 2, "the commands that read it are digest, summary, report, events, trace, show, entities, history, diff, status, list"},
		{[]string{good}, []string{"nonsense"}, 2, `unknown command "nonsense"`},
	} {
		code, out, e := fromTC(t, tt.files, tt.args...)
		if code != tt.code || out != "" || !strings.Contains(e, tt.want) || !strings.HasPrefix(e, "tracectl: ") ||
			strings.Count(e, "\n") != 1 || strings.Contains(e, "goroutine") {
			t.Errorf("--from %v %v: exit %d, stdout %q, stderr %q; want exit %d and one line with %q", tt.files, tt.args, code, out, e, tt.code, tt.want)
		}
	}
	// The command an error points to reads the file as well.
	if code, _, e := fromTC(t, []string{good}, "history", "postgres", "orders", "8"); code != 1 ||
		!strings.Contains(e, "`tracectl --from run.ndjson entities` lists what the session changed") {
		t.Errorf("history of a key the file does not hold: exit %d %s", code, e)
	}
	// --from and the address of a service contradict each other.
	for _, flag := range []string{"--cp", "--collector"} {
		var out, errb bytes.Buffer
		if code := run([]string{"--from", good, flag, "http://127.0.0.1:9", "digest"}, &out, &errb); code != 2 || out.Len() != 0 ||
			!strings.Contains(errb.String(), "--from reads a file and asks no service: it cannot be combined with --cp or --collector") {
			t.Errorf("--from with %s: exit %d %s", flag, code, errb.String())
		}
	}
	var out, errb bytes.Buffer
	if code := run([]string{"--from", "", "digest"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "a file name is required") {
		t.Errorf("--from '': exit %d %s", code, errb.String())
	}
	for _, want := range []string{"tracectl --from FILE [--from FILE]... <read command>", "\n  --from FILE ", "tracectl --from a.ndjson --from b.ndjson diff"} {
		if !strings.Contains(usage, want) {
			t.Errorf("the usage text lacks %q", want)
		}
	}
}

// An export of a session without events is one line, its verdict: the gates
// of --from exit on it as they do for the session.
func TestFromGatesOnTheVerdictOfTheFile(t *testing.T) {
	dir := t.TempDir()
	line, err := core.ExportHeader("ts_none", &core.ExportSession{Incomplete: true,
		IncompleteReasons: []string{"service orders-api (instance orders-api-1): 0 of 6 recorded events reached the collector"}})
	if err != nil {
		panic(err)
	}
	f := filepath.Join(dir, "none.ndjson")
	if err := os.WriteFile(f, append(line, '\n'), 0o600); err != nil {
		panic(err)
	}
	for flag, want := range map[string]int{"--fail-on-error": 0, "--fail-on-incomplete": exitCheckFailed, "--fail-on-empty": exitCheckFailed} {
		for _, cmd := range []string{"summary", "report", "digest"} {
			code, _, e := fromTC(t, []string{f}, cmd, flag)
			if code != want || want != 0 && !strings.Contains(e, "check failed") {
				t.Errorf("%s %s of a file without events: exit %d, want %d\n%s", cmd, flag, code, want, e)
			}
			if flag == "--fail-on-incomplete" && !strings.Contains(e, "0 of 6 recorded events reached the collector") {
				t.Errorf("%s %s does not say why: %s", cmd, flag, e)
			}
		}
	}
}

// exportWith writes an export whose first line carries meta, as tracectl
// export writes it, and returns the lines.
func exportWith(t *testing.T, name string, meta *core.ExportSession, evs []core.Event) []string {
	t.Helper()
	var lines []string
	for i := range evs {
		m := meta
		if i > 0 {
			m = nil
		}
		line, err := core.ExportLine(&evs[i], m)
		if err != nil {
			panic(err)
		}
		lines = append(lines, string(line))
	}
	if err := os.WriteFile(name, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		panic(err)
	}
	return lines
}

// A file that lost its last lines at the end of a line looks whole: every
// line parses. The export says how many events it wrote, and a file with
// fewer is refused, where it was judged on the events that were left.
func TestFromRefusesAFileThatLostLines(t *testing.T) {
	t.Chdir(t.TempDir())
	evs := fromEvents("ts_a")
	lines := exportWith(t, "whole.ndjson", &core.ExportSession{State: "SEALED", Events: count(len(evs))}, evs)
	if code, _, e := fromTC(t, []string{"whole.ndjson"}, "summary", "--fail-on-error"); code != exitCheckFailed {
		panic(fmt.Sprintf("the whole file: exit %d %s", code, e))
	}
	// The first line alone: an event that went well... no: seq 1 failed; keep the two writes that went well.
	if err := os.WriteFile("part.ndjson", []byte(strings.Join(lines[:3], "\n")+"\n"), 0o600); err != nil {
		panic(err)
	}
	code, out, e := fromTC(t, []string{"part.ndjson"}, "summary", "--fail-on-error", "--fail-on-incomplete")
	if code != 1 || out != "" || !strings.Contains(e, "part.ndjson: holds 3 of the 4 events that were exported") || !strings.Contains(e, "cut short") {
		panic(fmt.Sprintf("a file that lost a line: exit %d, stdout %q, stderr %q", code, out, e))
	}
}

// The session keeps its name through an export, and can be named by it or
// by the file's name.
func TestFromKeepsTheSessionsName(t *testing.T) {
	t.Chdir(t.TempDir())
	evs := fromEvents("ts_a")
	exportWith(t, "run.ndjson", &core.ExportSession{Name: "nightly e2e #42", State: "SEALED", Events: count(len(evs))}, evs)
	_, out, _ := fromTC(t, []string{"run.ndjson"}, "summary")
	if !strings.Contains(out, "ts_a  nightly e2e #42  (SEALED") {
		panic("summary does not show the session's name:\n" + out)
	}
	for _, ref := range []string{"nightly e2e #42", "run", "ts_a"} {
		if code, _, e := fromTC(t, []string{"run.ndjson"}, "status", ref, "--short"); code != 0 {
			panic(fmt.Sprintf("status %q: exit %d %s", ref, code, e))
		}
	}
}

// A session exported while it ran is shown as running, not as sealed and
// complete; the report and the digest name commands that read the file.
func TestFromShowsASessionExportedWhileItRanAsLive(t *testing.T) {
	t.Chdir(t.TempDir())
	evs := fromEvents("ts_a")
	exportWith(t, "live.ndjson", &core.ExportSession{State: "ACTIVE", Events: count(len(evs))}, evs)
	_, out, _ := fromTC(t, []string{"live.ndjson"}, "digest")
	if !strings.Contains(out, "**Live**") || !strings.Contains(out, "state: ACTIVE; recording: not yet known") {
		panic("digest of a session exported while it ran:\n" + out)
	}
	if _, out, _ := fromTC(t, []string{"live.ndjson"}, "status", "--short"); !strings.Contains(out, "ts_a ACTIVE") {
		panic("status: " + out)
	}
	_, out, _ = fromTC(t, []string{"live.ndjson"}, "report")
	if !strings.Contains(out, "`tracectl --from live.ndjson trace --errors`") || strings.Contains(out, "tracectl trace ts_a") {
		panic("the report names commands that ask the services:\n" + out)
	}
}

// An artifact store often compresses, and a pipeline hands a file over on
// standard input: both are read like the file itself.
func TestFromReadsGzipAndStandardInput(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	plain := exportFile(t, dir, "run.ndjson", fromEvents("ts_a")...)
	raw, err := os.ReadFile(plain)
	if err != nil {
		panic(err)
	}
	var z bytes.Buffer
	zw := gzip.NewWriter(&z)
	if _, err := zw.Write(raw); err != nil {
		panic(err)
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	if err := os.WriteFile("run.ndjson.gz", z.Bytes(), 0o600); err != nil {
		panic(err)
	}
	wantCode, want, _ := fromTC(t, []string{plain}, "summary", "--fail-on-error")
	if wantCode != exitCheckFailed || want == "" {
		panic(fmt.Sprintf("the plain file: exit %d\n%s", wantCode, want))
	}
	if code, got, e := fromTC(t, []string{"run.ndjson.gz"}, "summary", "--fail-on-error"); code != wantCode || got != want {
		panic(fmt.Sprintf("the gzip file: exit %d\n%s%s\n--- want\n%s", code, got, e, want))
	}
	// Standard input, plain and compressed.
	for name, content := range map[string][]byte{"plain": raw, "gzip": z.Bytes()} {
		r, w, err := os.Pipe()
		if err != nil {
			panic(err)
		}
		go func() { _, _ = w.Write(content); _ = w.Close() }()
		old := os.Stdin
		os.Stdin = r
		code, got, e := fromTC(t, []string{"-"}, "summary", "--fail-on-error")
		os.Stdin = old
		_ = r.Close()
		// The session is named after its file where the export carries no name: "stdin" here, "run" there.
		if code != wantCode || got != strings.Replace(want, "ts_a  run  ", "ts_a  stdin  ", 1) {
			panic(fmt.Sprintf("standard input (%s): exit %d\n%s%s\n--- want\n%s", name, code, got, e, want))
		}
	}
	// A digest read from standard input cannot name a file to read again.
	r, w, _ := os.Pipe()
	go func() { _, _ = w.Write(raw); _ = w.Close() }()
	old := os.Stdin
	os.Stdin = r
	_, out, _ := fromTC(t, []string{"-"}, "digest")
	os.Stdin = old
	_ = r.Close()
	if !strings.Contains(out, "`tracectl --from FILE show <seq>`") {
		panic("the digest of standard input:\n" + out)
	}
}

// --from after the command would be an unknown flag of the command: said in
// those words, with the order that works.
func TestFromAfterTheCommandIsExplained(t *testing.T) {
	for _, args := range [][]string{{"status", "--short", "--from", "run.ndjson"}, {"digest", "--from=run.ndjson"}} {
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code != 2 || !strings.Contains(errb.String(), "--from goes before the command: tracectl --from FILE "+args[0]) {
			panic(fmt.Sprintf("%v: exit %d, stderr %q", args, code, errb.String()))
		}
	}
	// What follows "--" belongs to the command that run runs.
	var out, errb bytes.Buffer
	if code := run([]string{"--cp", "http://127.0.0.1:1", "run", "--timeout", "0s", "--", "true", "--from", "x"}, &out, &errb); !strings.Contains(errb.String(), "--timeout must be more than zero") {
		panic(fmt.Sprintf("run -- cmd --from: exit %d, stderr %q", code, errb.String()))
	}
}

// Two exports of one session cannot be open together (sessions are told
// apart by their id): said in those words, naming both files.
func TestFromTwoExportsOfOneSession(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	a := exportFile(t, dir, "early.ndjson", fromEvents("ts_a")[:2]...)
	b := exportFile(t, dir, "late.ndjson", fromEvents("ts_a")...)
	code, out, e := fromTC(t, []string{a, b}, "diff")
	if code != 1 || out != "" || !strings.Contains(e, "early.ndjson and late.ndjson are exports of the same session (ts_a)") ||
		!strings.Contains(e, "cannot be compared") || strings.Count(e, "\n") != 1 {
		panic(fmt.Sprintf("exit %d, stdout %q, stderr %q", code, out, e))
	}
}

// count is n as the event count of an export.
func count(n int) *int { return &n }

// Files on which every gate passed although the session they are of would
// not have passed (a review's findings): each is refused, or fails the gate
// that asks whether the recording is complete.
func TestFromDoesNotPassAFileThatProvesNothing(t *testing.T) {
	t.Chdir(t.TempDir())
	ev := func(seq int) string {
		return fmt.Sprintf(`{"session_id":"ts_a","seq":%d,"service":"api","kind":"api.in","name":"GET /x","start_ns":1000000000,"end_ns":2000000000}`, seq)
	}
	with := func(seq int, verdict string) string {
		e := ev(seq)
		return e[:len(e)-1] + `,"tsession_session":` + verdict + `}`
	}
	gates := []string{"summary", "--fail-on-error", "--fail-on-incomplete", "--fail-on-empty"}
	for _, tt := range []struct {
		name  string
		lines []string
		code  int
		want  string // in stderr
	}{
		{"a count that is a string", []string{with(1, `{"state":"SEALED","incomplete":true,"events":"3"}`), ev(2)}, 1, "tsession_session does not read as one"},
		{"a count that is not a whole number", []string{with(1, `{"state":"SEALED","events":3.5}`), ev(2)}, 1, "tsession_session does not read as one"},
		{"a flag that is a string", []string{with(1, `{"state":"SEALED","incomplete":"true","events":2}`), ev(2)}, 1, "tsession_session does not read as one"},
		{"a verdict that is a list", []string{with(1, `["SEALED"]`), ev(2)}, 1, "tsession_session does not read as one"},
		{"a negative count", []string{with(1, `{"state":"SEALED","events":-1}`), ev(2)}, 1, "counts -1 events"},
		{"a state that is none", []string{with(1, `{"state":"BOGUS","events":2}`), ev(2)}, 1, `the state "BOGUS"`},
		{"a state in lower case", []string{with(1, `{"state":"sealed","events":2}`), ev(2)}, 1, `the state "sealed"`},
		{"a second verdict", []string{with(1, `{"state":"SEALED","events":2}`), with(2, `{"state":"SEALED","incomplete":true,"events":2}`)}, 1, ":2: a second tsession_session"},
		{"a verdict on the last line only", []string{ev(1), with(2, `{"state":"SEALED","events":2}`)}, 1, ":2: a second tsession_session"},
		{"more events than were exported", []string{with(1, `{"state":"SEALED","events":1}`), ev(2)}, 1, "holds 2 of the 1 events"},
		{"no verdict: the first line is lost", []string{ev(2), ev(3)}, exitCheckFailed, "whether it is complete is unknown"},
		{"a verdict without a count, and lines lost", []string{with(1, `{"state":"SEALED"}`)}, exitCheckFailed, "whether it is complete is unknown"},
		{"a session that had not ended", []string{with(1, `{"state":"ACTIVE","events":2}`), ev(2)}, exitCheckFailed, "had not ended when the file was written (ACTIVE)"},
		{"a session that was draining", []string{with(1, `{"state":"DRAINING","events":2}`), ev(2)}, exitCheckFailed, "had not ended when the file was written (DRAINING)"},
		{"reasons under a flag that says complete", []string{with(1, `{"state":"SEALED","incomplete":false,"incomplete_reasons":["lost"],"events":2}`), ev(2)}, exitCheckFailed, "session is incomplete (lost)"},
		{"a count and no state", []string{with(1, `{"events":2}`), ev(2)}, exitCheckFailed, "whether it is complete is unknown"},
		{"a drain that timed out, and nothing else", []string{with(1, `{"state":"SEALED","drain_timed_out":true,"events":2}`), ev(2)}, exitCheckFailed, "session is incomplete"},
		{"two verdicts on one line, the clean one last", []string{with(1, `{"state":"SEALED","incomplete":true,"events":2}`)[:len(with(1, `{"state":"SEALED","incomplete":true,"events":2}`))-1] + `,"tsession_session":{"state":"SEALED","events":2}}`, ev(2)}, 1, "2 members named tsession_session"},
		{"a verdict in another case after the real one", []string{with(1, `{"state":"SEALED","incomplete":true,"events":2}`)[:len(with(1, `{"state":"SEALED","incomplete":true,"events":2}`))-1] + `,"TSESSION_SESSION":{"state":"SEALED","events":2}}`, ev(2)}, 1, "2 members named tsession_session"},
		{"a state written twice", []string{with(1, `{"state":"ACTIVE","state":"SEALED","events":2}`), ev(2)}, 1, `the member "state" twice`},
		{"a flag written twice", []string{with(1, `{"state":"SEALED","incomplete":true,"incomplete":false,"events":2}`), ev(2)}, 1, `the member "incomplete" twice`},
		{"a session id with shell characters", []string{strings.Replace(with(1, `{"state":"SEALED","events":1}`), `"ts_a"`, `"ts_a;id|sh"`, 1)}, 1, "the session id is not one"},
		{"a failed seal under a flag that says complete", []string{with(1, `{"state":"SEALED","seal_error":"timeout","events":2}`), ev(2)}, exitCheckFailed, "session is incomplete"},
	} {
		if err := os.WriteFile("f.ndjson", []byte(strings.Join(tt.lines, "\n")+"\n"), 0o600); err != nil {
			panic(err)
		}
		code, out, e := fromTC(t, []string{"f.ndjson"}, gates...)
		if code != tt.code || !strings.Contains(e, tt.want) {
			t.Errorf("%s: exit %d, want %d and %q in stderr\nstdout %q\nstderr %q", tt.name, code, tt.code, tt.want, out, e)
		}
	}
	// The whole, proper file passes: the cases above fail for what is wrong with them.
	if err := os.WriteFile("f.ndjson", []byte(with(1, `{"state":"SEALED","events":2}`)+"\n"+ev(2)+"\n"), 0o600); err != nil {
		panic(err)
	}
	if code, out, e := fromTC(t, []string{"f.ndjson"}, gates...); code != 0 || e != "" {
		panic(fmt.Sprintf("a whole export: exit %d\n%s%s", code, out, e))
	}
	// A file without a verdict is still read, for a person: with a note, and without a gate it exits 0.
	if err := os.WriteFile("f.ndjson", []byte(ev(2)+"\n"+ev(3)+"\n"), 0o600); err != nil {
		panic(err)
	}
	if code, out, e := fromTC(t, []string{"f.ndjson"}, "summary"); code != 0 || !strings.Contains(out, "events     2") || !strings.Contains(e, "note: f.ndjson does not say how its session ended") {
		panic(fmt.Sprintf("a file without a verdict, no gate: exit %d\n%s%s", code, out, e))
	}
}

// A session is named by its id first; a name or a file's name that fits two
// sessions is refused, not resolved to one of them.
func TestFromSessionReferences(t *testing.T) {
	t.Chdir(t.TempDir())
	n := 1
	file := func(name, id, sessionName string) {
		e := core.Event{SessionID: id, Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /" + id, StartNs: 1e9, EndNs: 2e9}
		line, err := core.ExportLine(&e, &core.ExportSession{Name: sessionName, State: "SEALED", Events: &n})
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(name, append(line, '\n'), 0o600); err != nil {
			panic(err)
		}
	}
	file("a.ndjson", "ts_a", "ts_nightly") // a name that looks like an id
	file("ts_a.ndjson", "ts_c", "a")       // a file named like the other's session, a session named like the other's file
	both := []string{"a.ndjson", "ts_a.ndjson"}
	for ref, want := range map[string]string{"ts_a": "ts_a SEALED", "ts_c": "ts_c SEALED", "ts_nightly": "ts_a SEALED"} {
		if code, out, e := fromTC(t, both, "status", ref, "--short"); code != 0 || !strings.HasPrefix(out, want) {
			t.Errorf("status %s: exit %d, %q %q; want %q", ref, code, out, e, want)
		}
	}
	// "a" is the first file's name and the second session's name.
	if code, _, e := fromTC(t, both, "status", "a", "--short"); code != 1 || !strings.Contains(e, `"a" names 2 sessions`) || !strings.Contains(e, "name the session by its id") {
		t.Errorf("an ambiguous reference: exit %d %q", code, e)
	}
}

// What a file calls its session is printed: it cannot steer the terminal.
func TestFromCleansTheNameOfAFile(t *testing.T) {
	t.Chdir(t.TempDir())
	n := 1
	e := core.Event{SessionID: "ts_a", Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9}
	line, err := core.ExportLine(&e, &core.ExportSession{Name: "x\x1b[31mRED\x1b[0m\r\nfake line", State: "SEALED", Events: &n})
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("f.ndjson", append(line, '\n'), 0o600); err != nil {
		panic(err)
	}
	_, out, _ := fromTC(t, []string{"f.ndjson"}, "summary")
	if strings.ContainsAny(out, "\x1b\r") || !strings.Contains(out, "x[31mRED[0m fake line") {
		panic(fmt.Sprintf("summary: %q", out))
	}
	// A session id that is not one is refused.
	bad := strings.Replace(string(line), `"ts_a"`, `"ts_a\u001b[31m"`, 1)
	if err := os.WriteFile("bad.ndjson", []byte(bad+"\n"), 0o600); err != nil {
		panic(err)
	}
	if code, _, e := fromTC(t, []string{"bad.ndjson"}, "summary"); code != 1 || !strings.Contains(e, "the session id is not one") || strings.Contains(e, "\x1b") {
		panic(fmt.Sprintf("a session id with an escape: exit %d %q", code, e))
	}
}

// Rules of --from that a review could change without a test noticing.
func TestFromSmallRules(t *testing.T) {
	// The word a digest writes for the file: quoted where a shell would read it otherwise, FILE where no
	// word can stand for it.
	for path, want := range map[string]string{
		"run.ndjson":       "run.ndjson",
		"my run.ndjson":    "'my run.ndjson'",
		"=ls":              "'=ls'",
		"~user/run.ndjson": "'~user/run.ndjson'",
		"-":                "FILE",
		"-rf":              "FILE",
		"/dev/stdin":       "FILE",
		"/dev/fd/3":        "FILE",
		"it's.ndjson":      "FILE",
		"a`b.ndjson":       "FILE",
		"a\nb.ndjson":      "FILE",
	} {
		if got := shellWord(path, false); got != want {
			t.Errorf("shellWord(%q) = %q, want %q", path, got, want)
		}
	}
	// --from after the command, in each way of writing a flag.
	for _, flag := range []string{"--from", "-from", "--from=x.ndjson", "-from=x.ndjson"} {
		var out, errb bytes.Buffer
		if code := run([]string{"summary", flag}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "--from goes before the command") {
			t.Errorf("summary %s: exit %d, stderr %q", flag, code, errb.String())
		}
	}

	t.Chdir(t.TempDir())
	// A generated name is marked as one, as for the live session.
	n := 1
	e := core.Event{SessionID: "ts_a", Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9}
	line, err := core.ExportLine(&e, &core.ExportSession{Name: "run 12:00:01", NameAuto: true, State: "SEALED", Events: &n})
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("auto.ndjson", append(line, '\n'), 0o600); err != nil {
		panic(err)
	}
	if _, out, _ := fromTC(t, []string{"auto.ndjson"}, "summary"); !strings.Contains(out, "run 12:00:01 (auto)") {
		t.Errorf("a generated name: %q", out)
	}
	// A file that begins with the first byte of gzip's two and is not gzip is read as what it is: refused
	// as not an export, not as a broken gzip stream.
	if err := os.WriteFile("odd.ndjson", []byte("\x1f{}\n"), 0o600); err != nil {
		panic(err)
	}
	if code, _, errOut := fromTC(t, []string{"odd.ndjson"}, "summary"); code != 1 || strings.Contains(errOut, "gzip") || !strings.Contains(errOut, "not an exported event") {
		t.Errorf("a file that starts with 0x1f: exit %d, %q", code, errOut)
	}
}

// --from - with a terminal on standard input would wait without a word.
func TestFromStandardInputThatIsATerminal(t *testing.T) {
	tty, err := os.Open("/dev/tty")
	if err != nil {
		t.Skip("no terminal to test with: ", err)
	}
	defer tty.Close()
	old := os.Stdin
	os.Stdin = tty
	defer func() { os.Stdin = old }()
	if code, out, e := fromTC(t, []string{"-"}, "summary"); code != 1 || out != "" || !strings.Contains(e, "which is a terminal") {
		panic(fmt.Sprintf("exit %d, stdout %q, stderr %q", code, out, e))
	}
}

// A run whose command failed without a failed event (a test that asserts on
// a response) is a failed run: the file says so, and the error gate fails on
// it as tracectl run exited with the command's status.
func TestFromFailsOnTheCommandOfTheRun(t *testing.T) {
	t.Chdir(t.TempDir())
	e := core.Event{SessionID: "ts_a", Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9}
	write := func(name string, status *int) {
		n := 1
		line, err := core.ExportLine(&e, &core.ExportSession{State: "SEALED", Events: &n, CommandStatus: status})
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(name, append(line, '\n'), 0o600); err != nil {
			panic(err)
		}
	}
	seven, zero := 7, 0
	write("failed.ndjson", &seven)
	write("ok.ndjson", &zero)
	write("exported.ndjson", nil) // by tracectl export: no command
	for _, cmd := range []string{"summary", "report", "digest"} {
		code, _, errOut := fromTC(t, []string{"failed.ndjson"}, cmd, "--fail-on-error")
		if code != exitCheckFailed || !strings.Contains(errOut, "the command of the run that wrote the file failed (exit status 7)") {
			t.Errorf("%s --fail-on-error on a run whose command failed: exit %d, %q", cmd, code, errOut)
		}
		// The other gates ask other questions.
		if code, _, errOut := fromTC(t, []string{"failed.ndjson"}, cmd, "--fail-on-incomplete", "--fail-on-empty"); code != 0 {
			t.Errorf("%s without --fail-on-error: exit %d, %q", cmd, code, errOut)
		}
		for _, f := range []string{"ok.ndjson", "exported.ndjson"} {
			if code, _, errOut := fromTC(t, []string{f}, cmd, "--fail-on-error"); code != 0 {
				t.Errorf("%s --fail-on-error on %s: exit %d, %q", cmd, f, code, errOut)
			}
		}
	}
}

// A file that is not there, named relatively: the error says where it was
// looked for (a command copied from a digest is run elsewhere).
func TestFromMissingRelativeFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	code, _, e := fromTC(t, []string{"run.ndjson"}, "digest")
	if code != 1 || !strings.Contains(e, "no such file") || !strings.Contains(e, "looked for in ") || strings.Count(e, "\n") != 1 {
		panic(fmt.Sprintf("exit %d, stderr %q", code, e))
	}
	// Standard input from the null device is an empty file, not a terminal.
	null, err := os.Open(os.DevNull)
	if err != nil {
		panic(err)
	}
	defer null.Close()
	old := os.Stdin
	os.Stdin = null
	defer func() { os.Stdin = old }()
	if code, _, e := fromTC(t, []string{"-"}, "summary"); code != 1 || strings.Contains(e, "terminal") {
		panic(fmt.Sprintf("--from - < /dev/null: exit %d, stderr %q", code, e))
	}
}
