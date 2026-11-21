package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/dashboard"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The dashboard serves two kinds of sessions itself: the demo's and the ones
// it opens from files written by `tracectl export`. Both answer under
// /api/cp and /api/col in the shapes of the real services, so every read
// command of tracectl works on them. These tests run the commands in
// process; integration/parity_test.go compares the answers with those of the
// real services.

// dashTC serves a dashboard and returns a tracectl pointed at it.
func dashTC(t *testing.T, cfg dashboard.Config) func(args ...string) (int, string, string) {
	t.Helper()
	h, err := dashboard.New(cfg)
	if err != nil {
		panic(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := run(append([]string{"--cp", srv.URL + "/api/cp", "--collector", srv.URL + "/api/col"}, args...), &out, &errb)
		return code, out.String(), errb.String()
	}
}

func TestReadCommandsWorkOnTheDemo(t *testing.T) {
	tc := dashTC(t, dashboard.Config{Demo: true})
	// Two sealed sessions, an incomplete one and one that is still ACTIVE.
	for _, id := range []string{"ts_demo0001", "ts_demo0005", "ts_demo0003", "ts_demo0004"} {
		for _, args := range [][]string{
			{"status", id, "--short"}, {"summary", id}, {"summary", id, "--json"}, {"events", id}, {"trace", id}, {"trace", id, "--json"},
			{"entities", id}, {"show", id, "4"}, {"report", id}, {"report", id, "--format", "junit"},
			{"digest", id}, {"digest", id, "--format", "json"},
		} {
			code, out, errOut := tc(args...)
			if code != 0 || errOut != "" || strings.TrimSpace(out) == "" {
				t.Errorf("tracectl %s: exit %d, stderr %q, %d bytes of output", strings.Join(args, " "), code, errOut, len(out))
			}
		}
	}

	// The summary is the collector's document: failures with their error,
	// mutations per store and entity, and whether the session is sealed.
	for id, sealed := range map[string]bool{"ts_demo0001": true, "ts_demo0004": false} {
		_, out, _ := tc("summary", id, "--json")
		var sum client.Summary
		dec := json.NewDecoder(strings.NewReader(out))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&sum); err != nil {
			panic(fmt.Sprintf("summary of %s: %v\n%s", id, err, out))
		}
		if sum.Sealed != sealed || sum.EventCount != 32 || len(sum.Failures) != 7 || sum.Failures[0].Err == "" || sum.Mutations["postgres"]["orders"] != 2 {
			t.Errorf("summary of %s: %s", id, out)
		}
	}
	if _, out, _ := tc("digest", "ts_demo0004"); !strings.Contains(out, "state: ACTIVE") || !strings.Contains(out, "**Live**") {
		t.Errorf("digest of the ACTIVE session does not say it is live:\n%s", out)
	}
	if _, out, _ := tc("digest", "ts_demo0005"); !strings.Contains(out, "state: SEALED") || !strings.Contains(out, "### Mutations") {
		t.Errorf("digest of a sealed session:\n%s", out)
	}
	if _, out, _ := tc("summary", "ts_demo0003"); !strings.Contains(out, "12 dropped by SDKs") || !strings.Contains(out, "1 orphaned") {
		t.Errorf("summary of the incomplete session:\n%s", out)
	}
	// A history is the collector's object, and a session is found by name.
	if code, out, errOut := tc("history", "checkout happy path + failures", "postgres", "orders", "ord_1001"); code != 0 ||
		!strings.Contains(out, `postgres/orders {"id":"ord_1001"}: 2 mutation(s)`) {
		t.Errorf("history by session name: exit %d\n%s%s", code, out, errOut)
	}
	// The list is the control plane's: every session, oldest first.
	_, out, _ := tc("list")
	var ids []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		ids = append(ids, strings.Fields(l)[0])
	}
	if got := strings.Join(ids, " "); got != "ts_demo0003 ts_demo0001 ts_demo0005 ts_demo0004 ts_demo0002" {
		t.Errorf("list: %s", got)
	}
	// export pages through /events: every event, the first one included.
	_, out, _ = tc("export", "ts_demo0001")
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 32 || !strings.Contains(lines[0], `"seq":1,`) {
		t.Errorf("export: %d lines, first %.80s", len(lines), lines[0])
	}
	if code, _, errOut := tc("doctor"); code != 0 {
		t.Errorf("doctor: exit %d %s", code, errOut)
	}
}

// An exported file opened with `dashboard -open` answers every read command
// with what the session it was exported from answers.
func TestReadCommandsWorkOnAnExportedFile(t *testing.T) {
	const id = "ts_demo0001"
	demo := dashTC(t, dashboard.Config{Demo: true})
	// The opened session is named after its file: name the file after the
	// session and the two print the same.
	file := filepath.Join(t.TempDir(), "checkout happy path + failures.ndjson")
	if code, _, errOut := demo("export", id, "-o", file); code != 0 {
		panic(fmt.Sprintf("export: %d %s", code, errOut))
	}
	off := dashTC(t, dashboard.Config{Open: []string{file}})

	for _, args := range [][]string{
		{"summary", id}, {"summary", id, "--json"}, {"summary", id, "--fail-on-error"},
		{"events", id}, {"events", id, "--json"}, {"events", id, "--errors"}, {"events", id, "--kind", "db.*", "--limit", "3"},
		{"trace", id}, {"trace", id, "--json"}, {"trace", id, "--errors"},
		{"entities", id}, {"entities", id, "--json"},
		{"history", id, "postgres", "orders", "ord_1001"}, {"history", id, "postgres", "orders", `{"id":"ord_1001"}`, "--json"},
		{"history", id, "postgres", "loyalty_ledger", "customer_id=cus_synth_001,order_id=ord_1001"},
		{"show", id, "4"}, {"show", id, "12", "--json"},
		{"report", id}, {"report", id, "--format", "junit"},
		{"digest", id}, {"digest", id, "--format", "json"}, {"digest", id, "--max-bytes", "4096"},
		{"export", id},
	} {
		name := strings.Join(args, " ")
		wantCode, want, _ := demo(args...)
		code, got, errOut := off(args...)
		if strings.TrimSpace(want) == "" {
			panic(fmt.Sprintf("tracectl %s prints nothing for the session itself", name))
		}
		if code != wantCode || got != want {
			t.Errorf("tracectl %s on the file: exit %d (session: %d) %s\n--- file\n%s\n--- session\n%s", name, code, wantCode, errOut, got, want)
		}
	}
	// What the file was written from is what it serves: exporting it again
	// gives the same bytes.
	b, err := os.ReadFile(file)
	if _, again, _ := off("export", id); err != nil || again != string(b) {
		t.Errorf("an export of the opened file differs from the file (%v)", err)
	}
	if code, out, _ := off("diff", id, id); code != 0 || !strings.Contains(out, "no differences") {
		t.Errorf("diff of the file with itself: %d\n%s", code, out)
	}
	// By the name of the file, too.
	if code, out, _ := off("status", "checkout happy path + failures", "--short"); code != 0 || strings.TrimSpace(out) != id+" SEALED" {
		t.Errorf("status by name: %d %q", code, out)
	}

	// A file is read-only, and the refusal says so.
	for _, args := range [][]string{{"stop", id}, {"abort", id}, {"delete", id}, {"rename", id, "other"}} {
		code, out, errOut := off(args...)
		if code != 1 || out != "" || !strings.Contains(errOut, "405") ||
			!strings.Contains(errOut, "read-only: session "+id+" is a file (checkout happy path + failures.ndjson, opened with dashboard -open)") {
			t.Errorf("tracectl %s on a file: exit %d\n%s%s", args[0], code, out, errOut)
		}
	}
	for _, args := range [][]string{{"kill", "on"}, {"start", "--name", "x"}} {
		if code, _, errOut := off(args...); code != 1 || !strings.Contains(errOut, "read-only") || !strings.Contains(errOut, "opened from files") {
			t.Errorf("tracectl %s on files: exit %d %s", strings.Join(args, " "), code, errOut)
		}
	}
	if code, out, _ := off("status", id, "--short"); code != 0 || strings.TrimSpace(out) != id+" SEALED" {
		t.Errorf("the session after the refused changes: %d %q", code, out)
	}
	if code, out, _ := off("kill", "status"); code != 0 || !strings.Contains(out, "kill switch: off") {
		t.Errorf("kill status: %d %q", code, out)
	}
}
