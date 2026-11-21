package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// --fail-on-unconfirmed is the strict form of the delivery check: it fails
// where --fail-on-incomplete passes, on a session of which the control plane
// could not tell whether every event arrived.
func TestFailOnUnconfirmed(t *testing.T) {
	const warn = "service orders-api (instance o-1): it follows the control plane, but no delivery report of it has arrived since it connected (its log says why); the collector has no events from it for this session, so whether it recorded any is unknown"
	sum := client.Summary{EventCount: 1}
	unconfirmed := client.Info{ID: "ts_1", State: "SEALED", DeliveryUnconfirmed: []string{warn, "another"}}
	if err := (&gates{onError: true, onIncomplete: true, onEmpty: true}).check(unconfirmed, sum, 0); err != nil {
		t.Fatalf("without the flag: %v", err)
	}
	err := (&gates{onUnconfirmed: true}).check(unconfirmed, sum, 0)
	if err == nil || err.Error() != "check failed: delivery is unconfirmed ("+warn+"; another)" {
		t.Fatalf("--fail-on-unconfirmed: %v", err)
	}
	if err := (&gates{onUnconfirmed: true}).check(client.Info{ID: "ts_1", State: "SEALED"}, sum, 0); err != nil {
		t.Fatalf("a confirmed session: %v", err)
	}
	// It does not stand in for --fail-on-incomplete, and adds to it.
	incomplete := client.Info{ID: "ts_1", State: "SEALED", Incomplete: true, IncompleteReasons: []string{"lost"}, DeliveryUnconfirmed: []string{warn}}
	if err := (&gates{onUnconfirmed: true, onIncomplete: true}).check(incomplete, sum, 0); err == nil ||
		!strings.Contains(err.Error(), "session is incomplete (lost); delivery is unconfirmed (") {
		t.Fatalf("both gates: %v", err)
	}
	// A session the control plane no longer knows: nothing confirms anything.
	if err := (&gates{onUnconfirmed: true}).check(client.Info{ID: "ts_1"}, sum, 0); err == nil || !strings.Contains(err.Error(), "whether its delivery was confirmed is unknown") {
		t.Fatalf("an unknown session: %v", err)
	}
	if err := (&gates{onUnconfirmed: true, onIncomplete: true}).check(client.Info{ID: "ts_1"}, sum, 0); err == nil || strings.Count(err.Error(), "no longer knows") != 1 {
		t.Fatalf("an unknown session, both gates: %v", err)
	}

	// The flag is one of summary, report, digest and run.
	env := newResultsEnv(t)
	if code, _, e := env.tc("start", "--name", "run-1"); code != 0 {
		t.Fatalf("start: %d %s", code, e)
	}
	for _, args := range [][]string{{"summary", "run-1"}, {"report", "run-1"}, {"digest", "run-1"}} {
		if code, _, e := env.tc(append(args, "--fail-on-unconfirmed")...); code != 0 {
			t.Errorf("%v --fail-on-unconfirmed on a running session without warnings: %d %s", args, code, e)
		}
	}
	if code, _, e := env.tc("run", "--fail-on-unconfirmed", "--", "true"); code != 0 {
		t.Errorf("run --fail-on-unconfirmed: %d %s", code, e)
	}
}

// The review's F7: with --max-bytes 4096 a long reason was cut at "0 of 1…"
// and "Omitted" said that nothing was left out. What is cut of the reasons
// is named there, with the command that prints them whole.
func TestDigestSaysWhenItShortensTheReasons(t *testing.T) {
	long := "service orders-api (instance " + strings.Repeat("pod-", 60) + "1): 0 of 12 recorded events reached the collector (last error: " + strings.Repeat("connection refused ", 30) + ")"
	sum := client.Summary{EventCount: 1, ByService: map[string]int{"inventory": 1}}
	info := client.Info{ID: "ts_1", Name: "checkout", State: "SEALED", Incomplete: true, IncompleteReasons: []string{long}}
	for i := range 6 {
		info.DeliveryUnconfirmed = append(info.DeliveryUnconfirmed, "service s"+string(rune('a'+i))+" (instance i): unknown")
	}
	src := newDigestSource(info, sum, client.Trace{}, nil)
	for _, r := range []digestRender{digestMarkdown{}, digestJSON{}} {
		out, err := src.fit(r, digestMinBytes)
		if err != nil || len(out) > digestMinBytes {
			t.Fatalf("%T: %d bytes, %v", r, len(out), err)
		}
		if strings.Contains(string(out), long) {
			t.Fatalf("%T: test setup: the reason was not shortened", r)
		}
		// One reason shortened and two of six unconfirmed not listed.
		wantAll(t, string(out), "3 of the reasons why the recording is incomplete or unconfirmed are shortened or not listed", "tracectl status ts_1", "and 2 more")
	}
	md, _ := src.fit(digestMarkdown{}, digestMinBytes)
	checkDigestMarkdown(t, string(md))
	if strings.Contains(string(md), "Nothing was left out") {
		t.Fatalf("the digest shortened the reasons and says that nothing was left out:\n%s", md)
	}

	// Nothing cut: nothing noted.
	short := client.Info{ID: "ts_1", Name: "checkout", State: "SEALED", Incomplete: true, IncompleteReasons: []string{"service a (instance i): 0 of 1 recorded events reached the collector"},
		DeliveryUnconfirmed: []string{"service b (instance i): unknown"}}
	out, err := newDigestSource(short, sum, client.Trace{}, nil).fit(digestJSON{}, digestMinBytes)
	if err != nil {
		t.Fatal(err)
	}
	if d := checkDigestJSON(t, string(out)); len(d.Omitted) != 0 {
		t.Fatalf("omitted %+v", d.Omitted)
	}
}

// doctor names a connected service whose delivery reports do not arrive: it
// follows the control plane, said that it reports, and has not been heard of.
func TestDoctorNamesAServiceWithoutDeliveryReports(t *testing.T) {
	long := time.Now().Add(-time.Minute)
	rep := doctorReport{ControlPlane: probeResult{OK: true}, Collector: probeResult{OK: true}, Subscribers: []client.Subscriber{
		{Service: "orders-api", Instance: "pod-1", DeliveryInstance: "pod-1-ab", ConnectedAt: long},
		{Service: "inventory", Instance: "pod-2", DeliveryInstance: "pod-2-cd", ConnectedAt: long, LastDeliveryReport: long},
		{Service: "legacy", Instance: "pod-3", ConnectedAt: long},                                    // an SDK that sends none
		{Service: "fresh", Instance: "pod-4", DeliveryInstance: "pod-4-ef", ConnectedAt: time.Now()}, // connected this moment
	}}
	var out bytes.Buffer
	rep.print(&out)
	for _, line := range strings.Split(out.String(), "\n") {
		if got, want := strings.Contains(line, "NO DELIVERY REPORTS"), strings.Contains(line, "orders-api"); got != want {
			t.Errorf("line %q: flagged=%v, want %v", line, got, want)
		}
	}
	if !strings.Contains(out.String(), "orders-api") {
		t.Fatalf("doctor:\n%s", out.String())
	}
}
