package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// A session whose events did not all reach the collector looks complete in
// the collector's summary. The control plane's verdict must reach every
// place a person or a gate looks.
func TestDeliveryShortfallIsShownAndFailsTheGate(t *testing.T) {
	const reason = "service orders-api (instance o-1): 0 of 7 recorded events reached the collector, 7 still undelivered (last error: connection refused)"
	info := client.Info{ID: "ts_1", Name: "checkout", State: "SEALED", Incomplete: true,
		IncompleteReasons:   []string{reason},
		DeliveryUnconfirmed: []string{"service batch (instance b-1): 3 of 5 recorded events reached the collector, 2 still undelivered; it stopped reporting"}}
	sum := client.Summary{EventCount: 1, ByService: map[string]int{"inventory": 1}} // what the collector knows: nothing wrong

	// The gates: the reason is in the message; without the gate nothing fails.
	g := gates{onError: true, onIncomplete: true, onEmpty: true}
	err := g.check(info, sum, 0)
	if err == nil || !strings.Contains(err.Error(), "session is incomplete ("+reason+")") {
		panic(fmt.Sprintf("--fail-on-incomplete: %v", err))
	}
	if err := (&gates{onError: true, onEmpty: true}).check(info, sum, 0); err != nil {
		panic(fmt.Sprintf("without --fail-on-incomplete: %v", err))
	}
	// An unconfirmed delivery is a warning: it does not fail the gate.
	warn := client.Info{ID: "ts_2", State: "SEALED", DeliveryUnconfirmed: info.DeliveryUnconfirmed}
	if err := g.check(warn, sum, 0); err != nil {
		panic(fmt.Sprintf("delivery_unconfirmed alone failed the gate: %v", err))
	}

	var out bytes.Buffer
	printSummary(&out, info, sum)
	for _, want := range []string{"(SEALED, incomplete, delivery unconfirmed)", "delivery   incomplete: " + reason, "delivery   unconfirmed: service batch (instance b-1)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("summary lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	printSummary(&out, client.Info{ID: "ts_3", State: "SEALED"}, sum)
	if strings.Contains(out.String(), "delivery") || strings.Contains(out.String(), "incomplete") {
		t.Errorf("summary of a healthy session:\n%s", out.String())
	}

	// The report: incomplete, with the reason under "Why it is incomplete".
	rep := buildReport(info, sum, client.Trace{}, nil)
	md, err := renderMarkdown(rep)
	if err != nil {
		panic(err)
	}
	if !rep.incomplete || len(rep.reasons) != 1 || !strings.Contains(rep.reasons[0], "Events did not reach the collector: service orders-api") ||
		strings.Contains(string(md), "**Passed**") || !strings.Contains(string(md), "Why it is incomplete") || !strings.Contains(string(md), "orders-api") {
		panic(fmt.Sprintf("report: incomplete=%v reasons=%q\n%s", rep.incomplete, rep.reasons, md))
	}
}

// delivery_unconfirmed is a warning: it fails nothing, so it has to be said
// wherever the verdict is, or a session that "passed" hides it.
func TestDeliveryUnconfirmedIsShownEverywhere(t *testing.T) {
	const warn = "service batch (instance b-1): the collector has 9 events, more than the 5 of its last report"
	info := client.Info{ID: "ts_2", Name: "checkout", State: "SEALED", DeliveryUnconfirmed: []string{warn}}
	sum := client.Summary{SessionID: "ts_2", Sealed: true, EventCount: 1, ByService: map[string]int{"batch": 1}}

	var out bytes.Buffer
	printSummary(&out, info, sum)
	wantAll(t, out.String(), "(SEALED, delivery unconfirmed)", "delivery   unconfirmed: "+warn)

	rep := buildReport(info, sum, client.Trace{}, nil)
	md, err := renderMarkdown(rep)
	if err != nil || rep.incomplete {
		panic(fmt.Sprintf("report: incomplete=%v %v", rep.incomplete, err))
	}
	wantAll(t, string(md), "**Passed**: no failed events. Whether every event arrived could not be confirmed for 1 service instance (see below).",
		"### Delivery not confirmed", "- service batch (instance b-1): the collector has 9 events")
	junit, err := renderJUnit(rep)
	if err != nil {
		panic(err)
	}
	wantAll(t, string(junit), `<property name="incomplete" value="false">`, `<property name="delivery_unconfirmed" value="`+warn+`">`, `failures="0"`)

	// An incomplete session: the reasons are properties too.
	bad := info
	bad.Incomplete, bad.IncompleteReasons = true, []string{"service orders-api (instance o-1): 0 of 7 recorded events reached the collector"}
	junit, err = renderJUnit(buildReport(bad, sum, client.Trace{}, nil))
	if err != nil {
		panic(err)
	}
	wantAll(t, string(junit), `<property name="incomplete" value="true">`,
		`<property name="incomplete_reason" value="Events did not reach the collector: service orders-api (instance o-1): 0 of 7 recorded events reached the collector">`,
		`<property name="delivery_unconfirmed" value="`+warn+`">`)
	// A reason that does not say that events are missing is not introduced as
	// one that does.
	bad.IncompleteReasons = []string{"the collector has 6 events from orders-api/o-1, which did not report how many it recorded"}
	junit, err = renderJUnit(buildReport(bad, sum, client.Trace{}, nil))
	if err != nil {
		panic(err)
	}
	wantAll(t, string(junit), `<property name="incomplete_reason" value="Delivery is not accounted for: the collector has 6 events from orders-api/o-1, which did not report how many it recorded">`)

	// The digest: in the verdict section and in the JSON, within the smallest
	// budget however many instances there are, and the same every time.
	many := info
	for i := range 40 {
		many.DeliveryUnconfirmed = append(many.DeliveryUnconfirmed, strings.Repeat("x", 300)+string(rune('a'+i%26)))
	}
	for _, in := range []client.Info{info, many} {
		src := newDigestSource(in, sum, client.Trace{}, nil)
		dmd, err := src.fit(digestMarkdown{}, digestMinBytes)
		if err != nil || len(dmd) > digestMinBytes {
			panic(fmt.Sprintf("digest markdown: %d bytes, %v", len(dmd), err))
		}
		checkDigestMarkdown(t, string(dmd))
		wantAll(t, string(dmd), "**Passed**", "- delivery unconfirmed: service batch (instance b-1)")
		js, err := src.fit(digestJSON{}, digestMinBytes)
		if err != nil || len(js) > digestMinBytes {
			panic(fmt.Sprintf("digest JSON: %d bytes, %v", len(js), err))
		}
		d := checkDigestJSON(t, string(js))
		if d.Verdict.Status != "passed" || d.Verdict.Recording != "complete" || len(d.Verdict.Unconfirmed) == 0 ||
			len(d.Verdict.Unconfirmed) > digestMaxUnconfirmed+1 || !strings.HasPrefix(d.Verdict.Unconfirmed[0], "service batch (instance b-1)") {
			panic(fmt.Sprintf("digest verdict %+v", d.Verdict))
		}
		if len(in.DeliveryUnconfirmed) > digestMaxUnconfirmed && d.Verdict.Unconfirmed[digestMaxUnconfirmed] != "and 37 more" {
			panic(fmt.Sprintf("digest verdict %+v", d.Verdict.Unconfirmed))
		}
		again, _ := newDigestSource(in, sum, client.Trace{}, nil).fit(digestJSON{}, digestMinBytes)
		if !bytes.Equal(js, again) {
			panic("the digest is not deterministic")
		}
	}
	// A session without the warning says nothing of it.
	clean, _ := newDigestSource(client.Info{ID: "ts_3", State: "SEALED"}, sum, client.Trace{}, nil).fit(digestJSON{}, digestMinBytes)
	if strings.Contains(string(clean), "unconfirmed") {
		panic(fmt.Sprintf("digest of a healthy session: %s", clean))
	}
}

// The header of a summary says "incomplete" whenever the gate would: also
// for a session the control plane alone marked incomplete, with nothing the
// collector's summary shows (a drain that timed out, a seal that failed). It
// said "(SEALED)" for those, and --fail-on-incomplete exited 3 on them.
func TestSummaryHeaderSaysIncompleteForTheControlPlanesVerdict(t *testing.T) {
	sum := client.Summary{SessionID: "ts_3", Sealed: true, EventCount: 1, ByService: map[string]int{"batch": 1}}
	for name, info := range map[string]client.Info{
		"a drain that timed out": {ID: "ts_3", Name: "slow", State: "SEALED", Incomplete: true, DrainTimedOut: true, MaxDrainSeconds: 60},
		"a seal that failed":     {ID: "ts_3", Name: "slow", State: "SEALED", Incomplete: true, SealError: "collector unavailable"},
	} {
		var out bytes.Buffer
		printSummary(&out, info, sum)
		if !strings.Contains(out.String(), "session    ts_3  slow  (SEALED, incomplete)\n") {
			panic(fmt.Sprintf("%s:\n%s", name, out.String()))
		}
		if err := (&gates{onIncomplete: true}).check(info, sum, 0); err == nil {
			panic(fmt.Sprintf("%s: the gate passes a session whose header says incomplete", name))
		}
	}
	var out bytes.Buffer
	printSummary(&out, client.Info{ID: "ts_3", Name: "fine", State: "SEALED"}, sum)
	if !strings.Contains(out.String(), "session    ts_3  fine  (SEALED)\n") {
		panic(out.String())
	}
}
