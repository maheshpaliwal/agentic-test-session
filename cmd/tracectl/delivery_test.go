package main

import (
	"bytes"
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
		t.Fatalf("--fail-on-incomplete: %v", err)
	}
	if err := (&gates{onError: true, onEmpty: true}).check(info, sum, 0); err != nil {
		t.Fatalf("without --fail-on-incomplete: %v", err)
	}
	// An unconfirmed delivery is a warning: it does not fail the gate.
	warn := client.Info{ID: "ts_2", State: "SEALED", DeliveryUnconfirmed: info.DeliveryUnconfirmed}
	if err := g.check(warn, sum, 0); err != nil {
		t.Fatalf("delivery_unconfirmed alone failed the gate: %v", err)
	}

	var out bytes.Buffer
	printSummary(&out, info, sum)
	for _, want := range []string{"(SEALED, incomplete)", "delivery   incomplete: " + reason, "delivery   unconfirmed: service batch (instance b-1)"} {
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
		t.Fatal(err)
	}
	if !rep.incomplete || len(rep.reasons) != 1 || !strings.Contains(rep.reasons[0], "Events did not reach the collector: service orders-api") ||
		strings.Contains(string(md), "**Passed**") || !strings.Contains(string(md), "Why it is incomplete") || !strings.Contains(string(md), "orders-api") {
		t.Fatalf("report: incomplete=%v reasons=%q\n%s", rep.incomplete, rep.reasons, md)
	}
}
