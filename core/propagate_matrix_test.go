package core

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"
)

// The compatibility matrix of docs/PROTOCOL.md ("Trace context propagation"):
// which parent a receiver records, for every combination of SDK version and
// application tracing on the hops before it. TestPropagationMatrix runs every
// combination against the real Inject and ExtractSpanContext and against
// copies of what SDKs before x-tsession-traceparent did (oldInject,
// oldExtract), checks the result against the rule stated in matrixLinks, and
// checks that the tables in the documentation are the ones it produced.
//
// To rewrite the tables after a change in behaviour:
//
//	TSESSION_UPDATE_DOCS=1 go test ./core -run TestPropagationMatrix

const (
	sdkOld          = "old"
	sdkNew          = "new"
	sdkNewOverwrite = "new, overwrite setting on"

	appNone   = "none"
	appBefore = "writes before tsession"
	appAfter  = "writes after tsession"
)

var (
	matrixWriters   = []string{sdkOld, sdkNew, sdkNewOverwrite}
	matrixReceivers = []string{sdkOld, sdkNew}
	matrixApps      = []string{appNone, appBefore, appAfter}
)

// matrixHop is a service that writes a carrier: its SDK, and whether its
// application tracing writes traceparent on the same carrier before tsession
// does (tsession's transport wraps it, or it set the message header first) or
// after.
type matrixHop struct{ sdk, app string }

func (h matrixHop) String() string { return h.sdk + " / " + h.app }

// send writes what the hop puts on c for a call made under span context sc.
// appVal is the traceparent of the hop's application tracing.
func (h matrixHop) send(c MapCarrier, sc SpanContext, appVal string) {
	if h.app == appBefore {
		c[KeyTraceparent] = appVal
	}
	if h.sdk == sdkOld {
		oldInject(c, sc)
	} else {
		Configure(Config{Service: "svc", Enabled: true, OverwriteTraceparent: h.sdk == sdkNewOverwrite})
		Inject(ContextWith(context.Background(), sc), c)
	}
	if h.app == appAfter {
		c[KeyTraceparent] = appVal
	}
}

// matrixReceive is the ingress of a service on the given SDK: the span context
// it continues, with the sender's clock observed as startSpan does.
func matrixReceive(sdk string, c MapCarrier) SpanContext {
	sc, ok := ExtractSpanContext(c)
	if sdk == sdkOld {
		sc, ok = oldExtract(c)
	}
	if !ok {
		panic("carrier without a session")
	}
	if sc.HLC > 0 {
		HLCObserve(sc.HLC)
	}
	return sc
}

// matrixCell is one combination and what happened.
type matrixCell struct {
	sender    matrixHop
	forwarder *matrixHop // nil: the sender calls the receiver directly
	receiver  string

	links  bool   // the receiver's parent is the span of the hop that sent to it
	parent string // what the receiver's parent is
	app    string // what became of the application's traceparent on the last hop
}

// last is the hop that sent to the receiver.
func (c matrixCell) last() matrixHop {
	if c.forwarder != nil {
		return *c.forwarder
	}
	return c.sender
}

// matrixLinks states which combinations link, as a rule about the hop that
// sent to the receiver. It is written from the design, not from the code: the
// test fails when the code and this rule disagree.
func matrixLinks(c matrixCell) bool {
	last := c.last()
	switch last.app {
	case appAfter:
		// traceparent is the application's. Only x-tsession-traceparent
		// names tsession's span: a new hop wrote it, a new receiver reads it.
		return last.sdk != sdkOld && c.receiver == sdkNew
	case appBefore:
		// An old SDK and the overwrite setting replace the application's
		// value, which every receiver then reads. Otherwise as above.
		return last.sdk != sdkNew || c.receiver == sdkNew
	}
	// No application tracing on the last hop.
	if c.forwarder == nil || last.sdk != sdkNew || c.receiver == sdkNew {
		return true
	}
	// A new forwarder passes on the traceparent it received unless it is a
	// copy of the x-tsession-traceparent that came with it; an old receiver
	// reads traceparent. It is such a copy when a new sender wrote both.
	return c.sender.sdk != sdkOld && (c.sender.app == appNone || (c.sender.sdk == sdkNewOverwrite && c.sender.app == appBefore))
}

const (
	matrixSenderApp    = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	matrixForwarderApp = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
)

// run fills in the outcome of c.
func (c *matrixCell) run() {
	root := SpanContext{SessionID: "s1", TraceID: NewTraceID(), SpanID: NewSpanID()}
	wire := MapCarrier{}
	c.sender.send(wire, root, matrixSenderApp)
	last, lastApp := root, matrixSenderApp
	if c.forwarder != nil {
		in := matrixReceive(c.forwarder.sdk, wire)
		last = SpanContext{SessionID: "s1", TraceID: in.TraceID, SpanID: NewSpanID()}
		wire = maps.Clone(wire) // the keys it received, passed on
		c.forwarder.send(wire, last, matrixForwarderApp)
		lastApp = matrixForwarderApp
	}
	got := matrixReceive(c.receiver, wire)

	c.links = got.TraceID == last.TraceID && got.SpanID == last.SpanID
	switch tp := "00-" + got.TraceID + "-" + got.SpanID + "-01"; {
	case c.links:
		c.parent = "correct"
	case tp == matrixSenderApp && c.forwarder != nil:
		c.parent = "the span of the first sender's application tracing"
	case tp == matrixSenderApp, tp == matrixForwarderApp:
		c.parent = "the application's span"
	case got.SpanID == root.SpanID:
		c.parent = "the first sender's span"
	default:
		c.parent = "unexpected: " + tp
	}
	switch {
	case c.last().app == appNone:
		c.app = "(none)"
	case wire[KeyTraceparent] == lastApp:
		c.app = "intact"
	default:
		c.app = "replaced"
	}
}

func matrixCells() (direct, forwarded []matrixCell) {
	var hops []matrixHop
	for _, sdk := range matrixWriters {
		for _, app := range matrixApps {
			hops = append(hops, matrixHop{sdk, app})
		}
	}
	for _, s := range hops {
		for _, r := range matrixReceivers {
			direct = append(direct, matrixCell{sender: s, receiver: r})
		}
	}
	for _, f := range hops {
		for _, r := range matrixReceivers {
			for _, s := range hops {
				forwarded = append(forwarded, matrixCell{sender: s, forwarder: &f, receiver: r})
			}
		}
	}
	return direct, forwarded
}

// matrixDoc renders the two tables of docs/PROTOCOL.md.
func matrixDoc(direct, forwarded []matrixCell) string {
	var b strings.Builder
	b.WriteString("Sender calls receiver:\n\n")
	b.WriteString("| Sender SDK | Application tracing on the sender | Receiver SDK | Receiver's tsession parent | Application's `traceparent` |\n|---|---|---|---|---|\n")
	for _, c := range direct {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", c.sender.sdk, c.sender.app, c.receiver, c.parent, c.app)
	}

	// One row per forwarder and receiver: the first senders with which the
	// receiver does not link to the forwarder.
	b.WriteString("\nSender calls a forwarder, which passes the keys it received on to the receiver. The receiver's parent should be\nthe forwarder's span. Combinations that are not listed link correctly:\n\n")
	b.WriteString("| Forwarder SDK | Application tracing on the forwarder | Receiver SDK | First sender (SDK / application tracing) | Receiver's tsession parent |\n|---|---|---|---|---|\n")
	total, wrong := 0, 0
	for i := 0; i < len(forwarded); {
		f, r := *forwarded[i].forwarder, forwarded[i].receiver
		// Group this forwarder and receiver's failing senders by outcome.
		var order []string
		by := map[string][]string{}
		n := 0
		for ; i < len(forwarded) && *forwarded[i].forwarder == f && forwarded[i].receiver == r; i++ {
			c := forwarded[i]
			n++
			if c.links {
				continue
			}
			if _, ok := by[c.parent]; !ok {
				order = append(order, c.parent)
			}
			by[c.parent] = append(by[c.parent], c.sender.String())
		}
		total += n
		for _, parent := range order {
			senders := strings.Join(by[parent], "; ")
			if len(by[parent]) == n {
				senders = "any"
			}
			wrong += len(by[parent])
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", f.sdk, f.app, r, senders, parent)
		}
	}
	fmt.Fprintf(&b, "\n%d of the %d forwarded combinations link correctly.\n", total-wrong, total)
	return b.String()
}

const (
	matrixDocFile  = "../docs/PROTOCOL.md"
	matrixDocBegin = "<!-- matrix:begin (written by core.TestPropagationMatrix; do not edit) -->\n"
	matrixDocEnd   = "<!-- matrix:end -->\n"
)

func TestPropagationMatrix(t *testing.T) {
	propSetup(t, Config{})
	direct, forwarded := matrixCells()
	for _, cells := range [][]matrixCell{direct, forwarded} {
		for i := range cells {
			c := &cells[i]
			c.run()
			via := "directly"
			if c.forwarder != nil {
				via = "via forwarder [" + c.forwarder.String() + "]"
			}
			t.Logf("sender [%s] %s to %s receiver: parent %s; application's traceparent %s", c.sender, via, c.receiver, c.parent, c.app)

			if want := matrixLinks(*c); c.links != want {
				t.Errorf("sender [%s] %s to %s receiver: links=%v (parent: %s), want links=%v", c.sender, via, c.receiver, c.links, c.parent, want)
			}
			if strings.HasPrefix(c.parent, "unexpected") {
				t.Errorf("sender [%s] %s to %s receiver: parent %s", c.sender, via, c.receiver, c.parent)
			}
			// What this change is for: without the setting, a new SDK never
			// replaces the traceparent of its own application's tracing.
			if c.last().sdk == sdkNew && c.app == "replaced" {
				t.Errorf("sender [%s] %s: the application's traceparent was replaced", c.sender, via)
			}
		}
	}

	doc := matrixDoc(direct, forwarded)
	t.Log("\n" + doc)
	md, err := os.ReadFile(matrixDocFile)
	if err != nil {
		t.Fatal(err)
	}
	head, rest, ok := strings.Cut(string(md), matrixDocBegin)
	old, tail, ok2 := strings.Cut(rest, matrixDocEnd)
	if !ok || !ok2 {
		t.Fatalf("%s: matrix markers not found", matrixDocFile)
	}
	if old == doc {
		return
	}
	if os.Getenv("TSESSION_UPDATE_DOCS") == "" {
		t.Fatalf("%s does not hold the matrix this test produced; rerun with TSESSION_UPDATE_DOCS=1 and review the difference.\n--- documented\n%s\n--- produced\n%s", matrixDocFile, old, doc)
	}
	if err := os.WriteFile(matrixDocFile, []byte(head+matrixDocBegin+doc+matrixDocEnd+tail), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s rewritten", matrixDocFile)
}
