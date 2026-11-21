package core

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// An application traceparent, as OpenTelemetry would write it: ids that are
// not tsession's.
const appTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func propSetup(t *testing.T, c Config) (context.Context, SpanContext) {
	t.Helper()
	c.Service, c.Enabled = "svc", true
	Configure(c)
	SetSessions(StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { Configure(Config{}); SetSessions(nil) })
	sc := SpanContext{SessionID: "s1", TraceID: NewTraceID(), SpanID: NewSpanID()}
	return ContextWith(context.Background(), sc), sc
}

func tpOf(sc SpanContext) string { return "00-" + sc.TraceID + "-" + sc.SpanID + "-01" }

// ownOf returns the trace context in c's x-tsession-traceparent, or "" when
// the key is absent or is not bound to c's x-tsession-hlc.
func ownOf(c Carrier) string {
	return ownTraceparent(c.Get(KeyTSessionTraceparent), c.Get(KeyHLC))
}

// oldExtract is ExtractSpanContext as SDKs before x-tsession-traceparent
// implemented it: the trace context comes from traceparent only.
func oldExtract(c Carrier) (SpanContext, bool) {
	sid := c.Get(KeySession)
	if sid == "" {
		return SpanContext{}, false
	}
	sc := SpanContext{SessionID: sid}
	if parts := strings.Split(c.Get(KeyTraceparent), "-"); len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		sc.TraceID, sc.SpanID = parts[1], parts[2]
	} else {
		sc.TraceID = NewTraceID()
	}
	if h := c.Get(KeyHLC); h != "" {
		sc.HLC, _ = strconv.ParseUint(h, 10, 64)
	}
	return sc, true
}

// oldInject is Inject as those SDKs implemented it.
func oldInject(c Carrier, sc SpanContext) {
	c.Set(KeyTraceparent, tpOf(sc))
	c.Set(KeySession, sc.SessionID)
	c.Set(KeyHLC, strconv.FormatUint(HLCNow(), 10))
}

func TestInjectWritesBothKeysWhenCarrierHasNone(t *testing.T) {
	ctx, sc := propSetup(t, Config{})
	for name, c := range map[string]Carrier{"map": MapCarrier{}, "http": HTTPHeaderCarrier(http.Header{})} {
		Inject(ctx, c)
		if got, want := c.Get(KeyTSessionTraceparent), tpOf(sc)+";"+c.Get(KeyHLC); got != want {
			t.Errorf("%s: %s = %q, want %q", name, KeyTSessionTraceparent, got, want)
		}
		if got := c.Get(KeyTraceparent); got != tpOf(sc) {
			t.Errorf("%s: traceparent = %q, want tsession's %q", name, got, tpOf(sc))
		}
		if _, err := strconv.ParseUint(c.Get(KeyHLC), 10, 64); c.Get(KeySession) != "s1" || err != nil {
			t.Errorf("%s: session or clock missing", name)
		}
	}
}

func TestInjectLeavesApplicationTraceparent(t *testing.T) {
	ctx, sc := propSetup(t, Config{})
	h := http.Header{}
	h.Set("Traceparent", appTP)
	h.Set("Tracestate", "vendor=1")
	Inject(ctx, HTTPHeaderCarrier(h))
	if got := h.Values("Traceparent"); len(got) != 1 || got[0] != appTP {
		t.Fatalf("application traceparent changed: %q", got)
	}
	if h.Get("Tracestate") != "vendor=1" {
		t.Fatal("tracestate changed")
	}
	if got := ownOf(HTTPHeaderCarrier(h)); got != tpOf(sc) {
		t.Fatalf("%s holds %q, want %q", KeyTSessionTraceparent, got, tpOf(sc))
	}
	got, ok := ExtractSpanContext(HTTPHeaderCarrier(h))
	if !ok || got.TraceID != sc.TraceID || got.SpanID != sc.SpanID {
		t.Fatalf("extracted %+v, want tsession's ids %+v", got, sc)
	}
}

// A carrier that was filled by an earlier Inject and is injected again (a
// message republished with its headers copied) must end with both keys naming
// the new span: tsession overwrites its own key, and a traceparent that is a
// copy of tsession's own old value.
func TestInjectReplacesItsOwnStaleValues(t *testing.T) {
	ctx, sc := propSetup(t, Config{})
	first := SpanContext{SessionID: "s1", TraceID: NewTraceID(), SpanID: NewSpanID()}
	c := MapCarrier{}
	Inject(ContextWith(context.Background(), first), c)
	if c.Get(KeyTraceparent) != tpOf(first) || ownOf(c) != tpOf(first) {
		t.Fatalf("first inject: %v", c)
	}
	Inject(ctx, c)
	if ownOf(c) != tpOf(sc) || c.Get(KeyTraceparent) != tpOf(sc) {
		t.Fatalf("stale context left behind: %v", c)
	}

	// With the application's traceparent in between, only the own key moves.
	c = MapCarrier{KeyTraceparent: appTP, KeyTSessionTraceparent: tpOf(first) + ";7", KeyHLC: "7"}
	Inject(ctx, c)
	if ownOf(c) != tpOf(sc) || c.Get(KeyTraceparent) != appTP {
		t.Fatalf("got %v", c)
	}
}

func TestInjectOverwriteTraceparentSetting(t *testing.T) {
	ctx, sc := propSetup(t, Config{OverwriteTraceparent: true})
	c := MapCarrier{KeyTraceparent: appTP}
	Inject(ctx, c)
	if c.Get(KeyTraceparent) != tpOf(sc) || ownOf(c) != tpOf(sc) {
		t.Fatalf("opt-out did not overwrite: %v", c)
	}
	// What the setting is for: an old reader now links correctly.
	if got, _ := oldExtract(c); got.SpanID != sc.SpanID || got.TraceID != sc.TraceID {
		t.Fatalf("old reader got %+v", got)
	}
}

func TestInjectWithoutSessionWritesNothing(t *testing.T) {
	propSetup(t, Config{})
	c := MapCarrier{KeyTraceparent: appTP}
	Inject(context.Background(), c)
	if len(c) != 1 || c[KeyTraceparent] != appTP {
		t.Fatalf("carrier touched without a session: %v", c)
	}
}

func TestExtractSpanContextSources(t *testing.T) {
	const (
		ownTrace, ownSpan = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
		tpTrace, tpSpan   = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	)
	own, tp := "00-"+ownTrace+"-"+ownSpan+"-01", "00-"+tpTrace+"-"+tpSpan+"-01"
	for _, tc := range []struct {
		name        string
		c           MapCarrier
		trace, span string // "" trace: any fresh id
	}{
		{"own key preferred", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7", KeyHLC: "7", KeyTraceparent: tp}, ownTrace, ownSpan},
		{"own key alone", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7", KeyHLC: "7"}, ownTrace, ownSpan},
		{"old upstream: traceparent only", MapCarrier{KeySession: "s1", KeyTraceparent: tp, KeyHLC: "7"}, tpTrace, tpSpan},
		// The clock was replaced after the own key was written: an old SDK
		// passed the carrier on and wrote its context into traceparent.
		{"own key out of date", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7", KeyHLC: "8", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"own key out of date, no traceparent", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7", KeyHLC: "8"}, "", ""},
		// The clock was dropped on the way (an allow-list): no old SDK did
		// that, it always writes one.
		{"clock dropped", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7", KeyTraceparent: tp}, ownTrace, ownSpan},
		{"own key without a clock value", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own, KeyHLC: "7", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"own key with an empty clock value", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"malformed own key falls back", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "00-zz-01;7", KeyHLC: "7", KeyTraceparent: tp}, tpTrace, tpSpan},
		// Two values joined into one by a proxy.
		{"joined own key falls back", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own + ";7, " + own + ";7", KeyHLC: "7", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"empty own key falls back", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"both malformed", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "x;7", KeyHLC: "7", KeyTraceparent: "y"}, "", ""},
		{"neither", MapCarrier{KeySession: "s1"}, "", ""},
	} {
		sc, ok := ExtractSpanContext(tc.c)
		if !ok || sc.SessionID != "s1" {
			t.Fatalf("%s: not extracted", tc.name)
		}
		if sc.SpanID != tc.span || (tc.trace != "" && sc.TraceID != tc.trace) || len(sc.TraceID) != 32 {
			t.Errorf("%s: got trace %q span %q", tc.name, sc.TraceID, sc.SpanID)
		}
	}
	if _, ok := ExtractSpanContext(MapCarrier{KeyTSessionTraceparent: own + ";7", KeyHLC: "7", KeyTraceparent: tp}); ok {
		t.Error("a trace context without a session id is not a session")
	}
}

// The defect the clock binding exists for: a new sender, then a service on an
// old SDK that passes the carrier on with its keys copied (a retry topic, a
// reverse proxy), then a new receiver. The old SDK writes its own span into
// traceparent and x-tsession-hlc and copies the x-tsession-traceparent it does
// not know; the receiver must link to the old service, not to the sender.
func TestExtractIgnoresOwnKeyCopiedByOldHop(t *testing.T) {
	ctx, sender := propSetup(t, Config{})
	for _, app := range []string{"", appTP} {
		c := MapCarrier{}
		if app != "" {
			c[KeyTraceparent] = app // the sender's application tracing
		}
		Inject(ctx, c)

		in, _ := oldExtract(c)
		HLCObserve(in.HLC) // as the old SDK's ingress span did
		fwd := SpanContext{SessionID: "s1", TraceID: in.TraceID, SpanID: NewSpanID()}
		oldInject(c, fwd)
		if c.Get(KeyTSessionTraceparent) == "" || ownOf(c) != "" {
			t.Fatalf("app=%q: want the sender's own key still on the carrier and out of date: %v", app, c)
		}

		got, ok := ExtractSpanContext(c)
		if !ok || got.SpanID != fwd.SpanID || got.TraceID != fwd.TraceID {
			t.Errorf("app=%q: receiver's parent is %s/%s, want the forwarder's %s/%s (the sender's span is %s)",
				app, got.TraceID, got.SpanID, fwd.TraceID, fwd.SpanID, sender.SpanID)
		}
	}
}

// No hop that received a carrier can write the clock value the carrier came
// with: observing it moves the local clock past it.
func TestForwarderClockDiffersFromSenders(t *testing.T) {
	ctx, _ := propSetup(t, Config{})
	c := MapCarrier{}
	Inject(ctx, c)
	// A sender whose clock is ahead of this process by less than the skew
	// limit, which is the case where the local clock alone would not do.
	h, _ := strconv.ParseUint(c[KeyHLC], 10, 64)
	ahead := h + MaxHLCSkew/2
	c[KeyHLC] = strconv.FormatUint(ahead, 10)
	in, _ := ExtractSpanContext(c)
	HLCObserve(in.HLC)
	if now := HLCNow(); now <= ahead {
		t.Fatalf("forwarder's clock %d is not past the sender's %d", now, ahead)
	}
}

// The "equals the previous own key" rule compares the trace context inside
// the own key, whatever clock value follows it.
func TestWriteTraceparentReplacesCopyOfOwnKey(t *testing.T) {
	propSetup(t, Config{})
	const first, next = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01"
	for _, tc := range []struct {
		name string
		c    MapCarrier
		want string // traceparent afterwards
	}{
		{"copy of the own key", MapCarrier{KeyTraceparent: first, KeyTSessionTraceparent: first + ";7", KeyHLC: "7"}, next},
		{"copy of an out-of-date own key", MapCarrier{KeyTraceparent: first, KeyTSessionTraceparent: first + ";7", KeyHLC: "9"}, next},
		{"application's", MapCarrier{KeyTraceparent: appTP, KeyTSessionTraceparent: first + ";7", KeyHLC: "7"}, appTP},
		{"application's, no own key", MapCarrier{KeyTraceparent: appTP}, appTP},
		{"none", MapCarrier{}, next},
	} {
		WriteTraceparent(tc.c, next, "12")
		if tc.c[KeyTraceparent] != tc.want || tc.c[KeyTSessionTraceparent] != next+";12" || tc.c[KeyHLC] != "12" {
			t.Errorf("%s: %v", tc.name, tc.c)
		}
	}
}

// binaryCarrier is a carrier like SQS message attributes or an AMQP table: a
// key can hold a value that is not text, which Get cannot return.
type binaryCarrier struct {
	MapCarrier
	binary map[string][]byte
}

func (c binaryCarrier) Has(k string) bool {
	_, ok := c.binary[k]
	return ok || c.MapCarrier.Has(k)
}

func (c binaryCarrier) Set(k, v string) { delete(c.binary, k); c.MapCarrier.Set(k, v) }

// Whether the carrier has a traceparent is a question about the key, not
// about what Get makes of its value.
func TestInjectLeavesTraceparentThatIsNotText(t *testing.T) {
	ctx, sc := propSetup(t, Config{})

	bin := binaryCarrier{MapCarrier{}, map[string][]byte{KeyTraceparent: {1, 2, 3}}}
	Inject(ctx, bin)
	if _, text := bin.MapCarrier[KeyTraceparent]; text || len(bin.binary[KeyTraceparent]) != 3 {
		t.Errorf("binary traceparent replaced: %v %v", bin.MapCarrier, bin.binary)
	}
	if ownOf(bin) != tpOf(sc) {
		t.Errorf("own key not written: %v", bin.MapCarrier)
	}

	empty := MapCarrier{KeyTraceparent: ""}
	Inject(ctx, empty)
	if empty[KeyTraceparent] != "" || ownOf(empty) != tpOf(sc) {
		t.Errorf("empty traceparent: %v", empty)
	}

	h := http.Header{"Traceparent": {""}}
	Inject(ctx, HTTPHeaderCarrier(h))
	if v := h.Values("Traceparent"); len(v) != 1 || v[0] != "" || ownOf(HTTPHeaderCarrier(h)) != tpOf(sc) {
		t.Errorf("empty traceparent header: %v", h)
	}

	// An empty traceparent beside no own key is not "equal to the own key".
	c := MapCarrier{KeyTraceparent: "", KeyTSessionTraceparent: ""}
	Inject(ctx, c)
	if c[KeyTraceparent] != "" {
		t.Errorf("empty traceparent and own key: %v", c)
	}

	// The setting replaces it like any other.
	Configure(Config{Service: "svc", Enabled: true, OverwriteTraceparent: true})
	bin = binaryCarrier{MapCarrier{}, map[string][]byte{KeyTraceparent: {1, 2, 3}}}
	Inject(ctx, bin)
	if bin.MapCarrier[KeyTraceparent] != tpOf(sc) || len(bin.binary) != 0 {
		t.Errorf("overwrite setting: %v %v", bin.MapCarrier, bin.binary)
	}
}

// An application that stores its header with h["traceparent"] = ... instead of
// h.Set has one all the same: http.Header.Get does not find the key, but the
// header is sent. tsession must not add a Traceparent next to it.
func TestInjectSeesHTTPTraceparentUnderAnyKeyCase(t *testing.T) {
	ctx, sc := propSetup(t, Config{})
	// The headers are plain maps here: the keys are meant not to be canonical.
	for _, key := range []string{"traceparent", "TRACEPARENT", "TraceParent"} {
		h := map[string][]string{key: {appTP}, "tracestate": {"vendor=1"}}
		Inject(ctx, HTTPHeaderCarrier(h))
		if len(h[key]) != 1 || h[key][0] != appTP {
			t.Errorf("%s: application traceparent is now %q", key, h[key])
		}
		if _, added := h["Traceparent"]; added {
			t.Errorf("%s: a second traceparent header was added: %v", key, h)
		}
		if h["tracestate"][0] != "vendor=1" || len(h) != 5 {
			t.Errorf("%s: headers are now %v", key, h)
		}
		if got, ok := ExtractSpanContext(HTTPHeaderCarrier(h)); !ok || got.SpanID != sc.SpanID {
			t.Errorf("%s: extracted %+v", key, got)
		}
	}

	// The overwrite setting replaces the header where it is.
	Configure(Config{Service: "svc", Enabled: true, OverwriteTraceparent: true})
	h := map[string][]string{"traceparent": {appTP}}
	Inject(ctx, HTTPHeaderCarrier(h))
	if _, added := h["Traceparent"]; added || len(h["traceparent"]) != 1 || h["traceparent"][0] != tpOf(sc) {
		t.Errorf("overwrite setting: %v", h)
	}

	// tsession's own keys under such names are replaced as well, not doubled.
	Configure(Config{Service: "svc", Enabled: true})
	h = map[string][]string{"x-tsession-id": {"s0"}, "x-tsession-hlc": {"1"}, "x-tsession-traceparent": {"stale;1"}}
	Inject(ctx, HTTPHeaderCarrier(h))
	if len(h) != 4 || h["x-tsession-id"][0] != "s1" || h["x-tsession-hlc"][0] == "1" || h["x-tsession-traceparent"][0] == "stale;1" {
		t.Errorf("own keys under non-canonical names: %v", h)
	}
}

func TestCanonicalKeyMatchesNetHTTP(t *testing.T) {
	for _, k := range []string{KeyTraceparent, KeyTSessionTraceparent, KeySession, KeyHLC, "x-other", "Traceparent"} {
		if got, want := canonicalKey(k), http.CanonicalHeaderKey(k); got != want {
			t.Errorf("canonicalKey(%q) = %q, want %q", k, got, want)
		}
	}
}

func TestBeginIngressParentFromOwnKey(t *testing.T) {
	_, sc := propSetup(t, Config{})
	rec := &MemRecorder{}
	SetRecorder(rec)
	t.Cleanup(func() { SetRecorder(nil) })
	c := MapCarrier{KeySession: "s1", KeyTSessionTraceparent: tpOf(sc) + ";7", KeyHLC: "7", KeyTraceparent: appTP}
	_, sp := BeginIngress(context.Background(), KindAPIIn, "in", c)
	if sp == nil {
		t.Fatal("no span")
	}
	sp.End()
	ev := rec.Events()
	if len(ev) != 1 || ev[0].ParentID != sc.SpanID || ev[0].TraceID != sc.TraceID {
		t.Fatalf("parent link: %+v", ev)
	}
}
