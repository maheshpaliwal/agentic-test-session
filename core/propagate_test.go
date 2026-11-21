package core

import (
	"context"
	"net/http"
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
	return sc, true
}

// oldInject is what those SDKs wrote.
func oldInject(c Carrier, sc SpanContext) {
	c.Set(KeyTraceparent, tpOf(sc))
	c.Set(KeySession, sc.SessionID)
	c.Set(KeyHLC, "1")
}

func TestInjectWritesBothKeysWhenCarrierHasNone(t *testing.T) {
	ctx, sc := propSetup(t, Config{})
	for name, c := range map[string]Carrier{"map": MapCarrier{}, "http": HTTPHeaderCarrier(http.Header{})} {
		Inject(ctx, c)
		if got := c.Get(KeyTSessionTraceparent); got != tpOf(sc) {
			t.Errorf("%s: %s = %q, want %q", name, KeyTSessionTraceparent, got, tpOf(sc))
		}
		if got := c.Get(KeyTraceparent); got != tpOf(sc) {
			t.Errorf("%s: traceparent = %q, want tsession's %q", name, got, tpOf(sc))
		}
		if c.Get(KeySession) != "s1" || c.Get(KeyHLC) == "" {
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
	if got := h.Get(KeyTSessionTraceparent); got != tpOf(sc) {
		t.Fatalf("%s = %q, want %q", KeyTSessionTraceparent, got, tpOf(sc))
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
	Inject(ctx, c)
	if c.Get(KeyTSessionTraceparent) != tpOf(sc) || c.Get(KeyTraceparent) != tpOf(sc) {
		t.Fatalf("stale context left behind: %v", c)
	}

	// With the application's traceparent in between, only the own key moves.
	c = MapCarrier{KeyTraceparent: appTP, KeyTSessionTraceparent: tpOf(first)}
	Inject(ctx, c)
	if c.Get(KeyTSessionTraceparent) != tpOf(sc) || c.Get(KeyTraceparent) != appTP {
		t.Fatalf("got %v", c)
	}
}

func TestInjectOverwriteTraceparentSetting(t *testing.T) {
	ctx, sc := propSetup(t, Config{OverwriteTraceparent: true})
	c := MapCarrier{KeyTraceparent: appTP}
	Inject(ctx, c)
	if c.Get(KeyTraceparent) != tpOf(sc) || c.Get(KeyTSessionTraceparent) != tpOf(sc) {
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
		{"own key preferred", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own, KeyTraceparent: tp}, ownTrace, ownSpan},
		{"own key alone", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: own}, ownTrace, ownSpan},
		{"old upstream: traceparent only", MapCarrier{KeySession: "s1", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"malformed own key falls back", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "00-zz-01", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"empty own key falls back", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "", KeyTraceparent: tp}, tpTrace, tpSpan},
		{"both malformed", MapCarrier{KeySession: "s1", KeyTSessionTraceparent: "x", KeyTraceparent: "y"}, "", ""},
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
	if _, ok := ExtractSpanContext(MapCarrier{KeyTSessionTraceparent: own, KeyTraceparent: tp}); ok {
		t.Error("a trace context without a session id is not a session")
	}
}

// The version matrix of docs/PROTOCOL.md ("Trace context"), one row per case.
func TestPropagationAcrossSDKVersions(t *testing.T) {
	ctx, sc := propSetup(t, Config{})

	t.Run("old upstream, new downstream", func(t *testing.T) {
		c := MapCarrier{}
		oldInject(c, sc)
		got, ok := ExtractSpanContext(c)
		if !ok || got.TraceID != sc.TraceID || got.SpanID != sc.SpanID {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("new upstream, old downstream, no application traceparent", func(t *testing.T) {
		c := MapCarrier{}
		Inject(ctx, c)
		got, ok := oldExtract(c)
		if !ok || got.TraceID != sc.TraceID || got.SpanID != sc.SpanID {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("new upstream, old downstream, application traceparent", func(t *testing.T) {
		c := MapCarrier{KeyTraceparent: appTP}
		Inject(ctx, c)
		got, ok := oldExtract(c)
		// The event still joins the session, with the application's ids.
		if !ok || got.SessionID != "s1" {
			t.Fatal("session lost")
		}
		if got.SpanID == sc.SpanID || !strings.Contains(appTP, got.TraceID+"-"+got.SpanID) {
			t.Fatalf("old reader got %+v; the note says it reads the application's ids", got)
		}
	})
	t.Run("old upstream overwrote, new downstream", func(t *testing.T) {
		c := MapCarrier{KeyTraceparent: appTP}
		oldInject(c, sc) // the defect: the application's header is gone
		got, _ := ExtractSpanContext(c)
		if got.SpanID != sc.SpanID {
			t.Fatalf("got %+v", got)
		}
	})
}

func TestBeginIngressParentFromOwnKey(t *testing.T) {
	_, sc := propSetup(t, Config{})
	rec := &MemRecorder{}
	SetRecorder(rec)
	t.Cleanup(func() { SetRecorder(nil) })
	c := MapCarrier{KeySession: "s1", KeyTSessionTraceparent: tpOf(sc), KeyTraceparent: appTP}
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
