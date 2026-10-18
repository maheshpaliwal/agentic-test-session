package core

import (
	"net/http"
	"testing"
)

func withPriority(t *testing.T, spec string) {
	t.Helper()
	Configure(Config{Service: "svc", Enabled: true, BindPriority: ParseBindPriority(spec)})
	t.Cleanup(func() { Configure(Config{}) })
}

func prioSessions() StaticSessions {
	return StaticSessions{
		Active: map[string]bool{"sU": true, "sN": true, "sX": true},
		Bindings: []Binding{
			{"sU", "header", "x-user-id", "100"},
			{"sN", "header", "name", "alice"},
			{"sX", "cookie", "sid", "c9"},
		},
	}
}

func ingress(t *testing.T, rec *MemRecorder, m map[string]string, car MapCarrier) *Event {
	t.Helper()
	n := len(rec.Events())
	_, sp := BeginIngressAttrs(t.Context(), KindAPIIn, "a", car, &mapAttrs{m: m})
	if sp == nil {
		return nil
	}
	sp.End()
	ev := rec.Events()
	if len(ev) != n+1 {
		t.Fatalf("events %d", len(ev))
	}
	return &ev[n]
}

func TestParseBindPriority(t *testing.T) {
	got := ParseBindPriority(" header:X-User-Id, jwt:sub,bogus:x,header:,nocolon,cookie:Sid,header:x-user-id,query:q ")
	want := []string{"header:x-user-id", "jwt_claim:sub", "cookie:Sid", "query:q"}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Fatalf("%v", got)
		}
	}
	if ParseBindPriority("") != nil {
		t.Fatal("empty")
	}
}

func TestPriorityPresentBound(t *testing.T) {
	rec := setIngress(t, prioSessions())
	withPriority(t, "header:x-user-id,header:name")
	setSessions := prioSessions()
	SetSessions(setSessions)
	ev := ingress(t, rec, map[string]string{"header:x-user-id": "100", "header:name": "alice"}, MapCarrier{})
	if ev == nil || ev.SessionID != "sU" || ev.Attrs["bound_by"] != "header:x-user-id" || ev.Attrs["bound_fallback"] != "" {
		t.Fatalf("%+v", ev)
	}
}

func TestPriorityPresentUnboundNoFallThrough(t *testing.T) {
	rec := setIngress(t, prioSessions())
	withPriority(t, "header:x-user-id,header:name")
	SetSessions(prioSessions())
	// uid present but unbound; name would match alice's session: must be untraced.
	if ev := ingress(t, rec, map[string]string{"header:x-user-id": "999", "header:name": "alice"}, MapCarrier{}); ev != nil {
		t.Fatalf("fell through: %+v", ev)
	}
	// also beats exclusive mode
	s := prioSessions()
	s.ExclusiveID = "sX"
	SetSessions(s)
	if ev := ingress(t, rec, map[string]string{"header:x-user-id": "999"}, MapCarrier{}); ev != nil {
		t.Fatalf("exclusive overrode: %+v", ev)
	}
}

func TestPriorityAbsentFallsBack(t *testing.T) {
	rec := setIngress(t, prioSessions())
	withPriority(t, "header:x-user-id,header:name")
	SetSessions(prioSessions())
	ev := ingress(t, rec, map[string]string{"header:name": "alice"}, MapCarrier{})
	if ev == nil || ev.SessionID != "sN" || ev.Attrs["bound_by"] != "header:name" || ev.Attrs["bound_fallback"] != "true" {
		t.Fatalf("%+v", ev)
	}
	// no priority key present: remaining (non-listed) keys, OR manner
	ev = ingress(t, rec, map[string]string{"cookie:sid": "c9"}, MapCarrier{})
	if ev == nil || ev.SessionID != "sX" || ev.Attrs["bound_fallback"] != "true" {
		t.Fatalf("%+v", ev)
	}
	// value never recorded
	for _, v := range ev.Attrs {
		if v == "c9" {
			t.Fatal("value leaked")
		}
	}
}

func TestPriorityUnsetKeepsOR(t *testing.T) {
	rec := setIngress(t, prioSessions())
	// uid unbound value + name bound: OR => matches via name (old behaviour)
	ev := ingress(t, rec, map[string]string{"header:x-user-id": "999", "header:name": "alice"}, MapCarrier{})
	if ev == nil || ev.SessionID != "sN" || ev.Attrs["bound_fallback"] != "" {
		t.Fatalf("%+v", ev)
	}
}

func TestAmbiguousUntraced(t *testing.T) {
	rec := setIngress(t, prioSessions())
	before := BindingAmbiguous()
	m := map[string]string{"header:x-user-id": "100", "header:name": "alice"} // two sessions
	if ev := ingress(t, rec, m, MapCarrier{}); ev != nil {
		t.Fatalf("ambiguous traced: %+v", ev)
	}
	if BindingAmbiguous() != before+1 {
		t.Fatal("counter")
	}
	// same session via two keys is not ambiguous
	s := prioSessions()
	s.Bindings = append(s.Bindings, Binding{"sU", "header", "name", "alice2"})
	SetSessions(s)
	if ev := ingress(t, rec, map[string]string{"header:x-user-id": "100", "header:name": "alice2"}, MapCarrier{}); ev == nil || ev.SessionID != "sU" {
		t.Fatalf("%+v", ev)
	}
}

func TestHeaderSessionBeatsPriorityAndAmbiguity(t *testing.T) {
	rec := setIngress(t, prioSessions())
	withPriority(t, "header:x-user-id,header:name")
	SetSessions(prioSessions())
	m := map[string]string{"header:x-user-id": "999", "header:name": "alice"}
	ev := ingress(t, rec, m, MapCarrier{KeySession: "sX"})
	if ev == nil || ev.SessionID != "sX" || ev.Attrs["bound_by"] != "" {
		t.Fatalf("%+v", ev)
	}
}

func TestPriorityNoBindingsNoExtraction(t *testing.T) {
	setIngress(t, StaticSessions{Active: map[string]bool{"s": true}})
	withPriority(t, "header:x-user-id,header:name")
	SetSessions(StaticSessions{Active: map[string]bool{"s": true}})
	a := &mapAttrs{m: map[string]string{"header:x-user-id": "1"}}
	ctx, c := t.Context(), MapCarrier{}
	if n := testing.AllocsPerRun(200, func() { BeginIngressAttrs(ctx, KindAPIIn, "a", c, a) }); n != 0 {
		t.Fatalf("allocs %v", n)
	}
	if a.calls != 0 {
		t.Fatal("extracted with no bindings")
	}
}

func TestPriorityMask(t *testing.T) {
	withPriority(t, "header:x-user-id,query:uid")
	SetSessions(StaticSessions{Active: map[string]bool{"s": true}}) // no active bindings
	h := http.Header{"X-User-Id": {"100"}, "Other": {"ok"}}
	out := RedactHeadersBound(h)
	if out["X-User-Id"][0] != RedactedValue || out["Other"][0] != "ok" {
		t.Fatalf("%v", out)
	}
	if q := MaskBoundQuery("uid=100&a=b"); q == "uid=100&a=b" || q[len(q)-3:] != "a=b" {
		t.Fatalf("%s", q)
	}
	Configure(Config{})
	if MaskBoundQuery("uid=100") != "uid=100" {
		t.Fatal("mask should be gone without priority")
	}
}
