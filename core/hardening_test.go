package core

import (
	"strings"
	"testing"
)

func TestHLCObserveClampsFutureRemote(t *testing.T) {
	before := HLCNow()
	bogus := physical() + MaxHLCSkew*100
	got := HLCObserve(bogus)
	if got >= bogus {
		t.Fatalf("bogus remote merged: %d >= %d", got, bogus)
	}
	if got <= before {
		t.Fatalf("not monotonic")
	}
	ok := physical() + (MaxHLCSkew / 2)
	if got := HLCObserve(ok); got <= ok {
		t.Fatalf("in-range remote should merge: %d <= %d", got, ok)
	}
	// max uint64 must not wrap
	if got := HLCObserve(^uint64(0)); got == 0 || got > physical()+MaxHLCSkew+(1<<40) {
		t.Fatalf("overflow guard failed: %d", got)
	}
}

func TestBeginIngressClampsRemoteHLC(t *testing.T) {
	SetSessions(StaticSessions{Active: map[string]bool{"s1": true}})
	Configure(Config{Service: "x", Enabled: true})
	defer func() { SetSessions(nil); Configure(Config{}) }()
	m := map[string]string{KeySession: "s1", KeyHLC: "18446744073709551610"}
	_, sp := BeginIngress(t.Context(), KindAPIIn, "x", MapCarrier(m))
	if sp == nil {
		t.Fatal("no span")
	}
	if sp.ev.HLC > physical()+MaxHLCSkew {
		t.Fatalf("span HLC %d beyond skew", sp.ev.HLC)
	}
}

func TestInstanceAlwaysUnique(t *testing.T) {
	Configure(Config{Instance: "fixed"})
	a := GetConfig().Instance
	Configure(Config{Instance: "fixed"})
	b := GetConfig().Instance
	defer Configure(Config{})
	if a == b || !strings.HasPrefix(a, "fixed-") || !strings.HasPrefix(b, "fixed-") {
		t.Fatalf("instances not unique/prefixed: %q %q", a, b)
	}
}

func TestPayloadRedactsBeforeTruncating(t *testing.T) {
	Configure(Config{MaxInlineBytes: 64, Enabled: true})
	defer Configure(Config{})
	// big valid JSON: secret sits right at the cut
	j := `{"a":"` + strings.Repeat("x", 40) + `","password":"hunter2hunter2","z":"` + strings.Repeat("y", 100) + `"}`
	p := Payload([]byte(j))
	if strings.Contains(string(p.Inline), "hunter2") || !p.Truncated || p.Size != len(j) {
		t.Fatalf("json: %+v %s", p, p.Inline)
	}
	// non-JSON text
	txt := "grant=x&" + strings.Repeat("a", 50) + "&password=hunter2hunter2&" + strings.Repeat("b", 100)
	p = Payload(txt)
	if strings.Contains(string(p.Inline), "hunter2") || !p.Truncated || !p.Redacted || p.Size != len(txt) {
		t.Fatalf("text: %+v %s", p, p.Inline)
	}
	p = Payload([]byte("not json token=abc123"))
	if strings.Contains(string(p.Inline), "abc123") {
		t.Fatalf("bytes: %s", p.Inline)
	}
}
