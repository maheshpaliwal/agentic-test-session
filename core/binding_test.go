package core

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func jwt(payload string, sig string) string {
	e := base64.RawURLEncoding
	return e.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." + e.EncodeToString([]byte(payload)) + "." + sig
}

func TestJWTClaim(t *testing.T) {
	ok := jwt(`{"sub":"u_1","n":42,"admin":true,"obj":{"a":1}}`, "bogus-signature")
	// Documented behaviour: the signature is NOT verified.
	if got := JWTClaim(ok, "sub"); got != "u_1" {
		t.Fatalf("sub=%q", got)
	}
	if JWTClaim(ok, "n") != "42" || JWTClaim(ok, "admin") != "true" {
		t.Fatal("scalar claims")
	}
	if JWTClaim(ok, "obj") != "" || JWTClaim(ok, "missing") != "" {
		t.Fatal("non-scalar/missing claim must be empty")
	}
	none := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"u_none"}`)) + "."
	if JWTClaim(none, "sub") != "u_none" {
		t.Fatal("alg:none is read like any other (unverified)")
	}
	for _, bad := range []string{"", "x", "a.b", "a.!!!.c", "a." + base64.RawURLEncoding.EncodeToString([]byte("notjson")) + ".c", "a.b.c.d"} {
		if JWTClaim(bad, "sub") != "" {
			t.Fatalf("malformed %q yielded claim", bad)
		}
	}
	huge := jwt(`{"sub":"`+strings.Repeat("a", 20000)+`"}`, "s")
	if JWTClaim(huge, "sub") != "" {
		t.Fatal("huge token must be ignored")
	}
	long := jwt(`{"sub":"`+strings.Repeat("a", 300)+`"}`, "s")
	if JWTClaim(long, "sub") != "" {
		t.Fatal("oversized claim value ignored")
	}
	if BearerToken("Bearer abc") != "abc" || BearerToken("bearer abc") != "abc" || BearerToken("Basic abc") != "" || BearerToken("Bearer") != "" {
		t.Fatal("BearerToken")
	}
}

func TestJWTCacheTTLAndBound(t *testing.T) {
	defer func() { jwtNow = time.Now }()
	now := time.Now()
	jwtNow = func() time.Time { return now }
	tok := jwt(`{"sub":"a"}`, "s")
	JWTClaim(tok, "sub")
	jwtCache.Lock()
	n := len(jwtCache.m)
	jwtCache.Unlock()
	if n == 0 {
		t.Fatal("not cached")
	}
	for i := 0; i < jwtCacheMax*2; i++ {
		JWTClaim(jwt(`{"sub":"x`+strings.Repeat("y", i%7)+`"}`, string(rune('a'+i%26))+strings.Repeat("z", i/26)), "sub")
	}
	jwtCache.Lock()
	n = len(jwtCache.m)
	jwtCache.Unlock()
	if n > jwtCacheMax {
		t.Fatalf("cache unbounded: %d", n)
	}
	now = now.Add(2 * jwtCacheTTL)
	if JWTClaim(tok, "sub") != "a" {
		t.Fatal("expired entry must be recomputed")
	}
}

type mapAttrs struct {
	m     map[string]string
	calls int
}

func (a *mapAttrs) Get(s, n string) string { a.calls++; return a.m[s+":"+n] }

func TestBindingSetMatch(t *testing.T) {
	bs := NewBindingSet([]Binding{
		{"s1", "header", "X-User-ID", "u1"},
		{"s2", "cookie", "sid", "c2"},
		{"s3", "bogus", "n", "v"},                       // dropped
		{"s4", "header", "h", strings.Repeat("v", 300)}, // dropped
	})
	if len(bs.Keys()) != 2 {
		t.Fatalf("keys %v", bs.Keys())
	}
	a := &mapAttrs{m: map[string]string{"header:x-user-id": "u1"}}
	if id, k, ok := bs.Match(a); !ok || id != "s1" || k.String() != "header:x-user-id" {
		t.Fatal(id, k, ok)
	}
	if _, _, ok := bs.Match(&mapAttrs{m: map[string]string{"header:x-user-id": "U1"}}); ok {
		t.Fatal("values are case-sensitive")
	}
	if _, _, ok := (BindingSet{}).Match(a); ok {
		t.Fatal("empty matches")
	}
}

func setIngress(t *testing.T, s SessionSet) *MemRecorder {
	rec := &MemRecorder{}
	Configure(Config{Service: "svc", Enabled: true})
	SetRecorder(rec)
	SetSessions(s)
	t.Cleanup(func() { Configure(Config{}); SetRecorder(nil); SetSessions(nil) })
	return rec
}

func TestBeginIngressAttrsPrecedence(t *testing.T) {
	rec := setIngress(t, StaticSessions{
		Active:      map[string]bool{"hdr": true, "bound": true, "ex": true},
		ExclusiveID: "ex",
		Bindings:    []Binding{{"bound", "header", "x-user", "u1"}},
	})
	attrs := &mapAttrs{m: map[string]string{"header:x-user": "u1"}}
	// binding beats exclusive
	_, sp := BeginIngressAttrs(t.Context(), KindAPIIn, "a", MapCarrier{}, attrs)
	sp.End()
	// header beats binding
	_, sp = BeginIngressAttrs(t.Context(), KindAPIIn, "b", MapCarrier{KeySession: "hdr"}, attrs)
	sp.End()
	// no attrs/nil attrs => exclusive
	_, sp = BeginIngressAttrs(t.Context(), KindAPIIn, "c", MapCarrier{}, nil)
	sp.End()
	// BeginIngress unchanged
	_, sp = BeginIngress(t.Context(), KindAPIIn, "d", MapCarrier{})
	sp.End()
	// stale header session => untraced even with a binding
	if _, sp = BeginIngressAttrs(t.Context(), KindAPIIn, "e", MapCarrier{KeySession: "gone"}, attrs); sp != nil {
		t.Fatal("stale header should be untraced")
	}
	ev := rec.Events()
	if len(ev) != 4 || ev[0].SessionID != "bound" || ev[1].SessionID != "hdr" || ev[2].SessionID != "ex" || ev[3].SessionID != "ex" {
		t.Fatalf("%+v", ev)
	}
	if ev[0].Attrs["bound_by"] != "header:x-user" || strings.Contains(ev[0].Attrs["bound_by"], "u1") {
		t.Fatalf("attrs %v", ev[0].Attrs)
	}
	if _, has := ev[1].Attrs["bound_by"]; has {
		t.Fatal("header attribution must not claim bound_by")
	}
}

func TestNoBindingsNoExtraction(t *testing.T) {
	setIngress(t, StaticSessions{Active: map[string]bool{"s": true}})
	a := &mapAttrs{m: map[string]string{"header:x-user": "u1"}}
	_, sp := BeginIngressAttrs(t.Context(), KindAPIIn, "a", MapCarrier{}, a)
	if sp != nil || a.calls != 0 {
		t.Fatalf("no bindings => untraced and zero extraction; span=%v calls=%d", sp, a.calls)
	}
	var jwtBefore int
	jwtCache.Lock()
	jwtBefore = len(jwtCache.m)
	jwtCache.Unlock()
	ctx, c := t.Context(), MapCarrier{}
	allocs := testing.AllocsPerRun(200, func() { BeginIngressAttrs(ctx, KindAPIIn, "a", c, a) })
	if allocs != 0 {
		t.Fatalf("no-binding untraced path allocates: %v", allocs)
	}
	jwtCache.Lock()
	if len(jwtCache.m) != jwtBefore {
		t.Fatal("JWT decoded with no bindings")
	}
	jwtCache.Unlock()
}

func BenchmarkIngressNoBindings(b *testing.B) {
	Configure(Config{Service: "svc", Enabled: true})
	SetSessions(StaticSessions{Active: map[string]bool{"s": true}})
	defer func() { Configure(Config{}); SetSessions(nil) }()
	c, a := MapCarrier{}, &mapAttrs{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		BeginIngressAttrs(b.Context(), KindAPIIn, "a", c, a)
	}
}
