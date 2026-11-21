package core

import "testing"

// bareSet implements SessionSet only: it cannot say whether anything is active.
type bareSet struct{}

func (bareSet) IsActive(string) bool      { return false }
func (bareSet) Exclusive() (string, bool) { return "", false }

func TestAnyActive(t *testing.T) {
	defer func() { SetSessions(nil); Configure(Config{}) }()
	for _, tc := range []struct {
		name    string
		enabled bool
		set     SessionSet
		want    bool
	}{
		{"no session set installed", true, nil, false},
		{"empty set", true, StaticSessions{}, false},
		{"only inactive entries", true, StaticSessions{Active: map[string]bool{"s": false}}, false},
		{"one active session", true, StaticSessions{Active: map[string]bool{"s": true}}, true},
		{"exclusive session alone", true, StaticSessions{ExclusiveID: "ex"}, true},
		{"set that cannot tell", true, bareSet{}, true},
		{"sdk disabled", false, StaticSessions{Active: map[string]bool{"s": true}}, false},
		{"sdk disabled, set that cannot tell", false, bareSet{}, false},
	} {
		Configure(Config{Service: "x", Enabled: tc.enabled})
		SetSessions(tc.set)
		if got := AnyActive(); got != tc.want {
			t.Errorf("%s: AnyActive() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func BenchmarkAnyActive(b *testing.B) {
	Configure(Config{Service: "x", Enabled: true})
	SetSessions(StaticSessions{})
	defer func() { SetSessions(nil); Configure(Config{}) }()
	b.ReportAllocs()
	for b.Loop() {
		if AnyActive() {
			panic("active")
		}
	}
}
