package runtime

import (
	"net/http/httptest"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

var _ core.AnyActiver = (*Subscriber)(nil)

func TestSnapshotAnyActive(t *testing.T) {
	s := &Subscriber{}
	s.snap.Store(&snapshot{})
	if s.AnyActive() {
		panic("active before any state arrived")
	}
	for _, tc := range []struct {
		name string
		set  activeSet
		want bool
	}{
		{"nothing", activeSet{}, false},
		{"one active", activeSet{Active: []string{"ts_1"}}, true},
		{"exclusive only", activeSet{Exclusive: "ts_x"}, true},
		{"active but killed", activeSet{Active: []string{"ts_1"}, Kill: true}, false},
		{"exclusive but killed", activeSet{Exclusive: "ts_x", Kill: true}, false},
	} {
		s.apply(tc.set)
		if got := s.AnyActive(); got != tc.want {
			t.Errorf("%s: AnyActive() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// core.AnyActive follows the control plane through the installed subscriber.
func TestSubscriberFeedsCoreAnyActive(t *testing.T) {
	cp := newFakeCP(`{"active":[],"exclusive":"","kill":false}`)
	srv := httptest.NewServer(cp.mux())
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetSessions(s)
	defer func() { core.SetSessions(nil); core.Configure(core.Config{}) }()

	eventually(t, "first state", s.Ready)
	if core.AnyActive() {
		panic("active with an empty set")
	}
	cp.set(`{"active":["ts_1"],"exclusive":"","kill":false}`)
	eventually(t, "session start", core.AnyActive)
	cp.set(`{"active":["ts_1"],"exclusive":"","kill":true}`)
	eventually(t, "kill", func() bool { return !core.AnyActive() })
	cp.set(`{"active":[],"exclusive":"ts_x","kill":false}`)
	eventually(t, "exclusive", core.AnyActive)
	cp.set(`{"active":[],"exclusive":"","kill":false}`)
	eventually(t, "session end", func() bool { return !core.AnyActive() })
}
