package metrics

import (
	"math"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestExposition(t *testing.T) {
	r := New()
	c := r.Counter("b_events_total", "Events seen.")
	v := r.CounterVec("a_rejected_total", "Rejected, by reason.", "reason", "kind")
	r.GaugeFunc("c_sessions", "Live sessions.", func() float64 { return 3 })
	r.GaugeVecFunc("d_by_state", "Sessions by state.", []string{"state"}, func() []Sample {
		return []Sample{{Labels: []string{"DRAINING"}, Value: 1}, {Labels: []string{"ACTIVE"}, Value: 2.5},
			{Labels: []string{"too", "many"}, Value: 9}} // malformed: skipped
	})
	c.Inc()
	c.Add(4)
	c.Add(-10) // ignored
	v.With("oversize", "x").Add(2)
	v.With(`quo"te\back`+"\nline", "y").Inc()
	v.With("oversize", "x").Inc()

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	want := `# HELP a_rejected_total Rejected, by reason.
# TYPE a_rejected_total counter
a_rejected_total{reason="oversize",kind="x"} 3
a_rejected_total{reason="quo\"te\\back\nline",kind="y"} 1
# HELP b_events_total Events seen.
# TYPE b_events_total counter
b_events_total 5
# HELP c_sessions Live sessions.
# TYPE c_sessions gauge
c_sessions 3
# HELP d_by_state Sessions by state.
# TYPE d_by_state gauge
d_by_state{state="ACTIVE"} 2.5
d_by_state{state="DRAINING"} 1
`
	if got := rec.Body.String(); got != want {
		t.Fatalf("exposition mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
	if c.Value() != 5 {
		t.Fatalf("value %d", c.Value())
	}
}

func TestFormatFloat(t *testing.T) {
	for in, want := range map[float64]string{0: "0", 1.5: "1.5", 1e21: "1e+21", math.Inf(1): "+Inf", math.Inf(-1): "-Inf"} {
		if got := formatFloat(in); got != want {
			t.Errorf("formatFloat(%v) = %q, want %q", in, got, want)
		}
	}
	if formatFloat(math.NaN()) != "NaN" {
		t.Error("NaN")
	}
}

func TestRegistrationErrorsPanic(t *testing.T) {
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected a panic", name)
			}
		}()
		f()
	}
	r := New()
	r.Counter("ok_total", "")
	mustPanic("duplicate", func() { r.Counter("ok_total", "") })
	mustPanic("bad name", func() { r.Counter("1bad", "") })
	mustPanic("bad name chars", func() { r.Counter("has-dash", "") })
	mustPanic("empty name", func() { r.Counter("", "") })
	mustPanic("bad label", func() { r.CounterVec("v_total", "", "bad label") })
	mustPanic("label arity", func() { r.CounterVec("w_total", "", "a").With("x", "y") })
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	c := r.Counter("n_total", "")
	v := r.CounterVec("v_total", "", "k")
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				c.Inc()
				v.With(string(rune('a' + (g+i)%4))).Inc()
				if i%100 == 0 {
					_, _ = r.WriteTo(&strings.Builder{})
				}
			}
		}()
	}
	wg.Wait()
	if c.Value() != 8000 {
		t.Fatalf("lost increments: %d", c.Value())
	}
	var sum uint64
	for _, k := range []string{"a", "b", "c", "d"} {
		sum += v.With(k).Value()
	}
	if sum != 8000 {
		t.Fatalf("vec lost increments: %d", sum)
	}
}
