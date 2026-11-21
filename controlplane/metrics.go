package controlplane

import (
	"github.com/maheshpaliwal/agentic-test-session/internal/metrics"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

// telemetry is the control plane's own instrumentation, served at GET
// /metrics. Every Manager has its own registry.
type telemetry struct {
	reg *metrics.Registry

	started      *metrics.Counter
	finished     *metrics.CounterVec // state: SEALED | EXPIRED | ABORTED
	sealFailures *metrics.Counter
	// Background retries of a seal that failed past its budget.
	sealRetries    *metrics.CounterVec // result: success | failure
	sealsAbandoned *metrics.Counter
	deleted        *metrics.Counter
	// Panics recovered in the control plane's own goroutines.
	panics *metrics.CounterVec // goroutine: sweep | drain | seal_retry
}

func newTelemetry(m *Manager) *telemetry {
	r := metrics.New()
	version, goVersion := svc.Version()
	r.BuildInfo("controlplane", version, goVersion)
	r.GaugeVecFunc("tsession_controlplane_sessions", "Sessions that have not finished, by state.",
		[]string{"state"}, func() []metrics.Sample {
			m.mu.Lock()
			defer m.mu.Unlock()
			n := map[State]int{StateActive: 0, StateDraining: 0}
			for _, s := range m.live {
				n[s.State]++
			}
			out := make([]metrics.Sample, 0, len(n))
			for st, c := range n {
				out = append(out, metrics.Sample{Labels: []string{string(st)}, Value: float64(c)})
			}
			return out
		})
	r.GaugeFunc("tsession_controlplane_seals_pending",
		"Finished sessions whose collector seal failed and is still being retried.",
		func() float64 {
			m.mu.Lock()
			defer m.mu.Unlock()
			return float64(len(m.pending))
		})
	r.GaugeFunc("tsession_controlplane_subscribers", "Open active-set streams (one per connected SDK).",
		func() float64 {
			m.mu.Lock()
			defer m.mu.Unlock()
			return float64(len(m.subs))
		})
	r.GaugeFunc("tsession_controlplane_kill_switch", "1 while the global kill switch is on.",
		func() float64 {
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.kill {
				return 1
			}
			return 0
		})
	r.CounterFunc("tsession_controlplane_store_read_errors_total",
		"Stored sessions that could not be read (I/O error or damaged archive file); they are missing from lists.",
		func() float64 {
			if cs, ok := m.cfg.Store.(CheckedStore); ok {
				return float64(cs.ReadErrors())
			}
			return 0
		})
	return &telemetry{
		reg:     r,
		started: r.Counter("tsession_controlplane_sessions_started_total", "Sessions started."),
		finished: r.CounterVec("tsession_controlplane_sessions_finished_total",
			"Sessions that reached a terminal state, by state.", "state"),
		sealFailures: r.Counter("tsession_controlplane_seal_failures_total",
			"Sessions whose collector seal failed after all retries (recorded incomplete)."),
		sealRetries: r.CounterVec("tsession_controlplane_seal_retries_total",
			"Background attempts to seal a session whose seal failed past its budget, by result.", "result"),
		sealsAbandoned: r.Counter("tsession_controlplane_seals_abandoned_total",
			"Sessions whose pending seal was given up after the pending budget (seal_error stays)."),
		deleted: r.Counter("tsession_controlplane_sessions_deleted_total", "Sessions deleted through the API."),
		panics: r.CounterVec("tsession_controlplane_goroutine_panics_total",
			"Panics recovered in a background goroutine, by goroutine (each is logged with its stack; sweep and drain are restarted with backoff).", "goroutine"),
	}
}
