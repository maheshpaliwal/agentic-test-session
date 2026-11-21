package collector

import (
	"github.com/maheshpaliwal/agentic-test-session/internal/metrics"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

// telemetry is the collector's own instrumentation, served at GET /metrics.
// Every Collector has its own registry, so several can live in one process.
type telemetry struct {
	reg *metrics.Registry

	ingested     *metrics.CounterVec // result: accepted | duplicate
	rejected     *metrics.CounterVec // reason
	ingestErrors *metrics.CounterVec // cause: backpressure | shutdown | storage
	// sessionFailures counts the sessions of ingest requests that failed, by
	// the same causes; partialFailures the requests in which only some did.
	sessionFailures *metrics.CounterVec
	partialFailures *metrics.Counter
	seals           *metrics.CounterVec // result: complete | incomplete | error
	deleted         *metrics.CounterVec // cause: api | retention
	quarantined     *metrics.Counter
	appendFailures  *metrics.Counter
}

func newTelemetry() *telemetry {
	r := metrics.New()
	version, goVersion := svc.Version()
	r.BuildInfo("collector", version, goVersion)
	return &telemetry{
		reg: r,
		ingested: r.CounterVec("tsession_collector_ingested_events_total",
			"Events received by ingest, by result (accepted or duplicate).", "result"),
		rejected: r.CounterVec("tsession_collector_rejected_events_total",
			"Events refused by ingest, by reason.", "reason"),
		ingestErrors: r.CounterVec("tsession_collector_ingest_errors_total",
			"Ingest requests that failed, by cause (backpressure, shutdown or storage).", "cause"),
		sessionFailures: r.CounterVec("tsession_collector_ingest_failed_sessions_total",
			"Sessions whose events an ingest request could not take, by cause (backpressure, shutdown or storage).", "cause"),
		partialFailures: r.Counter("tsession_collector_ingest_partial_failures_total",
			"Ingest requests that failed for some of their sessions and were accepted for the others."),
		seals: r.CounterVec("tsession_collector_seals_total",
			"Seal requests, by result (complete, incomplete or error).", "result"),
		deleted: r.CounterVec("tsession_collector_deleted_sessions_total",
			"Sessions deleted, by cause (api or retention).", "cause"),
		quarantined: r.Counter("tsession_collector_quarantined_events_total",
			"Poison events isolated and dropped because the store kept rejecting them."),
		appendFailures: r.Counter("tsession_collector_store_append_failures_total",
			"Failed attempts to append a batch of events to the store (each is retried)."),
	}
}

// observe registers the gauges that read live sequencer state.
func (t *telemetry) observe(s *Sequencer) {
	t.reg.GaugeFunc("tsession_collector_sessions_in_memory",
		"Sessions that currently have an in-memory actor.", func() float64 { return float64(s.actorCount()) })
	t.reg.GaugeFunc("tsession_collector_buffered_events",
		"Events waiting in reorder windows, not yet in the store.", func() float64 { return float64(s.buffered()) })
}
