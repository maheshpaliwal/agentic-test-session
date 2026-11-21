package runtime

import (
	"cmp"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Config configures Start. Zero values get defaults.
type Config struct {
	Service         string
	Instance        string
	ControlPlaneURL string // default http://127.0.0.1:7070
	CollectorURL    string // default http://127.0.0.1:7071
	BufferSize      int    // ring capacity (default 8192)
	Exporter        ExporterConfig
	// Encoder overrides the ingest wire format (default ProtobufEncoder, the
	// only format the collector accepts); it overrides Exporter.Encoder when set.
	Encoder BatchEncoder
	// PatchDefaultTransport wraps http.DefaultTransport with Transport
	// (idempotent; restored by Stop).
	PatchDefaultTransport bool
	// PropagateHosts restricts session-header injection to these host suffixes
	// (empty = everyone). NoPropagate hosts never get headers and always win.
	// Requests to excluded hosts are still recorded as api.out.
	PropagateHosts []string
	NoPropagate    []string
	// BindPriority is the ordered binding fallback chain (env
	// TSESSION_BIND_PRIORITY). Local config, not read from the control plane:
	// every service in one environment must use the same list.
	BindPriority []core.BindingKey
	// OverwriteTraceparent makes propagation replace a traceparent the
	// application set (env TSESSION_OVERWRITE_TRACEPARENT). Off by default:
	// tsession carries its trace context in x-tsession-traceparent and leaves
	// the application's traceparent alone. See core.Config.
	OverwriteTraceparent bool
}

// Stats is a point-in-time diagnostics snapshot.
type Stats struct {
	// BindingAmbiguous counts requests left untraced because their bindings
	// matched two different sessions.
	BindingAmbiguous uint64
}

// GetStats returns diagnostics counters.
func GetStats() Stats { return Stats{BindingAmbiguous: core.BindingAmbiguous()} }

type state struct {
	rec      *Recorder
	exp      *Exporter
	sub      *Subscriber
	rep      *reporter
	origTrip http.RoundTripper
	patched  bool
}

var (
	mu  sync.Mutex
	cur *state
)

// ErrStarted is returned by Start when the runtime is already running.
var ErrStarted = errors.New("tsession runtime already started")

// Start enables the SDK: configures core, installs the recorder and the
// control-plane-backed session set. It returns immediately; all network work
// happens in background goroutines.
func Start(cfg Config) error {
	mu.Lock()
	defer mu.Unlock()
	if cur != nil {
		return ErrStarted
	}
	cfg.ControlPlaneURL = cmp.Or(cfg.ControlPlaneURL, "http://127.0.0.1:7070")
	cfg.CollectorURL = cmp.Or(cfg.CollectorURL, "http://127.0.0.1:7071")
	cfg.Exporter.CollectorURL = cfg.CollectorURL
	if cfg.Encoder != nil {
		cfg.Exporter.Encoder = cfg.Encoder
	}

	if w := core.WrappersWhileDisabled(); len(w) > 0 {
		log.Printf("tsession: %s called before the SDK started, so what was wrapped records nothing; start the SDK first (import sdk/autoinit in that package, or wrap inside main)", strings.Join(w, ", "))
	}
	core.Configure(core.Config{Service: cfg.Service, Instance: cfg.Instance, Enabled: true, BindPriority: cfg.BindPriority,
		OverwriteTraceparent: cfg.OverwriteTraceparent})
	s := &state{rec: NewRecorder(cfg.BufferSize)}
	s.exp = NewExporter(s.rec, cfg.Exporter)
	// The reports name the instance as the events do (core made it unique for
	// this process), not as the stream does; the stream says which that is.
	inst := core.GetConfig().Instance
	s.sub = newSubscriber(cfg.ControlPlaneURL, cfg.Service, cfg.Instance, inst)
	s.rep = newReporter(s.rec, s.exp, s.sub, cfg.Service, inst, reporterConfig{})
	core.SetRecorder(s.rec)
	core.SetSessions(s.sub)
	if cfg.PatchDefaultTransport {
		if _, ok := http.DefaultTransport.(*Transport); !ok {
			s.origTrip = http.DefaultTransport
			http.DefaultTransport = &Transport{Base: s.origTrip, PropagateHosts: cfg.PropagateHosts, NoPropagate: cfg.NoPropagate}
			s.patched = true
		}
	}
	cur = s
	return nil
}

// Flush synchronously exports buffered events (bounded). No-op if not started.
func Flush(timeout time.Duration) bool {
	mu.Lock()
	s := cur // one read: cannot panic
	mu.Unlock()
	if s == nil {
		return true
	}
	return s.exp.Flush(timeout)
}

// Stop flushes, shuts down background work and restores core to disabled.
func Stop() {
	mu.Lock()
	s := cur // a read and an assignment: cannot panic
	cur = nil
	mu.Unlock()
	if s == nil {
		return
	}
	core.SetSessions(nil)
	s.exp.Close(2 * time.Second)
	core.SetRecorder(nil)
	s.rep.close(2 * time.Second) // the last word on what was and was not delivered
	core.Configure(core.Config{})
	s.sub.Close()
	if s.patched {
		if _, ok := http.DefaultTransport.(*Transport); ok {
			http.DefaultTransport = s.origTrip
		}
	}
}

// ActiveRecorder returns the active ring (nil if not started); for diagnostics.
func ActiveRecorder() *Recorder {
	mu.Lock()
	defer mu.Unlock()
	if cur == nil {
		return nil
	}
	return cur.rec
}
