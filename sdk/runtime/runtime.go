package runtime

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"tsession/core"
)

// Config configures Start. Zero values get defaults.
type Config struct {
	Service         string
	Instance        string
	ControlPlaneURL string // default http://127.0.0.1:7070
	CollectorURL    string // default http://127.0.0.1:7071
	BufferSize      int    // ring capacity (default 8192)
	Exporter        ExporterConfig
	// PatchDefaultTransport wraps http.DefaultTransport with Transport
	// (idempotent; restored by Stop).
	PatchDefaultTransport bool
}

type state struct {
	rec      *Recorder
	exp      *Exporter
	sub      *Subscriber
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
	if cfg.ControlPlaneURL == "" {
		cfg.ControlPlaneURL = "http://127.0.0.1:7070"
	}
	if cfg.CollectorURL == "" {
		cfg.CollectorURL = "http://127.0.0.1:7071"
	}
	cfg.Exporter.CollectorURL = cfg.CollectorURL

	core.Configure(core.Config{Service: cfg.Service, Instance: cfg.Instance, Enabled: true})
	s := &state{rec: NewRecorder(cfg.BufferSize)}
	s.exp = NewExporter(s.rec, cfg.Exporter)
	s.sub = NewSubscriber(cfg.ControlPlaneURL)
	core.SetRecorder(s.rec)
	core.SetSessions(s.sub)
	if cfg.PatchDefaultTransport {
		if _, ok := http.DefaultTransport.(*Transport); !ok {
			s.origTrip = http.DefaultTransport
			http.DefaultTransport = &Transport{Base: s.origTrip}
			s.patched = true
		}
	}
	cur = s
	return nil
}

// Flush synchronously exports buffered events (bounded). No-op if not started.
func Flush(timeout time.Duration) bool {
	mu.Lock()
	s := cur
	mu.Unlock()
	if s == nil {
		return true
	}
	return s.exp.Flush(timeout)
}

// Stop flushes, shuts down background work and restores core to disabled.
func Stop() {
	mu.Lock()
	s := cur
	cur = nil
	mu.Unlock()
	if s == nil {
		return
	}
	core.SetSessions(nil)
	s.exp.Close(2 * time.Second)
	core.SetRecorder(nil)
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
