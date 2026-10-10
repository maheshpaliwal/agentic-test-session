package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Service        string
	Instance       string
	Enabled        bool
	MaxInlineBytes int // payload cap; default 8192
}

var cfg atomic.Pointer[Config]

func init() { cfg.Store(&Config{MaxInlineBytes: 8192}) }

// Configure sets process-wide config. Safe to call once at startup (autoinit).
func Configure(c Config) {
	if c.MaxInlineBytes <= 0 {
		c.MaxInlineBytes = 8192
	}
	if c.Instance == "" {
		h, _ := os.Hostname()
		c.Instance = fmt.Sprintf("%s-%d-%s", h, os.Getpid(), randHex(3))
	}
	cfg.Store(&c)
}

func GetConfig() Config { return *cfg.Load() }
func Enabled() bool     { return cfg.Load().Enabled }

// Recorder receives finished events. Implementations must not block.
type Recorder interface{ Record(Event) }

type noopRecorder struct{}

func (noopRecorder) Record(Event) {}

type recBox struct{ r Recorder }

var rec atomic.Pointer[recBox]

func init() { rec.Store(&recBox{noopRecorder{}}) }

func SetRecorder(r Recorder) {
	if r == nil {
		r = noopRecorder{}
	}
	rec.Store(&recBox{r})
}

func record(e Event) { rec.Load().r.Record(e) }

// SessionSet answers "is this session accepting events?". Active includes
// DRAINING sessions (workers still run). Fed by the control plane stream.
type SessionSet interface {
	IsActive(id string) bool
	// Exclusive returns the session id to stamp onto requests that carry no
	// session header ("exclusive" mode), if any.
	Exclusive() (string, bool)
}

type noSessions struct{}

func (noSessions) IsActive(string) bool      { return false }
func (noSessions) Exclusive() (string, bool) { return "", false }

type setBox struct{ s SessionSet }

var sessions atomic.Pointer[setBox]

func init() { sessions.Store(&setBox{noSessions{}}) }

func SetSessions(s SessionSet) {
	if s == nil {
		s = noSessions{}
	}
	sessions.Store(&setBox{s})
}

func activeSessions() SessionSet { return sessions.Load().s }

// ---- ids & clocks ----

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func NewTraceID() string { return randHex(16) }
func NewSpanID() string  { return randHex(8) }

var (
	hlcMu   sync.Mutex
	hlcLast uint64
	localSq atomic.Uint64
)

func physical() uint64 { return uint64(time.Now().UnixMilli()) << 16 }

// HLCNow returns a strictly increasing hybrid logical timestamp.
func HLCNow() uint64 {
	hlcMu.Lock()
	defer hlcMu.Unlock()
	if p := physical(); p > hlcLast {
		hlcLast = p
	} else {
		hlcLast++
	}
	return hlcLast
}

// HLCObserve merges a remote timestamp and returns the new local value.
func HLCObserve(remote uint64) uint64 {
	hlcMu.Lock()
	defer hlcMu.Unlock()
	m := max(hlcLast, remote)
	if p := physical(); p > m {
		hlcLast = p
	} else {
		hlcLast = m + 1
	}
	return hlcLast
}

func nextLocalSeq() uint64 { return localSq.Add(1) }
