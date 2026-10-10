package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type activeSet struct {
	Active    []string `json:"active"`
	Exclusive string   `json:"exclusive"`
	Kill      bool     `json:"kill"`
}

type snapshot struct {
	active    map[string]struct{}
	exclusive string
	kill      bool
}

// Subscriber tracks the control plane's active set and implements
// core.SessionSet. It follows the SSE stream and falls back to polling the
// snapshot endpoint while the stream is down.
type Subscriber struct {
	base       string
	maxBackoff time.Duration
	idle       time.Duration // no bytes for this long => reconnect
	client     *http.Client  // own transport: unaffected by DefaultTransport patching
	snap       atomic.Pointer[snapshot]
	ready      atomic.Bool
	streaming  atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber starts following controlPlaneURL in the background.
func NewSubscriber(controlPlaneURL string) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Subscriber{
		base:       strings.TrimRight(controlPlaneURL, "/"),
		maxBackoff: 5 * time.Second,
		idle:       45 * time.Second,
		client:     &http.Client{Transport: newPlainTransport()},
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	s.snap.Store(&snapshot{})
	go s.run(ctx)
	return s
}

// IsActive reports whether id is ACTIVE/DRAINING and the kill switch is off.
func (s *Subscriber) IsActive(id string) bool {
	sn := s.snap.Load()
	if sn.kill {
		return false
	}
	_, ok := sn.active[id]
	return ok
}

// Exclusive returns the exclusive session id, if any (never while killed).
func (s *Subscriber) Exclusive() (string, bool) {
	sn := s.snap.Load()
	if sn.kill || sn.exclusive == "" {
		return "", false
	}
	return sn.exclusive, true
}

// Ready is true once any state has been received from the control plane.
func (s *Subscriber) Ready() bool { return s.ready.Load() }

// Close stops the subscriber.
func (s *Subscriber) Close() {
	s.cancel()
	<-s.done
}

func (s *Subscriber) apply(a activeSet) {
	sn := &snapshot{active: make(map[string]struct{}, len(a.Active)), exclusive: a.Exclusive, kill: a.Kill}
	for _, id := range a.Active {
		sn.active[id] = struct{}{}
	}
	s.snap.Store(sn)
	s.ready.Store(true)
}

func (s *Subscriber) run(ctx context.Context) {
	defer close(s.done)
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		got := s.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		if got {
			backoff = 200 * time.Millisecond
		}
		s.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, s.maxBackoff)
	}
}

func (s *Subscriber) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/v1/active", nil)
	if err != nil {
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var a activeSet
	if json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&a) == nil {
		s.apply(a)
	}
}

// stream follows one SSE connection until it ends; reports whether any event
// was applied.
func (s *Subscriber) stream(parent context.Context) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+"/v1/active/stream", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	s.streaming.Store(true)
	defer s.streaming.Store(false)

	watchdog := time.AfterFunc(s.idle, cancel)
	defer watchdog.Stop()

	got := false
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	var data strings.Builder
	for sc.Scan() {
		watchdog.Reset(s.idle)
		line := sc.Text()
		switch {
		case line == "":
			if data.Len() > 0 {
				var a activeSet
				if json.Unmarshal([]byte(data.String()), &a) == nil {
					s.apply(a)
					got = true
				}
				data.Reset()
			}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return got
}
