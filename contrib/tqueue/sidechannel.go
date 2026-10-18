package tqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// DefaultTTL is how long side-channel carriers live.
const DefaultTTL = time.Hour

// SideChannel stores an injected carrier out-of-band for queues whose payload
// is opaque (asynq, Sidekiq-style). Get returns ErrNotFound on miss/expiry.
type SideChannel interface {
	Put(ctx context.Context, key string, carrier map[string]string, ttl time.Duration) error
	Get(ctx context.Context, key string) (map[string]string, error)
}

// SideKey is the key for a message id.
func SideKey(msgID string) string { return "ts:ctx:" + msgID }

// DefaultSideTimeout bounds side-channel Put/Get calls made on the app's
// send/handle path.
const DefaultSideTimeout = 200 * time.Millisecond

// DefaultMemMaxEntries is MemSideChannel's default size cap.
const DefaultMemMaxEntries = 10000

// Span flags set when propagation is skipped.
const (
	FlagAttrLimit   = "propagation_skipped:attr_limit"
	FlagSideChannel = "propagation_skipped:side_channel"
)

// MemSideChannel is an in-memory SideChannel with TTL. Expired entries are
// swept lazily on Put (at most once per second, or immediately when full) and
// the size is capped at MaxEntries (default DefaultMemMaxEntries; the entries
// closest to expiry are evicted first).
type MemSideChannel struct {
	Now        func() time.Time // optional clock override
	MaxEntries int              // <=0 means DefaultMemMaxEntries
	mu         sync.Mutex
	m          map[string]memEntry
	lastSweep  time.Time
}

type memEntry struct {
	c   map[string]string
	exp time.Time
}

func (s *MemSideChannel) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *MemSideChannel) max() int {
	if s.MaxEntries > 0 {
		return s.MaxEntries
	}
	return DefaultMemMaxEntries
}

// sweep drops expired entries. Caller holds mu.
func (s *MemSideChannel) sweep(now time.Time) {
	for k, e := range s.m {
		if !e.exp.IsZero() && !now.Before(e.exp) {
			delete(s.m, k)
		}
	}
	s.lastSweep = now
}

func (s *MemSideChannel) Put(_ context.Context, key string, c map[string]string, ttl time.Duration) error {
	cp := make(map[string]string, len(c))
	for k, v := range c {
		cp[k] = v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]memEntry{}
	}
	now := s.now()
	_, replacing := s.m[key]
	if now.Sub(s.lastSweep) >= time.Second || (!replacing && len(s.m) >= s.max()) {
		s.sweep(now)
	}
	for !replacing && len(s.m) >= s.max() {
		// still full: evict the entry that expires first (never-expiring last).
		var vk string
		var ve memEntry
		first := true
		for k, e := range s.m {
			if first || (!e.exp.IsZero() && (ve.exp.IsZero() || e.exp.Before(ve.exp))) {
				vk, ve, first = k, e, false
			}
		}
		delete(s.m, vk)
	}
	var exp time.Time
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	s.m[key] = memEntry{cp, exp}
	return nil
}

func (s *MemSideChannel) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

func (s *MemSideChannel) Get(_ context.Context, key string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok || (!e.exp.IsZero() && !s.now().Before(e.exp)) {
		delete(s.m, key)
		return nil, ErrNotFound
	}
	cp := make(map[string]string, len(e.c))
	for k, v := range e.c {
		cp[k] = v
	}
	return cp, nil
}

// RedisClient is the minimal surface a Redis library must expose; adapt
// go-redis (or any client) with a few lines. Get must return found=false on a
// missing key.
type RedisClient interface {
	SetEX(ctx context.Context, key, value string, ttl time.Duration) error
	Get(ctx context.Context, key string) (value string, found bool, err error)
}

// RedisSideChannel stores carriers as JSON in Redis.
type RedisSideChannel struct{ Client RedisClient }

func (r RedisSideChannel) Put(ctx context.Context, key string, c map[string]string, ttl time.Duration) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return r.Client.SetEX(ctx, key, string(b), ttl)
}

func (r RedisSideChannel) Get(ctx context.Context, key string) (map[string]string, error) {
	v, found, err := r.Client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(v), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// ViaOption configures PublishVia / ConsumeVia.
type ViaOption func(*viaCfg)

type viaCfg struct{ timeout time.Duration }

// WithSideTimeout bounds side-channel calls (default DefaultSideTimeout).
func WithSideTimeout(d time.Duration) ViaOption { return func(c *viaCfg) { c.timeout = d } }

func viaTimeout(opts []ViaOption) time.Duration {
	c := viaCfg{}
	for _, o := range opts {
		o(&c)
	}
	return c.timeout
}

// putBounded runs sc.Put with a timeout (d<=0 means DefaultSideTimeout). A
// slow or panicking side channel yields an error; the call never outlives the
// timeout from the caller's view (the Put goroutine may finish later).
func putBounded(ctx context.Context, sc SideChannel, key string, c map[string]string, d time.Duration) error {
	if d <= 0 {
		d = DefaultSideTimeout
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("tqueue: side-channel panic: %v", r)
			}
		}()
		done <- sc.Put(cctx, key, c, DefaultTTL)
	}()
	select {
	case err := <-done:
		return err
	case <-cctx.Done():
		return cctx.Err()
	}
}

func getBounded(ctx context.Context, sc SideChannel, key string, d time.Duration) (map[string]string, error) {
	if d <= 0 {
		d = DefaultSideTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	type res struct {
		m   map[string]string
		err error
	}
	done := make(chan res, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- res{nil, fmt.Errorf("tqueue: side-channel panic: %v", r)}
			}
		}()
		m, err := sc.Get(cctx, key)
		done <- res{m, err}
	}()
	select {
	case r := <-done:
		return r.m, r.err
	case <-cctx.Done():
		return nil, cctx.Err()
	}
}

// PublishVia publishes with the trace context stored in sc under
// ts:ctx:{msgID} instead of in the message. The Put is bounded by a short
// timeout (default 200ms, WithSideTimeout); on failure or timeout the span is
// flagged propagation_skipped:side_channel and the send proceeds regardless:
// the side channel never blocks or fails the app's send.
func PublishVia(ctx context.Context, sc SideChannel, msgID string, m Message, send func(ctx context.Context) error, opts ...ViaOption) error {
	carrier := core.MapCarrier{}
	to := viaTimeout(opts)
	return publish(ctx, m, carrier, func(c context.Context, span *core.Span) error {
		if sc != nil && msgID != "" && len(carrier) > 0 {
			if err := putBounded(c, sc, SideKey(msgID), carrier, to); err != nil {
				flag(span, FlagSideChannel)
			}
		}
		return send(c)
	})
}

// ConsumeVia looks the carrier up by message id (bounded by the same timeout);
// on a miss, error or timeout the handler runs untraced (unless an exclusive
// session applies).
func ConsumeVia(ctx context.Context, sc SideChannel, d Delivery, handler func(ctx context.Context) error, opts ...ViaOption) error {
	carrier := core.MapCarrier{}
	if sc != nil && d.MessageID != "" && core.Enabled() {
		if m, err := getBounded(ctx, sc, SideKey(d.MessageID), viaTimeout(opts)); err == nil {
			carrier = core.MapCarrier(m)
		}
	}
	return Consume(ctx, carrier, d, handler)
}

// WrapHandler adapts an opaque-payload handler (asynq-style: ctx, id, payload)
// so each invocation is traced via the side channel. The returned function is
// safe to wrap repeatedly.
func WrapHandler(sc SideChannel, system, queue, job string, maxAttempts int,
	h func(ctx context.Context, msgID string, attempt int, payload []byte) error,
) func(ctx context.Context, msgID string, attempt int, payload []byte) error {
	return func(ctx context.Context, msgID string, attempt int, payload []byte) error {
		d := Delivery{System: system, Queue: queue, JobName: job, Params: bodyParams(payload),
			Attempt: attempt, MessageID: msgID, MaxAttempts: maxAttempts}
		return ConsumeVia(ctx, sc, d, func(c context.Context) error { return h(c, msgID, attempt, payload) })
	}
}
