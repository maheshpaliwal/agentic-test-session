package tqueue

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"tsession/core"
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

// MemSideChannel is an in-memory SideChannel with TTL.
type MemSideChannel struct {
	Now func() time.Time // optional clock override
	mu  sync.Mutex
	m   map[string]memEntry
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
	var exp time.Time
	if ttl > 0 {
		exp = s.now().Add(ttl)
	}
	s.m[key] = memEntry{cp, exp}
	return nil
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

// PublishVia publishes with the trace context stored in sc under
// ts:ctx:{msgID} instead of in the message. Side-channel failures never fail
// the publish.
func PublishVia(ctx context.Context, sc SideChannel, msgID string, m Message, send func(ctx context.Context) error) error {
	carrier := core.MapCarrier{}
	return Publish(ctx, m, carrier, func(c context.Context) error {
		if sc != nil && msgID != "" && len(carrier) > 0 {
			_ = sc.Put(c, SideKey(msgID), carrier, DefaultTTL)
		}
		return send(c)
	})
}

// ConsumeVia looks the carrier up by message id; on a miss the handler runs
// untraced (unless an exclusive session applies).
func ConsumeVia(ctx context.Context, sc SideChannel, d Delivery, handler func(ctx context.Context) error) error {
	carrier := core.MapCarrier{}
	if sc != nil && d.MessageID != "" && core.Enabled() {
		if m, err := sc.Get(ctx, SideKey(d.MessageID)); err == nil {
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
		d := Delivery{System: system, Queue: queue, JobName: job, Params: json.RawMessage(rawOrString(payload)),
			Attempt: attempt, MessageID: msgID, MaxAttempts: maxAttempts}
		return ConsumeVia(ctx, sc, d, func(c context.Context) error { return h(c, msgID, attempt, payload) })
	}
}

func rawOrString(b []byte) []byte {
	if json.Valid(b) {
		return b
	}
	s, _ := json.Marshal(string(b))
	return s
}
