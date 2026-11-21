package tredis

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/redisx"
)

// Option configures the hook.
type Option func(*hook)

// WithBeforeImage enables before/after image capture for keys matching any of
// the glob patterns (path.Match syntax, e.g. "user:*"; note * does not cross
// '/').
func WithBeforeImage(patterns ...string) Option {
	return func(h *hook) { h.Patterns = append(h.Patterns, patterns...) }
}

// WithReader sets the client used to read images. Wrap does this itself.
func WithReader(c redis.Cmdable) Option { return func(h *hook) { h.Reader = asReader(c) } }

// WithRedactKeys force-masks the values of keys matching any glob (path.Match
// syntax), in addition to keys whose name segments are sensitive per core.
func WithRedactKeys(globs ...string) Option {
	return func(h *hook) { h.Keys = append(h.Keys, globs...) }
}

// WithValueCapture controls whether values are recorded. With false, all
// values (arguments, replies, images) are dropped from recorded events; keys,
// command names and counts are still recorded.
func WithValueCapture(on bool) Option { return func(h *hook) { h.Capture = on } }

// WithIgnoreKeys replaces the default ignore list ("ts:ctx:*", the SDK's own
// queue context side channel). Commands whose keys ALL match are not traced.
func WithIgnoreKeys(globs ...string) Option { return func(h *hook) { h.Ignore = globs } }

// WithPipelineImageCap bounds the keys whose images are read per pipeline, and
// per command outside one (default 20); the rest are flagged
// Attrs["image_skipped"]="cap".
func WithPipelineImageCap(n int) Option { return func(h *hook) { h.PipelineCap = n } }

// WithImageTimeout bounds each image read (default 200ms).
func WithImageTimeout(d time.Duration) Option { return func(h *hook) { h.ImageTimeout = d } }

// WithImageDeadline bounds the total time one command or pipeline may spend
// reading images, separately before and after it runs (default 1s; 0 removes
// the bound). Reads past it are flagged Attrs["image_skipped"]="deadline".
func WithImageDeadline(d time.Duration) Option { return func(h *hook) { h.ImageDeadline = d } }

type hook struct{ redisx.Tracer }

func newHook(c redis.Cmdable, opts []Option) *hook {
	h := &hook{redisx.NewTracer()}
	h.Reader, h.Nil, h.Reply, h.PoppedKey = asReader(c), redis.Nil, replyOf, poppedKey
	for _, o := range opts {
		o(h)
	}
	return h
}

// reader adapts a go-redis client to redisx.Reader.
type reader struct{ c redis.Cmdable }

func asReader(c redis.Cmdable) redisx.Reader {
	if c == nil {
		return nil
	}
	return reader{c}
}

func (r reader) Type(ctx context.Context, key string) (string, error) {
	return r.c.Type(ctx, key).Result()
}

func (r reader) Get(ctx context.Context, key string) (string, error) {
	return r.c.Get(ctx, key).Result()
}

func (r reader) HScan(ctx context.Context, key string, count int64) ([]string, bool, error) {
	kv, cursor, err := r.c.HScan(ctx, key, 0, "", count).Result()
	return kv, cursor != 0, err
}

func (r reader) LRange(ctx context.Context, key string, stop int64) ([]string, error) {
	return r.c.LRange(ctx, key, 0, stop).Result()
}

func (r reader) SScan(ctx context.Context, key string, count int64) ([]string, bool, error) {
	l, cursor, err := r.c.SScan(ctx, key, 0, "", count).Result()
	return l, cursor != 0, err
}

func (r reader) ZRange(ctx context.Context, key string, stop int64) ([]redisx.Z, error) {
	z, err := r.c.ZRangeWithScores(ctx, key, 0, stop).Result()
	out := make([]redisx.Z, len(z))
	for i, e := range z {
		out[i] = redisx.Z{Member: e.Member, Score: e.Score}
	}
	return out, err
}

// Hook returns the tracing hook for client.AddHook.
func Hook(opts ...Option) redis.Hook {
	return newHook(nil, opts)
}

var wrapped sync.Map // redis.UniversalClient -> struct{}; the idempotency marker

// Wrap installs the hook on c and returns it. It is a no-op when the SDK is
// disabled, and wrapping the same client twice installs a single hook.
func Wrap(c redis.UniversalClient, opts ...Option) redis.UniversalClient {
	install(c, opts)
	return c
}

func install(c redis.UniversalClient, opts []Option) {
	defer func() { _ = recover() }() // never fail the host (e.g. typed-nil or unhashable client)
	if c == nil || !core.Enabled() {
		return
	}
	if _, loaded := wrapped.LoadOrStore(c, struct{}{}); loaded {
		return
	}
	c.AddHook(newHook(c, opts))
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if redisx.Skipped(ctx) {
			return next(ctx, cmd)
		}
		o := h.Begin(ctx, cmd)
		err := next(ctx, cmd)
		h.End(ctx, o, cmd, err)
		return err
	}
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if redisx.Skipped(ctx) {
			return next(ctx, cmds)
		}
		ops := redisx.BeginPipeline(ctx, &h.Tracer, cmds)
		err := next(ctx, cmds)
		redisx.EndPipeline(ctx, &h.Tracer, ops, cmds)
		return err
	}
}

// poppedKey returns the key a multi-key pop (BLPOP, BZPOPMIN, LMPOP, ...)
// popped from, as named by its reply, or "" when the reply does not tell.
func poppedKey(cmd redisx.Cmd) string {
	switch c := cmd.(type) {
	case *redis.StringSliceCmd: // BLPOP / BRPOP: [key, element]
		if v := c.Val(); len(v) > 0 {
			return v[0]
		}
	case *redis.ZWithKeyCmd:
		if v := c.Val(); v != nil {
			return v.Key
		}
	case *redis.KeyValuesCmd:
		k, _ := c.Val()
		return k
	case *redis.ZSliceWithKeyCmd:
		k, _ := c.Val()
		return k
	case *redis.Cmd:
		if v, ok := c.Val().([]any); ok && len(v) > 0 {
			k, _ := v[0].(string)
			return k
		}
	}
	return ""
}

// replyOf extracts the reply of the common command types. scalar marks
// replies that cannot carry user text (ints, bools, status).
func replyOf(cmd redisx.Cmd) (v any, scalar, ok bool) {
	switch c := cmd.(type) {
	case *redis.StringCmd:
		return c.Val(), false, true
	case *redis.IntCmd:
		return c.Val(), true, true
	case *redis.StatusCmd:
		return c.Val(), true, true
	case *redis.BoolCmd:
		return c.Val(), true, true
	case *redis.FloatCmd:
		return c.Val(), false, true
	case *redis.StringSliceCmd:
		return c.Val(), false, true
	case *redis.SliceCmd:
		return c.Val(), false, true
	case *redis.MapStringStringCmd:
		return c.Val(), false, true
	case *redis.Cmd:
		return c.Val(), false, true
	}
	return nil, false, false
}
