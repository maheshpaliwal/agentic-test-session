package tredisv8

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/redisx"
)

// Option configures the hook.
type Option func(*hook)

// WithBeforeImage enables before/after image capture for keys matching any of
// the glob patterns, e.g. "user:*". The syntax of every glob of this package
// is that of path.Match, except that '*' and '?' match any byte, '/'
// included, as in a Redis KEYS pattern.
func WithBeforeImage(patterns ...string) Option {
	return func(h *hook) { h.Patterns = append(h.Patterns, patterns...) }
}

// WithReader sets the client used to read images. Wrap does this itself.
func WithReader(c redis.Cmdable) Option { return func(h *hook) { h.Reader = asReader(c) } }

// WithRedactKeys force-masks the values of keys matching any glob, in addition
// to keys whose name segments are sensitive per core. It panics on a
// malformed glob, which would otherwise match nothing and leave the values
// it was written for unmasked.
func WithRedactKeys(globs ...string) Option {
	redisx.MustGlobs("tredisv8.WithRedactKeys", globs)
	return func(h *hook) { h.Keys = append(h.Keys, globs...) }
}

// KeyMask says how the secret part of a key name is recorded, see WithKeyMask.
type KeyMask = redisx.KeyMask

const (
	// KeyMaskHash records a short hash, "session:#a1b2c3d4e5f6" (the default).
	// It is keyed by a random secret of the process that is never exported:
	// one key reads the same for as long as the process runs, and the secret
	// cannot be found by hashing candidates. Another process (another
	// service, this one after a restart) records the same key differently.
	KeyMaskHash = redisx.KeyMaskHash
	// KeyMaskSession records a short hash keyed by the session id: one key
	// reads the same in every service of a session, and whoever reads an
	// export can hash candidates to find or confirm the secret.
	KeyMaskSession = redisx.KeyMaskSession
	// KeyMaskFlat records "session:[REDACTED]": all such keys read alike.
	KeyMaskFlat = redisx.KeyMaskFlat
	// KeyMaskOff records key names as they are.
	KeyMaskOff = redisx.KeyMaskOff
)

// WithKeyMask sets how the secret part of a key name is recorded, wherever a
// name is recorded: span names, arguments, mutations, images, diffs, replies.
// See "Secret key names" in the package comment for which part of a name
// counts as secret and what each mode does and does not hide.
func WithKeyMask(m KeyMask) Option { return func(h *hook) { h.KeyMask = m } }

// WithMaskKeys adds keys whose name holds a secret: globs matched one ':'
// separated segment at a time, the last segment taking the rest of the key.
// The segments under a wildcard are masked: "auth:token:*" records
// auth:token:<jwt> as "auth:token:#a1b2c3d4e5f6". It panics on a malformed
// glob, which would otherwise match nothing and leave the names unmasked.
func WithMaskKeys(globs ...string) Option {
	redisx.MustGlobs("tredisv8.WithMaskKeys", globs)
	return func(h *hook) { h.MaskKeys = append(h.MaskKeys, globs...) }
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
	h.TxFailed = redis.TxFailedErr
	// The caller's ctx carries its own op (v8 passes the Before ctx to After):
	// image reads must neither be traced nor finish that op.
	h.ImageCtx = func(ctx context.Context) context.Context {
		return context.WithValue(context.WithValue(ctx, opKey{}, nil), pipeKey{}, nil)
	}
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

// Hook returns the tracing hook for client.AddHook. Before-images need a
// reader: pass WithReader (Wrap/WrapAny do this for you). Unlike Wrap and
// WrapAny it may be installed before the SDK starts: it looks the SDK up on
// every command.
func Hook(opts ...Option) redis.Hook {
	return newHook(nil, opts)
}

// Hookable is satisfied by *redis.Client, *redis.ClusterClient, *redis.Ring
// and redis.UniversalClient.
type Hookable interface {
	redis.Cmdable
	AddHook(redis.Hook)
}

var wrapped sync.Map // client -> struct{}; the idempotency marker

// Wrap installs the hook on c and returns it. It is a no-op when the SDK is
// disabled, and wrapping the same client twice installs a single hook.
func Wrap[C Hookable](c C, opts ...Option) C {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tredisv8.Wrap")
	}
	install(c, c, opts)
	return c
}

// WrapAny is Wrap for a client you only hold as an interface value, e.g. the
// Cmdable embedded in an internal wrapper (rc.Cmdable): it installs the hook
// when v has AddHook(redis.Hook) and reports whether it did. v is also used as
// the before-image reader when it is a redis.Cmdable.
func WrapAny(v any, opts ...Option) bool {
	if !core.Enabled() {
		core.NoteWrapWhileDisabled("tredisv8.WrapAny")
	}
	h, ok := v.(interface{ AddHook(redis.Hook) })
	if !ok || v == nil {
		return false
	}
	rd, _ := v.(redis.Cmdable)
	return install(v, h, opts, rd)
}

func install(key any, h interface{ AddHook(redis.Hook) }, opts []Option, reader ...redis.Cmdable) (done bool) {
	defer func() { // never fail the host (e.g. unhashable key)
		if recover() != nil {
			done = false
		}
	}()
	if key == nil || !core.Enabled() {
		return false
	}
	if _, loaded := wrapped.LoadOrStore(key, struct{}{}); loaded {
		return false
	}
	var rd redis.Cmdable
	if len(reader) > 0 {
		rd = reader[0]
	} else if c, ok := key.(redis.Cmdable); ok {
		rd = c
	}
	h.AddHook(newHook(rd, opts))
	return true
}

type opKey struct{}
type pipeKey struct{}

// BeforeProcess starts the span; the op rides the returned context.
func (h *hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if redisx.Skipped(ctx) {
		return ctx, nil
	}
	if o := h.Begin(ctx, cmd); o != nil {
		ctx = context.WithValue(ctx, opKey{}, o)
	}
	return ctx, nil
}

func (h *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if redisx.Skipped(ctx) {
		return nil
	}
	o, _ := ctx.Value(opKey{}).(*redisx.Op)
	h.End(ctx, o, cmd, cmd.Err())
	return nil
}

func (h *hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if redisx.Skipped(ctx) {
		return ctx, nil
	}
	return context.WithValue(ctx, pipeKey{}, redisx.BeginPipeline(ctx, &h.Tracer, cmds)), nil
}

func (h *hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	if redisx.Skipped(ctx) {
		return nil
	}
	ops, _ := ctx.Value(pipeKey{}).([]*redisx.Op)
	redisx.EndPipeline(ctx, &h.Tracer, ops, cmds)
	return nil
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
	case *redis.StringStringMapCmd:
		return c.Val(), false, true
	case *redis.Cmd:
		return c.Val(), false, true
	}
	return nil, false, false
}
