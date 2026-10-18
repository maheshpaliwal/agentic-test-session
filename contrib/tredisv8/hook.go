package tredisv8

import (
	"context"
	"errors"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"tsession/core"
	"tsession/internal/redisx"
)

// Option configures the hook.
type Option func(*hook)

// WithBeforeImage enables before/after image capture for keys matching any of
// the glob patterns (path.Match syntax, e.g. "user:*"; note * does not cross
// '/').
func WithBeforeImage(patterns ...string) Option {
	return func(h *hook) { h.patterns = append(h.patterns, patterns...) }
}

// WithReader sets the client used to read images. Wrap does this itself.
func WithReader(c redis.Cmdable) Option { return func(h *hook) { h.reader = c } }

// WithRedactKeys force-masks the values of keys matching any glob (path.Match
// syntax), in addition to keys whose name segments are sensitive per core.
func WithRedactKeys(globs ...string) Option {
	return func(h *hook) { h.Keys = append(h.Keys, globs...) }
}

// WithValueCapture(false) drops all values (arguments, replies, images) from
// recorded events; keys, command names and counts are still recorded.
func WithValueCapture(on bool) Option { return func(h *hook) { h.Capture = on } }

// WithIgnoreKeys replaces the default ignore list ("ts:ctx:*", the SDK's own
// queue context side channel). Commands whose keys ALL match are not traced.
func WithIgnoreKeys(globs ...string) Option { return func(h *hook) { h.ignore = globs } }

// WithPipelineImageCap bounds before-image reads per pipeline (default 20);
// the rest are flagged Attrs["image_skipped"]="cap".
func WithPipelineImageCap(n int) Option { return func(h *hook) { h.pipelineCap = n } }

// WithImageTimeout bounds each image read (default 200ms).
func WithImageTimeout(d time.Duration) Option { return func(h *hook) { h.imageTimeout = d } }

type hook struct {
	patterns []string
	reader   redis.Cmdable
	redisx.Redactor
	ignore       []string
	pipelineCap  int
	imageTimeout time.Duration
}

func newHook(reader redis.Cmdable, opts []Option) *hook {
	h := &hook{reader: reader, Redactor: redisx.Redactor{Capture: true}, ignore: []string{"ts:ctx:*"}, pipelineCap: 20, imageTimeout: 200 * time.Millisecond}
	for _, o := range opts {
		o(h)
	}
	return h
}

func (h *hook) ignored(args []any, keys []int) bool {
	if len(keys) == 0 || len(h.ignore) == 0 {
		return false
	}
	for _, i := range keys {
		k := redisx.Str(args[i])
		hit := false
		for _, p := range h.ignore {
			if ok, _ := path.Match(p, k); ok {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

type skipKey struct{}

// Hook returns the tracing hook for client.AddHook. Before-images need a
// reader: pass WithReader (Wrap/WrapAny do this for you).
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
	install(c, c, opts)
	return c
}

// WrapAny is Wrap for a client you only hold as an interface value, e.g. the
// Cmdable embedded in an internal wrapper (rc.Cmdable): it installs the hook
// when v has AddHook(redis.Hook) and reports whether it did. v is also used as
// the before-image reader when it is a redis.Cmdable.
func WrapAny(v any, opts ...Option) bool {
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
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return ctx, nil
	}
	if o := h.begin(ctx, cmd, nil); o != nil {
		ctx = context.WithValue(ctx, opKey{}, o)
	}
	return ctx, nil
}

func (h *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return nil
	}
	if o, _ := ctx.Value(opKey{}).(*op); o != nil {
		o.finish(ctx, h, cmd, cmd.Err())
	}
	return nil
}

func (h *hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return ctx, nil
	}
	ops := make([]*op, len(cmds))
	budget := h.pipelineCap
	for i, c := range cmds {
		name := strings.ToLower(c.Name())
		if name == "multi" || name == "exec" {
			continue
		}
		ops[i] = h.begin(ctx, c, &budget)
		if ops[i] != nil {
			ops[i].span.SetAttr("pipeline_size", strconv.Itoa(len(cmds)))
			ops[i].span.SetAttr("pipeline_index", strconv.Itoa(i))
		}
	}
	return context.WithValue(ctx, pipeKey{}, ops), nil
}

func (h *hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return nil
	}
	ops, _ := ctx.Value(pipeKey{}).([]*op)
	for i, o := range ops {
		if i < len(cmds) {
			o.finish(ctx, h, cmds[i], cmds[i].Err())
		}
	}
	return nil
}

// op is one in-flight command span plus what is needed to build mutations.
type op struct {
	span   *core.Span
	spec   redisx.Spec
	args   []any
	masked []string // args with credentials/values masked, index-aligned
	name   string
	keys   []int
	before map[int]core.PayloadRef // by arg index
}

// begin starts the span for cmd. budget is non-nil inside a pipeline and holds
// the remaining before-image reads for it.
func (h *hook) begin(ctx context.Context, cmd redis.Cmder, budget *int) *op {
	if isHandshake(cmd.Name()) {
		return nil
	}
	args := cmd.Args()
	s, _ := redisx.Lookup(cmd.Name())
	keys := redisx.KeysOf(cmd.Name(), s, args)
	if h.ignored(args, keys) {
		return nil
	}
	kind := core.KindCacheRead
	if s.Write {
		kind = core.KindCacheWrite
	}
	_, span := core.Begin(ctx, kind, redisx.SpanName(args, keys))
	if span == nil {
		return nil
	}
	masked := h.MaskArgs(cmd.Name(), args, keys)
	span.SetReq(redisx.CapArgs(masked))
	if budget != nil {
		span.SetAttr("pipeline", "true")
	}
	o := &op{span: span, spec: s, args: args, keys: keys, masked: masked, name: cmd.Name()}
	if s.Write && h.Capture && h.reader != nil && len(h.patterns) > 0 {
		o.before = map[int]core.PayloadRef{}
		for _, i := range keys {
			k := redisx.Str(args[i])
			if !h.matches(k) {
				continue
			}
			if h.KeySensitive(k) { // never read a sensitive value just to mask it
				o.before[i] = core.Payload(core.RedactedValue)
				continue
			}
			if budget != nil {
				if *budget <= 0 {
					span.SetAttr("image_skipped", "cap")
					continue
				}
				*budget--
			}
			ref, ok := h.image(ctx, k)
			if !ok {
				span.SetAttr("image_skipped", "error")
				continue
			}
			o.before[i] = ref
		}
	}
	return o
}

func (o *op) finish(ctx context.Context, h *hook, cmd redis.Cmder, err error) {
	if o == nil {
		return
	}
	defer o.span.End()
	if err != nil && !errors.Is(err, redis.Nil) {
		o.span.SetErr(err)
		return
	}
	if err == nil {
		if v, scalar, ok := replyOf(cmd); ok {
			if v, ok := h.MaskReply(o.name, o.args, o.keys, v, scalar); ok {
				o.span.SetResp(v)
			}
		}
	}
	if !o.spec.Write || err != nil {
		return
	}
	name := strings.ToLower(cmd.Name())
	for _, i := range o.keys {
		key := redisx.Str(o.args[i])
		m := core.Mutation{
			Store:  "redis",
			Entity: redisx.KeyPattern(key),
			PK:     map[string]any{"key": key},
			Op:     name,
			Source: "sdk",
		}
		if b, ok := o.before[i]; ok {
			m.Before = b
			if h.KeySensitive(key) {
				m.After = core.Payload(core.RedactedValue)
			} else if ref, ok := h.image(ctx, key); ok {
				m.After = ref
			} else {
				o.span.SetAttr("image_skipped", "error")
			}
		} else if !h.Capture {
			// values are not captured
		} else if rest := o.afterArgs(i); rest != nil {
			m.After = core.Payload(rest)
		}
		o.span.AddMutation(m)
	}
}

// afterArgs extracts the value-ish arguments that belong to the key at index i.
func (o *op) afterArgs(i int) any {
	var rest []string
	switch {
	case o.spec.Step > 1:
		rest = o.masked[i+1 : min(i+o.spec.Step, len(o.masked))]
	case o.spec.First == o.spec.Last:
		rest = o.masked[i+1:]
	}
	if len(rest) == 0 {
		return nil
	}
	if len(rest) == 1 {
		return rest[0]
	}
	return rest
}

func (h *hook) matches(key string) bool {
	for _, p := range h.patterns {
		if ok, _ := path.Match(p, key); ok {
			return true
		}
	}
	return false
}

const imageLimit = 1000 // max elements captured from collection types

// image reads the current value of key by its type, bounded by the image
// timeout, with redaction applied. ok=false means the read failed or timed
// out: image capture must never affect the host call.
func (h *hook) image(ctx context.Context, key string) (core.PayloadRef, bool) {
	// The caller's ctx carries its own op (v8 passes the Before ctx to After):
	// image reads must neither be traced nor finish that op.
	ctx = context.WithValue(context.WithValue(ctx, skipKey{}, true), opKey{}, nil)
	ctx = context.WithValue(ctx, pipeKey{}, nil)
	if h.imageTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.imageTimeout)
		defer cancel()
	}
	typ, err := h.reader.Type(ctx, key).Result()
	if err != nil {
		return core.PayloadRef{}, false
	}
	var v any
	switch typ {
	case "string":
		var s string
		if s, err = h.reader.Get(ctx, key).Result(); err == nil {
			v = redisx.RedactStr(s)
		}
	case "hash":
		var m map[string]string
		if m, err = h.reader.HGetAll(ctx, key).Result(); err == nil {
			for f, x := range m {
				if redisx.NameSensitive(f) {
					m[f] = core.RedactedValue
				} else {
					m[f] = redisx.RedactStr(x)
				}
			}
			v = m
		}
	case "list", "set":
		var l []string
		if typ == "list" {
			l, err = h.reader.LRange(ctx, key, 0, imageLimit-1).Result()
		} else {
			l, err = h.reader.SMembers(ctx, key).Result()
		}
		for i := range l {
			l[i] = redisx.RedactStr(l[i])
		}
		v = l
	case "zset":
		var z []redis.Z
		if z, err = h.reader.ZRangeWithScores(ctx, key, 0, imageLimit-1).Result(); err == nil {
			m := make(map[string]float64, len(z))
			for _, e := range z {
				m[redisx.RedactStr(redisx.Str(e.Member))] = e.Score
			}
			v = m
		}
	case "none":
		return core.PayloadRef{}, true
	default:
		v = "<" + typ + ">"
	}
	if err != nil {
		return core.PayloadRef{}, false
	}
	return core.Payload(v), true
}

// replyOf extracts the reply of the common command types. scalar marks
// replies that cannot carry user text (ints, bools, status).
func replyOf(cmd redis.Cmder) (v any, scalar, ok bool) {
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

// isHandshake reports connection-setup commands issued by the client library
// itself; they are noise, not application behaviour.
func isHandshake(name string) bool {
	switch strings.ToLower(name) {
	case "hello", "client", "auth", "select", "readonly", "command":
		return true
	}
	return false
}
