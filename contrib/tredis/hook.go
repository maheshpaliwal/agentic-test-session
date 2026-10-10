package tredis

import (
	"context"
	"errors"
	"net"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"tsession/core"
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

type hook struct {
	patterns []string
	reader   redis.Cmdable
}

type skipKey struct{}

// Hook returns the tracing hook for client.AddHook.
func Hook(opts ...Option) redis.Hook {
	h := &hook{}
	for _, o := range opts {
		o(h)
	}
	return h
}

var wrapped sync.Map // redis.UniversalClient -> struct{}; the idempotency marker

// Wrap installs the hook on c and returns it. It is a no-op when the SDK is
// disabled, and wrapping the same client twice installs a single hook.
func Wrap(c redis.UniversalClient, opts ...Option) redis.UniversalClient {
	if c == nil || !core.Enabled() {
		return c
	}
	if _, loaded := wrapped.LoadOrStore(c, struct{}{}); loaded {
		return c
	}
	h := &hook{reader: c}
	for _, o := range opts {
		o(h)
	}
	c.AddHook(h)
	return c
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if skip, _ := ctx.Value(skipKey{}).(bool); skip {
			return next(ctx, cmd)
		}
		op := h.begin(ctx, cmd, false)
		err := next(ctx, cmd)
		op.finish(ctx, h, cmd, err)
		return err
	}
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if skip, _ := ctx.Value(skipKey{}).(bool); skip {
			return next(ctx, cmds)
		}
		ops := make([]*op, len(cmds))
		for i, c := range cmds {
			name := strings.ToLower(c.Name())
			if name == "multi" || name == "exec" {
				continue
			}
			ops[i] = h.begin(ctx, c, true)
			if ops[i] != nil {
				ops[i].span.SetAttr("pipeline_size", strconv.Itoa(len(cmds)))
				ops[i].span.SetAttr("pipeline_index", strconv.Itoa(i))
			}
		}
		err := next(ctx, cmds)
		for i, o := range ops {
			o.finish(ctx, h, cmds[i], cmds[i].Err())
		}
		return err
	}
}

// op is one in-flight command span plus what is needed to build mutations.
type op struct {
	span   *core.Span
	spec   spec
	args   []any
	keys   []int
	before map[int]core.PayloadRef // by arg index
}

func (h *hook) begin(ctx context.Context, cmd redis.Cmder, pipelined bool) *op {
	if isHandshake(cmd.Name()) {
		return nil
	}
	args := cmd.Args()
	s, _ := lookup(cmd.Name())
	keys := keysOf(cmd.Name(), s, args)
	kind := core.KindCacheRead
	if s.write {
		kind = core.KindCacheWrite
	}
	_, span := core.Begin(ctx, kind, spanName(args, keys))
	if span == nil {
		return nil
	}
	span.SetReq(reqArgs(args))
	if pipelined {
		span.SetAttr("pipeline", "true")
	}
	o := &op{span: span, spec: s, args: args, keys: keys}
	if s.write && h.reader != nil && len(h.patterns) > 0 {
		o.before = map[int]core.PayloadRef{}
		for _, i := range keys {
			if k := str(args[i]); h.matches(k) {
				o.before[i] = h.image(ctx, k)
			}
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
		if v, ok := replyOf(cmd); ok {
			o.span.SetResp(v)
		}
	}
	if !o.spec.write || err != nil {
		return
	}
	name := strings.ToLower(cmd.Name())
	for _, i := range o.keys {
		key := str(o.args[i])
		m := core.Mutation{
			Store:  "redis",
			Entity: keyPattern(key),
			PK:     map[string]any{"key": key},
			Op:     name,
			Source: "sdk",
		}
		if b, ok := o.before[i]; ok {
			m.Before = b
			m.After = h.image(ctx, key)
		} else if rest := o.afterArgs(i); rest != nil {
			m.After = core.Payload(rest)
		}
		o.span.AddMutation(m)
	}
}

// afterArgs extracts the value-ish arguments that belong to the key at index i.
func (o *op) afterArgs(i int) any {
	var rest []any
	switch {
	case o.spec.step > 1:
		rest = o.args[i+1 : min(i+o.spec.step, len(o.args))]
	case o.spec.first == o.spec.last:
		rest = o.args[i+1:]
	}
	if len(rest) == 0 {
		return nil
	}
	if len(rest) == 1 {
		return str(rest[0])
	}
	out := make([]string, len(rest))
	for j, a := range rest {
		out[j] = str(a)
	}
	return out
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

// image reads the current value of key by its type. Failures yield a zero
// ref: image capture must never affect the host call.
func (h *hook) image(ctx context.Context, key string) core.PayloadRef {
	ctx = context.WithValue(ctx, skipKey{}, true)
	typ, err := h.reader.Type(ctx, key).Result()
	if err != nil {
		return core.PayloadRef{}
	}
	var v any
	switch typ {
	case "string":
		v, err = h.reader.Get(ctx, key).Result()
	case "hash":
		v, err = h.reader.HGetAll(ctx, key).Result()
	case "list":
		v, err = h.reader.LRange(ctx, key, 0, imageLimit-1).Result()
	case "set":
		v, err = h.reader.SMembers(ctx, key).Result()
	case "zset":
		var z []redis.Z
		if z, err = h.reader.ZRangeWithScores(ctx, key, 0, imageLimit-1).Result(); err == nil {
			m := make(map[string]float64, len(z))
			for _, e := range z {
				m[str(e.Member)] = e.Score
			}
			v = m
		}
	case "none":
		return core.PayloadRef{}
	default:
		v = "<" + typ + ">"
	}
	if err != nil {
		return core.PayloadRef{}
	}
	return core.Payload(v)
}

// replyOf extracts a printable reply from the common command types.
func replyOf(cmd redis.Cmder) (any, bool) {
	switch c := cmd.(type) {
	case *redis.StringCmd:
		return c.Val(), true
	case *redis.IntCmd:
		return c.Val(), true
	case *redis.StatusCmd:
		return c.Val(), true
	case *redis.BoolCmd:
		return c.Val(), true
	case *redis.FloatCmd:
		return c.Val(), true
	case *redis.StringSliceCmd:
		return c.Val(), true
	case *redis.SliceCmd:
		return c.Val(), true
	case *redis.MapStringStringCmd:
		return c.Val(), true
	case *redis.Cmd:
		return c.Val(), true
	}
	return nil, false
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
