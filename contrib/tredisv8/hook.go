package tredisv8

import (
	"context"
	"errors"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/redisx"
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

// WithPipelineImageCap bounds the keys whose images are read per pipeline, and
// per command outside one (default 20); the rest are flagged
// Attrs["image_skipped"]="cap".
func WithPipelineImageCap(n int) Option { return func(h *hook) { h.pipelineCap = n } }

// WithImageTimeout bounds each image read (default 200ms).
func WithImageTimeout(d time.Duration) Option { return func(h *hook) { h.imageTimeout = d } }

// WithImageDeadline bounds the total time one command or pipeline may spend
// reading images, separately before and after it runs (default 1s; 0 removes
// the bound). Reads past it are flagged Attrs["image_skipped"]="deadline".
func WithImageDeadline(d time.Duration) Option { return func(h *hook) { h.imageDeadline = d } }

type hook struct {
	patterns []string
	reader   redis.Cmdable
	redisx.Redactor
	ignore        []string
	pipelineCap   int
	imageTimeout  time.Duration
	imageDeadline time.Duration
}

func newHook(reader redis.Cmdable, opts []Option) *hook {
	h := &hook{reader: reader, Redactor: redisx.Redactor{Capture: true}, ignore: []string{"ts:ctx:*"}, pipelineCap: 20, imageTimeout: 200 * time.Millisecond, imageDeadline: time.Second}
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
	if o := h.begin(ctx, cmd, false, h.newBatch(false)); o != nil {
		ctx = context.WithValue(ctx, opKey{}, o)
	}
	return ctx, nil
}

func (h *hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return nil
	}
	if o, _ := ctx.Value(opKey{}).(*op); o != nil {
		o.after = o.imaged
		o.finish(ctx, h, cmd, cmd.Err(), h.deadline())
	}
	return nil
}

func (h *hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if skip, _ := ctx.Value(skipKey{}).(bool); skip {
		return ctx, nil
	}
	ops := make([]*op, len(cmds))
	b := h.newBatch(true)
	for i, c := range cmds {
		name := strings.ToLower(c.Name())
		if name == "multi" || name == "exec" {
			continue
		}
		ops[i] = h.begin(ctx, c, true, b)
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
	h.finishPipeline(ctx, ops, cmds)
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
	before map[int]core.PayloadRef // before-images, by arg index
	imaged []int                   // arg indices of keys under image capture
	after  []int                   // subset of imaged whose after-image this op reads
}

// batch is the image-read allowance of one pipeline, or of one command
// outside a pipeline. It is nil when image capture is not configured.
type batch struct {
	pipeline bool
	reads    int             // keys whose image may still be read
	deadline time.Time       // end of the before-image phase; zero = none
	imaged   map[string]bool // pipeline: key -> first write got a before-image
}

func (h *hook) newBatch(pipeline bool) *batch {
	if !h.Capture || h.reader == nil || len(h.patterns) == 0 {
		return nil
	}
	return &batch{pipeline: pipeline, reads: h.pipelineCap, deadline: h.deadline()}
}

// deadline returns the end of an image phase starting now (zero = unbounded).
func (h *hook) deadline() time.Time {
	if h.imageDeadline <= 0 {
		return time.Time{}
	}
	return time.Now().Add(h.imageDeadline)
}

func expired(deadline time.Time) bool { return !deadline.IsZero() && !time.Now().Before(deadline) }

// begin starts the span for cmd. b is the image allowance (nil when images
// are off).
func (h *hook) begin(ctx context.Context, cmd redis.Cmder, pipeline bool, b *batch) *op {
	if isHandshake(cmd.Name()) {
		return nil
	}
	args := cmd.Args()
	s := redisx.Classify(cmd.Name(), args)
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
	if pipeline {
		span.SetAttr("pipeline", "true")
	}
	o := &op{span: span, spec: s, args: args, keys: keys, masked: masked, name: cmd.Name()}
	if s.Write && b != nil {
		h.beforeImages(ctx, o, b)
	}
	return o
}

// beforeImages reads the before-image of every key o writes that matches a
// WithBeforeImage pattern. Inside a pipeline only the first write of a key
// gets one: later writes of the same key would read the same stale value.
func (h *hook) beforeImages(ctx context.Context, o *op, b *batch) {
	o.before = map[int]core.PayloadRef{}
	for _, i := range redisx.WrittenKeys(o.name, o.args, o.keys, "") {
		k := redisx.Str(o.args[i])
		if !h.matches(k) {
			continue
		}
		if captured, seen := b.imaged[k]; seen {
			if captured {
				o.imaged = append(o.imaged, i)
			}
			continue
		}
		ref, ok := h.beforeImage(ctx, o.span, k, b)
		if b.pipeline {
			if b.imaged == nil {
				b.imaged = map[string]bool{}
			}
			b.imaged[k] = ok
		}
		if ok {
			o.before[i] = ref
			o.imaged = append(o.imaged, i)
		}
	}
}

func (h *hook) beforeImage(ctx context.Context, span *core.Span, key string, b *batch) (core.PayloadRef, bool) {
	if h.KeySensitive(key) { // never read a sensitive value just to mask it
		return core.Payload(core.RedactedValue), true
	}
	if b.reads <= 0 {
		span.SetAttr("image_skipped", "cap")
		return core.PayloadRef{}, false
	}
	b.reads--
	if expired(b.deadline) {
		span.SetAttr("image_skipped", "deadline")
		return core.PayloadRef{}, false
	}
	ref, ok := h.image(ctx, key, b.deadline)
	if !ok {
		span.SetAttr("image_skipped", "error")
	}
	return ref, ok
}

// finishPipeline ends the spans of a pipeline. The after-image of a key
// written more than once belongs to its last successful write.
func (h *hook) finishPipeline(ctx context.Context, ops []*op, cmds []redis.Cmder) {
	var last map[string]*op
	for i, o := range ops {
		if o == nil || i >= len(cmds) || cmds[i].Err() != nil {
			continue
		}
		for _, a := range o.imaged {
			if last == nil {
				last = map[string]*op{}
			}
			last[redisx.Str(o.args[a])] = o
		}
	}
	for _, o := range ops {
		if o == nil {
			continue
		}
		for _, a := range o.imaged {
			if last[redisx.Str(o.args[a])] == o {
				o.after = append(o.after, a)
			}
		}
	}
	deadline := h.deadline()
	for i, o := range ops {
		if i < len(cmds) {
			o.finish(ctx, h, cmds[i], cmds[i].Err(), deadline)
		}
	}
}

// finish records the reply and the mutations and ends the span. deadline
// bounds the after-image reads.
func (o *op) finish(ctx context.Context, h *hook, cmd redis.Cmder, err error, deadline time.Time) {
	if o == nil {
		return
	}
	defer o.span.End()
	if err != nil && !errors.Is(err, redis.Nil) {
		o.span.SetErr(err)
		return
	}
	var reply any
	if err == nil {
		if v, scalar, ok := replyOf(cmd); ok {
			reply = v
			if v, ok := h.MaskReply(o.name, o.args, o.keys, v, scalar); ok {
				o.span.SetResp(v)
			}
		}
	}
	if !o.spec.Write || err != nil || redisx.NoOp(o.name, reply) {
		return // a read, a miss, or a write that changed nothing: no mutation
	}
	name := strings.ToLower(cmd.Name())
	popped := ""
	if redisx.IsPop(name) {
		popped = poppedKey(cmd)
	}
	for _, i := range redisx.WrittenKeys(name, o.args, o.keys, popped) {
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
		}
		switch {
		case slices.Contains(o.after, i):
			if h.KeySensitive(key) {
				m.After = core.Payload(core.RedactedValue)
			} else if expired(deadline) {
				o.span.SetAttr("image_skipped", "deadline")
			} else if ref, ok := h.image(ctx, key, deadline); ok {
				m.After = ref
			} else {
				o.span.SetAttr("image_skipped", "error")
			}
		case h.Capture:
			if rest := o.afterArgs(i); rest != nil {
				m.After = core.Payload(rest)
			}
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
	case o.spec.First > 0 && o.spec.First == o.spec.Last:
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

// poppedKey returns the key a multi-key pop (BLPOP, BZPOPMIN, LMPOP, ...)
// popped from, as named by its reply, or "" when the reply does not tell.
func poppedKey(cmd redis.Cmder) string {
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
// timeout and the phase deadline, with redaction applied. Collections are
// capped at about imageLimit elements. ok=false means the read failed or
// timed out: image capture must never affect the host call.
func (h *hook) image(ctx context.Context, key string, deadline time.Time) (core.PayloadRef, bool) {
	// The caller's ctx carries its own op (v8 passes the Before ctx to After):
	// image reads must neither be traced nor finish that op.
	ctx = context.WithValue(context.WithValue(ctx, skipKey{}, true), opKey{}, nil)
	ctx = context.WithValue(ctx, pipeKey{}, nil)
	if h.imageTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.imageTimeout)
		defer cancel()
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
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
		var kv []string // field, value, field, value, ...
		if kv, _, err = h.reader.HScan(ctx, key, 0, "", imageLimit).Result(); err == nil {
			m := make(map[string]string, len(kv)/2)
			for i := 0; i+1 < len(kv) && len(m) < imageLimit; i += 2 {
				if f := kv[i]; redisx.NameSensitive(f) {
					m[f] = core.RedactedValue
				} else {
					m[f] = redisx.RedactStr(kv[i+1])
				}
			}
			v = m
		}
	case "list", "set":
		var l []string
		if typ == "list" {
			l, err = h.reader.LRange(ctx, key, 0, imageLimit-1).Result()
		} else {
			l, _, err = h.reader.SScan(ctx, key, 0, "", imageLimit).Result()
		}
		if len(l) > imageLimit {
			l = l[:imageLimit]
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
