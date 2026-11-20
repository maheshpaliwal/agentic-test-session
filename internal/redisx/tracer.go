package redisx

import (
	"context"
	"errors"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Cmd is the part of a go-redis Cmder the tracer needs. The Cmder of both go-redis v8 and v9 satisfies it.
type Cmd interface {
	Name() string
	Args() []any
	Err() error
}

// Z is one sorted-set member with its score.
type Z struct {
	Member any
	Score  float64
}

// Reader reads the current value of a key for before/after images. Each go-redis version adapts its client to it.
type Reader interface {
	Type(ctx context.Context, key string) (string, error)
	Get(ctx context.Context, key string) (string, error)
	// HScan returns up to about count entries as field, value, field, value, ...
	HScan(ctx context.Context, key string, count int64) ([]string, error)
	// LRange returns elements 0..stop.
	LRange(ctx context.Context, key string, stop int64) ([]string, error)
	SScan(ctx context.Context, key string, count int64) ([]string, error)
	// ZRange returns members 0..stop with their scores.
	ZRange(ctx context.Context, key string, stop int64) ([]Z, error)
}

// Tracer is the version-independent part of the Redis hook: it turns commands into spans and mutations.
// The go-redis v8 and v9 wrappers embed it and supply the version-specific pieces.
type Tracer struct {
	Redactor
	Patterns      []string // keys under before/after image capture (path.Match globs)
	Ignore        []string // commands whose keys all match are not traced
	PipelineCap   int      // keys imaged per pipeline, and per command outside one
	ImageTimeout  time.Duration
	ImageDeadline time.Duration

	Reader    Reader                                // nil disables images
	Nil       error                                 // the client's "key does not exist" error
	Reply     func(Cmd) (v any, scalar, ok bool)    // reply of the common command types
	PoppedKey func(Cmd) string                      // key a multi-key pop popped from, "" if unknown
	ImageCtx  func(context.Context) context.Context // optional extra context setup for image reads
}

// NewTracer returns a Tracer with the default limits.
func NewTracer() Tracer {
	return Tracer{Redactor: Redactor{Capture: true}, Ignore: []string{"ts:ctx:*"}, PipelineCap: 20,
		ImageTimeout: 200 * time.Millisecond, ImageDeadline: time.Second}
}

type skipKey struct{}

// Skipped reports whether ctx belongs to one of the tracer's own image reads, which must not be traced.
func Skipped(ctx context.Context) bool {
	skip, _ := ctx.Value(skipKey{}).(bool)
	return skip
}

// Op is one in-flight command span plus what is needed to build mutations.
type Op struct {
	span   *core.Span
	spec   Spec
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

func (t *Tracer) newBatch(pipeline bool) *batch {
	if !t.Capture || t.Reader == nil || len(t.Patterns) == 0 {
		return nil
	}
	return &batch{pipeline: pipeline, reads: t.PipelineCap, deadline: t.deadline()}
}

// deadline returns the end of an image phase starting now (zero = unbounded).
func (t *Tracer) deadline() time.Time {
	if t.ImageDeadline <= 0 {
		return time.Time{}
	}
	return time.Now().Add(t.ImageDeadline)
}

func expired(deadline time.Time) bool { return !deadline.IsZero() && !time.Now().Before(deadline) }

// Begin starts the span of a command issued outside a pipeline. It returns nil when the command is not traced.
func (t *Tracer) Begin(ctx context.Context, cmd Cmd) (o *Op) {
	if !core.Active(ctx) {
		return nil // outside a session: no classification, no allocation
	}
	defer core.Recover() // tracing must never break the command
	return t.begin(ctx, cmd, false, t.newBatch(false))
}

// End finishes the span started by Begin. err is the command's error.
func (t *Tracer) End(ctx context.Context, o *Op, cmd Cmd, err error) {
	defer core.Recover()
	if o != nil {
		o.after = o.imaged
		o.finish(ctx, t, cmd, err, t.deadline())
	}
}

// BeginPipeline starts one span per command of a pipeline; the result is index-aligned with cmds and holds nil
// for commands that are not traced.
func BeginPipeline[C Cmd](ctx context.Context, t *Tracer, cmds []C) (ops []*Op) {
	if !core.Active(ctx) {
		return nil
	}
	defer core.Recover()
	ops = make([]*Op, len(cmds))
	b := t.newBatch(true)
	for i, c := range cmds {
		name := strings.ToLower(c.Name())
		if name == "multi" || name == "exec" {
			continue
		}
		ops[i] = t.begin(ctx, c, true, b)
		if ops[i] != nil {
			ops[i].span.SetAttr("pipeline_size", strconv.Itoa(len(cmds)))
			ops[i].span.SetAttr("pipeline_index", strconv.Itoa(i))
		}
	}
	return ops
}

// EndPipeline ends the spans started by BeginPipeline. The after-image of a key
// written more than once belongs to its last successful write.
func EndPipeline[C Cmd](ctx context.Context, t *Tracer, ops []*Op, cmds []C) {
	defer core.Recover()
	var last map[string]*Op
	for i, o := range ops {
		if o == nil || i >= len(cmds) || cmds[i].Err() != nil {
			continue
		}
		for _, a := range o.imaged {
			if last == nil {
				last = map[string]*Op{}
			}
			last[Str(o.args[a])] = o
		}
	}
	for _, o := range ops {
		if o == nil {
			continue
		}
		for _, a := range o.imaged {
			if last[Str(o.args[a])] == o {
				o.after = append(o.after, a)
			}
		}
	}
	deadline := t.deadline()
	for i, o := range ops {
		if i < len(cmds) {
			o.finish(ctx, t, cmds[i], cmds[i].Err(), deadline)
		}
	}
}

// begin starts the span for cmd. b is the image allowance (nil when images are off).
func (t *Tracer) begin(ctx context.Context, cmd Cmd, pipeline bool, b *batch) *Op {
	if IsHandshake(cmd.Name()) {
		return nil
	}
	args := cmd.Args()
	s := Classify(cmd.Name(), args)
	keys := KeysOf(cmd.Name(), s, args)
	if t.ignored(args, keys) {
		return nil
	}
	kind := core.KindCacheRead
	if s.Write {
		kind = core.KindCacheWrite
	}
	_, span := core.Begin(ctx, kind, SpanName(args, keys))
	if span == nil {
		return nil
	}
	masked := t.MaskArgs(cmd.Name(), args, keys)
	span.SetReq(CapArgs(masked))
	if pipeline {
		span.SetAttr("pipeline", "true")
	}
	o := &Op{span: span, spec: s, args: args, keys: keys, masked: masked, name: cmd.Name()}
	if s.Write && b != nil {
		t.beforeImages(ctx, o, b)
	}
	return o
}

func (t *Tracer) ignored(args []any, keys []int) bool {
	if len(keys) == 0 || len(t.Ignore) == 0 {
		return false
	}
	for _, i := range keys {
		if !matchAny(t.Ignore, Str(args[i])) {
			return false
		}
	}
	return true
}

func matchAny(globs []string, key string) bool {
	return slices.ContainsFunc(globs, func(p string) bool {
		ok, _ := path.Match(p, key)
		return ok
	})
}

// beforeImages reads the before-image of every key o writes that matches an
// image pattern. Inside a pipeline only the first write of a key gets one:
// later writes of the same key would read the same stale value.
func (t *Tracer) beforeImages(ctx context.Context, o *Op, b *batch) {
	o.before = map[int]core.PayloadRef{}
	for _, i := range WrittenKeys(o.name, o.args, o.keys, "") {
		k := Str(o.args[i])
		if !matchAny(t.Patterns, k) {
			continue
		}
		if captured, seen := b.imaged[k]; seen {
			if captured {
				o.imaged = append(o.imaged, i)
			}
			continue
		}
		ref, ok := t.beforeImage(ctx, o.span, k, b)
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

func (t *Tracer) beforeImage(ctx context.Context, span *core.Span, key string, b *batch) (core.PayloadRef, bool) {
	if t.KeySensitive(key) { // never read a sensitive value just to mask it
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
	ref, ok := t.image(ctx, key, b.deadline)
	if !ok {
		span.SetAttr("image_skipped", "error")
	}
	return ref, ok
}

// finish records the reply and the mutations and ends the span. deadline
// bounds the after-image reads.
func (o *Op) finish(ctx context.Context, t *Tracer, cmd Cmd, err error, deadline time.Time) {
	if o == nil {
		return
	}
	defer o.span.End()
	if err != nil && !errors.Is(err, t.Nil) {
		o.span.SetErr(err)
		return
	}
	var reply any
	if err == nil {
		if v, scalar, ok := t.Reply(cmd); ok {
			reply = v
			if v, ok := t.MaskReply(o.name, o.args, o.keys, v, scalar); ok {
				o.span.SetResp(v)
			}
		}
	}
	if !o.spec.Write || err != nil || NoOp(o.name, reply) {
		return // a read, a miss, or a write that changed nothing: no mutation
	}
	name := strings.ToLower(cmd.Name())
	popped := ""
	if IsPop(name) {
		popped = t.PoppedKey(cmd)
	}
	for _, i := range WrittenKeys(name, o.args, o.keys, popped) {
		key := Str(o.args[i])
		m := core.Mutation{
			Store:  "redis",
			Entity: KeyPattern(key),
			PK:     map[string]any{"key": key},
			Op:     name,
			Source: "sdk",
		}
		if b, ok := o.before[i]; ok {
			m.Before = b
		}
		switch {
		case slices.Contains(o.after, i):
			if t.KeySensitive(key) {
				m.After = core.Payload(core.RedactedValue)
			} else if expired(deadline) {
				o.span.SetAttr("image_skipped", "deadline")
			} else if ref, ok := t.image(ctx, key, deadline); ok {
				m.After = ref
			} else {
				o.span.SetAttr("image_skipped", "error")
			}
		case t.Capture:
			if rest := o.afterArgs(i); rest != nil {
				m.After = core.Payload(rest)
			}
		}
		o.span.AddMutation(m)
	}
}

// afterArgs extracts the value-ish arguments that belong to the key at index i.
func (o *Op) afterArgs(i int) any {
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

// ImageLimit is the maximum number of elements captured from a collection.
const ImageLimit = 1000

// image reads the current value of key by its type, bounded by the image
// timeout and the phase deadline, with redaction applied. Collections are
// capped at about ImageLimit elements. ok=false means the read failed or
// timed out: image capture must never affect the host call.
func (t *Tracer) image(ctx context.Context, key string, deadline time.Time) (core.PayloadRef, bool) {
	ctx = context.WithValue(ctx, skipKey{}, true)
	if t.ImageCtx != nil {
		ctx = t.ImageCtx(ctx)
	}
	if t.ImageTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.ImageTimeout)
		defer cancel()
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	typ, err := t.Reader.Type(ctx, key)
	if err != nil {
		return core.PayloadRef{}, false
	}
	var v any
	switch typ {
	case "string":
		var s string
		if s, err = t.Reader.Get(ctx, key); err == nil {
			v = RedactStr(s)
		}
	case "hash":
		var kv []string // field, value, field, value, ...
		if kv, err = t.Reader.HScan(ctx, key, ImageLimit); err == nil {
			m := make(map[string]string, len(kv)/2)
			for i := 0; i+1 < len(kv) && len(m) < ImageLimit; i += 2 {
				if f := kv[i]; NameSensitive(f) {
					m[f] = core.RedactedValue
				} else {
					m[f] = RedactStr(kv[i+1])
				}
			}
			v = m
		}
	case "list", "set":
		var l []string
		if typ == "list" {
			l, err = t.Reader.LRange(ctx, key, ImageLimit-1)
		} else {
			l, err = t.Reader.SScan(ctx, key, ImageLimit)
		}
		if len(l) > ImageLimit {
			l = l[:ImageLimit]
		}
		for i := range l {
			l[i] = RedactStr(l[i])
		}
		v = l
	case "zset":
		var z []Z
		if z, err = t.Reader.ZRange(ctx, key, ImageLimit-1); err == nil {
			m := make(map[string]float64, len(z))
			for _, e := range z {
				m[RedactStr(Str(e.Member))] = e.Score
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

// IsHandshake reports connection-setup commands issued by the client library
// itself; they are noise, not application behaviour.
func IsHandshake(name string) bool {
	switch strings.ToLower(name) {
	case "hello", "client", "auth", "select", "readonly", "command":
		return true
	}
	return false
}
