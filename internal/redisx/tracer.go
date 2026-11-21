package redisx

import (
	"context"
	"errors"
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
	// more reports that the hash holds entries the scan did not reach.
	HScan(ctx context.Context, key string, count int64) (kv []string, more bool, err error)
	// LRange returns elements 0..stop.
	LRange(ctx context.Context, key string, stop int64) ([]string, error)
	SScan(ctx context.Context, key string, count int64) (members []string, more bool, err error)
	// ZRange returns members 0..stop with their scores.
	ZRange(ctx context.Context, key string, stop int64) ([]Z, error)
}

// Tracer is the version-independent part of the Redis hook: it turns commands into spans and mutations.
// The go-redis v8 and v9 wrappers embed it and supply the version-specific pieces.
type Tracer struct {
	Redactor
	Patterns      []string // keys under before/after image capture (globs, see Match)
	Ignore        []string // commands whose keys all match are not traced
	PipelineCap   int      // keys imaged per pipeline, and per command outside one
	ImageTimeout  time.Duration
	ImageDeadline time.Duration

	Reader    Reader                                // nil disables images
	Nil       error                                 // the client's "key does not exist" error
	TxFailed  error                                 // the client's "EXEC aborted: a watched key changed" error
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
	span    *core.Span
	session string // keys the hash of masked key names
	spec    Spec
	args    []any
	masked  []string // args with credentials/values masked, index-aligned
	name    string
	keys    []int
	before  map[int]core.PayloadRef // before-images, by arg index
	snaps   map[int]*snap           // what the before-image reads saw, for the diff
	imaged  []int                   // arg indices of keys under image capture
	after   []int                   // subset of imaged whose after-image this op reads
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
	// The client frames a transaction with MULTI and EXEC, which are not the
	// application's commands: size and index count what it queued.
	size, first := len(cmds), 0
	tx := isTx(cmds)
	if tx {
		size, first = size-2, 1
	}
	for i, c := range cmds {
		name := strings.ToLower(c.Name())
		if name == "multi" || name == "exec" {
			continue
		}
		ops[i] = t.begin(ctx, c, true, b)
		if ops[i] != nil {
			ops[i].span.SetAttr("pipeline_size", strconv.Itoa(size))
			ops[i].span.SetAttr("pipeline_index", strconv.Itoa(i-first))
			if tx {
				ops[i].span.SetAttr("transaction", "true")
			}
		}
	}
	return ops
}

// isTx reports whether a pipeline is a MULTI ... EXEC transaction.
func isTx[C Cmd](cmds []C) bool {
	return len(cmds) >= 2 && strings.EqualFold(cmds[0].Name(), "multi") &&
		strings.EqualFold(cmds[len(cmds)-1].Name(), "exec")
}

// txOutcome says how the EXEC of a transaction ended: committed (its commands
// ran; one of them can still have failed by itself), aborted (a watched key
// changed, nothing ran) or failed (EXEC was refused or never answered).
func (t *Tracer) txOutcome(exec error) string {
	switch {
	case exec == nil:
		return "committed"
	case t.TxFailed != nil && errors.Is(exec, t.TxFailed):
		return "aborted"
	}
	return "failed"
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
	outcome := ""
	if isTx(cmds) {
		outcome = t.txOutcome(cmds[len(cmds)-1].Err())
	}
	deadline := t.deadline()
	for i, o := range ops {
		if i >= len(cmds) || o == nil {
			continue
		}
		if outcome != "" {
			o.span.SetAttr("transaction_outcome", outcome)
		}
		o.finish(ctx, t, cmds[i], cmds[i].Err(), deadline)
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
	sc, _ := core.FromContext(ctx)
	first := ""
	if len(keys) > 0 {
		first = t.ShowKey(sc.SessionID, Str(args[keys[0]]))
	}
	_, span := core.Begin(ctx, kind, SpanName(args, first))
	if span == nil {
		return nil
	}
	masked := t.MaskArgs(cmd.Name(), args, keys)
	for _, k := range keys {
		if k < len(masked) {
			masked[k] = t.ShowKey(sc.SessionID, masked[k])
		}
	}
	if _, known := Lookup(cmd.Name()); !known {
		for i := 1; i < len(masked); i++ { // any argument may be a key
			masked[i] = t.ShowKey(sc.SessionID, masked[i])
		}
	}
	span.SetReq(CapArgs(masked))
	if pipeline {
		span.SetAttr("pipeline", "true")
	}
	o := &Op{span: span, session: sc.SessionID, spec: s, args: args, keys: keys, masked: masked, name: cmd.Name()}
	if IsPublish(cmd.Name()) {
		// Told apart from a write of a key by the names tqueue gives a publish.
		span.SetAttr("system", "redis")
		span.SetAttr("queue", first)
	} else if s.Write && b != nil {
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

// beforeImages reads the before-image of every key o writes that matches an
// image pattern. Inside a pipeline only the first write of a key gets one:
// later writes of the same key would read the same stale value.
func (t *Tracer) beforeImages(ctx context.Context, o *Op, b *batch) {
	o.before, o.snaps = map[int]core.PayloadRef{}, map[int]*snap{}
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
		ref, seen, ok := t.beforeImage(ctx, o.span, k, b)
		if b.pipeline {
			if b.imaged == nil {
				b.imaged = map[string]bool{}
			}
			b.imaged[k] = ok
		}
		if ok {
			o.before[i] = ref
			o.snaps[i] = seen
			o.imaged = append(o.imaged, i)
		}
	}
}

// beforeImage returns the before-image of key and what the read saw (nil for
// a sensitive key, which is not read).
func (t *Tracer) beforeImage(ctx context.Context, span *core.Span, key string, b *batch) (core.PayloadRef, *snap, bool) {
	if t.KeySensitive(key) { // never read a sensitive value just to mask it
		return core.Payload(core.RedactedValue), nil, true
	}
	if b.reads <= 0 {
		span.SetAttr("image_skipped", "cap")
		return core.PayloadRef{}, nil, false
	}
	b.reads--
	if expired(b.deadline) {
		span.SetAttr("image_skipped", "deadline")
		return core.PayloadRef{}, nil, false
	}
	ref, seen, ok := t.image(ctx, key, b.deadline)
	if !ok {
		span.SetAttr("image_skipped", "error")
	}
	return ref, seen, ok
}

// finish records the reply and the mutations and ends the span. deadline
// bounds the after-image reads.
func (o *Op) finish(ctx context.Context, t *Tracer, cmd Cmd, err error, deadline time.Time) {
	if o == nil {
		return
	}
	defer o.span.End()
	if err != nil && !errors.Is(err, t.Nil) {
		o.span.SetErr(o.maskErr(err))
		return
	}
	var reply any
	if err == nil {
		if v, scalar, ok := t.Reply(cmd); ok {
			reply = v
			if v, ok := t.MaskReply(o.name, o.args, o.keys, v, scalar); ok {
				o.span.SetResp(o.replyKeys(t, v))
			}
		}
	}
	if !o.spec.Write || err != nil || NoOp(o.name, reply) || IsPublish(o.name) {
		return // a read, a miss, a write that changed nothing or a message: no mutation
	}
	name := strings.ToLower(cmd.Name())
	popped := ""
	if IsPop(name) {
		popped = t.PoppedKey(cmd)
	}
	truncated := false // the span is flagged once
	for _, i := range WrittenKeys(name, o.args, o.keys, popped) {
		key := Str(o.args[i])
		shown := t.ShowKey(o.session, key)
		m := core.Mutation{
			Store:  "redis",
			Entity: KeyPattern(shown),
			PK:     map[string]any{"key": shown},
			Op:     name,
			Source: "sdk",
		}
		if b, ok := o.before[i]; ok {
			m.Before = b
		}
		switch {
		case slices.Contains(o.after, i):
			if t.KeySensitive(key) {
				// Not read, so whether the value changed is not known.
				m.After = core.Payload(core.RedactedValue)
				o.span.SetAttr("diff_skipped", "sensitive_key")
			} else if expired(deadline) {
				o.span.SetAttr("image_skipped", "deadline")
			} else if ref, seen, ok := t.image(ctx, key, deadline); ok {
				m.After = ref
				var cut bool
				if m.Diff, cut = diffSnaps(o.snaps[i], seen); cut && !truncated {
					truncated = true
					o.span.AddFlag("diff_truncated")
				}
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

// maskErr masks in err's text the arguments that are recorded masked: a
// server error can quote them ("unknown command ..., with args beginning
// with: ...").
func (o *Op) maskErr(err error) error {
	text := err.Error()
	for i := 1; i < len(o.args) && i < len(o.masked); i++ {
		if raw := Str(o.args[i]); len(raw) >= 4 && raw != o.masked[i] {
			text = strings.ReplaceAll(text, raw, o.masked[i])
		}
	}
	if text == err.Error() {
		return err
	}
	return errors.New(text)
}

// replyKeys masks the key names a redacted reply v carries: KEYS and RANDOMKEY
// return key names, and a multi-key pop names the key it popped from first.
func (o *Op) replyKeys(t *Tracer, v any) any {
	show := func(k string) string { return t.ShowKey(o.session, k) }
	name := strings.ToLower(o.name)
	switch l := v.(type) {
	case string:
		if name == "randomkey" {
			return show(l)
		}
	case []string:
		switch {
		case name == "keys":
			for i := range l {
				l[i] = show(l[i])
			}
		case IsPop(name) && len(l) > 0:
			l[0] = show(l[0])
		}
	case []any:
		if IsPop(name) && len(l) > 0 {
			if s, ok := l[0].(string); ok {
				l[0] = show(s)
			}
		}
	}
	return v
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
// capped at about ImageLimit elements. It returns the image and what the read
// saw, for the diff. ok=false means the read failed or timed out: image
// capture must never affect the host call.
func (t *Tracer) image(ctx context.Context, key string, deadline time.Time) (core.PayloadRef, *snap, bool) {
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
		return core.PayloadRef{}, nil, false
	}
	seen := &snap{typ: typ}
	var v any
	switch typ {
	case "string":
		var s string
		if s, err = t.Reader.Get(ctx, key); err == nil {
			seen.items = []item{{shown: RedactStr(s), sum: sumOf(s)}}
			v = seen.items[0].shown
		}
	case "hash":
		var kv []string // field, value, field, value, ...
		if kv, seen.capped, err = t.Reader.HScan(ctx, key, ImageLimit); err == nil {
			m := make(map[string]string, len(kv)/2)
			for i := 0; i+1 < len(kv); i += 2 {
				if len(m) == ImageLimit {
					seen.capped = true
					break
				}
				f, shown := kv[i], core.RedactedValue
				if !NameSensitive(f) {
					shown = RedactStr(kv[i+1])
				}
				m[f] = shown
				seen.items = append(seen.items, item{name: f, shown: shown, sum: sumOf(kv[i+1])})
			}
			v = m
		}
	case "list", "set":
		var l []string
		if typ == "list" {
			l, err = t.Reader.LRange(ctx, key, ImageLimit-1)
			seen.capped = len(l) >= ImageLimit // the list may go on
		} else {
			l, seen.capped, err = t.Reader.SScan(ctx, key, ImageLimit)
		}
		if len(l) > ImageLimit {
			l, seen.capped = l[:ImageLimit], true
		}
		for i := range l {
			sum := sumOf(l[i])
			l[i] = RedactStr(l[i])
			seen.items = append(seen.items, item{shown: l[i], sum: sum})
		}
		v = l
	case "zset":
		var z []Z
		if z, err = t.Reader.ZRange(ctx, key, ImageLimit-1); err == nil {
			seen.capped = len(z) >= ImageLimit
			m := make(map[string]float64, len(z))
			for _, e := range z {
				raw := Str(e.Member)
				member := RedactStr(raw)
				m[member] = e.Score
				seen.items = append(seen.items, item{name: member, shown: member, score: e.Score, sum: sumOf(raw)})
			}
			v = m
		}
	case "none":
		return core.PayloadRef{}, seen, true
	default:
		v = "<" + typ + ">"
	}
	if err != nil {
		return core.PayloadRef{}, nil, false
	}
	return core.Payload(v), seen, true
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
