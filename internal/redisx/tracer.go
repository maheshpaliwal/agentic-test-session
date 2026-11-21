package redisx

import (
	"context"
	"errors"
	"io"
	"net"
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
	FixedErrs []error                               // client errors with a fixed text (closed client, pool timeout)
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
	span      *core.Span
	session   string // scopes the hash of masked key names
	spec      Spec
	args      []any
	masked    []string // args as they are recorded, index-aligned
	sensitive bool     // an argument is recorded masked, or a key is sensitive
	sensKey   bool     // a key is sensitive: no value of any of the command's keys is read
	name      string
	keys      []int
	before    map[int]core.PayloadRef // before-images, by arg index
	snaps     map[int]*snap           // what the before-image reads saw, for the diff
	imaged    []int                   // arg indices of keys under image capture
	after     []int                   // subset of imaged whose after-image this op reads
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
	masked, sensKey, sensitive := t.record(sc.SessionID, cmd.Name(), args, keys)
	first := ""
	if len(keys) > 0 {
		first = masked[keys[0]]
	}
	_, span := core.Begin(ctx, kind, SpanName(args, first))
	if span == nil {
		return nil
	}
	span.SetReq(CapArgs(masked))
	if pipeline {
		span.SetAttr("pipeline", "true")
	}
	o := &Op{span: span, session: sc.SessionID, spec: s, args: args, keys: keys, masked: masked,
		sensitive: sensitive, sensKey: sensKey, name: cmd.Name()}
	if IsPublish(cmd.Name()) {
		// Told apart from a write of a key by the names tqueue gives a publish.
		span.SetAttr("system", "redis")
		span.SetAttr("queue", first)
	} else if s.Write && b != nil {
		t.beforeImages(ctx, o, b)
	}
	return o
}

// record returns the arguments as they are recorded, index-aligned with args:
// credentials and values masked by the command's semantics (MaskArgs), then
// every argument masked as a key name. The second step does not depend on the
// command: a key that the command table does not list as one (the keys after
// STREAMS of XREAD, a MATCH or BY pattern, a key handed to a script as ARGV,
// any argument of a command the tracer does not know) is recorded the way it
// is as the key of SET. sensKey reports that a key is sensitive, and sensitive
// that one is or that an argument is not recorded as it was sent.
func (t *Tracer) record(session, name string, args []any, keys []int) (masked []string, sensKey, sensitive bool) {
	masked, sensKey = t.MaskArgs(name, args, keys)
	sensitive = sensKey
	next := 0 // keys is ascending
	for i := 1; i < len(masked); i++ {
		raw := Str(args[i])
		switch {
		case next < len(keys) && keys[next] == i:
			next++
			masked[i] = t.ShowKey(session, raw)
		default:
			masked[i] = t.ShowText(session, masked[i])
		}
		sensitive = sensitive || masked[i] != raw
	}
	return masked, sensKey, sensitive
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
		k := Str(o.args[i]) // the key as Redis knows it, not as it is recorded
		if !matchAny(t.Patterns, k) {
			continue
		}
		if captured, seen := b.imaged[k]; seen {
			if captured {
				o.imaged = append(o.imaged, i)
			}
			continue
		}
		ref, seen, ok := t.beforeImage(ctx, o, k, b)
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

// beforeImage returns the before-image of key and what the read saw.
func (t *Tracer) beforeImage(ctx context.Context, o *Op, key string, b *batch) (core.PayloadRef, *snap, bool) {
	if b.reads <= 0 {
		o.span.SetAttr("image_skipped", "cap")
		return core.PayloadRef{}, nil, false
	}
	b.reads--
	if expired(b.deadline) {
		o.span.SetAttr("image_skipped", "deadline")
		return core.PayloadRef{}, nil, false
	}
	ref, seen, ok := t.image(ctx, o.session, key, o.sensKey, b.deadline)
	if !ok {
		o.span.SetAttr("image_skipped", "error")
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
	miss := err != nil && errors.Is(err, t.Nil)
	if err != nil && !miss {
		o.span.SetErr(o.recordedErr(t, err))
		return
	}
	var reply any
	if err == nil {
		if v, scalar, ok := t.Reply(cmd); ok {
			reply = v
			if v, ok := t.MaskReply(o.session, o.name, o.args, o.keys, v, scalar, o.sensitive); ok {
				o.span.SetResp(v)
			}
		}
	}
	name := strings.ToLower(cmd.Name())
	if !o.spec.Write || IsPublish(name) || miss && !writesOnNil(name, o.args) || NoOp(name, reply) {
		return // a read, a miss, a write that changed nothing or a message: no mutation
	}
	if queued(name, o.args, reply) {
		// Sent between a MULTI and an EXEC that the application issued itself
		// (Do("multi")): nothing has changed yet, and EXEC or DISCARD decides.
		o.span.SetAttr("queued", "true")
		return
	}
	popped := ""
	if IsPop(name) {
		popped = t.PoppedKey(cmd)
	}
	truncated, concurrent := false, false // the span is flagged once
	for _, i := range WrittenKeys(name, o.args, o.keys, popped) {
		key := Str(o.args[i])
		shown := o.masked[i]
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
			if expired(deadline) {
				o.span.SetAttr("image_skipped", "deadline")
			} else if ref, seen, ok := t.image(ctx, o.session, key, o.sensKey, deadline); ok {
				m.After = ref
				was := o.snaps[i]
				if seen.hidden || was != nil && was.hidden {
					// The value is not read, so whether it changed is not known.
					o.span.SetAttr("diff_skipped", "sensitive_key")
					break
				}
				var cut bool
				if m.Diff, cut = diffSnaps(was, seen); cut && !truncated {
					truncated = true
					o.span.AddFlag("diff_truncated")
				}
				if !concurrent && foreignChange(name, o.args, i, was, seen) {
					concurrent = true
					o.span.AddFlag("diff_concurrent")
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

// writesOnNil reports whether the command wrote although its reply is nil:
// GETSET and SET ... GET answer with the old value, which a new key has not.
// (SET ... XX GET on a key that does not exist answers nil and writes nothing.)
func writesOnNil(lname string, args []any) bool {
	switch lname {
	case "getset":
		return true
	case "set":
		return hasOption(args, 3, "GET") && !hasOption(args, 3, "XX")
	}
	return false
}

// valueReplies are writes that answer with a stored value, which can be any
// text, "QUEUED" included.
var valueReplies = map[string]bool{
	"getset": true, "getdel": true, "getex": true, "lpop": true, "rpop": true, "spop": true, "lmove": true,
	"rpoplpush": true, "blmove": true, "brpoplpush": true, "hgetdel": true, "hgetex": true, "lindex": true,
	"eval": true, "evalsha": true, "fcall": true, "json.arrpop": true,
}

// queued reports whether the reply says the command was queued by a MULTI.
func queued(lname string, args []any, reply any) bool {
	s, ok := reply.(string)
	return ok && s == "QUEUED" && !valueReplies[lname] && !(lname == "set" && hasOption(args, 3, "GET"))
}

// errWithheld ends the recorded text of an error whose message is not kept.
const errWithheld = "[text withheld: the command has masked arguments]"

// errCodes are the first words of Redis error replies. Only these are kept of
// a withheld error: a script chooses the text of its error, first word
// included.
var errCodes = map[string]bool{
	"ERR": true, "WRONGTYPE": true, "NOSCRIPT": true, "MOVED": true, "ASK": true, "CLUSTERDOWN": true,
	"CROSSSLOT": true, "TRYAGAIN": true, "READONLY": true, "NOAUTH": true, "NOPERM": true, "WRONGPASS": true,
	"OOM": true, "BUSY": true, "BUSYKEY": true, "BUSYGROUP": true, "NOGROUP": true, "EXECABORT": true,
	"LOADING": true, "MASTERDOWN": true, "MISCONF": true, "NOREPLICAS": true, "NOTBUSY": true,
	"UNKILLABLE": true, "NOPROTO": true, "IOERR": true, "UNBLOCKED": true, "DENIED": true, "INPROG": true,
}

// errPhrases are fixed parts of Redis error messages; the first one found in
// a withheld message is kept, to say what kind of error it was.
var errPhrases = []string{
	"unknown command", "unknown subcommand", "wrong number of arguments", "syntax error",
	"value is not an integer or out of range", "value is not a valid float", "no such key", "index out of range",
	"Operation against a key holding the wrong kind of value", "No matching script",
	"increment or decrement would overflow", "invalid expire time", "timeout is not a float or out of range",
	"timeout is negative", "invalid cursor", "Error compiling script", "Error running script", "not allowed",
}

// recordedErr returns err as it is recorded.
//
// A command that had a masked argument or a sensitive key keeps only what
// cannot hold a piece of an argument or of a stored value: the error code
// from a fixed list (ERR, WRONGTYPE, NOSCRIPT, ...) and, when the message
// holds one, a phrase from a fixed list ("unknown command"); client-side
// errors keep a fixed description. Replacing the arguments in the text does
// not work: Redis quotes a prefix of them ("unknown command 'x', with args
// beginning with: ..."), and a script can put any part of its KEYS and ARGV,
// in any form, into the error it raises.
//
// The error of any other command is scrubbed like a value, word by word.
func (o *Op) recordedErr(t *Tracer, err error) error {
	if !o.sensitive {
		text := err.Error()
		if shown := o.scrubWords(t, text); shown != text {
			return errors.New(shown)
		}
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return err // fixed texts
	case t.TxFailed != nil && errors.Is(err, t.TxFailed):
		return t.TxFailed
	}
	for _, fixed := range t.FixedErrs {
		if errors.Is(err, fixed) {
			return fixed
		}
	}
	var server interface{ RedisError() }
	if errors.As(err, &server) {
		return errors.New(serverErrClass(err.Error()))
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return errors.New("i/o timeout " + errWithheld)
		}
		return errors.New("network error " + errWithheld)
	}
	return errors.New("error " + errWithheld)
}

// serverErrClass returns what is kept of the message of a Redis error reply.
func serverErrClass(msg string) string {
	out := "error"
	if code, _, _ := strings.Cut(msg, " "); errCodes[code] {
		out = code
	}
	for _, p := range errPhrases {
		if strings.Contains(msg, p) {
			out += " " + p
			break
		}
	}
	return out + " " + errWithheld
}

// scrubWords redacts free text that can quote keys and values: it is scrubbed
// as a value, and each word of it is masked as a key name would be.
func (o *Op) scrubWords(t *Tracer, text string) string {
	text = RedactStr(text)
	const stops = " \t\r\n'\"`,;()"
	var b strings.Builder
	for len(text) > 0 {
		n := strings.IndexAny(text, stops)
		if n < 0 {
			n = len(text)
		}
		b.WriteString(t.ShowText(o.session, text[:n]))
		if n < len(text) {
			b.WriteByte(text[n])
			n++
		}
		text = text[n:]
	}
	return b.String()
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
// timeout and the phase deadline, with redaction applied: values are scrubbed,
// and hash field names and members are masked as key names are (ShowText), so
// a field reads the same in the arguments, the images and the diff.
// Collections are capped at about ImageLimit elements. It returns the image
// and what the read saw, for the diff. A key that does not exist has no image.
// A sensitive key is asked for its type only: its value is never read just to
// mask it, and its image is the redaction marker. hide says the same of a key
// whose name is not sensitive: a command with a sensitive key can move its
// value to any of its other keys (RENAME, COPY, LMOVE, SMOVE, the *STORE
// family, a script). ok=false means the read failed or timed out: image
// capture must never affect the host call.
func (t *Tracer) image(ctx context.Context, session, key string, hide bool, deadline time.Time) (core.PayloadRef, *snap, bool) {
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
	if typ == "none" {
		return core.PayloadRef{}, seen, true
	}
	if hide || t.KeySensitive(key) {
		seen.hidden = true
		return core.Payload(core.RedactedValue), seen, true
	}
	text := func(s string) string { return t.Text(session, s) }
	var v any
	switch typ {
	case "string":
		var s string
		if s, err = t.Reader.Get(ctx, key); err == nil {
			seen.items = []item{{shown: text(s), sum: sumOf(s)}}
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
					shown = text(kv[i+1])
				}
				// Two fields can be recorded alike (KeyMaskFlat): they are told
				// apart by what is stored, and numbered in the image.
				name := t.ShowText(session, f)
				m[uniqueKey(m, name)] = shown
				seen.items = append(seen.items, item{id: sumOf(f), name: name, shown: shown, sum: sumOf(kv[i+1])})
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
			l[i] = text(l[i])
			seen.items = append(seen.items, item{id: sum, shown: l[i], sum: sum})
		}
		v = l
	case "zset":
		var z []Z
		if z, err = t.Reader.ZRange(ctx, key, ImageLimit-1); err == nil {
			seen.capped = len(z) >= ImageLimit
			m := make(map[string]any, len(z))
			for _, e := range z {
				raw := Str(e.Member)
				// Two members can be recorded alike ("token=[REDACTED]"): they
				// are told apart by what is stored, and numbered in the image.
				name := text(raw)
				m[uniqueKey(m, name)] = scoreValue(e.Score)
				seen.items = append(seen.items, item{id: sumOf(raw), name: name, score: e.Score})
			}
			v = m
		}
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
