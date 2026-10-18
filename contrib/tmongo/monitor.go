package tmongo

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"tsession/core"
)

// Option configures Monitor.
type Option func(*tracer)

// WithIgnoreCommands suppresses spans for additional command names.
func WithIgnoreCommands(names ...string) Option {
	return func(t *tracer) {
		for _, n := range names {
			t.ignore[strings.ToLower(n)] = true
		}
	}
}

// noisy commands are connection plumbing, not application behaviour.
var noisy = []string{"hello", "ismaster", "ping", "saslstart", "saslcontinue", "endsessions",
	"buildinfo", "killcursors", "getnonce", "logout", "authenticate"}

// strip lists envelope fields removed from the recorded command.
var strip = map[string]bool{"lsid": true, "$clusterTime": true, "$db": true, "txnNumber": true,
	"$readPreference": true, "signature": true, "$configTime": true, "$topologyTime": true,
	"readConcern": true, "writeConcern": true, "maxTimeMS": true, "apiVersion": true,
	"apiStrict": true, "apiDeprecationErrors": true}

type tracer struct {
	ignore  map[string]bool
	pending sync.Map // RequestID -> *inflight
}

type inflight struct {
	span *core.Span
	cmd  string
	ent  string
	body doc
	txID string

	// deferred commitTransaction span (begun on completion)
	ctx   context.Context
	label string
	db    string
}

// endCommit emits the db.tx.commit (or, on failure, db.tx.rollback with
// commit_failed=true, matching tsql) event for a finished commitTransaction.
func (in *inflight) endCommit(err error, durUS int64) {
	kind := core.KindDBTxCommit
	if err != nil {
		kind = core.KindDBTxRollbck
	}
	_, sp := core.Begin(in.ctx, kind, in.label)
	if sp == nil {
		return
	}
	sp.SetAttr("db", in.db)
	sp.SetAttr("duration_us", strconv.FormatInt(durUS, 10))
	if in.txID != "" {
		sp.SetTx(in.txID)
	}
	if err != nil {
		sp.SetAttr("commit_failed", "true")
		sp.SetErr(err)
	}
	sp.End()
}

// monitors holds every monitor built here; it is the idempotency marker for Apply.
var monitors sync.Map // *event.CommandMonitor -> struct{}

// Monitor returns a command monitor that records spans into the active session.
func Monitor(opts ...Option) *event.CommandMonitor {
	t := &tracer{ignore: map[string]bool{}}
	for _, n := range noisy {
		t.ignore[n] = true
	}
	for _, o := range opts {
		o(t)
	}
	m := &event.CommandMonitor{Started: t.started, Succeeded: t.succeeded, Failed: t.failed}
	monitors.Store(m, struct{}{})
	return m
}

// Apply installs the monitor on clientOpts, chaining any monitor already set.
// It is a no-op when the SDK is disabled or the options are already traced.
func Apply(clientOpts *options.ClientOptions, opts ...Option) *options.ClientOptions {
	if clientOpts == nil || !core.Enabled() {
		return clientOpts
	}
	prev := clientOpts.Monitor
	if prev != nil {
		if _, ours := monitors.Load(prev); ours {
			return clientOpts
		}
	}
	m := Monitor(opts...)
	if prev != nil {
		m = chain(m, prev)
		monitors.Store(m, struct{}{})
	}
	return clientOpts.SetMonitor(m)
}

func chain(a, b *event.CommandMonitor) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(ctx context.Context, e *event.CommandStartedEvent) {
			a.Started(ctx, e)
			if b.Started != nil {
				b.Started(ctx, e)
			}
		},
		Succeeded: func(ctx context.Context, e *event.CommandSucceededEvent) {
			a.Succeeded(ctx, e)
			if b.Succeeded != nil {
				b.Succeeded(ctx, e)
			}
		},
		Failed: func(ctx context.Context, e *event.CommandFailedEvent) {
			a.Failed(ctx, e)
			if b.Failed != nil {
				b.Failed(ctx, e)
			}
		},
	}
}

func (t *tracer) started(ctx context.Context, e *event.CommandStartedEvent) {
	defer func() { _ = recover() }()
	name := strings.ToLower(e.CommandName)
	if t.ignore[name] {
		return
	}
	var d bson.D
	if bson.Unmarshal(e.Command, &d) != nil || len(d) == 0 {
		return
	}
	body := redactDoc(plain(d)).(doc)
	ent := entityOf(name, d[0], body)
	kind := core.KindDBRead
	switch {
	case name == "committransaction":
		kind = core.KindDBTxCommit
	case name == "aborttransaction":
		kind = core.KindDBTxRollbck
	case isWrite(name, body):
		kind = core.KindDBWrite
	}
	label := e.CommandName + " " + e.DatabaseName
	if ent != "" {
		label += "." + ent
	}
	if name == "committransaction" {
		// The span kind depends on the outcome (a failed commit is a rollback,
		// as in tsql), so the span is begun when the reply arrives.
		if _, probe := core.Begin(ctx, kind, label); probe == nil {
			return
		}
		t.pending.Store(e.RequestID, &inflight{ctx: ctx, label: label, db: e.DatabaseName, cmd: name, body: body, txID: txIDOf(body)})
		return
	}
	_, span := core.Begin(ctx, kind, label)
	if span == nil {
		return
	}
	in := &inflight{span: span, cmd: name, ent: ent, body: body, txID: txIDOf(body)}
	req := make(doc, len(body))
	for k, v := range body {
		if !strip[k] {
			req[k] = v
		}
	}
	span.SetReq(req)
	span.SetAttr("db", e.DatabaseName)
	if in.txID != "" {
		span.SetTx(in.txID)
	}
	t.pending.Store(e.RequestID, in)
}

func (t *tracer) succeeded(_ context.Context, e *event.CommandSucceededEvent) {
	defer func() { _ = recover() }()
	v, ok := t.pending.LoadAndDelete(e.RequestID)
	if !ok {
		return
	}
	in := v.(*inflight)
	if in.cmd == "committransaction" {
		in.endCommit(nil, e.Duration.Microseconds())
		return
	}
	defer in.span.End()
	var rd bson.D
	if bson.Unmarshal(e.Reply, &rd) != nil {
		return
	}
	reply := redactDoc(plain(rd)).(doc)
	in.span.SetResp(summarize(reply))
	in.span.SetAttr("duration_us", strconv.FormatInt(e.Duration.Microseconds(), 10))
	if we := asArr(reply["writeErrors"]); len(we) > 0 {
		in.span.AddFlag("write_errors")
	}
	if isWrite(in.cmd, in.body) {
		for _, m := range mutationsFor(in, reply) {
			in.span.AddMutation(m)
		}
	} else if rs, ok := readSummary(in, reply); ok {
		in.span.AddRead(rs)
	}
}

func (t *tracer) failed(_ context.Context, e *event.CommandFailedEvent) {
	defer func() { _ = recover() }()
	v, ok := t.pending.LoadAndDelete(e.RequestID)
	if !ok {
		return
	}
	in := v.(*inflight)
	if in.cmd == "committransaction" {
		in.endCommit(e.Failure, e.Duration.Microseconds())
		return
	}
	in.span.SetErr(e.Failure)
	in.span.End()
}

// entityOf returns the collection a command targets. For most commands it is
// the value of the first field; getMore carries it in "collection".
func entityOf(name string, first bson.E, body doc) string {
	if name == "getmore" {
		return str(body["collection"])
	}
	if s, ok := first.Value.(string); ok && !strings.HasPrefix(first.Key, "$") {
		return s
	}
	if name == "bulkwrite" {
		if ns := asArr(body["nsInfo"]); len(ns) > 0 {
			s := str(asDoc(ns[0])["ns"])
			return s[strings.Index(s, ".")+1:]
		}
	}
	return ""
}

// txIDOf derives a stable id from lsid + txnNumber for multi-document transactions.
func txIDOf(body doc) string {
	if body["autocommit"] != false {
		return ""
	}
	b, _ := asDoc(body["lsid"])["id"].(doc)
	data, _ := b["$binary"].([]byte)
	if len(data) == 0 {
		return ""
	}
	return hex.EncodeToString(data) + "-" + strconv.FormatInt(num(body["txnNumber"]), 10)
}
