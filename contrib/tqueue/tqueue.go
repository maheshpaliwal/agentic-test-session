package tqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"tsession/core"
)

// Message describes a job being published.
type Message struct {
	System  string // kafka, sqs, amqp, asynq, ...
	Queue   string // topic / queue / exchange
	JobName string
	Params  any // full job params; recorded as Req (redacted, capped)
	Delay   time.Duration
	Options map[string]string // free-form, recorded as span attrs

	flags []string // span flags decided by adapters before publishing
}

// Delivery describes a job being handled.
type Delivery struct {
	System      string
	Queue       string
	JobName     string
	Params      any
	Attempt     int // 1-based; 0 is treated as 1
	MessageID   string
	MaxAttempts int // >0 enables worker.retry / worker.dlq on handler error

	// RetryExpected tells tqueue whether a failed attempt will be redelivered
	// (SQS visibility timeout, Kafka consumer retry, ...). When set it takes
	// precedence over MaxAttempts. When a retry is expected the attempt's
	// worker.end carries Attrs["retry_pending"]="true" and a worker.retry is
	// emitted, so the collector keeps the pending-async entry open. With an
	// unknown retry policy (RetryExpected nil and MaxAttempts 0) tqueue cannot
	// tell and every attempt's worker.end closes the entry (no retry_pending).
	RetryExpected func(attempt int, err error) bool
}

// retryPending reports whether a failed attempt will be followed by another.
func (d Delivery) retryPending(attempt int, err error) bool {
	if err == nil {
		return false
	}
	if d.RetryExpected != nil {
		return d.RetryExpected(attempt, err)
	}
	return d.MaxAttempts > 0 && attempt < d.MaxAttempts
}

type pubMarker struct{ system, queue, job string }
type pubKey struct{}
type consKey struct{}

func (m Message) name() string {
	if m.JobName != "" {
		return m.JobName
	}
	return m.Queue
}

func (d Delivery) name() string {
	if d.JobName != "" {
		return d.JobName
	}
	return d.Queue
}

// Publish records a queue.publish span (Req = job params), injects the trace
// context into carrier and calls send with the publish span's context. When
// tracing is off or no session applies, send runs untouched. Wrapping the
// same publish twice (nested Publish with identical message identity under the
// same ctx) records only once.
func Publish(ctx context.Context, m Message, carrier core.Carrier, send func(ctx context.Context) error) error {
	if send == nil {
		return nil
	}
	return publish(ctx, m, carrier, func(c context.Context, _ *core.Span) error { return send(c) })
}

// publish is Publish with access to the publish span (nil when untraced).
func publish(ctx context.Context, m Message, carrier core.Carrier, send func(ctx context.Context, span *core.Span) error) error {
	if !core.Enabled() || carrier == nil {
		return send(ctx, nil)
	}
	mk := pubMarker{m.System, m.Queue, m.JobName}
	if v, ok := ctx.Value(pubKey{}).(pubMarker); ok && v == mk {
		return send(ctx, nil)
	}
	sctx, span := core.Begin(ctx, core.KindQueuePub, m.name())
	if span == nil {
		return send(ctx, nil)
	}
	for _, f := range m.flags {
		span.AddFlag(f)
	}
	sctx = context.WithValue(sctx, pubKey{}, mk)
	span.SetReq(m.Params)
	setAttrs(span, m.System, m.Queue, m.Options)
	if m.Delay > 0 {
		span.SetAttr("delay_ms", strconv.FormatInt(m.Delay.Milliseconds(), 10))
	}
	core.Inject(sctx, carrier)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				span.SetErr(fmt.Errorf("panic: %v", r))
				span.End()
				panic(r)
			}
		}()
		err = send(sctx, span)
	}()
	span.SetErr(err)
	span.End()
	return err
}

func setAttrs(s *core.Span, system, queue string, opts map[string]string) {
	if system != "" {
		s.SetAttr("system", system)
	}
	if queue != "" {
		s.SetAttr("queue", queue)
	}
	for k, v := range opts {
		s.SetAttr("opt."+k, v)
	}
}

// Consume extracts the trace from carrier and runs handler under it. It emits
// worker.start (immediately), then worker.end for the handler outcome, plus
// worker.retry or worker.dlq when the handler fails and MaxAttempts is set.
// Without a session in the carrier (and no exclusive session) the handler just
// runs untraced (no events). A failed attempt that will be retried (see
// Delivery.RetryExpected / MaxAttempts) marks its worker.end retry_pending. Panics are recorded and re-raised.
func Consume(ctx context.Context, carrier core.Carrier, d Delivery, handler func(ctx context.Context) error) error {
	if handler == nil {
		return nil
	}
	if !core.Enabled() || carrier == nil {
		return handler(ctx)
	}
	if v, ok := ctx.Value(consKey{}).(Delivery); ok && v.MessageID == d.MessageID && v.Queue == d.Queue &&
		v.Attempt == d.Attempt && v.JobName == d.JobName {
		return handler(ctx) // already wrapped
	}
	attempt := d.Attempt
	if attempt < 1 {
		attempt = 1
	}
	hctx, start := core.BeginIngress(ctx, core.KindWorkerStart, d.name(), carrier)
	if start == nil {
		return handler(ctx)
	}
	pub := ""
	if sc, ok := core.ExtractSpanContext(carrier); ok {
		pub = sc.SpanID
	}
	start.SetReq(d.Params)
	start.SetAttempt(attempt)
	setAttrs(start, d.System, d.Queue, nil)
	if d.MessageID != "" {
		start.SetAttr("message_id", d.MessageID)
	}
	start.End() // worker.start is a point event

	// Nested publishes inside the handler must not be mistaken for this one.
	hctx = context.WithValue(hctx, pubKey{}, nil)
	hctx = context.WithValue(hctx, consKey{}, d)

	// worker.end starts now so its duration covers the handler.
	_, end := core.Begin(hctx, core.KindWorkerEnd, d.name())
	if pub != "" {
		end.AddLink(pub)
	}
	end.SetAttempt(attempt)
	setAttrs(end, d.System, d.Queue, nil)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				perr := fmt.Errorf("panic: %v", r)
				finish(hctx, end, d, attempt, pub, perr)
				panic(r)
			}
		}()
		err = handler(hctx)
	}()
	finish(hctx, end, d, attempt, pub, err)
	return err
}

// finish closes worker.end and emits retry/dlq. Both are children of
// worker.start (hctx carries start as the current span).
func finish(hctx context.Context, end *core.Span, d Delivery, attempt int, pub string, err error) {
	end.SetErr(err)
	retry := d.retryPending(attempt, err)
	if retry {
		end.SetAttr("retry_pending", "true")
	}
	if err != nil {
		end.SetResp(map[string]any{"ok": false, "error": err.Error()})
	} else {
		end.SetResp(map[string]any{"ok": true})
	}
	end.End()
	if err == nil || (!retry && d.MaxAttempts <= 0) {
		return
	}
	kind := core.KindWorkerDLQ
	if retry {
		kind = core.KindWorkerRetry
	}
	_, s := core.Begin(hctx, kind, d.name())
	if pub != "" {
		s.AddLink(pub)
	}
	s.SetAttempt(attempt)
	s.SetErr(err)
	if d.MaxAttempts > 0 {
		s.SetAttr("max_attempts", strconv.Itoa(d.MaxAttempts))
	}
	s.End()
}

// ErrNotFound is returned by SideChannel.Get on a miss or expiry.
var ErrNotFound = errors.New("tqueue: side-channel key not found")

// bodyParams turns a raw message body into a value core.Payload can redact by
// key: valid JSON is decoded to structured data and passed through
// core.RedactAny; anything else is scrubbed with core.RedactText.
func bodyParams(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) == nil {
			return core.RedactAny(v)
		}
	}
	return core.RedactText(string(b))
}

func flag(s *core.Span, f string) {
	if s != nil {
		s.AddFlag(f)
	}
}
