package tqueue

import (
	"context"
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
	if !core.Enabled() || carrier == nil {
		return send(ctx)
	}
	mk := pubMarker{m.System, m.Queue, m.JobName}
	if v, ok := ctx.Value(pubKey{}).(pubMarker); ok && v == mk {
		return send(ctx)
	}
	sctx, span := core.Begin(ctx, core.KindQueuePub, m.name())
	if span == nil {
		return send(ctx)
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
		err = send(sctx)
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
// runs. Panics are recorded and re-raised.
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
	if err != nil {
		end.SetResp(map[string]any{"ok": false, "error": err.Error()})
	} else {
		end.SetResp(map[string]any{"ok": true})
	}
	end.End()
	if err == nil || d.MaxAttempts <= 0 {
		return
	}
	kind := core.KindWorkerDLQ
	if attempt < d.MaxAttempts {
		kind = core.KindWorkerRetry
	}
	_, s := core.Begin(hctx, kind, d.name())
	if pub != "" {
		s.AddLink(pub)
	}
	s.SetAttempt(attempt)
	s.SetErr(err)
	s.SetAttr("max_attempts", strconv.Itoa(d.MaxAttempts))
	s.End()
}

// ErrNotFound is returned by SideChannel.Get on a miss or expiry.
var ErrNotFound = errors.New("tqueue: side-channel key not found")
