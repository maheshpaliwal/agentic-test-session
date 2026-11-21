package tqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// kafkaCarrier maps the carrier onto kafka message headers.
type kafkaCarrier struct{ h *[]kafka.Header }

func (c kafkaCarrier) Get(key string) string {
	for _, h := range *c.h {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaCarrier) Set(key, val string) {
	key = strings.ToLower(key)
	for i, h := range *c.h {
		if strings.EqualFold(h.Key, key) {
			(*c.h)[i].Value = []byte(val)
			return
		}
	}
	*c.h = append(*c.h, kafka.Header{Key: key, Value: []byte(val)})
}

// KafkaWriter is satisfied by *kafka.Writer.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// KafkaOptions tunes ProduceMessagesOpts.
type KafkaOptions struct {
	// PerMessageSpans: one queue.publish span per message so every message
	// links to exactly one worker. nil means the default: true when ctx
	// carries an active session. When false, one publish span is shared by all
	// messages (the first consumer's worker.end closes its pending entry).
	// Either way the batch is written with a single WriteMessages call.
	PerMessageSpans *bool
}

// sessionActive reports whether Publish would record a span for ctx.
func sessionActive(ctx context.Context) bool {
	if !core.Enabled() {
		return false
	}
	if _, ok := core.FromContext(ctx); !ok {
		return false
	}
	_, sp := core.Begin(ctx, core.KindQueuePub, "")
	return sp != nil // never ended, so never recorded
}

// ProduceMessages writes messages with trace headers. With an active session
// in ctx each message gets its own queue.publish span, and the batch (copies
// carrying the headers) is written with one WriteMessages call, exactly as the
// caller batched it; otherwise (tracing off / no session) the original batch
// is written once, untouched.
func ProduceMessages(ctx context.Context, w KafkaWriter, msgs ...kafka.Message) error {
	return ProduceMessagesOpts(ctx, w, KafkaOptions{}, msgs...)
}

// ProduceMessagesOpts is ProduceMessages with options.
func ProduceMessagesOpts(ctx context.Context, w KafkaWriter, opts KafkaOptions, msgs ...kafka.Message) error {
	if len(msgs) == 0 || !sessionActive(ctx) {
		return w.WriteMessages(ctx, msgs...)
	}
	if opts.PerMessageSpans != nil && !*opts.PerMessageSpans {
		return produceBatch(ctx, w, msgs)
	}
	return produceEach(ctx, w, msgs)
}

// produceEach writes the batch in one WriteMessages call under one
// queue.publish span per message. Splitting the batch into one call per
// message would change the application's batching: a synchronous kafka.Writer
// blocks each call until its batch flushes.
func produceEach(ctx context.Context, w KafkaWriter, msgs []kafka.Message) (err error) {
	cp := make([]kafka.Message, len(msgs))
	spans := make([]*core.Span, len(msgs)) // nil where a message is not traced
	wctx, marked := ctx, false
	for i, m := range msgs {
		m.Headers = append([]kafka.Header(nil), m.Headers...)
		cp[i] = m
		mk := pubMarker{"kafka", m.Topic, m.Topic}
		if v, ok := ctx.Value(pubKey{}).(pubMarker); ok && v == mk {
			continue // already wrapped by an enclosing publish
		}
		sctx, span := core.Begin(ctx, core.KindQueuePub, m.Topic)
		if span == nil {
			continue
		}
		span.SetReq(rendered(bodyParams(m.Value)))
		setAttrs(span, "kafka", m.Topic, nil)
		core.Inject(sctx, kafkaCarrier{&cp[i].Headers})
		spans[i] = span
		if !marked { // nested publishes of the same topic are not recorded twice
			wctx, marked = context.WithValue(ctx, pubKey{}, mk), true
		}
	}
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		// kafka.WriteErrors reports the outcome per message; anything else
		// applies to the whole batch.
		var each kafka.WriteErrors
		if !errors.As(err, &each) || len(each) != len(spans) {
			each = nil
		}
		for i, span := range spans {
			if each != nil {
				span.SetErr(each[i])
			} else {
				span.SetErr(err)
			}
			span.End()
		}
		if r != nil {
			panic(r)
		}
	}()
	return w.WriteMessages(wctx, cp...)
}

func produceBatch(ctx context.Context, w KafkaWriter, msgs []kafka.Message) error {
	cp := make([]kafka.Message, len(msgs))
	vals := make([]any, len(msgs))
	for i, m := range msgs {
		m.Headers = append([]kafka.Header(nil), m.Headers...)
		cp[i] = m
		vals[i] = bodyParams(m.Value)
	}
	carrier := core.MapCarrier{}
	info := Message{System: "kafka", Queue: msgs[0].Topic, JobName: msgs[0].Topic, Params: vals}
	return Publish(ctx, info, carrier, func(c context.Context) error {
		for i := range cp {
			kc := kafkaCarrier{&cp[i].Headers}
			for k, v := range carrier {
				switch k {
				case core.KeyTraceparent:
					// Decided per message by WriteTraceparent: a message
					// may carry the application's own.
				case core.KeyTSessionTraceparent:
					core.WriteTraceparent(kc, v)
				default:
					kc.Set(k, v)
				}
			}
		}
		return w.WriteMessages(c, cp...)
	})
}

// ConsumeMessage runs handler as a traced worker for a fetched message.
func ConsumeMessage(ctx context.Context, msg kafka.Message, handler func(ctx context.Context) error) error {
	h := append([]kafka.Header(nil), msg.Headers...)
	d := Delivery{
		System: "kafka", Queue: msg.Topic, JobName: msg.Topic, Params: bodyParams(msg.Value), Attempt: 1,
		MessageID: msg.Topic + "/" + strconv.Itoa(msg.Partition) + "/" + strconv.FormatInt(msg.Offset, 10),
	}
	return Consume(ctx, kafkaCarrier{&h}, d, handler)
}
