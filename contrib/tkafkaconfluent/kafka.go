package tkafkaconfluent

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tqueue"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

const (
	maxInjectBytes   = 1024 // total key+value bytes the SDK may add to a record
	maxInjectHeaders = 8    // number of headers the SDK may add
)

// Producer is satisfied by *kafka.Producer; it is an interface so the logic
// can be tested without a broker.
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
}

// Option tunes Consume.
type Option func(*options)

type options struct {
	retryExpected func(attempt int, err error) bool
	maxAttempts   int
	attempt       func(*kafka.Message) int
}

// WithRetryExpected tells the tracer whether a failed attempt will be
// redelivered (see tqueue.Delivery.RetryExpected); it takes precedence over
// WithMaxAttempts.
func WithRetryExpected(f func(attempt int, err error) bool) Option {
	return func(o *options) { o.retryExpected = f }
}

// WithMaxAttempts enables worker.retry / worker.dlq on handler errors.
func WithMaxAttempts(n int) Option { return func(o *options) { o.maxAttempts = n } }

// WithAttempt derives the 1-based attempt number from the message (Kafka has
// no native counter; e.g. read your own retry-count header). Default 1.
func WithAttempt(f func(*kafka.Message) int) Option { return func(o *options) { o.attempt = f } }

// headerCarrier maps core.Carrier onto kafka headers. Get is case-insensitive;
// Set replaces a same-named header (so the message never carries duplicates)
// and appends otherwise, never touching unrelated headers. limit>0 bounds what
// Set may add (bytes, count); an over-budget Set is dropped silently.
type headerCarrier struct {
	h        *[]kafka.Header
	limited  bool
	bytesRem int
	countRem int
}

func newCarrier(h *[]kafka.Header, limited bool) *headerCarrier {
	return &headerCarrier{h: h, limited: limited, bytesRem: maxInjectBytes, countRem: maxInjectHeaders}
}

func (c *headerCarrier) Get(key string) string {
	for _, h := range *c.h {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c *headerCarrier) Set(key, val string) {
	key = strings.ToLower(key)
	for i, h := range *c.h {
		if strings.EqualFold(h.Key, key) {
			if c.limited && len(val) > len(h.Value) && len(val)-len(h.Value) > c.bytesRem {
				return
			}
			(*c.h)[i].Value = []byte(val)
			return
		}
	}
	if c.limited {
		n := len(key) + len(val)
		if c.countRem <= 0 || n > c.bytesRem {
			return
		}
		c.countRem--
		c.bytesRem -= n
	}
	*c.h = append(*c.h, kafka.Header{Key: key, Value: []byte(val)})
}

func topicOf(m *kafka.Message) string {
	if m.TopicPartition.Topic == nil {
		return ""
	}
	return *m.TopicPartition.Topic
}

// bodyParams turns a raw record value into something core can redact by key:
// valid JSON is decoded (numbers kept exact) and run through core.RedactAny,
// anything else through core.RedactText.
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

// params builds the recorded Req/params for a record. Never panics.
func params(m *kafka.Message, withOffset bool) (out any) {
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()
	p := map[string]any{"topic": topicOf(m), "value": bodyParams(m.Value)}
	if len(m.Key) > 0 {
		p["key"] = core.RedactText(string(m.Key))
	}
	if m.TopicPartition.Partition != kafka.PartitionAny {
		p["partition"] = m.TopicPartition.Partition
	}
	if withOffset {
		p["offset"] = int64(m.TopicPartition.Offset)
	}
	return p
}

// Produce publishes msg through p, recording a queue.publish span and
// injecting the trace context into msg.Headers (existing headers are kept).
// With tracing off or no session in ctx it is exactly p.Produce(msg, delivery).
func Produce(ctx context.Context, p Producer, msg *kafka.Message, delivery chan kafka.Event) error {
	if msg == nil || !core.Enabled() {
		return p.Produce(msg, delivery)
	}
	if _, ok := core.FromContext(ctx); !ok {
		return p.Produce(msg, delivery)
	}
	orig := msg.Headers
	hdrs := append([]kafka.Header(nil), orig...) // never mutate a caller-shared backing array
	topic := topicOf(msg)
	m := tqueue.Message{System: "kafka", Queue: topic, JobName: topic, Params: params(msg, false)}
	err := tqueue.Publish(ctx, m, newCarrier(&hdrs, true), func(context.Context) error {
		msg.Headers = hdrs
		return p.Produce(msg, delivery)
	})
	return err
}

// Consume runs handler as a traced worker for a fetched message: worker.start
// (continuing the producer's trace from msg.Headers), worker.end for the
// handler outcome, and worker.retry / worker.dlq per the options. Without a
// session in the headers (or tracing disabled) the handler just runs.
func Consume(ctx context.Context, msg *kafka.Message, handler func(ctx context.Context) error, opts ...Option) error {
	if handler == nil {
		return nil
	}
	if msg == nil || !core.Enabled() {
		return handler(ctx)
	}
	var o options
	for _, f := range opts {
		f(&o)
	}
	attempt := 1
	if o.attempt != nil {
		if a := o.attempt(msg); a > 0 {
			attempt = a
		}
	}
	h := append([]kafka.Header(nil), msg.Headers...)
	topic := topicOf(msg)
	d := tqueue.Delivery{
		System: "kafka", Queue: topic, JobName: topic, Params: params(msg, true), Attempt: attempt,
		MessageID:     topic + "/" + strconv.Itoa(int(msg.TopicPartition.Partition)) + "/" + strconv.FormatInt(int64(msg.TopicPartition.Offset), 10),
		MaxAttempts:   o.maxAttempts,
		RetryExpected: o.retryExpected,
	}
	return tqueue.Consume(ctx, newCarrier(&h, false), d, handler)
}
