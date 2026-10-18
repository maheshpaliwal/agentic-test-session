package tqueue

import (
	"context"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"

	"tsession/core"
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
	// PerMessageSpans: one queue.publish span (and one WriteMessages call) per
	// message so every message links to exactly one worker. nil means the
	// default: true when ctx carries an active session. When false, the batch
	// is written in a single call under one publish span that all messages
	// share (the first consumer's worker.end closes its pending entry).
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
// in ctx each message gets its own queue.publish span and WriteMessages call;
// otherwise (tracing off / no session) the original batch is written once,
// untouched.
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
	for _, m := range msgs {
		m.Headers = append([]kafka.Header(nil), m.Headers...)
		info := Message{System: "kafka", Queue: m.Topic, JobName: m.Topic, Params: bodyParams(m.Value)}
		err := Publish(ctx, info, kafkaCarrier{&m.Headers}, func(c context.Context) error {
			return w.WriteMessages(c, m)
		})
		if err != nil {
			return err
		}
	}
	return nil
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
				kc.Set(k, v)
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
