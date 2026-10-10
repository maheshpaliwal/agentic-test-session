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

// ProduceMessages writes each message with trace headers under its own
// queue.publish span (so every message links to exactly one worker). Messages
// are written one call per message when tracing is active; with tracing off
// they are passed through in a single call.
func ProduceMessages(ctx context.Context, w KafkaWriter, msgs ...kafka.Message) error {
	if !core.Enabled() {
		return w.WriteMessages(ctx, msgs...)
	}
	for _, m := range msgs {
		m.Headers = append([]kafka.Header(nil), m.Headers...)
		info := Message{System: "kafka", Queue: m.Topic, JobName: m.Topic, Params: m.Value}
		err := Publish(ctx, info, kafkaCarrier{&m.Headers}, func(c context.Context) error {
			return w.WriteMessages(c, m)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ConsumeMessage runs handler as a traced worker for a fetched message.
func ConsumeMessage(ctx context.Context, msg kafka.Message, handler func(ctx context.Context) error) error {
	h := append([]kafka.Header(nil), msg.Headers...)
	d := Delivery{
		System: "kafka", Queue: msg.Topic, JobName: msg.Topic, Params: msg.Value, Attempt: 1,
		MessageID: msg.Topic + "/" + strconv.Itoa(msg.Partition) + "/" + strconv.FormatInt(msg.Offset, 10),
	}
	return Consume(ctx, kafkaCarrier{&h}, d, handler)
}
