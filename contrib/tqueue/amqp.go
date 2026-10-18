package tqueue

import (
	"context"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type amqpCarrier amqp.Table

func (c amqpCarrier) Get(key string) string {
	if v, ok := c[strings.ToLower(key)]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (c amqpCarrier) Set(key, val string) { c[strings.ToLower(key)] = val }

// AMQPChannel is satisfied by *amqp.Channel.
type AMQPChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// PublishAMQP publishes with trace context in message headers.
func PublishAMQP(ctx context.Context, ch AMQPChannel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if !core.Enabled() {
		return ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	}
	h := amqp.Table{}
	for k, v := range msg.Headers {
		h[k] = v
	}
	msg.Headers = h
	q := exchange + "/" + key
	info := Message{System: "amqp", Queue: q, JobName: key, Params: bodyParams(msg.Body)}
	return Publish(ctx, info, amqpCarrier(h), func(c context.Context) error {
		return ch.PublishWithContext(c, exchange, key, mandatory, immediate, msg)
	})
}

// ConsumeDelivery runs handler as a traced worker for a delivery. Attempt
// comes from x-delivery-count (quorum queues) or Redelivered.
func ConsumeDelivery(ctx context.Context, queue string, dl amqp.Delivery, maxAttempts int, handler func(ctx context.Context) error) error {
	attempt := 1
	switch n := dl.Headers["x-delivery-count"].(type) {
	case int64:
		attempt = int(n) + 1
	case int32:
		attempt = int(n) + 1
	default:
		if dl.Redelivered {
			attempt = 2
		}
	}
	h := amqpCarrier{}
	for k, v := range dl.Headers {
		h[strings.ToLower(k)] = v
	}
	d := Delivery{System: "amqp", Queue: queue, JobName: dl.RoutingKey, Params: bodyParams(dl.Body), Attempt: attempt,
		MessageID: firstNonEmpty(dl.MessageId, fmt.Sprint(dl.DeliveryTag)), MaxAttempts: maxAttempts}
	return Consume(ctx, h, d, handler)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
