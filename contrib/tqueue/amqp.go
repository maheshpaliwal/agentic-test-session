package tqueue

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type amqpCarrier amqp.Table

// Get returns a string header, or one sent as bytes: an application that
// writes its traceparent as a byte string must still be seen to have one.
func (c amqpCarrier) Get(key string) string {
	switch v := c[strings.ToLower(key)].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return ""
}

func (c amqpCarrier) Set(key, val string) { c[strings.ToLower(key)] = val }

// amqpHeaders is the carrier for the headers of a message being published,
// which have the names the application gave them. AMQP header names are
// case-sensitive, but a header named Traceparent is the application's trace
// context as much as traceparent is: a key matches a header whose name
// differs from it in case only, and Set replaces that header where it is.
// (A consumed message is read through amqpCarrier, with names lowercased.)
type amqpHeaders amqp.Table

// name returns the name under which c holds key.
func (c amqpHeaders) name(key string) (string, bool) {
	key = strings.ToLower(key)
	if _, ok := c[key]; ok {
		return key, true
	}
	for name := range c {
		if len(name) == len(key) && strings.EqualFold(name, key) {
			return name, true
		}
	}
	return key, false
}

func (c amqpHeaders) Get(key string) string {
	name, _ := c.name(key)
	return amqpCarrier(c).Get(name)
}

// Has implements core.PresenceCarrier: a header of any type counts, so an
// application traceparent that is neither a string nor bytes is left alone.
func (c amqpHeaders) Has(key string) bool { _, ok := c.name(key); return ok }

func (c amqpHeaders) Set(key, val string) {
	name, _ := c.name(key)
	c[name] = val
}

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
	maps.Copy(h, msg.Headers)
	msg.Headers = h
	q := exchange + "/" + key
	info := Message{System: "amqp", Queue: q, JobName: key, Params: bodyParams(msg.Body)}
	return Publish(ctx, info, amqpHeaders(h), func(c context.Context) error {
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
		MessageID: cmp.Or(dl.MessageId, fmt.Sprint(dl.DeliveryTag)), MaxAttempts: maxAttempts}
	return Consume(ctx, h, d, handler)
}
