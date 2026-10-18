// Package tkafkaconfluent traces confluent-kafka-go (v1.9.x, cgo/librdkafka)
// producers and consumers into the active test session. All trace logic lives
// in tsession/contrib/tqueue; this package only adapts kafka.Message headers
// to a core.Carrier and keeps the cgo-dependent surface tiny.
//
// Producing (queue.publish, Req = decoded+redacted value with topic/key/
// partition; trace context injected into msg.Headers, existing headers kept):
//
//	err := tkafkaconfluent.Produce(ctx, producer, &kafka.Message{...}, deliveryChan)
//
// Consuming (worker.start / worker.end, worker.retry / worker.dlq):
//
//	msg, _ := consumer.ReadMessage(timeout)
//	err := tkafkaconfluent.Consume(ctx, msg, func(ctx context.Context) error { ... },
//		tkafkaconfluent.WithRetryExpected(func(attempt int, err error) bool { return true }))
//
// Without a session in ctx (producer) or in the message headers (consumer),
// or when the SDK is disabled, both helpers just call through: Produce calls
// p.Produce once with the message untouched, Consume runs the handler once.
// Tracing never fails the application. Injection is skipped when it would add
// more than 1 KiB / 8 headers, so a record is never made unsendable by it.
package tkafkaconfluent
