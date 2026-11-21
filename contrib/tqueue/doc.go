// Package tqueue traces asynchronous work. The core (this file set minus the
// kafka/sqs/amqp adapters) is transport-agnostic and stdlib-only: Publish
// emits a queue.publish span and injects context into a Carrier; Consume
// extracts it, emits worker.start / worker.end (and retry/dlq) and runs the
// handler under the continued trace.
//
//	// producer: headers travel with the message and carry the session
//	headers := core.MapCarrier{}
//	job := tqueue.Message{System: "kafka", Queue: "orders", JobName: "fulfil", Params: params}
//	err := tqueue.Publish(ctx, job, headers, func(ctx context.Context) error {
//		return send(ctx, body, headers)
//	})
//
//	// consumer: the handler runs under the publisher's trace
//	d := tqueue.Delivery{System: "kafka", Queue: "orders", JobName: "fulfil", Attempt: attempt, MaxAttempts: 3}
//	err = tqueue.Consume(ctx, headers, d, func(ctx context.Context) error {
//		return handle(ctx, body)
//	})
//
// ProduceMessages and ConsumeMessage (kafka-go), SendMessage and
// ConsumeSQSMessage (SQS), PublishAMQP and ConsumeDelivery (AMQP) do this
// with the client's own headers. A queue whose messages carry no headers
// uses PublishVia, ConsumeVia and WrapHandler with a SideChannel. All of them
// look the SDK up on every call, so they may be set up before it starts.
//
// No-session behaviour: tracing is strictly best effort. A consumer whose
// carrier holds no active session (and with no exclusive session configured),
// a publish whose context carries no session, or tracing disabled, all run
// untraced: no events are emitted, nothing is injected, and the application's
// send/handler still runs exactly once. A consumer invoked without a context
// that carries the trace is therefore untraced, never failed.
//
// Retries: a failed attempt that will be redelivered (Delivery.RetryExpected
// or attempt < MaxAttempts) gets worker.end with retry_pending="true" plus a
// worker.retry, so the collector keeps the publish's pending-async entry open;
// the final failing attempt emits worker.dlq and a worker.end without the
// attribute. With an unknown retry policy tqueue closes on every attempt.
package tqueue
