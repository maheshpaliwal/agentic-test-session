// Package tqueue traces asynchronous work. The core (this file set minus the
// kafka/sqs/amqp adapters) is transport-agnostic and stdlib-only: Publish
// emits a queue.publish span and injects context into a Carrier; Consume
// extracts it, emits worker.start / worker.end (and retry/dlq) and runs the
// handler under the continued trace.
package tqueue
