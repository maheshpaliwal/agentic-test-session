package tqueue_test

import (
	"context"
	"fmt"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tqueue"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// The setup of the package comment: keep the two in step.
func Example() {
	ctx := context.Background() // in a service: the context of the request that publishes

	// Producer: headers travel with the message and carry the session.
	headers := core.MapCarrier{}
	job := tqueue.Message{System: "kafka", Queue: "orders", JobName: "fulfil", Params: map[string]any{"order_id": 42}}
	err := tqueue.Publish(ctx, job, headers, func(ctx context.Context) error {
		return nil // send the message with headers
	})
	fmt.Println("published:", err)

	// Consumer: the handler runs under the publisher's trace.
	d := tqueue.Delivery{System: "kafka", Queue: "orders", JobName: "fulfil", Attempt: 1, MaxAttempts: 3}
	err = tqueue.Consume(ctx, headers, d, func(ctx context.Context) error {
		return nil // handle the job; pass ctx on to tsql, tredis, thttp
	})
	fmt.Println("handled:", err)
	// Output:
	// published: <nil>
	// handled: <nil>
}
