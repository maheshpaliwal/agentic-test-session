package tqueue

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"tsession/core"
)

type sqsCarrier map[string]types.MessageAttributeValue

func (c sqsCarrier) Get(key string) string {
	if v, ok := c[strings.ToLower(key)]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}

func (c sqsCarrier) Set(key, val string) {
	c[strings.ToLower(key)] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(val)}
}

// SQSAPI is the subset of *sqs.Client used by the helpers.
type SQSAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// SendMessage publishes with trace context in MessageAttributes. The input is
// copied, never mutated.
func SendMessage(ctx context.Context, c SQSAPI, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if !core.Enabled() || in == nil {
		return c.SendMessage(ctx, in, optFns...)
	}
	cp := *in
	attrs := make(map[string]types.MessageAttributeValue, len(in.MessageAttributes)+3)
	for k, v := range in.MessageAttributes {
		attrs[k] = v
	}
	cp.MessageAttributes = attrs
	queue := aws.ToString(in.QueueUrl)
	info := Message{System: "sqs", Queue: queue, JobName: queue, Params: aws.ToString(in.MessageBody),
		Delay: secs(in.DelaySeconds)}
	var out *sqs.SendMessageOutput
	err := Publish(ctx, info, sqsCarrier(attrs), func(c2 context.Context) error {
		var e error
		out, e = c.SendMessage(c2, &cp, optFns...)
		return e
	})
	return out, err
}

// ReceiveMessage receives with the attribute names needed to continue traces.
func ReceiveMessage(ctx context.Context, c SQSAPI, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if !core.Enabled() || in == nil {
		return c.ReceiveMessage(ctx, in, optFns...)
	}
	cp := *in
	cp.MessageAttributeNames = append(append([]string(nil), in.MessageAttributeNames...),
		core.KeyTraceparent, core.KeySession, core.KeyHLC)
	cp.MessageSystemAttributeNames = append(append([]types.MessageSystemAttributeName(nil), in.MessageSystemAttributeNames...),
		types.MessageSystemAttributeNameApproximateReceiveCount)
	return c.ReceiveMessage(ctx, &cp, optFns...)
}

// ConsumeSQSMessage runs handler as a traced worker for a received message.
// maxReceives (>0) enables retry/dlq events based on ApproximateReceiveCount.
func ConsumeSQSMessage(ctx context.Context, queueURL string, msg types.Message, maxReceives int, handler func(ctx context.Context) error) error {
	attrs := sqsCarrier{}
	for k, v := range msg.MessageAttributes {
		attrs[strings.ToLower(k)] = v
	}
	attempt, _ := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	d := Delivery{System: "sqs", Queue: queueURL, JobName: queueURL, Params: aws.ToString(msg.Body),
		Attempt: attempt, MessageID: aws.ToString(msg.MessageId), MaxAttempts: maxReceives}
	return Consume(ctx, attrs, d, handler)
}

func secs(n int32) time.Duration { return time.Duration(n) * time.Second }
