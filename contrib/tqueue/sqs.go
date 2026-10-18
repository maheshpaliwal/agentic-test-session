package tqueue

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// maxSQSAttrs is the SQS limit on message attributes per message.
const maxSQSAttrs = 10

// tsAttr is the compact single-attribute carrier ("k=v&k=v", url-encoded),
// used when fewer than 3 attribute slots are free.
const tsAttr = "ts"

type sqsCarrier map[string]types.MessageAttributeValue

func (c sqsCarrier) Get(key string) string {
	key = strings.ToLower(key)
	if v, ok := c[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	if v, ok := c[tsAttr]; ok && v.StringValue != nil {
		if q, err := url.ParseQuery(*v.StringValue); err == nil {
			return q.Get(key)
		}
	}
	return ""
}

func (c sqsCarrier) Set(key, val string) {
	c[strings.ToLower(key)] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(val)}
}

func encodeTS(m core.MapCarrier) string {
	q := url.Values{}
	for k, v := range m {
		q.Set(k, v)
	}
	return q.Encode()
}

// SQSOptions tunes the SQS helpers; the zero value is valid.
type SQSOptions struct {
	// SideChannel carries the trace context when no message attribute slot is
	// free. The key is derived from the queue and the MD5 of the body, so
	// identical bodies in flight on one queue share (overwrite) one entry.
	SideChannel SideChannel
	// PutTimeout bounds the side-channel Put/Get (default DefaultSideTimeout).
	PutTimeout time.Duration
	// RetryExpected overrides the maxReceives-based retry decision on consume.
	RetryExpected func(attempt int, err error) bool
}

func sqsSideKey(queue, md5hex string) string { return SideKey("sqs:" + queue + ":" + md5hex) }

func md5Hex(s string) string { h := md5.Sum([]byte(s)); return hex.EncodeToString(h[:]) }

// SQSAPI is the subset of *sqs.Client used by the helpers.
type SQSAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// SendMessage publishes with trace context in MessageAttributes. The input is
// copied, never mutated. See SendMessageOpts for the attribute-limit handling.
func SendMessage(ctx context.Context, c SQSAPI, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return SendMessageOpts(ctx, c, SQSOptions{}, in, optFns...)
}

// SendMessageOpts is SendMessage with options. SQS allows 10 message
// attributes: with 3 free slots the carrier uses 3 attributes; with 1-2 free it
// uses one compact "ts" attribute; with none it uses opts.SideChannel when
// configured, else propagation is skipped and the publish span is flagged
// propagation_skipped:attr_limit (side-channel failure: ...:side_channel).
func SendMessageOpts(ctx context.Context, c SQSAPI, opts SQSOptions, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
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
	body := aws.ToString(in.MessageBody)
	info := Message{System: "sqs", Queue: queue, JobName: queue, Params: bodyParams([]byte(body)),
		Delay: secs(in.DelaySeconds)}

	room := maxSQSAttrs - len(attrs)
	var carrier core.Carrier = sqsCarrier(attrs)
	mode := "attrs"
	switch {
	case room >= 3:
	case room >= 1:
		mode, carrier = "ts", core.MapCarrier{}
	case opts.SideChannel != nil:
		mode, carrier = "side", core.MapCarrier{}
	default:
		mode, carrier = "skip", core.MapCarrier{}
		info.flags = append(info.flags, FlagAttrLimit)
	}
	var out *sqs.SendMessageOutput
	err := publish(ctx, info, carrier, func(c2 context.Context, span *core.Span) error {
		switch m := carrier.(type) {
		case core.MapCarrier:
			if len(m) > 0 {
				switch mode {
				case "ts":
					sqsCarrier(attrs).Set(tsAttr, encodeTS(m))
				case "side":
					if e := putBounded(c2, opts.SideChannel, sqsSideKey(queue, md5Hex(body)), m, opts.PutTimeout); e != nil {
						flag(span, FlagSideChannel)
					}
				}
			}
		}
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
		core.KeyTraceparent, core.KeySession, core.KeyHLC, tsAttr)
	cp.MessageSystemAttributeNames = append(append([]types.MessageSystemAttributeName(nil), in.MessageSystemAttributeNames...),
		types.MessageSystemAttributeNameApproximateReceiveCount)
	return c.ReceiveMessage(ctx, &cp, optFns...)
}

// ConsumeSQSMessage runs handler as a traced worker for a received message.
// maxReceives (>0) enables retry/dlq events based on ApproximateReceiveCount.
func ConsumeSQSMessage(ctx context.Context, queueURL string, msg types.Message, maxReceives int, handler func(ctx context.Context) error) error {
	return ConsumeSQSMessageOpts(ctx, queueURL, msg, maxReceives, SQSOptions{}, handler)
}

// ConsumeSQSMessageOpts is ConsumeSQSMessage with options (side-channel
// lookup when the message carries no trace attributes, RetryExpected).
func ConsumeSQSMessageOpts(ctx context.Context, queueURL string, msg types.Message, maxReceives int, opts SQSOptions, handler func(ctx context.Context) error) error {
	attrs := sqsCarrier{}
	for k, v := range msg.MessageAttributes {
		attrs[strings.ToLower(k)] = v
	}
	var carrier core.Carrier = attrs
	if opts.SideChannel != nil && core.Enabled() && attrs.Get(core.KeySession) == "" {
		md := aws.ToString(msg.MD5OfBody)
		if md == "" {
			md = md5Hex(aws.ToString(msg.Body))
		}
		if m, err := getBounded(ctx, opts.SideChannel, sqsSideKey(queueURL, md), opts.PutTimeout); err == nil {
			carrier = core.MapCarrier(m)
		}
	}
	attempt, _ := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	d := Delivery{System: "sqs", Queue: queueURL, JobName: queueURL, Params: bodyParams([]byte(aws.ToString(msg.Body))),
		Attempt: attempt, MessageID: aws.ToString(msg.MessageId), MaxAttempts: maxReceives, RetryExpected: opts.RetryExpected}
	return Consume(ctx, carrier, d, handler)
}

func secs(n int32) time.Duration { return time.Duration(n) * time.Second }
