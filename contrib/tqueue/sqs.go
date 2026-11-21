package tqueue

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"maps"
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

// ctxAttr is the compact single-attribute carrier ("k=v&k=v", url-encoded),
// used when the attributes of the full form do not fit (see SendMessageOpts).
// The name is namespaced so it cannot collide with an application attribute.
const ctxAttr = "x-tsession-ctx"

// legacyCtxAttr is the compact carrier name written by earlier versions. It is
// still read, so messages from older producers keep their trace; it is never
// written, because applications use "ts" for their own data.
const legacyCtxAttr = "ts"

// sqsOwnKeys are the attributes of the full form that only tsession writes.
// The fourth attribute of the full form, traceparent, may be the
// application's.
var sqsOwnKeys = [...]string{core.KeyTSessionTraceparent, core.KeySession, core.KeyHLC}

// sqsCarrier reads the trace context of a received message, in either form.
type sqsCarrier map[string]types.MessageAttributeValue

func (c sqsCarrier) direct(key string) string {
	if v, ok := c[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}

func (c sqsCarrier) compact(key string) string {
	for _, name := range [...]string{ctxAttr, legacyCtxAttr} {
		if q, err := url.ParseQuery(c.direct(name)); err == nil && q.Get(key) != "" {
			return q.Get(key)
		}
	}
	return ""
}

// Get reads key from where the session id is. A message whose session id is
// in the compact attribute was sent in the compact form, which writes no other
// attribute: one named like a carrier key (traceparent) is then the
// application's, and the compact value is used. Otherwise the attribute is
// used, and the compact value only when there is none.
func (c sqsCarrier) Get(key string) string {
	key = strings.ToLower(key)
	d, k := c.direct(key), c.compact(key)
	if d == "" || (k != "" && c.direct(core.KeySession) == "") {
		return k
	}
	return d
}

func (c sqsCarrier) Set(key, val string) {
	c[strings.ToLower(key)] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(val)}
}

// sqsAttrs is the carrier for writing the full form into the attributes of a
// message being sent. Unlike sqsCarrier it never looks inside a compact
// attribute: whether the message already has a traceparent is a question
// about its attributes only.
type sqsAttrs map[string]types.MessageAttributeValue

func (c sqsAttrs) Get(key string) string { return sqsCarrier(c).direct(strings.ToLower(key)) }
func (c sqsAttrs) Set(key, val string)   { sqsCarrier(c).Set(key, val) }

// stale reports which carrier attributes c already has from an earlier
// publish: a compact attribute that holds a session id (an application
// attribute of that name does not), and a traceparent equal to
// x-tsession-traceparent, which tsession wrote.
func (c sqsAttrs) stale() (compact, traceparent bool) {
	q, err := url.ParseQuery(c.Get(ctxAttr))
	tp := c.Get(core.KeyTraceparent)
	return err == nil && q.Get(core.KeySession) != "", tp != "" && tp == c.Get(core.KeyTSessionTraceparent)
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
// attributes, and the carrier picks the first form that fits:
//
//   - Full: x-tsession-traceparent, x-tsession-id and x-tsession-hlc, plus
//     traceparent when the message has none. That is 4 new attributes, or 3
//     when the application set its own traceparent, which is left untouched.
//   - Compact: one "x-tsession-ctx" attribute holding the same keys, when the
//     full form does not fit and one slot is free. No other attribute is
//     written, so an application traceparent is untouched here too.
//   - opts.SideChannel, when configured and no slot is free, or the
//     application already set an attribute named x-tsession-ctx (it is never
//     overwritten).
//   - Otherwise propagation is skipped and the publish span is flagged
//     propagation_skipped:attr_limit (side-channel failure: ...:side_channel).
//
// Carrier attributes that are already on the message, because it is a
// received message sent on with its attributes, name the span that published
// it the first time. They are replaced, and removed when this publish uses
// another form, and do not count against the limit.
func SendMessageOpts(ctx context.Context, c SQSAPI, opts SQSOptions, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if !core.Enabled() || in == nil {
		return c.SendMessage(ctx, in, optFns...)
	}
	cp := *in
	attrs := make(map[string]types.MessageAttributeValue, len(in.MessageAttributes)+len(sqsOwnKeys)+1)
	maps.Copy(attrs, in.MessageAttributes)
	cp.MessageAttributes = attrs
	queue := aws.ToString(in.QueueUrl)
	body := aws.ToString(in.MessageBody)
	info := Message{System: "sqs", Queue: queue, JobName: queue, Params: bodyParams([]byte(body)),
		Delay: secs(in.DelaySeconds)}

	// The form is chosen from counts only: the attributes are not changed
	// until a publish span exists, so a message outside a session is sent as
	// the application built it.
	used := len(attrs)
	staleCtx, staleTP := sqsAttrs(attrs).stale()
	if staleCtx {
		used--
	}
	need, ownPresent := 0, 0
	for _, k := range sqsOwnKeys {
		if has(attrs, k) {
			ownPresent++
		} else {
			need++
		}
	}
	if !has(attrs, core.KeyTraceparent) {
		need++
	}
	if staleTP {
		ownPresent++
	}
	var carrier core.Carrier = sqsAttrs(attrs)
	mode := "attrs"
	switch {
	case maxSQSAttrs-used >= need:
	case maxSQSAttrs-(used-ownPresent) >= 1 && (staleCtx || !has(attrs, ctxAttr)):
		mode, carrier = "ctx", core.MapCarrier{}
	case opts.SideChannel != nil:
		mode, carrier = "side", core.MapCarrier{}
	default:
		mode, carrier = "skip", core.MapCarrier{}
		info.flags = append(info.flags, FlagAttrLimit)
	}
	var out *sqs.SendMessageOutput
	err := publish(ctx, info, carrier, func(c2 context.Context, span *core.Span) error {
		if span != nil {
			// Whatever carrier the message arrived with is stale now.
			if staleCtx {
				delete(attrs, ctxAttr)
			}
			if mode != "attrs" {
				for _, k := range sqsOwnKeys {
					delete(attrs, k)
				}
				if staleTP {
					delete(attrs, core.KeyTraceparent)
				}
			}
		}
		if m, ok := carrier.(core.MapCarrier); ok && len(m) > 0 {
			switch mode {
			case "ctx":
				sqsCarrier(attrs).Set(ctxAttr, encodeTS(m))
			case "side":
				if e := putBounded(c2, opts.SideChannel, sqsSideKey(queue, md5Hex(body)), m, opts.PutTimeout); e != nil {
					flag(span, FlagSideChannel)
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
		core.KeyTSessionTraceparent, core.KeyTraceparent, core.KeySession, core.KeyHLC, ctxAttr, legacyCtxAttr)
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
// lookup when the message carries no trace attributes, RetryExpected). While
// no session is active the handler runs at once and the side channel is not
// consulted.
func ConsumeSQSMessageOpts(ctx context.Context, queueURL string, msg types.Message, maxReceives int, opts SQSOptions, handler func(ctx context.Context) error) error {
	if handler == nil {
		return nil
	}
	if !core.AnyActive() {
		return handler(ctx) // no span could be recorded: skip the lookup
	}
	attrs := sqsCarrier{}
	for k, v := range msg.MessageAttributes {
		attrs[strings.ToLower(k)] = v
	}
	var carrier core.Carrier = attrs
	if opts.SideChannel != nil && attrs.Get(core.KeySession) == "" {
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

func has(attrs map[string]types.MessageAttributeValue, name string) bool {
	_, ok := attrs[name]
	return ok
}

func secs(n int32) time.Duration { return time.Duration(n) * time.Second }
