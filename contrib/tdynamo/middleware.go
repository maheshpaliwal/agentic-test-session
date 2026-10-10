package tdynamo

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"

	"tsession/core"
)

const (
	store        = "dynamodb"
	middlewareID = "TSessionDynamoDB"
)

// Option configures the middleware.
type Option func(*tracer)

// WithKeySchema declares key attribute names per table (hash key first), used
// to derive the PK of PutItem/BatchWriteItem/TransactWriteItems puts.
func WithKeySchema(schema map[string][]string) Option {
	return func(t *tracer) {
		for tbl, ks := range schema {
			t.schema.Store(tbl, ks)
		}
	}
}

// WithUpdateReturn chooses the ReturnValues injected into UpdateItem when the
// caller set none: types.ReturnValueAllNew (default) or ReturnValueAllOld.
func WithUpdateReturn(rv types.ReturnValue) Option {
	return func(t *tracer) { t.updateReturn = rv }
}

type tracer struct {
	updateReturn types.ReturnValue
	schema       sync.Map // table -> []string key attribute names
}

// Middleware returns an APIOptions function that installs the tracer. Adding
// it twice to one stack is a no-op.
func Middleware(opts ...Option) func(*middleware.Stack) error {
	t := &tracer{updateReturn: types.ReturnValueAllNew}
	for _, o := range opts {
		o(t)
	}
	return func(s *middleware.Stack) error {
		if _, ok := s.Initialize.Get(middlewareID); ok {
			return nil
		}
		return s.Initialize.Add(middleware.InitializeMiddlewareFunc(middlewareID, t.handle), middleware.Before)
	}
}

// Apply appends the middleware to opts.APIOptions (no-op when disabled).
func Apply(opts *dynamodb.Options, o ...Option) {
	if opts == nil || !core.Enabled() {
		return
	}
	opts.APIOptions = append(opts.APIOptions, Middleware(o...))
}

// WithTracing is an option func for dynamodb.NewFromConfig / client.Options.
func WithTracing(o ...Option) func(*dynamodb.Options) {
	return func(opts *dynamodb.Options) { Apply(opts, o...) }
}

func (t *tracer) handle(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
	middleware.InitializeOutput, middleware.Metadata, error) {
	switch in.Parameters.(type) {
	case *dynamodb.GetItemInput, *dynamodb.QueryInput, *dynamodb.ScanInput, *dynamodb.BatchGetItemInput,
		*dynamodb.TransactGetItemsInput, *dynamodb.PutItemInput, *dynamodb.UpdateItemInput,
		*dynamodb.DeleteItemInput, *dynamodb.BatchWriteItemInput, *dynamodb.TransactWriteItemsInput,
		*dynamodb.DescribeTableInput, *dynamodb.CreateTableInput:
	default:
		return next.HandleInitialize(ctx, in)
	}
	var (
		span     *core.Span
		injected bool
		op       string
	)
	func() {
		defer func() { _ = recover() }() // tracing must never break the call
		op, injected, in.Parameters, span = t.begin(ctx, in.Parameters)
	}()
	out, md, err := next.HandleInitialize(ctx, in)
	if span == nil && !isSchemaOp(op) {
		return out, md, err
	}
	func() {
		defer func() { _ = recover() }()
		defer span.End()
		if err != nil {
			span.SetErr(err)
			return
		}
		t.finish(span, in.Parameters, out.Result, injected)
	}()
	return out, md, err
}

func isSchemaOp(op string) bool { return op == "DescribeTable" || op == "CreateTable" }

// begin starts the span, copies the input if a ReturnValues injection is
// needed, and returns the (possibly replaced) parameters.
func (t *tracer) begin(ctx context.Context, p any) (op string, injected bool, params any, span *core.Span) {
	orig := p
	var kind core.Kind = core.KindDBRead
	var table string
	req := map[string]any{}
	switch v := p.(type) {
	case *dynamodb.DescribeTableInput:
		return "DescribeTable", false, p, nil
	case *dynamodb.CreateTableInput:
		return "CreateTable", false, p, nil
	case *dynamodb.GetItemInput:
		op, table = "GetItem", str(v.TableName)
		req["key"] = plainItem(v.Key)
		t.learnKey(table, v.Key)
	case *dynamodb.QueryInput:
		op, table = "Query", str(v.TableName)
		req["key_condition"], req["filter"] = str(v.KeyConditionExpression), str(v.FilterExpression)
		req["values"] = plainItem(v.ExpressionAttributeValues)
		req["index"] = str(v.IndexName)
	case *dynamodb.ScanInput:
		op, table = "Scan", str(v.TableName)
		req["filter"], req["values"] = str(v.FilterExpression), plainItem(v.ExpressionAttributeValues)
		req["index"] = str(v.IndexName)
	case *dynamodb.BatchGetItemInput:
		op = "BatchGetItem"
		keys := map[string]any{}
		for tbl, ka := range v.RequestItems {
			var l []any
			for _, k := range ka.Keys {
				t.learnKey(tbl, k)
				l = append(l, plainItem(k))
			}
			keys[tbl] = l
		}
		req["keys"] = keys
	case *dynamodb.TransactGetItemsInput:
		op = "TransactGetItems"
		var l []any
		for _, it := range v.TransactItems {
			if it.Get != nil {
				t.learnKey(str(it.Get.TableName), it.Get.Key)
				l = append(l, map[string]any{"table": str(it.Get.TableName), "key": plainItem(it.Get.Key)})
			}
		}
		req["keys"] = l
	case *dynamodb.PutItemInput:
		kind, op, table = core.KindDBWrite, "PutItem", str(v.TableName)
		req["item"] = plainItem(v.Item)
		if v.ReturnValues == "" {
			cp := *v
			cp.ReturnValues, p, injected = types.ReturnValueAllOld, &cp, true
		}
	case *dynamodb.DeleteItemInput:
		kind, op, table = core.KindDBWrite, "DeleteItem", str(v.TableName)
		req["key"] = plainItem(v.Key)
		t.learnKey(table, v.Key)
		if v.ReturnValues == "" {
			cp := *v
			cp.ReturnValues, p, injected = types.ReturnValueAllOld, &cp, true
		}
	case *dynamodb.UpdateItemInput:
		kind, op, table = core.KindDBWrite, "UpdateItem", str(v.TableName)
		req["key"], req["update"] = plainItem(v.Key), str(v.UpdateExpression)
		req["values"] = plainItem(v.ExpressionAttributeValues)
		t.learnKey(table, v.Key)
		if v.ReturnValues == "" {
			cp := *v
			cp.ReturnValues, p, injected = t.updateReturn, &cp, true
		}
	case *dynamodb.BatchWriteItemInput:
		kind, op = core.KindDBWrite, "BatchWriteItem"
		req["tables"] = tableNames(v.RequestItems)
	case *dynamodb.TransactWriteItemsInput:
		kind, op = core.KindDBWrite, "TransactWriteItems"
		req["items"] = len(v.TransactItems)
	}
	name := op
	if table != "" {
		name += " " + table
		req["table"] = table
	}
	_, span = core.Begin(ctx, kind, name)
	if span == nil { // no session: leave the caller's input untouched
		return op, false, orig, nil
	}
	span.SetReq(prune(req))
	return op, injected, p, span
}

func tableNames(m map[string][]types.WriteRequest) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// prune drops empty entries so requests stay compact.
func prune(m map[string]any) map[string]any {
	for k, v := range m {
		switch t := v.(type) {
		case nil:
			delete(m, k)
		case string:
			if t == "" {
				delete(m, k)
			}
		case map[string]any:
			if len(t) == 0 {
				delete(m, k)
			}
		}
	}
	return m
}

func str(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---- key schema ----

func (t *tracer) learnKey(table string, key map[string]types.AttributeValue) {
	if table == "" || len(key) == 0 {
		return
	}
	if _, ok := t.schema.Load(table); ok {
		return
	}
	names := make([]string, 0, len(key))
	for k := range key {
		names = append(names, k)
	}
	sort.Strings(names)
	t.schema.Store(table, names)
}

// pk picks the key attributes out of a full item (or returns a Key as-is).
func (t *tracer) pk(table string, item map[string]types.AttributeValue) map[string]any {
	names, _ := t.schema.Load(table)
	ks, _ := names.([]string)
	if len(ks) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, k := range ks {
		if v, ok := item[k]; ok {
			out[k] = plainAV(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (t *tracer) learnSchema(ks []types.KeySchemaElement, table string) {
	names := make([]string, 0, len(ks))
	for _, k := range ks {
		names = append(names, str(k.AttributeName))
	}
	if table != "" && len(names) > 0 {
		t.schema.Store(table, names)
	}
}

// ---- response handling ----

func (t *tracer) finish(span *core.Span, in any, result any, injected bool) {
	mut := func(table, op string, pk map[string]any, before, after map[string]types.AttributeValue) {
		m := core.Mutation{Store: store, Entity: table, PK: pk, Op: op, Source: "sdk"}
		if before != nil {
			m.Before = core.Payload(plainItem(before))
		}
		if after != nil {
			m.After = core.Payload(plainItem(after))
		}
		span.AddMutation(m)
	}
	switch v := in.(type) {
	case *dynamodb.DescribeTableInput:
		if o, ok := result.(*dynamodb.DescribeTableOutput); ok && o.Table != nil {
			t.learnSchema(o.Table.KeySchema, str(o.Table.TableName))
		}
	case *dynamodb.CreateTableInput:
		t.learnSchema(v.KeySchema, str(v.TableName))
	case *dynamodb.GetItemInput:
		o, _ := result.(*dynamodb.GetItemOutput)
		rs := core.ReadSummary{Store: store, Entity: str(v.TableName), Statement: "GetItem"}
		if o != nil && len(o.Item) > 0 {
			rs.RowCount = 1
			rs.PKs = []string{pkString(v.Key)}
			rs.Sample = core.Payload(plainItem(o.Item))
		}
		span.AddRead(rs)
	case *dynamodb.QueryInput:
		if o, ok := result.(*dynamodb.QueryOutput); ok {
			span.AddRead(t.listSummary(str(v.TableName), "Query", int(o.Count), o.Items))
		}
	case *dynamodb.ScanInput:
		if o, ok := result.(*dynamodb.ScanOutput); ok {
			span.AddRead(t.listSummary(str(v.TableName), "Scan", int(o.Count), o.Items))
		}
	case *dynamodb.BatchGetItemInput:
		if o, ok := result.(*dynamodb.BatchGetItemOutput); ok {
			for tbl, items := range o.Responses {
				span.AddRead(t.listSummary(tbl, "BatchGetItem", len(items), items))
			}
		}
	case *dynamodb.TransactGetItemsInput:
		if o, ok := result.(*dynamodb.TransactGetItemsOutput); ok {
			for i, r := range o.Responses {
				tbl := ""
				if i < len(v.TransactItems) && v.TransactItems[i].Get != nil {
					tbl = str(v.TransactItems[i].Get.TableName)
				}
				n := 0
				if len(r.Item) > 0 {
					n = 1
				}
				var items []map[string]types.AttributeValue
				if n == 1 {
					items = append(items, r.Item)
				}
				span.AddRead(t.listSummary(tbl, "TransactGetItems", n, items))
			}
		}
	case *dynamodb.PutItemInput:
		o, _ := result.(*dynamodb.PutItemOutput)
		tbl := str(v.TableName)
		var old map[string]types.AttributeValue
		if o != nil && (injected || v.ReturnValues == types.ReturnValueAllOld) {
			old = o.Attributes
		}
		op := "insert"
		if !emptyItem(old) {
			op = "update"
		} else if !injected && v.ReturnValues != types.ReturnValueAllOld {
			op = "put" // old state unknown
		}
		mut(tbl, op, t.pk(tbl, v.Item), nilIfEmpty(old), v.Item)
		if injected && o != nil {
			o.Attributes = nil
		}
	case *dynamodb.DeleteItemInput:
		o, _ := result.(*dynamodb.DeleteItemOutput)
		tbl := str(v.TableName)
		var old map[string]types.AttributeValue
		if o != nil && (injected || v.ReturnValues == types.ReturnValueAllOld) {
			old = o.Attributes
		}
		if !(injected && emptyItem(old)) { // injected + nothing returned = nothing deleted
			mut(tbl, "delete", plainKey(v.Key), nilIfEmpty(old), nil)
		}
		if injected && o != nil {
			o.Attributes = nil
		}
	case *dynamodb.UpdateItemInput:
		o, _ := result.(*dynamodb.UpdateItemOutput)
		tbl := str(v.TableName)
		rv := v.ReturnValues
		var before, after map[string]types.AttributeValue
		if o != nil {
			switch rv {
			case types.ReturnValueAllOld:
				before = o.Attributes
			case types.ReturnValueAllNew:
				after = o.Attributes
			}
		}
		m := core.Mutation{Store: store, Entity: tbl, PK: plainKey(v.Key), Op: "update", Source: "sdk"}
		if before != nil {
			m.Before = core.Payload(plainItem(before))
		}
		if after != nil {
			m.After = core.Payload(plainItem(after))
		} else if v.UpdateExpression != nil { // only the pre-image is known; keep the intent
			m.After = core.Payload(map[string]any{"update_expression": str(v.UpdateExpression),
				"values": plainItem(v.ExpressionAttributeValues)})
		}
		span.AddMutation(m)
		if injected && o != nil {
			o.Attributes = nil
		}
	case *dynamodb.BatchWriteItemInput:
		var o *dynamodb.BatchWriteItemOutput
		o, _ = result.(*dynamodb.BatchWriteItemOutput)
		for _, tbl := range tableNames(v.RequestItems) {
			var unprocessed []types.WriteRequest
			if o != nil {
				unprocessed = o.UnprocessedItems[tbl]
			}
			for _, wr := range v.RequestItems[tbl] {
				if wasUnprocessed(wr, unprocessed) {
					span.AddFlag("unprocessed_items")
					continue
				}
				t.writeRequest(span, tbl, wr)
			}
		}
	case *dynamodb.TransactWriteItemsInput:
		span.SetAttr("transact", "true")
		for _, it := range v.TransactItems {
			switch {
			case it.Put != nil:
				tbl := str(it.Put.TableName)
				mut(tbl, "put", t.pk(tbl, it.Put.Item), nil, it.Put.Item)
			case it.Update != nil:
				tbl := str(it.Update.TableName)
				m := core.Mutation{Store: store, Entity: tbl, PK: plainKey(it.Update.Key), Op: "update", Source: "sdk",
					After: core.Payload(map[string]any{"update_expression": str(it.Update.UpdateExpression),
						"values": plainItem(it.Update.ExpressionAttributeValues)})}
				span.AddMutation(m)
			case it.Delete != nil:
				mut(str(it.Delete.TableName), "delete", plainKey(it.Delete.Key), nil, nil)
			}
		}
	}
}

func (t *tracer) writeRequest(span *core.Span, tbl string, wr types.WriteRequest) {
	switch {
	case wr.PutRequest != nil:
		span.AddMutation(core.Mutation{Store: store, Entity: tbl, PK: t.pk(tbl, wr.PutRequest.Item), Op: "put",
			After: core.Payload(plainItem(wr.PutRequest.Item)), Source: "sdk"})
	case wr.DeleteRequest != nil:
		span.AddMutation(core.Mutation{Store: store, Entity: tbl, PK: plainKey(wr.DeleteRequest.Key), Op: "delete", Source: "sdk"})
	}
}

func wasUnprocessed(wr types.WriteRequest, un []types.WriteRequest) bool {
	for _, u := range un {
		if fmt.Sprint(plainWR(u)) == fmt.Sprint(plainWR(wr)) {
			return true
		}
	}
	return false
}

func plainWR(w types.WriteRequest) any {
	if w.PutRequest != nil {
		return map[string]any{"put": plainItem(w.PutRequest.Item)}
	}
	if w.DeleteRequest != nil {
		return map[string]any{"del": plainItem(w.DeleteRequest.Key)}
	}
	return nil
}

func plainKey(k map[string]types.AttributeValue) map[string]any {
	if len(k) == 0 {
		return nil
	}
	return plainItem(k)
}

func nilIfEmpty(m map[string]types.AttributeValue) map[string]types.AttributeValue {
	if len(m) == 0 {
		return nil
	}
	return m
}

func pkString(k map[string]types.AttributeValue) string {
	p := core.Payload(plainItem(k))
	return string(p.Inline)
}

func (t *tracer) listSummary(table, stmt string, count int, items []map[string]types.AttributeValue) core.ReadSummary {
	rs := core.ReadSummary{Store: store, Entity: table, Statement: stmt, RowCount: count}
	names, _ := t.schema.Load(table)
	ks, _ := names.([]string)
	for i, it := range items {
		if i >= 20 {
			break
		}
		if len(ks) > 0 {
			sub := map[string]types.AttributeValue{}
			for _, k := range ks {
				if v, ok := it[k]; ok {
					sub[k] = v
				}
			}
			rs.PKs = append(rs.PKs, pkString(sub))
		}
	}
	if len(items) > 0 {
		rs.Sample = core.Payload(plainItem(items[0]))
	}
	return rs
}
