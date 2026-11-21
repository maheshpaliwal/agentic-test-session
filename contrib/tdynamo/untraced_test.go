package tdynamo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// Inputs sized like the ones the cost was measured with.

func putInput() *dynamodb.PutItemInput {
	item := map[string]types.AttributeValue{"id": s("order-1")}
	for i := 0; i < 29; i++ {
		item["attr"+strconv.Itoa(i)] = s("value-" + strconv.Itoa(i))
	}
	return &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item,
		ConditionExpression:      aws.String("attribute_not_exists(#id)"),
		ExpressionAttributeNames: map[string]string{"#id": "id"}}
}

func updateInput() *dynamodb.UpdateItemInput {
	return &dynamodb.UpdateItemInput{TableName: aws.String("orders"),
		Key:                      map[string]types.AttributeValue{"id": s("order-1")},
		UpdateExpression:         aws.String("SET #s = :s, #q = :q, updated_at = :u"),
		ConditionExpression:      aws.String("#s <> :s"),
		ExpressionAttributeNames: map[string]string{"#s": "status", "#q": "qty"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": s("shipped"), ":q": n("3"), ":u": n("1700000000")}}
}

func queryInput() *dynamodb.QueryInput {
	return &dynamodb.QueryInput{TableName: aws.String("orders"), IndexName: aws.String("by_customer"),
		KeyConditionExpression:   aws.String("customer = :c"),
		FilterExpression:         aws.String("#s = :s"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":c": s("cust-1"), ":s": s("open")}}
}

func getInput() *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{TableName: aws.String("orders"), Key: map[string]types.AttributeValue{"id": s("order-1")}}
}

// noSession enables the SDK with one active session and returns a context
// that belongs to none.
func noSession(tb testing.TB) context.Context {
	tb.Helper()
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	tb.Cleanup(func() { core.Configure(core.Config{}); core.SetSessions(nil) })
	return context.Background()
}

// passNext is the rest of the stack: it answers without doing anything.
var passNext = middleware.InitializeHandlerFunc(func(context.Context, middleware.InitializeInput) (
	middleware.InitializeOutput, middleware.Metadata, error) {
	return middleware.InitializeOutput{}, middleware.Metadata{}, nil
})

func newTracer() *tracer { return &tracer{updateReturn: types.ReturnValueAllNew} }

// maxUntracedAllocs is what one call outside a session may allocate in the
// middleware: nothing that grows with the request.
const maxUntracedAllocs = 1

func TestUntracedCallBuildsNothing(t *testing.T) {
	ctx := noSession(t)
	for name, in := range map[string]any{
		"PutItem":    putInput(),
		"UpdateItem": updateInput(),
		"Query":      queryInput(),
		"GetItem":    getInput(),
		"DeleteItem": &dynamodb.DeleteItemInput{TableName: aws.String("orders"), Key: getInput().Key},
	} {
		tr := newTracer()
		ii := middleware.InitializeInput{Parameters: in}
		_, _, _ = tr.handle(ctx, ii, passNext) // first call may learn the table's key
		got := testing.AllocsPerRun(200, func() { _, _, _ = tr.handle(ctx, ii, passNext) })
		if got > maxUntracedAllocs {
			t.Errorf("%s outside a session: %v allocs per call, want at most %d", name, got, maxUntracedAllocs)
		}
	}
}

func TestUntracedCallLeavesInputAlone(t *testing.T) {
	ctx := noSession(t)
	tr := newTracer()
	put, upd := putInput(), updateInput()
	del := &dynamodb.DeleteItemInput{TableName: aws.String("orders"), Key: getInput().Key}
	for _, in := range []any{put, upd, del} {
		var seen any
		next := middleware.InitializeHandlerFunc(func(_ context.Context, ii middleware.InitializeInput) (
			middleware.InitializeOutput, middleware.Metadata, error) {
			seen = ii.Parameters
			return middleware.InitializeOutput{}, middleware.Metadata{}, nil
		})
		if _, _, err := tr.handle(ctx, middleware.InitializeInput{Parameters: in}, next); err != nil {
			panic(err)
		}
		if seen != in {
			panic(fmt.Sprintf("%T: the request was replaced by a copy", in))
		}
	}
	if put.ReturnValues != "" || upd.ReturnValues != "" || del.ReturnValues != "" {
		panic("ReturnValues was set on the caller's input")
	}
}

func TestUntracedOpNames(t *testing.T) {
	ctx := noSession(t)
	tr := newTracer()
	for want, in := range map[string]any{
		"GetItem":            &dynamodb.GetItemInput{},
		"Query":              &dynamodb.QueryInput{},
		"Scan":               &dynamodb.ScanInput{},
		"BatchGetItem":       &dynamodb.BatchGetItemInput{},
		"TransactGetItems":   &dynamodb.TransactGetItemsInput{},
		"PutItem":            &dynamodb.PutItemInput{},
		"UpdateItem":         &dynamodb.UpdateItemInput{},
		"DeleteItem":         &dynamodb.DeleteItemInput{},
		"BatchWriteItem":     &dynamodb.BatchWriteItemInput{},
		"TransactWriteItems": &dynamodb.TransactWriteItemsInput{},
		"DescribeTable":      &dynamodb.DescribeTableInput{},
		"CreateTable":        &dynamodb.CreateTableInput{},
	} {
		op, injected, params, span := tr.begin(ctx, in)
		if op != want || injected || params != in || span != nil {
			t.Errorf("%T: op=%q injected=%v same=%v span=%v", in, op, injected, params == in, span)
		}
	}
}

// The key schema of a table is learned from requests outside a session too:
// a later traced write derives its primary key from it.
func TestKeyLearnedUntracedIsUsedWhenTraced(t *testing.T) {
	key := func(v string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"id": s(v)}
	}
	bg := context.Background()
	for name, learn := range map[string]func(c *dynamodb.Client){
		"GetItem": func(c *dynamodb.Client) {
			_, _ = c.GetItem(bg, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: key("1")})
		},
		"UpdateItem": func(c *dynamodb.Client) {
			_, _ = c.UpdateItem(bg, &dynamodb.UpdateItemInput{TableName: aws.String("t"), Key: key("1"),
				UpdateExpression: aws.String("SET v = :v"), ExpressionAttributeValues: map[string]types.AttributeValue{":v": n("1")}})
		},
		"DeleteItem": func(c *dynamodb.Client) {
			_, _ = c.DeleteItem(bg, &dynamodb.DeleteItemInput{TableName: aws.String("t"), Key: key("1")})
		},
		"BatchGetItem": func(c *dynamodb.Client) {
			_, _ = c.BatchGetItem(bg, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
				"t": {Keys: []map[string]types.AttributeValue{key("1")}}}})
		},
		"TransactGetItems": func(c *dynamodb.Client) {
			_, _ = c.TransactGetItems(bg, &dynamodb.TransactGetItemsInput{TransactItems: []types.TransactGetItem{
				{Get: &types.Get{TableName: aws.String("t"), Key: key("1")}}}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, st, rec, ctx := setup(t, func(target string) string {
				if target == "DeleteItem" {
					return `{"Attributes":{"id":{"S":"9"},"v":{"N":"1"}}}`
				}
				return `{}`
			})
			learn(c)
			if len(rec.Events()) != 0 {
				panic(fmt.Sprintf("the request outside a session was recorded: %+v", rec.Events()))
			}
			if strings.Contains(st.reqs[0], "ReturnValues") {
				panic(fmt.Sprintf("ReturnValues injected outside a session: %s", st.reqs[0]))
			}

			// PutItem carries no Key: its PK comes only from the learned schema.
			if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("t"),
				Item: map[string]types.AttributeValue{"id": s("z"), "v": n("2")}}); err != nil {
				panic(err)
			}
			if _, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("t"), Key: key("9")}); err != nil {
				panic(err)
			}
			ev := rec.Events()
			if len(ev) != 2 {
				panic(fmt.Sprintf("events=%d", len(ev)))
			}
			if m := ev[0].Mutations[0]; m.Op != "insert" || len(m.PK) != 1 || m.PK["id"] != "z" {
				panic(fmt.Sprintf("put: %+v", m))
			}
			if m := ev[1].Mutations[0]; m.Op != "delete" || len(m.PK) != 1 || m.PK["id"] != "9" || m.Before.IsZero() {
				panic(fmt.Sprintf("delete: %+v", m))
			}
		})
	}
}

func TestSchemaOpsLearnUntraced(t *testing.T) {
	bg := context.Background()
	for name, learn := range map[string]func(c *dynamodb.Client) error{
		"DescribeTable": func(c *dynamodb.Client) error {
			_, err := c.DescribeTable(bg, &dynamodb.DescribeTableInput{TableName: aws.String("t")})
			return err
		},
		"CreateTable": func(c *dynamodb.Client) error {
			_, err := c.CreateTable(bg, &dynamodb.CreateTableInput{TableName: aws.String("t"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("tenant"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange}},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("tenant"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}},
				BillingMode: types.BillingModePayPerRequest})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, _, rec, ctx := setup(t, func(target string) string {
				if target == "DescribeTable" {
					return `{"Table":{"TableName":"t","KeySchema":[{"AttributeName":"tenant","KeyType":"HASH"},{"AttributeName":"id","KeyType":"RANGE"}]}}`
				}
				return `{}`
			})
			if err := learn(c); err != nil {
				panic(err)
			}
			if len(rec.Events()) != 0 {
				panic("schema operations are never recorded")
			}
			if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("t"),
				Item: map[string]types.AttributeValue{"tenant": s("acme"), "id": s("z"), "v": n("2")}}); err != nil {
				panic(err)
			}
			m := rec.Events()[0].Mutations[0]
			if len(m.PK) != 2 || m.PK["tenant"] != "acme" || m.PK["id"] != "z" {
				panic(fmt.Sprintf("%+v", m))
			}
		})
	}
}

// ---- benchmarks: calls outside a session ----

func benchUntraced(b *testing.B, in any) {
	ctx := noSession(b)
	tr := newTracer()
	ii := middleware.InitializeInput{Parameters: in}
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = tr.handle(ctx, ii, passNext)
	}
}

func BenchmarkUntracedPutItem30(b *testing.B)  { benchUntraced(b, putInput()) }
func BenchmarkUntracedUpdateItem(b *testing.B) { benchUntraced(b, updateInput()) }
func BenchmarkUntracedQuery(b *testing.B)      { benchUntraced(b, queryInput()) }
func BenchmarkUntracedGetItem(b *testing.B)    { benchUntraced(b, getInput()) }
