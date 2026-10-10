package tdynamo

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"tsession/core"
)

// stub is an in-memory smithy HTTP client returning canned DynamoDB JSON.
type stub struct {
	mu    sync.Mutex
	reqs  []string // "Target body"
	reply func(target string) string
}

func (s *stub) Do(r *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(r.Body)
	target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "DynamoDB_20120810.")
	s.mu.Lock()
	s.reqs = append(s.reqs, target+" "+string(b))
	s.mu.Unlock()
	body := s.reply(target)
	return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(bytes.NewBufferString(body)),
		Header: http.Header{"Content-Type": {"application/x-amz-json-1.0"}}, Request: r}, nil
}

func setup(t *testing.T, reply func(string) string, o ...Option) (*dynamodb.Client, *stub, *core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	st := &stub{reply: reply}
	c := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		HTTPClient:   st,
		BaseEndpoint: aws.String("http://127.0.0.1:1"),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDSYNTHETIC", SecretAccessKey: "synthetic"}, nil
		}),
		RetryMaxAttempts: 1,
	}, WithTracing(o...))
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	return c, st, rec, ctx
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }

func TestPutItemInjectsAndStrips(t *testing.T) {
	c, st, rec, ctx := setup(t, func(string) string { return `{"Attributes":{"id":{"S":"1"},"v":{"N":"1"}}}` },
		WithKeySchema(map[string][]string{"t": {"id"}}))
	in := &dynamodb.PutItemInput{TableName: aws.String("t"), Item: map[string]types.AttributeValue{"id": s("1"), "v": n("2")}}
	out, err := c.PutItem(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if in.ReturnValues != "" || out.Attributes != nil {
		t.Fatal("caller input/response must be unchanged")
	}
	if !strings.Contains(st.reqs[0], `"ReturnValues":"ALL_OLD"`) {
		t.Fatalf("not injected: %s", st.reqs[0])
	}
	ev := rec.Events()
	if len(ev) != 1 || ev[0].Kind != core.KindDBWrite || ev[0].Name != "PutItem t" {
		t.Fatalf("%+v", ev)
	}
	m := ev[0].Mutations[0]
	if m.Store != "dynamodb" || m.Entity != "t" || m.Op != "update" || m.PK["id"] != "1" ||
		!strings.Contains(string(m.Before.Inline), `"v":1`) || !strings.Contains(string(m.After.Inline), `"v":2`) {
		t.Fatalf("%+v", m)
	}
}

func TestCallerReturnValuesRespected(t *testing.T) {
	c, st, rec, ctx := setup(t, func(string) string { return `{"Attributes":{"id":{"S":"1"}}}` })
	out, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("t"),
		Key: map[string]types.AttributeValue{"id": s("1")}, ReturnValues: types.ReturnValueAllOld})
	if err != nil || out.Attributes == nil {
		t.Fatal("caller asked for ALL_OLD and must receive it", err)
	}
	if strings.Count(st.reqs[0], "ReturnValues") != 1 {
		t.Fatal(st.reqs[0])
	}
	m := rec.Events()[0].Mutations[0]
	if m.Op != "delete" || m.PK["id"] != "1" || m.Before.IsZero() {
		t.Fatalf("%+v", m)
	}
}

func TestUpdateItemAllNewAndOld(t *testing.T) {
	c, st, rec, ctx := setup(t, func(string) string { return `{"Attributes":{"id":{"S":"1"},"v":{"N":"9"}}}` })
	in := &dynamodb.UpdateItemInput{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"id": s("1")},
		UpdateExpression: aws.String("SET v = :v"), ExpressionAttributeValues: map[string]types.AttributeValue{":v": n("9")}}
	out, err := c.UpdateItem(ctx, in)
	if err != nil || out.Attributes != nil {
		t.Fatal(err, out.Attributes)
	}
	if !strings.Contains(st.reqs[0], `"ALL_NEW"`) {
		t.Fatal(st.reqs[0])
	}
	m := rec.Events()[0].Mutations[0]
	if !strings.Contains(string(m.After.Inline), `"v":9`) || !m.Before.IsZero() || m.PK["id"] != "1" {
		t.Fatalf("%+v", m)
	}
}

func TestReadsAndSchemaLearning(t *testing.T) {
	c, _, rec, ctx := setup(t, func(target string) string {
		switch target {
		case "DescribeTable":
			return `{"Table":{"TableName":"t","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}}`
		case "GetItem":
			return `{"Item":{"id":{"S":"1"}}}`
		case "PutItem":
			return `{}`
		}
		return `{"Count":2,"ScannedCount":2,"Items":[{"id":{"S":"a"}},{"id":{"S":"b"}}]}`
	})
	if _, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("t")}); err != nil {
		t.Fatal(err)
	}
	c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("t"), Key: map[string]types.AttributeValue{"id": s("1")}})
	c.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("t"), KeyConditionExpression: aws.String("id = :i")})
	c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("t"), Item: map[string]types.AttributeValue{"id": s("z")}})
	ev := rec.Events()
	if len(ev) != 3 { // DescribeTable is not traced
		t.Fatalf("events=%d", len(ev))
	}
	if ev[0].Kind != core.KindDBRead || ev[0].Reads[0].RowCount != 1 {
		t.Fatalf("%+v", ev[0])
	}
	if q := ev[1].Reads[0]; q.RowCount != 2 || len(q.PKs) != 2 {
		t.Fatalf("%+v", q)
	}
	m := ev[2].Mutations[0]
	if m.Op != "insert" || m.PK["id"] != "z" {
		t.Fatalf("%+v", m)
	}
}

func TestBatchWriteAndErrorAndNoSession(t *testing.T) {
	c, _, rec, ctx := setup(t, func(string) string {
		return `{"UnprocessedItems":{"t":[{"DeleteRequest":{"Key":{"id":{"S":"2"}}}}]}}`
	}, WithKeySchema(map[string][]string{"t": {"id"}}))
	_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"t": {
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"id": s("1")}}},
		{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"id": s("2")}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	e := rec.Events()[0]
	if len(e.Mutations) != 1 || e.Mutations[0].Op != "put" || e.Mutations[0].PK["id"] != "1" || len(e.Flags) != 1 {
		t.Fatalf("%+v", e)
	}
	// no session in ctx: no events, input left alone
	c.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String("t")})
	if len(rec.Events()) != 1 {
		t.Fatal("unexpected event without session")
	}
}

func TestMiddlewareIdempotent(t *testing.T) {
	c, st, rec, ctx := setup(t, func(string) string { return `{}` })
	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("t"), Item: map[string]types.AttributeValue{"id": s("1")}}, WithTracing())
	if err != nil || len(rec.Events()) != 1 || len(st.reqs) != 1 {
		t.Fatalf("events=%d err=%v", len(rec.Events()), err)
	}
}
