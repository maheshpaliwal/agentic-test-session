package dynamostore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDB is an in-memory stand-in for the DynamoDB calls the store makes. It
// understands only the expressions the store generates, but enforces the real
// service limits that shape the design: 100 actions and 4MB per transaction,
// 400KB per item, and paged Query/Scan results.
type fakeDB struct {
	mu      sync.Mutex
	exists  bool
	status  types.TableStatus
	items   map[string]map[string]types.AttributeValue
	pageMax int // items per Query/Scan page
	creates int
	txCalls int
}

func newFakeDB() *fakeDB {
	return &fakeDB{items: map[string]map[string]types.AttributeValue{}, pageMax: 7}
}

func keyOf(it map[string]types.AttributeValue) string {
	return getS(it, aPK) + "\x00" + getS(it, aSK)
}

// itemSize follows DynamoDB's rule: attribute names plus values.
func itemSize(it map[string]types.AttributeValue) int {
	n := 0
	for k, v := range it {
		n += len(k)
		switch v := v.(type) {
		case *types.AttributeValueMemberS:
			n += len(v.Value)
		case *types.AttributeValueMemberN:
			n += len(v.Value)
		case *types.AttributeValueMemberB:
			n += len(v.Value)
		case *types.AttributeValueMemberL:
			for _, e := range v.Value {
				n += 1 + itemSize(map[string]types.AttributeValue{"": e})
			}
		default:
			n++
		}
	}
	return n
}

func (f *fakeDB) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.exists {
		return nil, &types.ResourceNotFoundException{}
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{
		TableName:   in.TableName,
		TableStatus: f.status,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(aPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(aSK), KeyType: types.KeyTypeRange},
		},
	}}, nil
}

func (f *fakeDB) CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	f.exists, f.status = true, types.TableStatusActive
	return &dynamodb.CreateTableOutput{}, nil
}

func (f *fakeDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	it := f.items[keyOf(in.Key)]
	if it == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: project(it, in.ProjectionExpression, in.ExpressionAttributeNames)}, nil
}

func (f *fakeDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if itemSize(in.Item) > 400*1024 {
		return nil, errors.New("ValidationException: item size exceeds 400KB")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[keyOf(in.Item)] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func project(it map[string]types.AttributeValue, expr *string, names map[string]string) map[string]types.AttributeValue {
	if expr == nil {
		return it
	}
	out := map[string]types.AttributeValue{}
	for _, p := range strings.Split(*expr, ",") {
		p = strings.TrimSpace(p)
		if n, ok := names[p]; ok {
			p = n
		}
		if v, ok := it[p]; ok {
			out[p] = v
		}
	}
	return out
}

func (f *fakeDB) page(ctx context.Context, keep func(map[string]types.AttributeValue) bool, filter func(map[string]types.AttributeValue) bool,
	start map[string]types.AttributeValue, limit *int32, expr *string, names map[string]string) (items []map[string]types.AttributeValue, last map[string]types.AttributeValue, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []map[string]types.AttributeValue
	for _, it := range f.items {
		if keep(it) {
			all = append(all, it)
		}
	}
	sort.Slice(all, func(i, j int) bool { return keyOf(all[i]) < keyOf(all[j]) })
	max := f.pageMax
	if limit != nil && int(*limit) < max {
		max = int(*limit)
	}
	var rest []map[string]types.AttributeValue
	for _, it := range all {
		if start == nil || keyOf(it) > keyOf(start) {
			rest = append(rest, it)
		}
	}
	more := len(rest) > max
	if more {
		rest = rest[:max]
	}
	for _, it := range rest {
		if filter == nil || filter(it) {
			items = append(items, project(it, expr, names))
		}
	}
	if more {
		l := rest[len(rest)-1]
		last = map[string]types.AttributeValue{aPK: l[aPK], aSK: l[aSK]}
	}
	return items, last, nil
}

func (f *fakeDB) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	pk, lo, hi := getS(in.ExpressionAttributeValues, ":pk"), getS(in.ExpressionAttributeValues, ":lo"), getS(in.ExpressionAttributeValues, ":hi")
	keep := func(it map[string]types.AttributeValue) bool {
		sk := getS(it, aSK)
		return getS(it, aPK) == pk && sk >= lo && sk <= hi
	}
	var filter func(map[string]types.AttributeValue) bool
	if in.FilterExpression != nil {
		filter = func(it map[string]types.AttributeValue) bool { _, ok := it[aTxEff]; return ok }
	}
	items, last, err := f.page(ctx, keep, filter, in.ExclusiveStartKey, in.Limit, in.ProjectionExpression, in.ExpressionAttributeNames)
	if err != nil {
		return nil, err
	}
	return &dynamodb.QueryOutput{Items: items, LastEvaluatedKey: last}, nil
}

func (f *fakeDB) Scan(ctx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	m, s := getS(in.ExpressionAttributeValues, ":m"), getS(in.ExpressionAttributeValues, ":s")
	filter := func(it map[string]types.AttributeValue) bool { sk := getS(it, aSK); return sk == m || sk == s }
	items, last, err := f.page(ctx, func(map[string]types.AttributeValue) bool { return true }, filter, in.ExclusiveStartKey, in.Limit, in.ProjectionExpression, in.ExpressionAttributeNames)
	if err != nil {
		return nil, err
	}
	return &dynamodb.ScanOutput{Items: items, LastEvaluatedKey: last}, nil
}

func cancelled(codes ...string) error {
	rs := make([]types.CancellationReason, len(codes))
	for i, c := range codes {
		rs[i] = types.CancellationReason{Code: aws.String(c)}
	}
	return &types.TransactionCanceledException{CancellationReasons: rs}
}

func (f *fakeDB) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n := len(in.TransactItems); n == 0 || n > maxTxActions {
		return nil, fmt.Errorf("ValidationException: %d actions in transaction", n)
	}
	total := 0
	seen := map[string]bool{}
	for _, a := range in.TransactItems {
		var it map[string]types.AttributeValue
		switch {
		case a.Put != nil:
			it = a.Put.Item
			if itemSize(it) > 400*1024 {
				return nil, errors.New("ValidationException: item size exceeds 400KB")
			}
		case a.Update != nil:
			it = a.Update.Key
		default:
			return nil, errors.New("fake: unsupported action")
		}
		total += itemSize(it)
		if k := keyOf(it); seen[k] {
			return nil, errors.New("ValidationException: duplicate item in transaction")
		} else {
			seen[k] = true
		}
	}
	if total > 4*1024*1024 {
		return nil, errors.New("ValidationException: transaction exceeds 4MB")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txCalls++
	reasons := make([]string, len(in.TransactItems))
	failed := false
	for i, a := range in.TransactItems {
		reasons[i] = "None"
		var key map[string]types.AttributeValue
		var cond *string
		var vals map[string]types.AttributeValue
		if a.Put != nil {
			key, cond = a.Put.Item, a.Put.ConditionExpression
		} else {
			key, cond, vals = a.Update.Key, a.Update.ConditionExpression, a.Update.ExpressionAttributeValues
		}
		cur := f.items[keyOf(key)]
		ok := true
		switch aws.ToString(cond) {
		case "":
		case "attribute_not_exists(#pk)":
			ok = cur == nil
		case "#head = :exp":
			ok = cur != nil && getU(cur, aHead) == getU(vals, ":exp")
		default:
			return nil, fmt.Errorf("fake: unsupported condition %q", aws.ToString(cond))
		}
		if !ok {
			reasons[i], failed = "ConditionalCheckFailed", true
		}
	}
	if failed {
		return nil, cancelled(reasons...)
	}
	for _, a := range in.TransactItems {
		if a.Put != nil {
			f.items[keyOf(a.Put.Item)] = a.Put.Item
			continue
		}
		u := a.Update
		it := map[string]types.AttributeValue{}
		for k, v := range f.items[keyOf(u.Key)] {
			it[k] = v
		}
		for k, v := range u.Key {
			it[k] = v
		}
		set := strings.TrimPrefix(aws.ToString(u.UpdateExpression), "SET ")
		for _, as := range strings.Split(set, ",") {
			l, r, _ := strings.Cut(as, "=")
			it[u.ExpressionAttributeNames[strings.TrimSpace(l)]] = u.ExpressionAttributeValues[strings.TrimSpace(r)]
		}
		f.items[keyOf(u.Key)] = it
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}
