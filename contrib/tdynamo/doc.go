// Package tdynamo records DynamoDB operations (aws-sdk-go-v2) into the active
// test session via an Initialize-step smithy middleware.
//
//	client := dynamodb.NewFromConfig(cfg, tdynamo.WithTracing())
//	// or: tdynamo.Apply(&opts) on a dynamodb.Options
//
// Reads (GetItem, Query, Scan, BatchGetItem, TransactGetItems) produce a
// core.ReadSummary; writes (PutItem, UpdateItem, DeleteItem, BatchWriteItem,
// TransactWriteItems) produce a core.Mutation per item with Store "dynamodb".
//
// Before/after images: when the caller did not choose a ReturnValues setting
// the middleware injects one on a copy of the input (ALL_OLD for Put/Delete;
// ALL_NEW for UpdateItem unless WithUpdateReturn says otherwise), records the
// returned item, and strips it from the response so the caller observes no
// difference. A caller-supplied ReturnValues is respected and never altered.
// A DeleteItem whose ALL_OLD (injected or the caller's) returns no item
// deleted nothing and yields no mutation.
//
// Expressions are recorded with "#n" names resolved and ":v" values inlined.
// A value is masked when it shares a comparison, assignment, function call or
// IN / BETWEEN list with a sensitive attribute anywhere in the request.
//
// Primary keys come from the Key of Get/Update/Delete. PutItem carries only
// Item, so its key attribute names are learned from earlier keyed calls or
// DescribeTable/CreateTable responses, or given with WithKeySchema.
package tdynamo
