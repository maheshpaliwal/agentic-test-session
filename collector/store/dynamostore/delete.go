package dynamostore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DeleteSession implements store.Store. DynamoDB has no "delete partition":
// the session's sort keys are read page by page and removed with
// BatchWriteItem. Items go in sort-key order, so the events are removed before
// META and SEAL; a delete that fails half way leaves the session listed and
// can simply be repeated.
func (s *Store) DeleteSession(ctx context.Context, sid string) error {
	if err := checkSession(sid); err != nil {
		return err
	}
	in := &dynamodb.QueryInput{
		TableName:                 &s.table,
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		ProjectionExpression:      aws.String("#sk"),
		ExpressionAttributeNames:  map[string]string{"#pk": aPK, "#sk": aSK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": strAV(sid)},
	}
	for {
		cc, cancel := s.cctx(ctx)
		page, err := s.db.Query(cc, in)
		cancel()
		if err != nil {
			return fmt.Errorf("dynamostore: query session keys: %w", err)
		}
		reqs := make([]types.WriteRequest, 0, len(page.Items))
		for _, it := range page.Items {
			reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{
				Key: map[string]types.AttributeValue{aPK: strAV(sid), aSK: it[aSK]},
			}})
		}
		for len(reqs) > 0 {
			n := min(len(reqs), maxBatchSize)
			if err := s.batchDelete(ctx, reqs[:n]); err != nil {
				return err
			}
			reqs = reqs[n:]
		}
		if len(page.LastEvaluatedKey) == 0 {
			return nil
		}
		in.ExclusiveStartKey = page.LastEvaluatedKey
	}
}

// batchDelete sends one BatchWriteItem (at most maxBatchSize requests) and
// retries what DynamoDB reports as unprocessed (throttling), backing off.
func (s *Store) batchDelete(ctx context.Context, reqs []types.WriteRequest) error {
	backoff := 50 * time.Millisecond
	for attempt := 0; len(reqs) > 0; attempt++ {
		if attempt >= 8 {
			return fmt.Errorf("dynamostore: delete: %d items still unprocessed after %d attempts", len(reqs), attempt)
		}
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 2*time.Second)
		}
		cc, cancel := s.cctx(ctx)
		out, err := s.db.BatchWriteItem(cc, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{s.table: reqs},
		})
		cancel()
		if err != nil {
			return fmt.Errorf("dynamostore: batch delete: %w", err)
		}
		reqs = out.UnprocessedItems[s.table]
	}
	return nil
}
