package tdynamo

import (
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// plainItem converts a DynamoDB item to JSON-friendly values. Numbers keep
// their exact text via json.Number.
func plainItem(item map[string]types.AttributeValue) map[string]any {
	if item == nil {
		return nil
	}
	out := make(map[string]any, len(item))
	for k, v := range item {
		out[k] = plainAV(v)
	}
	return out
}

func plainAV(v types.AttributeValue) any {
	switch t := v.(type) {
	case *types.AttributeValueMemberS:
		return t.Value
	case *types.AttributeValueMemberN:
		return json.Number(t.Value)
	case *types.AttributeValueMemberB:
		return base64.StdEncoding.EncodeToString(t.Value)
	case *types.AttributeValueMemberBOOL:
		return t.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberSS:
		return t.Value
	case *types.AttributeValueMemberNS:
		ns := make([]json.Number, len(t.Value))
		for i, n := range t.Value {
			ns[i] = json.Number(n)
		}
		return ns
	case *types.AttributeValueMemberBS:
		bs := make([]string, len(t.Value))
		for i, b := range t.Value {
			bs[i] = base64.StdEncoding.EncodeToString(b)
		}
		return bs
	case *types.AttributeValueMemberL:
		l := make([]any, len(t.Value))
		for i, e := range t.Value {
			l[i] = plainAV(e)
		}
		return l
	case *types.AttributeValueMemberM:
		return plainItem(t.Value)
	}
	return nil
}

// emptyItem reports a missing/empty item (DynamoDB omits Attributes when no
// item existed).
func emptyItem(m map[string]types.AttributeValue) bool { return len(m) == 0 }
