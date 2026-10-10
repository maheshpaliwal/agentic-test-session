package tmongo

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// plain converts decoded BSON into JSON-friendly Go values: documents become
// map[string]any, arrays []any, and driver-specific scalars become strings.
func plain(v any) any {
	switch t := v.(type) {
	case bson.D:
		m := make(map[string]any, len(t))
		for _, e := range t {
			m[e.Key] = plain(e.Value)
		}
		return m
	case bson.M:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = plain(e)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = plain(e)
		}
		return m
	case bson.A:
		return plainSlice(t)
	case []any:
		return plainSlice(t)
	case bson.ObjectID:
		return t.Hex()
	case bson.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case bson.Binary:
		return map[string]any{"$binary": t.Data, "subtype": t.Subtype}
	case bson.Decimal128:
		return t.String()
	case bson.Timestamp:
		return map[string]any{"t": t.T, "i": t.I}
	case bson.Null:
		return nil
	}
	return v
}

func plainSlice(a []any) []any {
	out := make([]any, len(a))
	for i, e := range a {
		out[i] = plain(e)
	}
	return out
}

type doc = map[string]any

func asDoc(v any) doc {
	d, _ := v.(doc)
	return d
}

func asArr(v any) []any {
	a, _ := v.([]any)
	return a
}

func num(v any) int64 {
	switch t := v.(type) {
	case int32:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	}
	return 0
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case nil:
		return false
	}
	return num(v) != 0
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
