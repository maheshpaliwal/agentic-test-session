package tdynamo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestExpressionValuesOfSensitiveAttributesMasked(t *testing.T) {
	names := map[string]string{"#s": "secret", "#n": "name"}
	vals := map[string]types.AttributeValue{}
	for _, p := range []string{":a", ":b", ":c", ":t", ":p", ":k", ":l", ":lo", ":hi"} {
		vals[p] = s("leak-" + p[1:])
	}
	for _, tc := range []struct {
		expr   string
		masked string // placeholders that must be masked
		clear  string // placeholders that must stay readable
	}{
		{"SET token = if_not_exists(token, :t)", ":t", ""},
		{"SET token = if_not_exists(token, :t), #n = :a", ":t", ":a"},
		{"ssn IN (:a, :b)", ":a :b", ""},
		{"ssn IN (:a, :b) AND #n = :c", ":a :b", ":c"},
		{"begins_with(password, :p)", ":p", ""},
		{"contains(#s, :p) OR contains(#n, :a)", ":p", ":a"},
		{":p = password", ":p", ""},
		{"SET api_key = :k, lookup = :k", ":k", ""},
		{"SET secret = list_append(secret, :l), tags = list_append(tags, :a)", ":l", ":a"},
		{"password BETWEEN :lo AND :hi AND #n = :a", ":lo :hi", ":a"},
		{"#n BETWEEN :lo AND :hi AND password = :p", ":p", ":lo :hi"},
		{"(password = :p AND #n = :a) OR NOT (#n = :b)", ":p", ":a :b"},
		{"SET #n = :a REMOVE password ADD cnt :c", "", ":a :c"},
		{"attribute_exists(password) AND #n = :a", "", ":a"},
	} {
		expr := tc.expr
		r := resolveExprs(names, vals, &expr)[0]
		for _, p := range strings.Fields(tc.masked) {
			if r.Values[p] != "[REDACTED]" || strings.Contains(r.Inline, "leak-"+p[1:]) {
				t.Errorf("%q: %s not masked: inline=%s values=%v", tc.expr, p, r.Inline, r.Values)
			}
		}
		for _, p := range strings.Fields(tc.clear) {
			if r.Values[p] != "leak-"+p[1:] || !strings.Contains(r.Inline, "leak-"+p[1:]) {
				t.Errorf("%q: %s over-redacted: inline=%s values=%v", tc.expr, p, r.Inline, r.Values)
			}
		}
		if strings.Contains(r.Expr, "leak-") || strings.Contains(r.Expr, "#") {
			t.Errorf("%q: Expr must keep placeholders and resolve names: %s", tc.expr, r.Expr)
		}
	}
}

// A placeholder shared by the update and condition expressions is masked in
// both when either binds it to a sensitive attribute.
func TestSensitivePlaceholderSharedAcrossExpressions(t *testing.T) {
	c, _, rec, ctx := setup(t, func(string) string { return `{}` })
	_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String("t"),
		Key:                       map[string]types.AttributeValue{"id": s("1")},
		UpdateExpression:          aws.String("SET password = :v, hint = if_not_exists(token, :t)"),
		ConditionExpression:       aws.String("previous <> :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": s("leak-shared"), ":t": s("leak-fn")},
		ReturnValues:              types.ReturnValueUpdatedOld, // After falls back to the update expression
	})
	if err != nil {
		t.Fatal(err)
	}
	e := rec.Events()[0]
	txt := string(e.Req.Inline) + fmt.Sprint(e.Mutations[0].PK) + string(e.Mutations[0].After.Inline)
	for _, bad := range []string{"leak-shared", "leak-fn"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q: %s", bad, txt)
		}
	}
}

func TestDeleteItemAllOldWithoutItemIsNotAMutation(t *testing.T) {
	key := map[string]types.AttributeValue{"id": s("1")}
	for _, tc := range []struct {
		name  string
		rv    types.ReturnValue
		reply string
		want  int
	}{
		{"caller ALL_OLD, nothing deleted", types.ReturnValueAllOld, `{}`, 0},
		{"injected ALL_OLD, nothing deleted", "", `{}`, 0},
		{"caller ALL_OLD, item deleted", types.ReturnValueAllOld, `{"Attributes":{"id":{"S":"1"}}}`, 1},
		{"caller NONE: outcome unknown", types.ReturnValueNone, `{}`, 1},
	} {
		c, _, rec, ctx := setup(t, func(string) string { return tc.reply })
		out, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("t"), Key: key, ReturnValues: tc.rv})
		if err != nil {
			t.Fatal(err)
		}
		if tc.rv == types.ReturnValueAllOld && tc.want == 1 && out.Attributes == nil {
			t.Errorf("%s: the caller's Attributes must be kept", tc.name)
		}
		if n := len(rec.Events()[0].Mutations); n != tc.want {
			t.Errorf("%s: mutations=%d, want %d", tc.name, n, tc.want)
		}
	}
}
