package core

import "testing"

func TestMutationAfterUnavailable(t *testing.T) {
	row := Payload(map[string]any{"qty": 10})
	for _, c := range []struct {
		name string
		m    Mutation
		want bool
	}{
		{"update read before, not after", Mutation{Op: "update", Before: row}, true},
		{"the same as an older SDK sent it", Mutation{Op: "update", Before: row, Diff: []FieldChange{{Field: "qty", Old: 10}}}, true},
		{"update with both images", Mutation{Op: "update", Before: row, After: row}, false},
		{"update with the after image only", Mutation{Op: "update", After: row}, false},
		{"update with no image", Mutation{Op: "update"}, false},
		{"delete", Mutation{Op: "delete", Before: row}, false},
		{"insert", Mutation{Op: "insert", After: row}, false},
	} {
		if got := c.m.AfterUnavailable(); got != c.want {
			t.Errorf("%s: AfterUnavailable = %v, want %v", c.name, got, c.want)
		}
	}
}
