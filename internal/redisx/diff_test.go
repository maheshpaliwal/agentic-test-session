package redisx

import (
	"fmt"
	"testing"
)

func TestDiffSnaps(t *testing.T) {
	str := func(v string) *snap { return &snap{typ: "string", items: []item{{shown: v}}} }
	list := func(capped bool, vs ...string) *snap {
		s := &snap{typ: "list", capped: capped}
		for _, v := range vs {
			s.items = append(s.items, item{shown: v})
		}
		return s
	}
	none := &snap{typ: "none"}
	for _, tc := range []struct {
		name      string
		b, a      *snap
		want      string
		truncated bool
	}{
		{"no before read", nil, str("x"), "[]", false},
		{"absent on both sides", none, none, "[]", false},
		{"type changed", str("x"), list(false, "x"), "[{type string list}]", false},
		{"a type that is not imaged", &snap{typ: "stream"}, &snap{typ: "stream"}, "[]", false},
		{"list: middle replaced", list(false, "a", "b", "c", "d"), list(false, "a", "X", "Y", "Z", "d"),
			"[{[1] b X} {[2] c Y} {[3] <nil> Z}]", false},
		{"list: pushed at the head", list(false, "a", "b"), list(false, "n", "a", "b"), "[{[0] <nil> n}]", false},
		{"list: read in part", list(true, "a", "b"), list(true, "n", "a"), "[]", true},
		{"list: read in part, then deleted", list(true, "a", "b"), none, "[{[0] a <nil>} {[1] b <nil>}]", true},
	} {
		got, truncated := diffSnaps(tc.b, tc.a)
		if s := fmt.Sprint(got); s != tc.want || truncated != tc.truncated {
			t.Errorf("%s: %s truncated=%v, want %s truncated=%v", tc.name, s, truncated, tc.want, tc.truncated)
		}
	}
}
