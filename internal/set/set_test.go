package set

import (
	"maps"
	"reflect"
	"testing"
)

func TestSet(t *testing.T) {
	s := Of("b", "a", "b")
	if len(s) != 2 || !s.Has("a") || s.Has("z") {
		t.Fatalf("got %v", s)
	}
	s.Add("c")
	if got := Sorted(s); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
	s.AddSeq(maps.Keys(map[string]int{"d": 1}))
	if !s.Has("d") {
		t.Fatal("AddSeq did not add")
	}
	var zero Set[int]
	if zero.Has(1) || Sorted(zero) != nil {
		t.Fatal("zero value must read as empty")
	}
}
