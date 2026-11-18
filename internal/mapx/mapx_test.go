package mapx

import (
	"reflect"
	"slices"
	"testing"
)

func TestSortedKeys(t *testing.T) {
	if got := SortedKeys(map[string]int{"b": 1, "a": 2, "c": 3}); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
	if got := SortedKeys(map[string]int(nil)); got == nil || len(got) != 0 {
		t.Fatalf("empty map must give an empty non-nil slice, got %#v", got)
	}
}

func TestValues(t *testing.T) {
	got := Values(map[string]int{"a": 2, "b": 1})
	slices.Sort(got)
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("got %v", got)
	}
	if got := Values(map[string]int(nil)); got == nil {
		t.Fatal("must be non-nil")
	}
}

func TestPick(t *testing.T) {
	type attrs map[string]int
	got := Pick(attrs{"a": 1, "b": 2, "c": 3}, "a", "c", "missing")
	if !reflect.DeepEqual(got, attrs{"a": 1, "c": 3}) {
		t.Fatalf("got %v", got)
	}
}
