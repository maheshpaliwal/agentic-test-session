package mapx

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func TestSortedKeys(t *testing.T) {
	if got := SortedKeys(map[string]int{"b": 1, "a": 2, "c": 3}); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		panic(fmt.Sprintf("got %v", got))
	}
	if got := SortedKeys(map[string]int(nil)); got == nil || len(got) != 0 {
		panic(fmt.Sprintf("empty map must give an empty non-nil slice, got %#v", got))
	}
}

func TestValues(t *testing.T) {
	got := Values(map[string]int{"a": 2, "b": 1})
	slices.Sort(got)
	if !reflect.DeepEqual(got, []int{1, 2}) {
		panic(fmt.Sprintf("got %v", got))
	}
	if got := Values(map[string]int(nil)); got == nil {
		panic("must be non-nil")
	}
}

func TestPick(t *testing.T) {
	type attrs map[string]int
	got := Pick(attrs{"a": 1, "b": 2, "c": 3}, "a", "c", "missing")
	if !reflect.DeepEqual(got, attrs{"a": 1, "c": 3}) {
		panic(fmt.Sprintf("got %v", got))
	}
}
