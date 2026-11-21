package mapx

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
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

func TestMapKeys(t *testing.T) {
	type headers map[string]int
	got := MapKeys(headers{"X-A": 1, "x-b": 2}, strings.ToLower)
	if want := map[string]int{"x-a": 1, "x-b": 2}; !reflect.DeepEqual(got, want) {
		panic(fmt.Sprintf("got %v, want %v", got, want))
	}
	if got := MapKeys(headers(nil), strings.ToLower); got == nil || len(got) != 0 {
		panic(fmt.Sprintf("nil map: got %#v, want an empty non-nil map", got))
	}
}
