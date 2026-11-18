package slicex

import (
	"reflect"
	"strconv"
	"testing"
)

func TestFilter(t *testing.T) {
	even := func(n int) bool { return n%2 == 0 }
	if got := Filter([]int{1, 2, 3, 4}, even); !reflect.DeepEqual(got, []int{2, 4}) {
		t.Fatalf("got %v", got)
	}
	if got := Filter([]int{1, 3}, even); got == nil || len(got) != 0 {
		t.Fatalf("no match must be empty and non-nil, got %#v", got)
	}
	if got := Filter([]int(nil), even); got != nil {
		t.Fatalf("nil input must give nil, got %#v", got)
	}
	type ids []int
	if _, ok := any(Filter(ids{2}, even)).(ids); !ok {
		t.Fatal("named slice type not preserved")
	}
}

func TestMap(t *testing.T) {
	if got := Map([]int{1, 2}, strconv.Itoa); !reflect.DeepEqual(got, []string{"1", "2"}) {
		t.Fatalf("got %v", got)
	}
	if got := Map([]int(nil), strconv.Itoa); got == nil || len(got) != 0 {
		t.Fatalf("empty input must give an empty non-nil slice, got %#v", got)
	}
}

func TestFilterMap(t *testing.T) {
	got := FilterMap([]string{"1", "x", "3"}, func(s string) (int, bool) {
		n, err := strconv.Atoi(s)
		return n, err == nil
	})
	if !reflect.DeepEqual(got, []int{1, 3}) {
		t.Fatalf("got %v", got)
	}
}

func TestToMap(t *testing.T) {
	type row struct {
		ID string
		N  int
	}
	got := ToMap([]row{{"a", 1}, {"b", 2}, {"a", 3}}, func(r row) string { return r.ID })
	want := map[string]row{"a": {"a", 3}, "b": {"b", 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestSortBy(t *testing.T) {
	type row struct{ ID string }
	s := []row{{"c"}, {"a"}, {"b"}}
	SortBy(s, func(r row) string { return r.ID })
	if !reflect.DeepEqual(s, []row{{"a"}, {"b"}, {"c"}}) {
		t.Fatalf("got %v", s)
	}
}

func TestSplitTrim(t *testing.T) {
	if got := SplitTrim(" a, ,b ,", ","); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("got %q", got)
	}
	if got := SplitTrim(" , ", ","); got != nil {
		t.Fatalf("got %q", got)
	}
}
