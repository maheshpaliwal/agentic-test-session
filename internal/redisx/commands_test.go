package redisx

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func args(s ...string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func TestKeysAndWrittenKeys(t *testing.T) {
	for _, tc := range []struct {
		cmd     []string
		write   bool
		keys    []int
		written []int // with the popped key unknown
	}{
		{[]string{"sinterstore", "dest", "a", "b"}, true, []int{1, 2, 3}, []int{1}},
		{[]string{"SUNIONSTORE", "dest", "a", "b"}, true, []int{1, 2, 3}, []int{1}},
		{[]string{"sdiffstore", "dest", "a"}, true, []int{1, 2}, []int{1}},
		{[]string{"pfmerge", "dest", "a", "b"}, true, []int{1, 2, 3}, []int{1}},
		{[]string{"bitop", "AND", "dest", "a", "b"}, true, []int{2, 3, 4}, []int{2}},
		{[]string{"copy", "src", "dst"}, true, []int{1, 2}, []int{2}},
		{[]string{"zrangestore", "dst", "src", "0", "-1"}, true, []int{1, 2}, []int{1}},
		{[]string{"geosearchstore", "dst", "src", "FROMLONLAT", "1", "2", "BYRADIUS", "1", "km"}, true, []int{1, 2}, []int{1}},
		{[]string{"zunionstore", "dest", "2", "a", "b", "WEIGHTS", "1", "2"}, true, []int{1, 3, 4}, []int{1}},
		{[]string{"zinterstore", "dest", "1", "a"}, true, []int{1, 3}, []int{1}},
		{[]string{"zdiffstore", "dest", "2", "a", "b"}, true, []int{1, 3, 4}, []int{1}},
		{[]string{"brpoplpush", "src", "dst", "1"}, true, []int{1, 2}, []int{1, 2}},
		{[]string{"blmove", "src", "dst", "LEFT", "RIGHT", "1"}, true, []int{1, 2}, []int{1, 2}},
		{[]string{"lmpop", "2", "a", "b", "LEFT"}, true, []int{2, 3}, []int{2, 3}},
		{[]string{"blmpop", "1", "2", "a", "b", "LEFT"}, true, []int{3, 4}, []int{3, 4}},
		{[]string{"zmpop", "1", "a", "MIN"}, true, []int{2}, []int{2}},
		{[]string{"bzmpop", "0", "2", "a", "b", "MAX"}, true, []int{3, 4}, []int{3, 4}},
		{[]string{"blpop", "a", "b", "0"}, true, []int{1, 2}, []int{1, 2}},
		{[]string{"sort", "l", "ALPHA"}, false, []int{1}, nil},
		{[]string{"SORT", "l", "LIMIT", "0", "5", "store", "dst"}, true, []int{1, 6}, []int{6}},
		{[]string{"rename", "a", "b"}, true, []int{1, 2}, []int{1, 2}},
		{[]string{"del", "a", "b"}, true, []int{1, 2}, []int{1, 2}},
		{[]string{"zunionstore", "dest"}, true, nil, nil}, // malformed: no panic
		{[]string{"lmpop"}, true, nil, nil},
	} {
		a := args(tc.cmd...)
		s := Classify(tc.cmd[0], a)
		if s.Write != tc.write {
			t.Errorf("%v: write=%v", tc.cmd, s.Write)
		}
		keys := KeysOf(tc.cmd[0], s, a)
		if !slices.Equal(keys, tc.keys) {
			t.Errorf("%v: keys=%v, want %v", tc.cmd, keys, tc.keys)
		}
		if !tc.write {
			continue
		}
		if w := WrittenKeys(tc.cmd[0], a, keys, ""); !slices.Equal(w, tc.written) {
			t.Errorf("%v: written=%v, want %v", tc.cmd, w, tc.written)
		}
	}
}

func TestWrittenKeysOfPop(t *testing.T) {
	a := args("BLPOP", "q1", "q2", "q3", "0")
	keys := KeysOf("BLPOP", Classify("BLPOP", a), a)
	if w := WrittenKeys("BLPOP", a, keys, "q2"); !slices.Equal(w, []int{2}) {
		t.Fatalf("popped q2: %v", w)
	}
	if w := WrittenKeys("BLPOP", a, keys, "elsewhere"); !slices.Equal(w, keys) {
		t.Fatalf("unmatched reply key must fall back to every key: %v", w)
	}
	if !IsPop("bzmpop") || IsPop("lpop") {
		t.Fatal("IsPop")
	}
	// the popped key only narrows pops
	d := args("del", "a", "b")
	if w := WrittenKeys("del", d, []int{1, 2}, "a"); len(w) != 2 {
		t.Fatalf("%v", w)
	}
}

func TestNoOp(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply any
		want  bool
	}{
		{"SET", false, true}, {"set", true, false}, {"set", "OK", false},
		{"setnx", int64(0), true}, {"SETNX", false, true}, {"setnx", int64(1), false},
		{"msetnx", int64(0), true}, {"renamenx", false, true},
		{"del", int64(0), true}, {"del", int64(2), false}, {"unlink", int64(0), true},
		{"hdel", int64(0), true}, {"srem", int64(0), true}, {"zrem", int64(0), true},
		{"expire", false, true}, {"pexpire", int64(0), true}, {"expireat", false, true},
		{"pexpireat", false, true}, {"persist", false, true}, {"persist", true, false},
		{"incr", int64(0), false},  // 0 is a value, not a count
		{"lpush", int64(0), false}, // not a conditional write
		{"del", nil, false},        // reply unknown
	} {
		if got := NoOp(tc.name, tc.reply); got != tc.want {
			t.Errorf("NoOp(%s, %v)=%v", tc.name, tc.reply, got)
		}
	}
}

func TestTxOutcome(t *testing.T) {
	failed := errors.New("redis: transaction failed")
	tr := &Tracer{TxFailed: failed}
	for want, err := range map[string]error{
		"committed": nil,
		"aborted":   fmt.Errorf("exec: %w", failed),
		"failed":    errors.New("EXECABORT Transaction discarded because of previous errors."),
	} {
		if got := tr.txOutcome(err); got != want {
			t.Errorf("EXEC error %v: outcome %q, want %q", err, got, want)
		}
	}
}
