package redisx

import (
	"slices"
	"testing"
)

func maskOf(r Redactor, cmd ...string) []string {
	a := args(cmd...)
	return r.MaskArgs(cmd[0], a, KeysOf(cmd[0], Classify(cmd[0], a), a))
}

func TestMaskArgs(t *testing.T) {
	on, off := Redactor{Capture: true}, Redactor{}
	const x = masked
	for _, tc := range []struct {
		r    Redactor
		cmd  []string
		want []string
	}{
		// member arguments of reads on a sensitive key
		{on, []string{"sismember", "secret:set", "m"}, []string{"sismember", "secret:set", x}},
		{on, []string{"smismember", "secret:set", "m1", "m2"}, []string{"smismember", "secret:set", x, x}},
		{on, []string{"zscore", "user:1:password", "m"}, []string{"zscore", "user:1:password", x}},
		{on, []string{"lpos", "auth:token:l", "m", "RANK", "1"}, []string{"lpos", "auth:token:l", x, x, x}},
		{on, []string{"smove", "secret:a", "b", "m"}, []string{"smove", "secret:a", "b", x}},
		// ... and of plain keys: kept, but scrubbed
		{on, []string{"sismember", "s", "m"}, []string{"sismember", "s", "m"}},
		{on, []string{"sismember", "s", `{"password":"p"}`}, []string{"sismember", "s", `{"password":"[REDACTED]"}`}},
		{on, []string{"zrem", "z", `{"password":"p"}`}, []string{"zrem", "z", `{"password":"[REDACTED]"}`}},
		// field names and options are not values
		{on, []string{"hget", "secret:h", "field"}, []string{"hget", "secret:h", "field"}},
		{on, []string{"expire", "secret:k", "10"}, []string{"expire", "secret:k", "10"}},
		{on, []string{"getrange", "secret:k", "0", "3"}, []string{"getrange", "secret:k", "0", "3"}},
		// unknown commands: scrubbed, or dropped entirely without value capture
		{on, []string{"ECHO", "Bearer abcdefghijklmnop"}, []string{"ECHO", "Bearer [REDACTED]"}},
		{on, []string{"BF.ADD", "bf", "item"}, []string{"BF.ADD", "bf", "item"}},
		{off, []string{"BF.ADD", "bf", "item"}, []string{"BF.ADD", x, x}},
		// known keyless commands carry no values
		{off, []string{"scan", "0", "MATCH", "user:*"}, []string{"scan", "0", "MATCH", "user:*"}},
		// value capture off
		{off, []string{"sismember", "s", "m"}, []string{"sismember", "s", x}},
		{off, []string{"zrem", "z", "m"}, []string{"zrem", "z", x}},
		{off, []string{"set", "k", "v"}, []string{"set", "k", x}},
		{off, []string{"del", "a", "b"}, []string{"del", "a", "b"}},
		{off, []string{"sinterstore", "dest", "a", "b"}, []string{"sinterstore", "dest", "a", "b"}},
		{off, []string{"zunionstore", "dest", "1", "a"}, []string{"zunionstore", "dest", "1", "a"}},
	} {
		if got := maskOf(tc.r, tc.cmd...); !slices.Equal(got, tc.want) {
			t.Errorf("capture=%v %v:\n got %v\nwant %v", tc.r.Capture, tc.cmd, got, tc.want)
		}
	}
}
