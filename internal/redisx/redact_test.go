package redisx

import (
	"encoding/json"
	"slices"
	"testing"
)

func maskOf(r Redactor, cmd ...string) []string {
	a := args(cmd...)
	out, _ := r.MaskArgs(cmd[0], a, KeysOf(cmd[0], Classify(cmd[0], a), a))
	return out
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

// The reply rules, one family of commands at a time.
func TestMaskReply(t *testing.T) {
	const x = masked
	flat := Redactor{Capture: true, KeyMask: KeyMaskFlat}
	for _, tc := range []struct {
		name      string
		cmd       []string
		reply     any
		scalar    bool
		sensitive bool // an argument is recorded masked
		want      string
	}{
		// values of a sensitive key, however the command returns them
		{"GETRANGE", []string{"getrange", "password:a", "0", "-1"}, "hunter2", false, true, `"` + x + `"`},
		{"LRANGE", []string{"lrange", "user:1:token", "0", "-1"}, []string{"a", "b"}, false, true, `"` + x + `"`},
		{"SPOP", []string{"spop", "secret:s"}, "m", false, true, `"` + x + `"`},
		{"XRANGE of a sensitive key", []string{"xrange", "secret:x", "-", "+"}, []any{[]any{"1-0", []any{"f", "v"}}}, false, true, `"` + x + `"`},
		{"HGETALL as a map", []string{"hgetall", "secret:h"}, map[string]string{"f": "v"}, false, true, `{"f":"` + x + `"}`},
		{"MGET", []string{"mget", "user:1", "password:a"}, []any{"ann", "hunter2"}, false, true, `["ann","` + x + `"]`},
		{"SINTER with one sensitive key", []string{"sinter", "s", "secret:s"}, []string{"m"}, false, true, `"` + x + `"`},
		{"SET ... GET", []string{"set", "password:a", x, "GET"}, "hunter2", true, true, `"` + x + `"`},
		{"SET", []string{"set", "password:a", x}, "OK", true, true, `"OK"`},
		{"INCR", []string{"incr", "user:1:otp"}, int64(7), true, true, `"` + x + `"`},
		{"TTL", []string{"ttl", "password:a"}, int64(7), true, true, `7`},
		// values of a sensitive field or name
		{"HGET", []string{"hget", "user:1", "token"}, "t0k", false, false, `"` + x + `"`},
		{"HMGET", []string{"hmget", "user:1", "name", "token"}, []any{"ann", "t0k"}, false, false, `["ann","` + x + `"]`},
		{"HGETDEL", []string{"hgetdel", "user:1", "FIELDS", "2", "name", "token"}, []any{"ann", "t0k"}, false, false, `["ann","` + x + `"]`},
		{"HVALS", []string{"hvals", "user:1"}, []string{"ann", "t0k"}, false, false, `["` + x + `","` + x + `"]`},
		{"HVALS through Do", []string{"hvals", "user:1"}, []any{"ann", "t0k"}, false, false, `["` + x + `","` + x + `"]`},
		{"HGETALL through Do", []string{"hgetall", "user:1"}, []any{"name", "ann", "token", "t0k"}, false, false, `["name","ann","token","` + x + `"]`},
		{"HGETALL through Do, RESP3", []string{"hgetall", "user:1"}, map[any]any{"token": "t0k"}, false, false, `{"token":"` + x + `"}`},
		{"HSCAN through Do", []string{"hscan", "user:1", "0"}, []any{"0", []any{"token", "t0k", "name", "ann"}}, false, false, `["0",["token","` + x + `","name","ann"]]`},
		{"HRANDFIELD WITHVALUES, RESP3", []string{"hrandfield", "user:1", "2", "WITHVALUES"}, []any{[]any{"token", "t0k"}}, false, false, `[["token","` + x + `"]]`},
		{"XRANGE", []string{"xrange", "user:1:events", "-", "+"}, []any{[]any{"1-0", []any{"token", "t0k", "note", "n"}}}, false, false, `[["1-0",["token","` + x + `","note","n"]]]`},
		{"JSON.GET by path", []string{"json.get", "user:1", "$.password"}, `["hunter2"]`, false, false, `"` + x + `"`},
		{"SORT ... GET", []string{"sort", "ids", "BY", "nosort", "GET", "secret_*"}, []any{"s"}, false, false, `"` + x + `"`},
		{"CONFIG GET", []string{"config", "get", "*"}, []any{"masterauth", "pw", "port", "6379", "requirepass", "pw"}, false, false, `["masterauth","` + x + `","port","6379","requirepass","` + x + `"]`},
		{"CONFIG GET as a map", []string{"config", "get", "masteruser"}, map[string]string{"masteruser": "u"}, false, false, `{"masteruser":"` + x + `"}`},
		// replies that are masked whatever they hold
		{"ACL GETUSER", []string{"acl", "getuser", "u"}, []any{"flags", []any{"on"}, "passwords", []any{"abc"}}, false, false, `"` + x + `"`},
		{"ACL LIST", []string{"acl", "list"}, []any{"user default on nopass ~* +@all"}, false, false, `"` + x + `"`},
		{"DUMP", []string{"dump", "user:1"}, "\x00\x05hello", false, false, `"` + x + `"`},
		{"DEBUG", []string{"debug", "object", "user:1"}, "Value at:0x7f", false, false, `"` + x + `"`},
		{"SLOWLOG", []string{"slowlog", "get"}, []any{[]any{int64(1), []any{"set", "k", "v"}}}, false, false, `"` + x + `"`},
		// a script or an unknown command can answer with any part of a masked argument
		{"EVAL", []string{"eval", "return ARGV[1]", "0", "session:" + x}, "Ab3d", false, true, `"` + x + `"`},
		{"unknown", []string{"foo.bar", "session:" + x}, []any{"Ab3d"}, false, true, `"` + x + `"`},
		{"EVAL, nothing masked", []string{"eval", "return 1", "0"}, "one", false, false, `"one"`},
		{"unknown, nothing masked", []string{"echo", "hello"}, "hello", false, false, `"hello"`},
		// names in replies are masked as key names are
		{"KEYS", []string{"keys", "*"}, []string{"session:Ab3dE9", "token", "user:1"}, false, false, `["session:` + x + `","token","user:1"]`},
		{"KEYS through Do", []string{"keys", "*"}, []any{"session:Ab3dE9"}, false, false, `["session:` + x + `"]`},
		{"SCAN through Do", []string{"scan", "0"}, []any{"0", []any{"session:Ab3dE9", "user:1"}}, false, false, `["0",["session:` + x + `","user:1"]]`},
		{"RANDOMKEY", []string{"randomkey"}, "sess:Ab3dE9", false, false, `"sess:` + x + `"`},
		{"BLPOP", []string{"blpop", "sess:Ab3dE9:jobs", "1"}, []string{"sess:Ab3dE9:jobs", "job"}, false, true, `["sess:` + x + `:jobs","job"]`},
		{"SMEMBERS", []string{"smembers", "refs"}, []string{"session:Ab3dE9", `{"password":"p"}`}, false, false, `["session:` + x + `","{\"password\":\"` + x + `\"}"]`},
		{"HKEYS", []string{"hkeys", "user:1"}, []string{"token:Ab3dE9", "name"}, false, false, `["token:` + x + `","name"]`},
		{"HGETALL field names", []string{"hgetall", "user:1"}, map[string]string{"sess_Ab3dE9": "1"}, false, false, `{"sess_` + x + `":"1"}`},
		// shapes the tracer does not know are not recorded as they are
		{"unknown shape", []string{"get", "k"}, struct{ S string }{"Ab3d"}, false, false, `"` + x + `"`},
		{"numbers and nil", []string{"zrange", "z", "0", "-1", "WITHSCORES"}, []any{"m", 1.5, nil, int64(2), true}, false, false, `["m",1.5,null,2,true]`},
	} {
		a := args(tc.cmd...)
		keys := KeysOf(tc.cmd[0], Classify(tc.cmd[0], a), a)
		v, ok := flat.MaskReply("s1", tc.cmd[0], a, keys, tc.reply, tc.scalar, tc.sensitive)
		b, _ := json.Marshal(v)
		if !ok || string(b) != tc.want {
			t.Errorf("%s: recorded %s (ok=%v), want %s", tc.name, b, ok, tc.want)
		}
	}
	if _, ok := (&Redactor{}).MaskReply("s1", "get", args("get", "k"), []int{1}, "v", false, false); ok {
		t.Error("a value is recorded without value capture")
	}
}
