package redisx

import (
	"fmt"
	"testing"
)

func TestDiffSnaps(t *testing.T) {
	str := func(v string) *snap { return &snap{typ: "string", items: []item{{shown: v, sum: sumOf(v)}}} }
	masked := func(raw string) *snap {
		return &snap{typ: "string", items: []item{{shown: "[REDACTED]", sum: sumOf(raw)}}}
	}
	list := func(capped bool, vs ...string) *snap {
		s := &snap{typ: "list", capped: capped}
		for _, v := range vs {
			s.items = append(s.items, item{shown: v, sum: sumOf(v)})
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
		{"redacted and unchanged", masked("hunter2"), masked("hunter2"), "[]", false},
		{"redacted and changed", masked("hunter2"), masked("hunter3"), "[{value [REDACTED] [REDACTED]}]", false},
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

// What the two reads around a command say about another writer. The check
// must never blame a command for its own effect.
func TestForeignChange(t *testing.T) {
	hash := func(kv ...string) *snap {
		s := &snap{typ: "hash"}
		for i := 0; i+1 < len(kv); i += 2 {
			s.items = append(s.items, item{id: sumOf(kv[i]), name: kv[i], shown: kv[i+1], sum: sumOf(kv[i+1])})
		}
		return s
	}
	zset := func(members ...string) *snap {
		s := &snap{typ: "zset"}
		for i, m := range members {
			s.items = append(s.items, item{id: sumOf(m), name: m, score: float64(i)})
		}
		return s
	}
	list := func(vs ...string) *snap {
		s := &snap{typ: "list"}
		for _, v := range vs {
			s.items = append(s.items, item{id: sumOf(v), shown: v, sum: sumOf(v)})
		}
		return s
	}
	str := func(v string) *snap { return &snap{typ: "string", items: []item{{shown: v, sum: sumOf(v)}}} }
	none := &snap{typ: "none"}
	capped := hash("a", "1", "other", "2")
	capped.capped = true
	hidden := &snap{typ: "string", hidden: true}
	for _, tc := range []struct {
		name string
		args []any
		b, a *snap
		want bool
	}{
		{"HSET its field", []any{"hset", "k", "a", "2"}, hash("a", "1"), hash("a", "2"), false},
		{"HSET a new key", []any{"hset", "k", "a", "2", "b", 3}, none, hash("a", "2", "b", "3"), false},
		{"HSET with values of any type", []any{"hset", "k", "a", 2.5, "b", true}, hash("a", "1"), hash("a", "2.5", "b", "1"), false},
		{"HSET and another field", []any{"hset", "k", "a", "2"}, hash("a", "1"), hash("a", "2", "other", "9"), true},
		{"HSET and a value that is another field's name", []any{"hset", "k", "a", "other"}, hash("a", "1"), hash("a", "other", "other", "9"), true},
		{"HSET and the key deleted", []any{"hset", "k", "a", "2"}, hash("a", "1", "b", "1"), none, true},
		{"HSET and the key replaced by a string", []any{"hset", "k", "a", "2"}, hash("a", "1"), str("x"), true},
		{"HSET, partial read", []any{"hset", "k", "a", "2"}, hash("a", "1"), capped, false},
		{"HSET, a field name of an uncertain type", []any{"hset", "k", 1.5, "2"}, none, hash("1.5", "2", "other", "9"), false},
		{"HDEL its fields", []any{"hdel", "k", "a", "b"}, hash("a", "1", "b", "1", "c", "1"), hash("c", "1"), false},
		{"HDEL the last field", []any{"hdel", "k", "a"}, hash("a", "1"), none, false},
		{"HDEL and a field added", []any{"hdel", "k", "a"}, hash("a", "1"), hash("z", "1"), true},
		{"HINCRBY", []any{"hincrby", "k", "n", 2}, hash("n", "1"), hash("n", "3"), false},
		{"ZADD with options and float scores", []any{"zadd", "k", "NX", "CH", 1.0, "m", 2.5, "n"}, none, zset("m", "n"), false},
		{"ZADD and another member", []any{"zadd", "k", 1.0, "m"}, zset("a"), zset("a", "m", "z"), true},
		{"ZINCRBY", []any{"zincrby", "k", 2.5, "m"}, zset("a", "m"), zset("m", "a"), true}, // a's score moved too
		{"ZREM", []any{"zrem", "k", "a"}, zset("a", "m"), zset("m"), true},                 // m's score moved
		{"SADD", []any{"sadd", "k", "m"}, &snap{typ: "set"}, &snap{typ: "set", items: []item{{id: sumOf("m"), sum: sumOf("m")}}}, false},
		{"LPOP", []any{"lpop", "k"}, list("a", "b", "b"), list("b", "b"), false},
		{"LPOP the last element", []any{"lpop", "k"}, list("a"), none, false},
		{"BRPOP after another client's push", []any{"brpop", "k", "1"}, none, list("a"), true},
		{"LTRIM", []any{"ltrim", "k", 1, -1}, list("a", "b", "b"), list("b", "b"), false},
		{"LPOP and a push", []any{"lpop", "k"}, list("a", "b"), list("b", "b"), true},
		{"DEL", []any{"del", "k"}, str("x"), none, false},
		{"DEL and a write", []any{"del", "k"}, str("x"), str("x"), false}, // indistinguishable from a DEL that did nothing
		{"DEL and another write", []any{"del", "k"}, str("x"), str("y"), true},
		{"SET", []any{"set", "k", "v", "EX", 10}, str("x"), str("v"), false},
		{"SET of a number", []any{"set", "k", 5}, none, str("5"), false},
		{"SET of a float", []any{"set", "k", 1e21}, none, str("1000000000000000000000"), false},
		{"SET overwritten", []any{"set", "k", "v"}, str("x"), str("w"), true},
		{"SET that expired at once", []any{"set", "k", "v", "PX", 1}, str("x"), none, false},
		{"SETEX", []any{"setex", "k", 10, "v"}, none, str("v"), false},
		{"SETEX overwritten", []any{"setex", "k", 10, "v"}, none, str("10"), true},
		{"a sensitive key", []any{"set", "k", "v"}, hidden, hidden, false},
		{"no before read", []any{"set", "k", "v"}, nil, str("w"), false},
		{"a command without a rule", []any{"append", "k", "v"}, str("x"), str("zzz"), false},
	} {
		if got := foreignChange(Str(tc.args[0]), tc.args, 1, tc.b, tc.a); got != tc.want {
			t.Errorf("%s: %v", tc.name, got)
		}
	}
}
