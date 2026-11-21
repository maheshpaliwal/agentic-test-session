package tredisv8

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// diffOf renders the diff of the only mutation of the last recorded event as
// "field: old -> new" lines, an absent side as "-".
func diffOf(t *testing.T, rec *core.MemRecorder) []string {
	t.Helper()
	evs := rec.Events()
	if len(evs) == 0 || len(evs[len(evs)-1].Mutations) != 1 {
		panic(fmt.Sprintf("want one mutation on the last event: %+v", evs))
	}
	side := func(v any) string {
		if v == nil {
			return "-"
		}
		b, _ := json.Marshal(v)
		return string(b)
	}
	var out []string
	for _, d := range evs[len(evs)-1].Mutations[0].Diff {
		out = append(out, d.Field+": "+side(d.Old)+" -> "+side(d.New))
	}
	return out
}

func wantDiff(t *testing.T, rec *core.MemRecorder, what string, want ...string) {
	t.Helper()
	if got := diffOf(t, rec); !slices.Equal(got, want) {
		t.Errorf("%s:\n got  %q\n want %q", what, got, want)
	}
}

func TestDiffPerType(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))

		c.Set(ctx, "user:1:name", "ann", 0)
		wantDiff(t, rec, "string created", `value: - -> "ann"`)
		c.Set(ctx, "user:1:name", "bob", 0)
		wantDiff(t, rec, "string changed", `value: "ann" -> "bob"`)
		c.Set(ctx, "user:1:name", "bob", 0)
		wantDiff(t, rec, "string rewritten with the same value")
		c.Del(ctx, "user:1:name")
		wantDiff(t, rec, "string deleted", `value: "bob" -> -`)

		c.HSet(ctx, "user:1", "name", "ann", "plan", "free")
		wantDiff(t, rec, "hash created", `name: - -> "ann"`, `plan: - -> "free"`)
		c.HSet(ctx, "user:1", "plan", "pro", "seats", "3")
		wantDiff(t, rec, "hash field changed and added", `plan: "free" -> "pro"`, `seats: - -> "3"`)
		c.HDel(ctx, "user:1", "seats")
		wantDiff(t, rec, "hash field removed", `seats: "3" -> -`)

		c.RPush(ctx, "user:1:log", "a", "b", "c")
		wantDiff(t, rec, "list created", `[0]: - -> "a"`, `[1]: - -> "b"`, `[2]: - -> "c"`)
		c.RPush(ctx, "user:1:log", "d")
		wantDiff(t, rec, "list appended", `[3]: - -> "d"`)
		c.LPop(ctx, "user:1:log")
		wantDiff(t, rec, "list popped at the head", `[0]: "a" -> -`)
		c.LSet(ctx, "user:1:log", 1, "C")
		wantDiff(t, rec, "list element replaced", `[1]: "c" -> "C"`)

		c.SAdd(ctx, "user:1:tags", "x", "y")
		wantDiff(t, rec, "set created", `added: - -> ["x","y"]`)
		c.SAdd(ctx, "user:1:tags", "y", "z")
		wantDiff(t, rec, "set member added", `added: - -> ["z"]`)
		c.SRem(ctx, "user:1:tags", "x")
		wantDiff(t, rec, "set member removed", `removed: ["x"] -> -`)

		c.ZAdd(ctx, "user:1:rank", &redis.Z{Score: 1, Member: "m"}, &redis.Z{Score: 2, Member: "n"})
		wantDiff(t, rec, "sorted set created", `m: - -> 1`, `n: - -> 2`)
		c.ZIncrBy(ctx, "user:1:rank", 4, "m")
		wantDiff(t, rec, "sorted set score changed", `m: 1 -> 5`)
		c.ZRem(ctx, "user:1:rank", "n")
		wantDiff(t, rec, "sorted set member removed", `n: 2 -> -`)

		c.Expire(ctx, "user:1", time.Minute)
		wantDiff(t, rec, "the images carry no TTL")

		c.Set(ctx, "other", "v", 0)
		wantDiff(t, rec, "a key without images has no diff")
		for _, e := range rec.Events() {
			if len(e.Flags) != 0 {
				t.Errorf("%s: flags %v", e.Name, e.Flags)
			}
		}
	})
}

func TestDiffAddsNoRoundTrips(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))
		c.HSet(context.Background(), "user:1", "plan", "free")
		ch := &countHook{}
		c.AddHook(ch)
		c.HSet(ctx, "user:1", "plan", "pro")
		wantDiff(t, rec, "hash", `plan: "free" -> "pro"`)
		if n := ch.n.Load(); n != 5 { // HSET, and TYPE + HSCAN before and after it
			panic(fmt.Sprintf("round trips=%d, want 5", n))
		}
	})
}

func TestDiffOfCappedCollections(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("big:*"))
		bg := context.Background()
		var members, fields, elems []any
		for i := range 1500 {
			members = append(members, fmt.Sprintf("m%04d", i))
			fields = append(fields, fmt.Sprintf("f%04d", i), "v")
			elems = append(elems, i)
		}
		c.SAdd(bg, "big:set", members...)
		c.HSet(bg, "big:hash", fields...)
		c.RPush(bg, "big:list", elems...)

		flagged := func(what string) {
			t.Helper()
			evs := rec.Events()
			if e := evs[len(evs)-1]; !slices.Contains(e.Flags, "diff_truncated") {
				t.Errorf("%s: flags %v, want diff_truncated", what, e.Flags)
			}
		}
		// Two partial reads of a set or a list cannot tell what was added.
		c.SAdd(ctx, "big:set", "one-more")
		wantDiff(t, rec, "set read in part")
		flagged("set")
		c.LPush(ctx, "big:list", "head")
		wantDiff(t, rec, "list read in part")
		flagged("list")
		// A field both reads returned did change; a field only one of them
		// returned may just have been out of reach of the other.
		c.HSet(ctx, "big:hash", "one-more", "v")
		for _, d := range diffOf(t, rec) {
			t.Errorf("hash read in part lists %s", d)
		}
		flagged("hash")

		// A deleted key: what the one read returned was removed, the list is cut.
		c.Del(ctx, "big:hash")
		got := diffOf(t, rec)
		if len(got) != 50 || !strings.HasSuffix(got[0], ` -> -`) {
			t.Errorf("deleted hash: %d changes, first %q", len(got), got[:min(len(got), 1)])
		}
		flagged("deleted hash")
	})
}

func TestDiffIsBounded(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("doc:*"))
		long := strings.Repeat("x", 5000)
		c.Set(ctx, "doc:1", long, 0)
		m := rec.Events()[0].Mutations[0]
		if s, _ := m.Diff[0].New.(string); len(s) > 300 || !strings.HasSuffix(s, "more bytes)") {
			panic(fmt.Sprintf("value of %d bytes in the diff: %.80s", len(s), s))
		}
		var fields []any
		for i := range 200 {
			fields = append(fields, fmt.Sprintf("f%03d", i), "v")
		}
		c.HSet(ctx, "doc:2", fields...)
		e := rec.Events()[1]
		if n := len(e.Mutations[0].Diff); n != 50 || !slices.Contains(e.Flags, "diff_truncated") {
			panic(fmt.Sprintf("%d changes, flags %v", n, e.Flags))
		}
	})
}

func TestDiffKeepsRedaction(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))
		c.HSet(ctx, "user:1", "password", "hunter2", "note", "Authorization: Bearer abc.def.ghi")
		c.HSet(ctx, "user:1", "name", "ann")
		c.Set(ctx, "user:2", `{"name":"ann","password":"hunter2"}`, 0)
		c.Set(ctx, "user:2", `{"name":"bob","password":"hunter2"}`, 0)
		c.Del(ctx, "user:1")
		b, _ := json.Marshal(rec.Events())
		for _, secret := range []string{"hunter2", "abc.def.ghi"} {
			if strings.Contains(string(b), secret) {
				t.Errorf("%q is recorded: %s", secret, b)
			}
		}
	})
}

func TestDiffInPipeline(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))
		c.HSet(context.Background(), "user:1", "plan", "free")
		if _, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.HSet(ctx, "user:1", "plan", "pro")
			p.Set(ctx, "user:2", "x", 0)
			return nil
		}); err != nil {
			panic(err)
		}
		evs := rec.Events()
		if d := evs[0].Mutations[0].Diff; len(d) != 1 || d[0].Field != "plan" || d[0].Old != "free" || d[0].New != "pro" {
			t.Errorf("hash: %+v", d)
		}
		if d := evs[1].Mutations[0].Diff; len(d) != 1 || d[0].Field != "value" || d[0].Old != nil || d[0].New != "x" {
			t.Errorf("string: %+v", d)
		}
	})
}
