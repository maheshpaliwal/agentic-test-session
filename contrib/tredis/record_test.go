package tredis

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Two members that redaction makes read alike are still two members.
func TestMembersRecordedAlikeStayApart(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))

		c.Do(ctx, "zadd", "user:1:rank", "1", "token=Zq1x")
		c.Do(ctx, "zadd", "user:1:rank", "2", "token=Zq2y")
		wantDiff(t, rec, "second member added", `token=[REDACTED]: - -> 2`)
		evs := rec.Events()
		m := evs[len(evs)-1].Mutations[0]
		var before, after map[string]any
		if json.Unmarshal(m.Before.Inline, &before) != nil || json.Unmarshal(m.After.Inline, &after) != nil ||
			len(before) != 1 || len(after) != 2 {
			t.Errorf("images %s -> %s", m.Before.Inline, m.After.Inline)
		}
		c.Do(ctx, "zadd", "user:1:rank", "5", "token=Zq2y")
		wantDiff(t, rec, "its score changed", `token=[REDACTED]: 2 -> 5`)
		c.ZRem(ctx, "user:1:rank", "token=Zq1x")
		wantDiff(t, rec, "first member removed", `token=[REDACTED]: 1 -> -`)

		c.SAdd(ctx, "user:1:tags", "token=Zq1x")
		c.SAdd(ctx, "user:1:tags", "token=Zq2y")
		wantDiff(t, rec, "second set member added", `added: - -> ["token=[REDACTED]"]`)
		c.SRem(ctx, "user:1:tags", "token=Zq1x")
		wantDiff(t, rec, "first set member removed", `removed: ["token=[REDACTED]"] -> -`)

		if b, _ := json.Marshal(rec.Events()); strings.Contains(string(b), "Zq1x") || strings.Contains(string(b), "Zq2y") {
			t.Errorf("a member is recorded as stored: %s", b)
		}
	})
}

// JSON has no number for an infinite score.
func TestInfiniteScoreIsRecorded(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))
		c.Do(ctx, "zadd", "user:1:rank", "+inf", "top", "-inf", "bottom", "1", "mid")
		wantDiff(t, rec, "created", `bottom: - -> "-inf"`, `mid: - -> 1`, `top: - -> "inf"`)
		c.Do(ctx, "zadd", "user:1:rank", "2", "top")
		wantDiff(t, rec, "changed", `top: "inf" -> 2`)
		for _, e := range rec.Events() {
			if _, err := json.Marshal(e); err != nil {
				t.Errorf("%s is not JSON: %v", e.Name, err)
			}
			if after := string(e.Mutations[0].After.Inline); !strings.Contains(after, `"bottom":"-inf"`) {
				t.Errorf("after-image %s", after)
			}
		}
	})
}

// The image patterns are for the keys of the application, not for their
// recorded form.
func TestImagePatternMatchesTheRealKey(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("session:tok-*"))
		key := "session:tok-" + leakToken
		c.Set(ctx, key, "v1", 0)
		c.Set(ctx, key, "v2", 0)
		since(t, rec, 0, "an imaged key with a secret name")
		m := rec.Events()[1].Mutations[0]
		if string(m.Before.Inline) != `"v1"` || string(m.After.Inline) != `"v2"` || len(m.Diff) != 1 {
			t.Errorf("%s -> %s, diff %+v", m.Before.Inline, m.After.Inline, m.Diff)
		}
	})
}

// A sensitive key is asked whether it exists, never for its value.
func TestImageOfASensitiveKeyTellsWhetherItExists(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("*"))
		c.Set(ctx, "password:alice", "hunter2", 0)
		c.Set(ctx, "password:alice", "hunter3", 0)
		c.Del(ctx, "password:alice")
		evs := rec.Events()
		for i, want := range [][2]string{{"", `"[REDACTED]"`}, {`"[REDACTED]"`, `"[REDACTED]"`}, {`"[REDACTED]"`, ""}} {
			m := evs[i].Mutations[0]
			if string(m.Before.Inline) != want[0] || string(m.After.Inline) != want[1] || len(m.Diff) != 0 {
				t.Errorf("%s: %q -> %q, diff %+v", evs[i].Name, m.Before.Inline, m.After.Inline, m.Diff)
			}
		}
		if evs[1].Attrs["diff_skipped"] != "sensitive_key" {
			t.Errorf("attributes %v", evs[1].Attrs)
		}
	})
}

// GETSET and SET ... GET answer nil for a key that did not exist, and wrote it.
func TestWriteThatAnswersNilIsAMutation(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		if err := c.GetSet(ctx, "user:1:a", "v").Err(); err != redis.Nil {
			t.Fatal(err)
		}
		c.Do(ctx, "set", "user:1:b", "v", "GET")
		c.Do(ctx, "set", "user:1:c", "v", "XX", "GET") // no such key: nothing is written
		c.GetDel(ctx, "user:1:c")
		evs := rec.Events()
		for i, want := range []int{1, 1, 0, 0} {
			if len(evs[i].Mutations) != want || evs[i].Err != "" {
				t.Errorf("%s: %d mutations, error %q", evs[i].Name, len(evs[i].Mutations), evs[i].Err)
			}
		}
	})
}

// A command sent between a MULTI and an EXEC the application issued itself
// has changed nothing when it is answered.
func TestCommandQueuedByARawMultiIsNotAMutation(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		_, rec, ctx := setupAt(t, addr)
		one := Wrap(redis.NewClient(&redis.Options{Addr: addr, DB: testDB, PoolSize: 1})) // one connection: MULTI is per connection
		defer one.Close()
		if err := one.Do(ctx, "multi").Err(); err != nil {
			t.Fatal(err)
		}
		if v, err := one.Do(ctx, "set", "user:20", "q").Result(); err != nil || v != "QUEUED" {
			t.Fatal(v, err)
		}
		if err := one.Do(ctx, "discard").Err(); err != nil {
			t.Fatal(err)
		}
		if n, _ := one.Exists(context.Background(), "user:20").Result(); n != 0 {
			t.Fatal("the key was written")
		}
		for _, e := range rec.Events() {
			if e.Name != "SET user:20" {
				continue
			}
			if len(e.Mutations) != 0 || e.Attrs["queued"] != "true" {
				t.Errorf("mutations %+v, attributes %v", e.Mutations, e.Attrs)
			}
			return
		}
		t.Error("the SET is not recorded")
	})
}
