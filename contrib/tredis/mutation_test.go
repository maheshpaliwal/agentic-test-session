package tredis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// mutatedKeys returns the keys of the mutations of the last recorded event.
func mutatedKeys(t *testing.T, rec *core.MemRecorder) (core.Event, []string) {
	t.Helper()
	evs := rec.Events()
	if len(evs) == 0 {
		panic("no events")
	}
	e := evs[len(evs)-1]
	var keys []string
	for _, m := range e.Mutations {
		keys = append(keys, m.PK["key"].(string))
	}
	return e, keys
}

func wantKeys(t *testing.T, rec *core.MemRecorder, kind core.Kind, want ...string) {
	t.Helper()
	e, got := mutatedKeys(t, rec)
	if e.Kind != kind || len(got) != len(want) {
		panic(fmt.Sprintf("%s: kind=%s mutated=%v, want kind=%s mutated=%v", e.Name, e.Kind, got, kind, want))
	}
	for i := range want {
		if got[i] != want[i] {
			panic(fmt.Sprintf("%s: mutated=%v, want %v", e.Name, got, want))
		}
	}
}

func TestStoreCommandsMutateOnlyDestination(t *testing.T) {
	c, rec, ctx := setup(t)
	bg := context.Background() // fixtures are not traced
	c.SAdd(bg, "a", "1", "2")
	c.SAdd(bg, "b", "2")
	c.ZAdd(bg, "za", redis.Z{Score: 1, Member: "m"})
	c.RPush(bg, "src", "x")
	c.Set(bg, "from", "v", 0)

	c.SInterStore(ctx, "dest", "a", "b")
	wantKeys(t, rec, core.KindCacheWrite, "dest")
	c.ZUnionStore(ctx, "zdest", &redis.ZStore{Keys: []string{"za"}})
	wantKeys(t, rec, core.KindCacheWrite, "zdest")
	if e, _ := mutatedKeys(t, rec); e.Name != "ZUNIONSTORE zdest" || !e.Mutations[0].After.IsZero() {
		panic(fmt.Sprintf("%+v", e))
	}
	c.Copy(ctx, "from", "to", 0, false)
	wantKeys(t, rec, core.KindCacheWrite, "to")
	c.BRPopLPush(ctx, "src", "dst", time.Second)
	wantKeys(t, rec, core.KindCacheWrite, "src", "dst")
}

func TestBlockingPopMutatesOnlyPoppedKey(t *testing.T) {
	c, rec, ctx := setup(t)
	c.RPush(context.Background(), "q2", "job")
	if v, err := c.BLPop(ctx, time.Second, "q1", "q2").Result(); err != nil || v[0] != "q2" {
		panic(fmt.Sprint(v, err))
	}
	wantKeys(t, rec, core.KindCacheWrite, "q2")
}

func TestSortWithoutStoreIsRead(t *testing.T) {
	c, rec, ctx := setup(t)
	c.Sort(ctx, "l", &redis.Sort{Alpha: true}) // miniredis has no SORT; the span is still classified
	if e := rec.Events()[0]; e.Kind != core.KindCacheRead || len(e.Mutations) != 0 {
		panic(fmt.Sprintf("%+v", e))
	}
}

func TestNoOpWritesYieldNoMutation(t *testing.T) {
	c, rec, ctx := setup(t)
	bg := context.Background()
	c.Set(bg, "have", "v", 0)
	c.Set(bg, "have2", "v", 0)

	noop := map[string]func(){
		"SET NX on existing key":   func() { c.SetNX(ctx, "have", "x", 0) },
		"SET XX on missing key":    func() { c.SetXX(ctx, "nope", "x", 0) },
		"SETNX on existing key":    func() { c.Do(ctx, "SETNX", "have", "x") },
		"MSETNX on existing key":   func() { c.MSetNX(ctx, "have", "x", "new", "y") },
		"RENAMENX onto existing":   func() { c.RenameNX(ctx, "have", "have2") },
		"DEL of missing key":       func() { c.Del(ctx, "nope") },
		"UNLINK of missing key":    func() { c.Unlink(ctx, "nope") },
		"HDEL of missing field":    func() { c.HDel(ctx, "nope", "f") },
		"SREM of missing member":   func() { c.SRem(ctx, "nope", "m") },
		"ZREM of missing member":   func() { c.ZRem(ctx, "nope", "m") },
		"EXPIRE of missing key":    func() { c.Expire(ctx, "nope", time.Minute) },
		"PEXPIRE of missing key":   func() { c.PExpire(ctx, "nope", time.Minute) },
		"EXPIREAT of missing key":  func() { c.ExpireAt(ctx, "nope", time.Now().Add(time.Minute)) },
		"PERSIST of missing key":   func() { c.Persist(ctx, "nope") },
		"PERSIST of key sans TTL":  func() { c.Persist(ctx, "have") },
		"HDEL of missing hash key": func() { c.HDel(ctx, "have-not", "f") },
	}
	for name, run := range noop {
		n := len(rec.Events())
		run()
		evs := rec.Events()
		if len(evs) != n+1 {
			panic(fmt.Sprintf("%s: the command must still be traced", name))
		}
		if e := evs[n]; e.Kind != core.KindCacheWrite || e.Err != "" || len(e.Mutations) != 0 {
			t.Errorf("%s: kind=%s err=%q resp=%s mutations=%d, want a write span without mutation",
				name, e.Kind, e.Err, e.Resp.Inline, len(e.Mutations))
		}
	}

	c.SetNX(ctx, "fresh", "x", 0)
	wantKeys(t, rec, core.KindCacheWrite, "fresh")
	c.Expire(ctx, "fresh", time.Minute)
	wantKeys(t, rec, core.KindCacheWrite, "fresh")
	c.Del(ctx, "fresh", "nope")
	wantKeys(t, rec, core.KindCacheWrite, "fresh", "nope") // 1 of 2 deleted: which one is unknown
}

func TestEvalMutationHasNoArgImage(t *testing.T) {
	c, rec, ctx := setup(t)
	if err := c.Eval(ctx, "return 1", []string{"k1", "k2"}, "argv1").Err(); err != nil {
		panic(err)
	}
	wantKeys(t, rec, core.KindCacheWrite, "k1", "k2")
	for _, m := range rec.Events()[0].Mutations {
		if !m.After.IsZero() {
			panic(fmt.Sprintf("other keys / ARGV recorded as the value of %v: %s", m.PK, m.After.Inline))
		}
	}
}
