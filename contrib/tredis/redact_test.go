package tredis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func allText(rec *core.MemRecorder) string {
	var sb strings.Builder
	for _, e := range rec.Events() {
		sb.WriteString(string(e.Req.Inline) + "|" + string(e.Resp.Inline) + "|" + e.Name + "|")
		for _, m := range e.Mutations {
			sb.WriteString(string(m.Before.Inline) + "|" + string(m.After.Inline) + "|")
		}
	}
	return sb.String()
}

func TestSensitiveKeyValuesMasked(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("*"))
	c.Set(ctx, "user:1:password", "hunter2-secret", 0)
	c.SetEx(ctx, "auth:token:9", "tok-abc-123", time.Minute)
	c.HSet(ctx, "user:2", "name", "bob", "api_key", "key-xyz-789")
	c.HSet(ctx, "user:2", map[string]any{"password": "pw-qqq"})
	c.MSet(ctx, "plain", "visible", "x:secret", "sec-val-1")
	c.LPush(ctx, "session_token:l", "lp-val-1")
	c.ZAdd(ctx, "secret:z", redis.Z{Score: 1, Member: "zmember-sec"})
	c.Set(ctx, "doc", `{"name":"bob","password":"jsonpw-1"}`, 0)
	c.Get(ctx, "user:1:password")
	c.HGet(ctx, "user:2", "api_key")
	c.HGet(ctx, "user:2", "name")
	c.HGetAll(ctx, "user:2")
	c.Get(ctx, "doc")
	c.MGet(ctx, "plain", "x:secret")
	c.Do(ctx, "AUTH", "u", "authpw-1")
	c.Do(ctx, "CONFIG", "SET", "requirepass", "cfgpw-1")
	txt := allText(rec)
	for _, bad := range []string{"hunter2-secret", "tok-abc-123", "key-xyz-789", "pw-qqq", "sec-val-1", "lp-val-1", "zmember-sec", "jsonpw-1", "authpw-1", "cfgpw-1"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q in %s", bad, txt)
		}
	}
	for _, good := range []string{"visible", "bob"} {
		if !strings.Contains(txt, good) {
			t.Errorf("over-redacted, missing %q", good)
		}
	}
}

func TestValueCaptureOff(t *testing.T) {
	c, rec, ctx := setup(t, WithValueCapture(false), WithBeforeImage("*"))
	c.Set(ctx, "k", "plain-val", 0)
	c.Get(ctx, "k")
	if strings.Contains(allText(rec), "plain-val") {
		panic(allText(rec))
	}
	if !strings.Contains(allText(rec), "SET k") {
		panic("name must remain")
	}
}

func TestRedactKeysGlob(t *testing.T) {
	c, rec, ctx := setup(t, WithRedactKeys("pii:*"))
	c.Set(ctx, "pii:1", "john-doe", 0)
	c.Get(ctx, "pii:1")
	if strings.Contains(allText(rec), "john-doe") {
		panic(allText(rec))
	}
}

func TestIgnoreCtxKeys(t *testing.T) {
	c, rec, ctx := setup(t)
	c.SetEx(ctx, "ts:ctx:abc", "v", time.Minute)
	c.Get(ctx, "ts:ctx:abc")
	c.Pipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, "ts:ctx:z", "1", 0); return nil })
	if n := len(rec.Events()); n != 0 {
		panic(fmt.Sprintf("ignored keys traced: %d", n))
	}
	c.Set(ctx, "ts:other", "v", 0)
	if len(rec.Events()) != 1 {
		panic("non-matching key must be traced")
	}
}

func TestPipelineImageCap(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("k:*"), WithPipelineImageCap(3))
	c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < 6; i++ {
			p.Set(ctx, "k:"+string(rune('a'+i)), "v", 0)
		}
		return nil
	})
	skipped := 0
	for _, e := range rec.Events() {
		if e.Attrs["image_skipped"] == "cap" {
			skipped++
		}
	}
	if skipped != 3 {
		panic(fmt.Sprintf("skipped=%d", skipped))
	}
}

func TestImageTimeout(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("k"), WithImageTimeout(time.Nanosecond))
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		panic(err)
	}
	if rec.Events()[0].Attrs["image_skipped"] != "error" {
		panic(fmt.Sprintf("%+v", rec.Events()[0]))
	}
}

// Real Redis, gated: TSESSION_TEST_REDIS_ADDR=127.0.0.1:56381
func TestRealRedis(t *testing.T) {
	addr := os.Getenv("TSESSION_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TSESSION_TEST_REDIS_ADDR not set")
	}
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	raw := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})
	c := Wrap(raw, WithBeforeImage("user:*"))
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	raw.FlushDB(context.Background())
	t.Cleanup(func() { raw.FlushDB(context.Background()); raw.Close() })

	c.HSet(ctx, "user:1", "name", "a", "password", "realpw-1")
	c.HSet(ctx, "user:1", "name", "b")
	c.Set(ctx, "user:1:password", "realpw-2", 0)
	c.SetEx(ctx, "ts:ctx:t1", "x", time.Minute)
	c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < 30; i++ {
			p.HSet(ctx, "user:p"+string(rune('a'+i%26))+string(rune('a'+i/26)), "f", "v")
		}
		return nil
	})
	c.Get(ctx, "user:1:password")
	c.HGetAll(ctx, "user:1")
	c.Do(ctx, "AUTH", "nouser", "realpw-3") // errors on a no-auth server; must still be masked
	txt := allText(rec)
	for _, bad := range []string{"realpw-1", "realpw-2", "realpw-3", "ts:ctx"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q: %s", bad, txt)
		}
	}
	var skipped, mut int
	for _, e := range rec.Events() {
		if e.Attrs["image_skipped"] == "cap" {
			skipped++
		}
		mut += len(e.Mutations)
	}
	if skipped != 10 {
		t.Errorf("image_skipped=%d want 10", skipped)
	}
	ev := rec.Events()
	if m := ev[1].Mutations[0]; !strings.Contains(string(m.Before.Inline), `"name":"a"`) || !strings.Contains(string(m.After.Inline), `"name":"b"`) {
		t.Errorf("real before/after: %s -> %s", m.Before.Inline, m.After.Inline)
	}
}

func TestMemberArgsOfSensitiveKeysMasked(t *testing.T) {
	c, rec, ctx := setup(t)
	c.SIsMember(ctx, "secret:set", "member-sec-1")
	c.ZScore(ctx, "user:1:password", "member-sec-2")
	c.LPos(ctx, "auth:token:list", "member-sec-3", redis.LPosArgs{})
	c.SMove(ctx, "secret:a", "secret:b", "member-sec-4")
	c.SIsMember(ctx, "plain:set", "visible-member")
	txt := allText(rec)
	for _, bad := range []string{"member-sec-1", "member-sec-2", "member-sec-3", "member-sec-4"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q in %s", bad, txt)
		}
	}
	if !strings.Contains(txt, "visible-member") || !strings.Contains(txt, "secret:set") {
		t.Errorf("over-redacted: %s", txt)
	}
}

func TestValueCaptureOffCoversReadsAndUnknownCommands(t *testing.T) {
	c, rec, ctx := setup(t, WithValueCapture(false))
	c.SIsMember(ctx, "s", "member-val-1")
	c.Do(ctx, "BF.ADD", "bf", "member-val-2")
	c.ZRem(ctx, "z", "member-val-3")
	txt := allText(rec)
	for _, bad := range []string{"member-val-1", "member-val-2", "member-val-3"} {
		if strings.Contains(txt, bad) {
			t.Errorf("capture off but recorded %q in %s", bad, txt)
		}
	}
	if !strings.Contains(txt, "SISMEMBER s") {
		t.Errorf("keys must remain: %s", txt)
	}
}

func TestArgsScrubbedInRequest(t *testing.T) {
	c, rec, ctx := setup(t)
	c.Do(ctx, "ECHO", "Bearer abcdefghijklmnop")
	c.ZRem(ctx, "plain", `{"password":"member-sec-5"}`)
	c.SIsMember(ctx, "plain:set", `{"token":"member-sec-6"}`)
	txt := allText(rec)
	for _, bad := range []string{"abcdefghijklmnop", "member-sec-5", "member-sec-6"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q in %s", bad, txt)
		}
	}
}
