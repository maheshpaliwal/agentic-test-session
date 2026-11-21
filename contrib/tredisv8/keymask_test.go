package tredisv8

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	tokenKey = "session:tok_SECRET123"
	jwtKey   = "auth:token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiI0MiJ9.c2lnbmF0dXJl"
	sidKey   = "sess:Abc123Def456" // a secret name, a value that is not sensitive: its images are read
)

var hashedKey = regexp.MustCompile(`^(session|auth:token|sess):#[0-9a-f]{12}$`)

func TestSecretKeyNamesAreMasked(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("sess:*"))
		bg := context.Background()
		c.Set(bg, sidKey, "v1", 0)
		c.RPush(bg, sidKey+":jobs", "job")

		c.Set(ctx, tokenKey, "v", 0)
		c.Get(ctx, tokenKey)
		c.HSet(ctx, jwtKey, "uid", "42")
		c.Set(ctx, sidKey, "v2", 0)
		c.Rename(ctx, tokenKey, tokenKey+"x")
		if _, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.Del(ctx, tokenKey+"x")
			p.Get(ctx, sidKey)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if keys, err := c.Keys(ctx, "sess:*").Result(); err != nil || len(keys) != 2 {
			t.Fatal(keys, err)
		}
		if v, err := c.BLPop(ctx, time.Second, sidKey+":jobs").Result(); err != nil || v[0] != sidKey+":jobs" {
			t.Fatal(v, err) // the application still gets the real key
		}
		c.Do(ctx, "BF.ADD", tokenKey, "item") // a command the tracer does not know
		if err := c.Watch(ctx, func(*redis.Tx) error { return nil }, tokenKey); err != nil {
			t.Fatal(err)
		}

		evs := rec.Events()
		for _, e := range evs {
			b, _ := json.Marshal(e)
			for _, secret := range []string{"tok_SECRET123", "eyJ", "c2lnbmF0dXJl", "Abc123Def456"} {
				if strings.Contains(string(b), secret) {
					t.Errorf("%q is recorded by %q: err=%q req=%s resp=%s", secret, e.Name, e.Err, e.Req.Inline, e.Resp.Inline)
				}
			}
		}
		set, get := evs[0], evs[1]
		key, _ := set.Mutations[0].PK["key"].(string)
		if !hashedKey.MatchString(key) || set.Name != "SET "+key || get.Name != "GET "+key {
			t.Errorf("key %q, names %q and %q", key, set.Name, get.Name)
		}
		if e := set.Mutations[0].Entity; e != "session:*" {
			t.Errorf("entity %q", e)
		}
		if !strings.Contains(string(set.Req.Inline), `"`+key+`"`) {
			t.Errorf("arguments %s", set.Req.Inline)
		}
		if m := evs[2].Mutations[0]; m.Entity != "auth:token:*" || !hashedKey.MatchString(m.PK["key"].(string)) {
			t.Errorf("%s %v", m.Entity, m.PK)
		}
		// The images are read under the real key.
		if m := evs[3].Mutations[0]; string(m.Before.Inline) != `"v1"` || string(m.After.Inline) != `"v2"` || len(m.Diff) != 1 {
			t.Errorf("%s -> %s, diff %+v", m.Before.Inline, m.After.Inline, m.Diff)
		}
		if ms := evs[4].Mutations; len(ms) != 2 || ms[0].PK["key"] != key || ms[1].PK["key"] == key {
			t.Errorf("rename: %+v", ms)
		}
	})
}

func TestKeyMaskOptions(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithKeyMask(KeyMaskFlat), WithMaskKeys("cart:*:items"))
		c.Set(ctx, tokenKey, "v", 0)
		c.SAdd(ctx, "cart:alice:items", "sku")
		c.SAdd(ctx, "cart:alice", "sku")
		evs := rec.Events()
		if m := evs[0].Mutations[0]; evs[0].Name != "SET session:[REDACTED]" || m.Entity != "session:*" || m.PK["key"] != "session:[REDACTED]" {
			t.Errorf("%q %+v", evs[0].Name, m)
		}
		if m := evs[1].Mutations[0]; m.Entity != "cart:*:items" || m.PK["key"] != "cart:[REDACTED]:items" {
			t.Errorf("%+v", m)
		}
		if m := evs[2].Mutations[0]; m.PK["key"] != "cart:alice" {
			t.Errorf("%+v", m)
		}
	})
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithKeyMask(KeyMaskOff), WithMaskKeys("cart:*"))
		c.Set(ctx, tokenKey, "v", 0)
		c.SAdd(ctx, "cart:alice", "sku")
		evs := rec.Events()
		if evs[0].Name != "SET "+tokenKey || evs[1].Mutations[0].PK["key"] != "cart:alice" {
			t.Errorf("masking is off: %q %v", evs[0].Name, evs[1].Mutations[0].PK)
		}
	})
}
