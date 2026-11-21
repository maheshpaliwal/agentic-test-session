package tredisv8

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A token as a session store keys its entries: long, mixed case, with digits.
const (
	leakMark  = "S3CR3T"
	leakToken = "Ab9" + leakMark + "xYz0123456789"
)

// since returns the events recorded after the first n, as JSON, and fails the
// test when one of them holds the marker.
func since(t *testing.T, rec *core.MemRecorder, n int, what string) string {
	t.Helper()
	b, err := json.Marshal(rec.Events()[n:])
	if err != nil {
		t.Fatalf("%s: the events are not JSON: %v", what, err)
	}
	if strings.Contains(string(b), leakMark) {
		t.Errorf("%s records the secret: %s", what, b)
	}
	return string(b)
}

// Redis and Lua quote a prefix of an argument in their errors, and a script
// can put any part of one into the error it raises.
func TestErrorOfACommandWithMaskedArgumentsIsWithheld(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		long := "session:Zq" + leakToken + strings.Repeat("Qz", 80)
		for _, tc := range []struct {
			what string
			run  func()
			want string // what is kept of the error
		}{
			{"unknown command, key cut by the server", func() { c.Do(ctx, "foo.bar", long, "x") }, "ERR unknown command "},
			{"unknown command", func() { c.Do(ctx, "foo.bar", "session:"+leakToken, "v") }, "ERR unknown command "},
			{"script error with a prefix of KEYS[1]", func() {
				c.Eval(ctx, "return redis.error_reply('bad '..string.sub(KEYS[1],1,20))", []string{"session:E" + leakToken})
			}, ""},
			{"script error with ARGV in upper case", func() {
				c.Eval(ctx, "return redis.error_reply(string.upper(ARGV[1]))", []string{"password:bob"}, "abc"+strings.ToLower(leakMark))
			}, ""},
			{"script that raises ARGV", func() { c.Eval(ctx, "error(ARGV[1])", nil, "session:"+leakToken) }, ""},
			{"wrong type", func() { c.Set(ctx, "session:"+leakToken, "v", 0); c.LPush(ctx, "session:"+leakToken, "x") },
				"WRONGTYPE Operation against a key holding the wrong kind of value "},
		} {
			n := len(rec.Events())
			tc.run()
			got := strings.ToLower(since(t, rec, n, tc.what))
			if strings.Contains(got, strings.ToLower(leakMark)) {
				t.Errorf("%s records the secret in another case: %s", tc.what, got)
			}
			evs := rec.Events()
			e := evs[len(evs)-1].Err
			if !strings.HasSuffix(e, "[text withheld: the command has masked arguments]") || !strings.HasPrefix(e, tc.want) {
				t.Errorf("%s: error %q, want it to start with %q", tc.what, e, tc.want)
			}
		}

		// Nothing is masked: the text is the server's.
		c.Set(ctx, "user:1", "v", 0)
		err := c.LPush(ctx, "user:1", "x").Err()
		evs := rec.Events()
		if e := evs[len(evs)-1].Err; err == nil || e != err.Error() {
			t.Errorf("recorded %q, the command failed with %v", e, err)
		}
	})
}

func TestKeysOutsideTheCommandTableAreMasked(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		key := "session:" + leakToken
		for what, run := range map[string]func(){
			"XREAD STREAMS key":      func() { c.Do(ctx, "xread", "count", "1", "streams", key, "0") },
			"XREADGROUP STREAMS key": func() { c.Do(ctx, "xreadgroup", "group", "g", "c", "streams", key, ">") },
			"SCAN MATCH pattern":     func() { c.Scan(ctx, 0, "session:"+leakToken[:12]+"*", 10) },
			"KEYS pattern":           func() { c.Keys(ctx, "session:"+leakToken[:12]+"*") },
			"SORT BY and GET":        func() { c.Do(ctx, "sort", "user:1:list", "by", key+":*", "get", key+":*") },
			"a key as ARGV":          func() { c.Eval(ctx, "return 1", nil, key) },
			"a key as ARGV, EVALSHA": func() { c.EvalSha(ctx, "ffffffffffffffffffffffffffffffffffffffff", nil, key) },
		} {
			n := len(rec.Events())
			run()
			if got := since(t, rec, n, what); !strings.Contains(got, `session:#`) {
				t.Errorf("%s: the name is recorded with its secret part hashed: %s", what, got)
			}
		}
	})
}

func TestRepliesAreMaskedOnEveryPath(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		bg := context.Background()
		key := "session:" + leakToken
		c.Set(bg, key, "v", 0)
		c.SAdd(bg, "user:1:sessions", key)
		c.HSet(bg, "user:1", "name", "ann", "token", leakMark)
		c.RPush(bg, "user:1:log", "token", leakMark)
		for what, run := range map[string]func(){
			"KEYS":                      func() { c.Keys(ctx, "*") },
			"KEYS through Do":           func() { c.Do(ctx, "keys", "session:*") },
			"SCAN through Do":           func() { c.Do(ctx, "scan", "0", "match", "session:*", "count", "100") },
			"RANDOMKEY through Do":      func() { c.Do(ctx, "randomkey") },
			"a script returning a key":  func() { c.Eval(ctx, "return KEYS[1]", []string{key}) },
			"a script returning a list": func() { c.Eval(ctx, "return {{KEYS[1]}, ARGV[1]}", []string{key}, key) },
			"a member that is a key":    func() { c.SMembers(ctx, "user:1:sessions") },
			"HGETALL through Do":        func() { c.Do(ctx, "hgetall", "user:1") },
			"HSCAN through Do":          func() { c.Do(ctx, "hscan", "user:1", "0") },
			"HVALS":                     func() { c.HVals(ctx, "user:1") },
			"HVALS through Do":          func() { c.Do(ctx, "hvals", "user:1") },
			"a name and its value":      func() { c.LRange(ctx, "user:1:log", 0, -1) },
		} {
			n := len(rec.Events())
			run()
			since(t, rec, n, what)
		}
		// The application still gets the real reply.
		if keys, err := c.Keys(ctx, "session:*").Result(); err != nil || len(keys) != 1 || keys[0] != key {
			t.Errorf("the application got %q, %v", keys, err)
		}
		// HVALS has no field names to tell which values are sensitive.
		c.HVals(ctx, "user:1")
		evs := rec.Events()
		if got := string(evs[len(evs)-1].Resp.Inline); got != `["[REDACTED]","[REDACTED]"]` {
			t.Errorf("HVALS recorded as %s", got)
		}
		// HGETALL has them, on every path.
		c.Do(ctx, "hgetall", "user:1")
		evs = rec.Events()
		if got := string(evs[len(evs)-1].Resp.Inline); !strings.Contains(got, `"ann"`) || !strings.Contains(got, `"[REDACTED]"`) {
			t.Errorf("HGETALL through Do recorded as %s", got)
		}
	})
}

// The reply of SET ... GET is the old value, although it arrives as a status.
func TestSetGetReplyIsAValue(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		bg := context.Background()
		c.Set(bg, "password:alice", leakMark, 0)
		c.Set(bg, "user:9", `{"password":"`+leakMark+`","name":"ann"}`, 0)
		if old, err := c.SetArgs(ctx, "password:alice", "new", redis.SetArgs{Get: true}).Result(); err != nil || old != leakMark {
			t.Fatalf("the application got %q, %v", old, err)
		}
		c.SetArgs(ctx, "user:9", "x", redis.SetArgs{Get: true})
		c.Set(ctx, "user:9", "y", 0)
		got := since(t, rec, 0, "SET ... GET")
		evs := rec.Events()
		if r := string(evs[0].Resp.Inline); r != `"[REDACTED]"` {
			t.Errorf("old value of a sensitive key recorded as %s", r)
		}
		if r := string(evs[1].Resp.Inline); !strings.Contains(r, "ann") || !strings.Contains(r, "[REDACTED]") {
			t.Errorf("old JSON value recorded as %s", r)
		}
		if r := string(evs[2].Resp.Inline); r != `"OK"` {
			t.Errorf("a plain SET answers %s: %s", r, got)
		}
	})
}

// CONFIG GET and ACL answer with credentials. miniredis has neither command.
func TestServerCredentialsAreMasked(t *testing.T) {
	realServer(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		bg := context.Background()
		if err := c.ConfigSet(bg, "masterauth", leakMark).Err(); err != nil {
			t.Skip("CONFIG SET is not allowed here:", err)
		}
		t.Cleanup(func() { c.ConfigSet(bg, "masterauth", "") })
		c.ConfigGet(ctx, "masterauth")
		c.Do(ctx, "config", "get", "masterauth")
		c.Do(ctx, "config", "get", "*")
		if got := since(t, rec, 0, "CONFIG GET"); !strings.Contains(got, "masterauth") {
			t.Errorf("the parameter name is recorded: %s", got)
		}
		c.Do(bg, "acl", "setuser", "tsession-leak-test", "on", ">"+leakMark, "~*", "+@all")
		t.Cleanup(func() { c.Do(bg, "acl", "deluser", "tsession-leak-test") })
		n := len(rec.Events())
		c.Do(ctx, "acl", "getuser", "tsession-leak-test")
		c.Do(ctx, "acl", "list")
		c.Do(ctx, "acl", "setuser", "tsession-leak-test", ">"+leakMark+"2", "bogus"+leakMark)
		since(t, rec, n, "ACL")
		if r := string(rec.Events()[n].Resp.Inline); r != `"[REDACTED]"` {
			t.Errorf("ACL GETUSER recorded as %s", r)
		}
	})
}

func TestSecretNamesInAnyCase(t *testing.T) {
	c, rec, ctx := setup(t)
	for _, name := range []string{"ApiKey", "Session", "SESSION", "tokens", "{session}"} {
		n := len(rec.Events())
		c.Set(ctx, name+":"+leakToken, "v", 0)
		since(t, rec, n, name)
		if got := rec.Events()[n].Name; !strings.HasPrefix(got, "SET "+name+":#") && !strings.HasPrefix(got, "SET "+name+":{#") {
			t.Errorf("%s:<token> is recorded as %q: the name stays, the token goes", name, got)
		}
	}
	for _, key := range []string{"session/" + leakToken, "session_" + leakToken} {
		n := len(rec.Events())
		c.Set(ctx, key, "v", 0)
		since(t, rec, n, key)
		if e := rec.Events()[n]; !strings.HasPrefix(e.Name, "SET "+key[:8]+"#") || e.Mutations[0].Entity != key[:8]+"*" {
			t.Errorf("%s recorded as %q, entity %q", key, e.Name, e.Mutations[0].Entity)
		}
	}
}

func TestMaskKeysGlobs(t *testing.T) {
	c, rec, ctx := setup(t, WithMaskKeys("reset:*"), WithRedactKeys("blob:*"))
	c.Set(ctx, "reset:Ab3/dE9+"+leakMark+"/x==", "v", 0) // base64 holds '/'
	c.Set(ctx, "blob:a/b", leakMark, 0)
	since(t, rec, 0, "a key with a '/'")
	if got := rec.Events()[0].Name; !strings.HasPrefix(got, "SET reset:#") || strings.Contains(got, "/") {
		t.Errorf("recorded as %q", got)
	}
	for _, opt := range []func(){
		func() { WithMaskKeys("ok:*", "reset:[a-") },
		func() { WithMaskKeys(`reset:\`) },
		func() { WithRedactKeys("blob:[") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("a malformed glob is accepted: it would match nothing")
				}
			}()
			opt()
		}()
	}
}

// A hash field name is masked like a key name, and reads the same in the
// arguments, the images, the diff and the replies.
func TestHashFieldNamesAreMasked(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*"))
		field := "token:" + leakToken
		c.HSet(ctx, "user:3", field, "1", "name", "ann")
		c.HSet(ctx, "user:3", field, "2")
		c.HGetAll(ctx, "user:3")
		c.HKeys(ctx, "user:3")
		c.HDel(ctx, "user:3", field)
		since(t, rec, 0, "a secret in a field name")
		evs := rec.Events()
		m := evs[1].Mutations[0]
		if len(m.Diff) != 1 || !strings.HasPrefix(m.Diff[0].Field, "token:#") {
			t.Fatalf("diff %+v", m.Diff)
		}
		shown := `"` + m.Diff[0].Field + `"`
		for what, got := range map[string]string{
			"arguments": string(evs[1].Req.Inline), "before-image": string(m.Before.Inline),
			"after-image": string(m.After.Inline), "HGETALL": string(evs[2].Resp.Inline),
			"HKEYS": string(evs[3].Resp.Inline), "HDEL": string(evs[4].Req.Inline),
		} {
			if !strings.Contains(got, shown) {
				t.Errorf("the field is %s in the diff and not in the %s: %s", shown, what, got)
			}
		}
	})
}

// A command with a sensitive key can move its value to a key that is not.
func TestValueMovedFromASensitiveKeyIsNotRead(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("*"))
		bg := context.Background()
		c.Set(bg, "auth:bob", leakMark, 0)
		c.Rename(ctx, "auth:bob", "plain")
		since(t, rec, 0, "RENAME to a plain key")
		ms := rec.Events()[0].Mutations
		if len(ms) != 2 || string(ms[1].After.Inline) != `"[REDACTED]"` || len(ms[0].After.Inline) != 0 {
			t.Errorf("%+v", ms)
		}
	})
}
