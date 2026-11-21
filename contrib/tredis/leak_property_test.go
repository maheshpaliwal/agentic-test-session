package tredis

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/redisx"
)

// leakWindow is the length of the shortest piece of a secret that counts as
// recorded.
const leakWindow = 6

// The backstop for everything this package promises to keep out of a
// recording. Each case puts a fresh random secret where the rules say it is
// hidden (a key segment after a secret name, a hash field name, a value under
// a sensitive name, a pattern, the KEYS and ARGV of a script), runs commands
// from the whole command table and some the tracer does not know, and then
// looks for any 6 bytes of the secret, in either case, in the JSON of every
// recorded event.
//
// TSESSION_LEAK_CASES sets the number of cases per configuration (default
// 1000) and TSESSION_LEAK_SEED repeats a run.
func TestNoSecretInRecordedEvents(t *testing.T) {
	cases := 1000
	if s := os.Getenv("TSESSION_LEAK_CASES"); s != "" {
		cases, _ = strconv.Atoi(s)
	}
	seed := time.Now().UnixNano()
	if s := os.Getenv("TSESSION_LEAK_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	for _, cfg := range []struct {
		name string
		opts []Option
	}{
		{"images off", nil},
		{"images on", []Option{WithBeforeImage("*")}},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			servers(t, func(t *testing.T, addr string) {
				c, rec, ctx := setupAt(t, addr, cfg.opts...)
				p := &leakProbe{t: t, rec: rec, ctx: ctx, rng: rand.New(rand.NewSource(seed)), found: map[string]int{}}
				p.do = func(ctx context.Context, args ...any) { c.Do(ctx, args...) }
				typed := []func(ctx context.Context, key, field, value string){
					func(ctx context.Context, key, _, _ string) { c.Get(ctx, key) },
					func(ctx context.Context, key, _, _ string) { c.GetRange(ctx, key, 0, -1) },
					func(ctx context.Context, key, _, _ string) { c.HGetAll(ctx, key) },
					func(ctx context.Context, key, _, _ string) { c.HVals(ctx, key) },
					func(ctx context.Context, key, _, _ string) { c.HKeys(ctx, key) },
					func(ctx context.Context, key, field, _ string) { c.HGet(ctx, key, field) },
					func(ctx context.Context, key, field, _ string) { c.HMGet(ctx, key, "name", field) },
					func(ctx context.Context, key, _, _ string) { c.LRange(ctx, key, 0, -1) },
					func(ctx context.Context, key, _, _ string) { c.SMembers(ctx, key) },
					func(ctx context.Context, key, _, _ string) { c.MGet(ctx, "user:1", key) },
					func(ctx context.Context, key, _, _ string) { c.Keys(ctx, key) },
					func(ctx context.Context, _, _, _ string) { c.Keys(ctx, "*") },
					func(ctx context.Context, key, _, _ string) { c.Scan(ctx, 0, key, 100) },
					func(ctx context.Context, _, _, _ string) { c.RandomKey(ctx) },
					func(ctx context.Context, key, _, _ string) { c.Type(ctx, key) },
					func(ctx context.Context, key, _, _ string) { c.Del(ctx, key) },
					func(ctx context.Context, key, _, value string) { c.Set(ctx, key, value, 0) },
					func(ctx context.Context, key, _, value string) { c.GetSet(ctx, key, value) },
					func(ctx context.Context, key, field, value string) { c.HSet(ctx, key, field, value) },
					func(ctx context.Context, key, _, value string) { c.SAdd(ctx, key, value) },
					func(ctx context.Context, key, _, value string) { c.RPush(ctx, key, value) },
				}
				p.typed = func(ctx context.Context, key, field, value string) {
					typed[p.rng.Intn(len(typed))](ctx, key, field, value)
				}
				p.eval = func(ctx context.Context, script string, keys []string, argv ...any) {
					c.Eval(ctx, script, keys, argv...)
				}
				for i := range cases {
					if i%200 == 0 {
						c.FlushDB(context.Background())
					}
					p.runCase()
				}
				t.Logf("seed %d: %d cases, %d commands, %d events checked, %d cases recorded a secret",
					seed, cases, p.commands, p.events, p.leaked)
				kinds := make([]string, 0, len(p.found))
				for k := range p.found {
					kinds = append(kinds, k)
				}
				sort.Strings(kinds)
				for _, k := range kinds {
					t.Logf("  %4d  %s", p.found[k], k)
				}
			})
		})
	}
}

// leakProbe runs the cases of TestNoSecretInRecordedEvents against one client.
type leakProbe struct {
	t   *testing.T
	rec *core.MemRecorder
	ctx context.Context // inside the session
	rng *rand.Rand

	do    func(ctx context.Context, args ...any)
	typed func(ctx context.Context, key, field, value string) // a random typed command
	eval  func(ctx context.Context, script string, keys []string, argv ...any)

	commands, events, leaked int
	found                    map[string]int // what leaked, by kind of case and command
	what                     string         // the command being run, for the report
}

const (
	secretAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	notHexLetters  = "ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ"
)

// secret returns a random token of 21 to 33 characters. Every three
// characters are a digit, a letter that is no hex digit and any character: a
// window of six can then be neither part of a word of the fixed text of an
// event nor part of a hex id or hash, so a match is a leak and never chance.
// The second character is an upper-case letter: the default rules take a
// lower-case word for a name.
func (p *leakProbe) secret() string {
	n := 21 + 3*p.rng.Intn(5)
	if p.rng.Intn(12) == 0 {
		n = 1200 + 3*p.rng.Intn(100) // a token of more than a kilobyte, as a JWT with many claims is
	}
	b := make([]byte, n)
	for i := range b {
		switch i % 3 {
		case 0:
			b[i] = byte('0' + p.rng.Intn(10))
		case 1:
			b[i] = notHexLetters[p.rng.Intn(len(notHexLetters))]
		default:
			b[i] = secretAlphabet[p.rng.Intn(len(secretAlphabet))]
		}
	}
	b[1] = notHexLetters[20+p.rng.Intn(20)]
	return string(b)
}

func pick[T any](rng *rand.Rand, of ...T) T { return of[rng.Intn(len(of))] }

// keyName returns a key (or hash field) name that holds s in a segment the
// default rules mask.
func (p *leakProbe) keyName(s string) string {
	switch p.rng.Intn(12) {
	case 0:
		return "session/" + s
	case 1:
		return "session_" + s
	case 2:
		return "{session}:" + s
	case 3:
		return "user:auth:" + s + ":profile"
	case 4:
		return "cache:eyJ" + s + ".eyJ" + s + "." + s // a JWT
	default:
		name := pick(p.rng, "session", "sess", "sid", "token", "auth:token", "password", "secret", "otp", "jwt",
			"Session", "SESSION", "ApiKey", "api_key", "Token", "tokens", "sessions", "passwords",
			// a digit in the name, and session names as frameworks compose them
			"oauth2_token", "token2", "api_key_v2", "auth0", "PHPREDIS_SESSION", "redisson:tomcat_session",
			"myapp.session", "user_sid")
		return name + ":" + s
	}
}

// pattern returns a glob that holds part of s.
func (p *leakProbe) pattern(s string) string {
	switch p.rng.Intn(3) {
	case 0:
		return "session:" + s[:8+p.rng.Intn(len(s)-8)] + "*"
	case 1:
		return "sess:*" + s[p.rng.Intn(len(s)-8):]
	default:
		return "token:" + s + ":*"
	}
}

// carrier returns a string that carries s the way a key, field name or
// pattern would.
func (p *leakProbe) carrier(s string) string {
	if p.rng.Intn(4) == 0 {
		return p.pattern(s)
	}
	return p.keyName(s)
}

// No negative number: miniredis panics on SSCAN with a negative cursor, and
// the fake server runs inside the test binary.
var fillers = []string{"1", "0.01", "x", "user:1", "2", "WITHSCORES", "MATCH", "COUNT", "GET", "STREAMS", "$",
	">", "LEFT", "RIGHT", "NX", "EX", "BY", "LIMIT", "*", "name"}

// Commands the tracer has no entry for.
var unknownCommands = []string{"foo.bar", "bf.add", "ft.search", "echo", "memory", "lolwut", "json.zzz",
	"cms.incrby", "topk.add", "ts.add", "getfoo", "pubsub",
	// real commands that read values and have no entry
	"zunion", "zinter", "zdiff", "sort_ro", "georadius", "zrevrangebylex", "xautoclaim", "lpos", "object"}

// Not run with random arguments: they reach outside the test database.
var skipped = map[string]bool{"move": true, "flushall": true, "flushdb": true}

func (p *leakProbe) anyCommand() string {
	for {
		names := redisx.Names()
		if p.rng.Intn(5) == 0 {
			names = unknownCommands
		}
		if n := names[p.rng.Intn(len(names))]; !skipped[n] {
			return n
		}
	}
}

// run runs one traced command; it may fail or time out, which is recorded too.
func (p *leakProbe) run(what string, fn func(ctx context.Context)) {
	ctx, cancel := context.WithTimeout(p.ctx, 300*time.Millisecond) // a blocking command with a bad timeout
	defer cancel()
	p.what = what
	p.commands++
	fn(ctx)
}

func (p *leakProbe) runCase() {
	bg := context.Background()
	s := p.secret()
	from := len(p.rec.Events())
	kind := ""
	switch p.rng.Intn(8) {
	case 0, 1: // a secret-bearing name in any position of any command
		kind = "name as an argument"
		name := p.anyCommand()
		args := []any{name}
		n := 1 + p.rng.Intn(5)
		at := p.rng.Intn(n)
		for i := range n {
			if i == at || p.rng.Intn(4) == 0 {
				args = append(args, p.carrier(s))
			} else {
				args = append(args, fillers[p.rng.Intn(len(fillers))])
			}
		}
		p.run(name, func(ctx context.Context) { p.do(ctx, args...) })
	case 2: // a stored key whose name holds the secret
		kind = "stored key name"
		key := p.keyName(s)
		p.do(bg, "set", key, "v")
		p.do(bg, "rpush", key+":jobs", "job")
		switch p.rng.Intn(6) {
		case 0:
			p.run("keys *", func(ctx context.Context) { p.do(ctx, "keys", "*") })
		case 1:
			p.run("scan", func(ctx context.Context) { p.do(ctx, "scan", "0", "count", "1000") })
		case 2:
			p.run("randomkey", func(ctx context.Context) { p.do(ctx, "randomkey") })
		case 3:
			p.run("keys <pattern>", func(ctx context.Context) { p.do(ctx, "keys", key[:len(key)-4]+"*") })
		case 4:
			name := pick(p.rng, "blpop", "brpop", "lmpop")
			p.run(name, func(ctx context.Context) {
				if name == "lmpop" {
					p.do(ctx, name, "1", key+":jobs", "LEFT")
				} else {
					p.do(ctx, name, key+":jobs", "0.01")
				}
			})
		default:
			p.run("typed", func(ctx context.Context) { p.typed(ctx, key, "f", "v2") })
		}
	case 3: // a hash field whose name holds the secret
		kind = "hash field name"
		key, field := "user:"+strconv.Itoa(p.rng.Intn(50)), pick(p.rng, "token:", "sess_", "Session:", "auth:token:")+s
		p.do(bg, "hset", key, field, "1", "name", "ann")
		switch p.rng.Intn(4) {
		case 0:
			name := pick(p.rng, "hgetall", "hkeys", "hrandfield", "hscan", "hvals", "hdel", "hset", "hsetnx",
				"hincrby", "hexists", "hget", "hmget", "hstrlen", "del")
			p.run(name, func(ctx context.Context) {
				switch name {
				case "hgetall", "hkeys", "hvals", "del":
					p.do(ctx, name, key)
				case "hrandfield":
					p.do(ctx, name, key, "5", "WITHVALUES")
				case "hscan":
					p.do(ctx, name, key, "0")
				case "hset", "hsetnx", "hincrby":
					p.do(ctx, name, key, field, "2")
				default:
					p.do(ctx, name, key, field)
				}
			})
		case 1:
			p.run("hset another field", func(ctx context.Context) { p.do(ctx, "hset", key, "name", "bob") })
		default:
			p.run("typed", func(ctx context.Context) { p.typed(ctx, key, field, "1") })
		}
	case 4: // a value in a hash field with a sensitive name
		kind = "value of a sensitive hash field"
		key, field := "user:"+strconv.Itoa(p.rng.Intn(50)), pick(p.rng, "token", "password", "api_key", "Authorization")
		p.do(bg, "hset", key, "name", "ann", field, s)
		switch p.rng.Intn(4) {
		case 0:
			name := pick(p.rng, "hgetall", "hvals", "hrandfield", "hscan", "hget", "hmget", "hset", "hsetnx",
				"hdel", "del", "hstrlen")
			p.run(name, func(ctx context.Context) {
				switch name {
				case "hgetall", "hvals", "del":
					p.do(ctx, name, key)
				case "hrandfield":
					p.do(ctx, name, key, "5", "WITHVALUES")
				case "hscan":
					p.do(ctx, name, key, "0")
				case "hset", "hsetnx":
					p.do(ctx, name, key, field, p.secret()+s[:10])
				case "hmget":
					p.do(ctx, name, key, "name", field)
				default:
					p.do(ctx, name, key, field)
				}
			})
		case 1:
			p.run("hset another field", func(ctx context.Context) { p.do(ctx, "hset", key, "name", "bob") })
		default:
			p.run("typed", func(ctx context.Context) { p.typed(ctx, key, field, "1") })
		}
	case 5: // a value under a key with a sensitive name
		kind = "value of a sensitive key"
		key := pick(p.rng, "password:alice", "user:1:token", "secret:x", "auth:bob", "api_key:svc", "tokens:user:1")
		p.do(bg, "del", key)
		switch p.rng.Intn(6) {
		case 0:
			p.do(bg, "set", key, s)
		case 1:
			p.do(bg, "rpush", key, s, "b")
		case 2:
			p.do(bg, "sadd", key, s)
		case 3:
			p.do(bg, "zadd", key, "1", s)
		case 4:
			p.do(bg, "hset", key, "f", s)
		default:
			p.do(bg, "xadd", key, "*", "f", s)
		}
		switch p.rng.Intn(3) {
		case 0:
			p.run("typed", func(ctx context.Context) { p.typed(ctx, key, "f", s) })
		case 1: // a write that takes a value, which is the secret
			args := pick(p.rng,
				[]any{"set", key, s}, []any{"set", key, s, "GET"}, []any{"setnx", key, s}, []any{"getset", key, s},
				[]any{"setex", key, "60", s}, []any{"psetex", key, "60000", s}, []any{"append", key, s},
				[]any{"setrange", key, "1", s}, []any{"mset", key, s}, []any{"lpush", key, s, s},
				[]any{"rpush", key, s}, []any{"rpushx", key, s}, []any{"lset", key, "0", s},
				[]any{"linsert", key, "BEFORE", s, s}, []any{"lrem", key, "0", s}, []any{"lpos", key, s},
				[]any{"sadd", key, s}, []any{"srem", key, s}, []any{"sismember", key, s},
				[]any{"smismember", key, s, s}, []any{"smove", key, key + ":b", s}, []any{"zadd", key, "1", s},
				[]any{"zadd", key, "NX", "1", s}, []any{"zincrby", key, "1", s}, []any{"zrem", key, s},
				[]any{"zscore", key, s}, []any{"zrank", key, s}, []any{"hset", key, "f", s},
				[]any{"hsetnx", key, "f", s}, []any{"hmset", key, "f", s}, []any{"xadd", key, "*", "f", s},
				[]any{"pfadd", key, s}, []any{"geoadd", key, "1", "1", s}, []any{"json.set", key, "$", s},
				[]any{"publish", key, s})
			p.run(args[0].(string), func(ctx context.Context) { p.do(ctx, args...) })
		default: // any command that takes its key first
			name := ""
			for name == "" {
				n := p.anyCommand()
				if spec, ok := redisx.Lookup(n); ok && spec.First == 1 {
					name = n
				}
			}
			args := []any{name, key}
			args = append(args, pick(p.rng, []any{}, []any{"1", "-1"}, []any{"1"}, []any{"-", "+"}, []any{"0.01"},
				[]any{"1", "-1", "WITHSCORES"}, []any{"f"}, []any{"1", "GET"}, []any{"f", "1"})...)
			p.run(name, func(ctx context.Context) { p.do(ctx, args...) })
		}
	case 6: // a member of a plain collection that reads as a secret-bearing key
		kind = "member that reads as a secret key"
		key, member := "user:"+strconv.Itoa(p.rng.Intn(50))+":refs", p.keyName(s)
		name := pick(p.rng, "sadd", "rpush", "zadd")
		if name == "zadd" {
			p.do(bg, name, key, "1", member)
		} else {
			p.do(bg, name, key, member)
		}
		reader := map[string][]any{"sadd": {"smembers", key}, "rpush": {"lrange", key, "0", "-1"}, "zadd": {"zrange", key, "0", "-1", "WITHSCORES"}}[name]
		switch p.rng.Intn(3) {
		case 0:
			p.run(reader[0].(string), func(ctx context.Context) { p.do(ctx, reader...) })
		case 1:
			p.run(name+" again", func(ctx context.Context) {
				if name == "zadd" {
					p.do(ctx, name, key, "2", p.keyName(s))
				} else {
					p.do(ctx, name, key, p.keyName(s))
				}
			})
		default:
			p.run("typed", func(ctx context.Context) { p.typed(ctx, key, "f", member) })
		}
	default: // the KEYS and ARGV of a script, which can hand any part of them back
		kind = "script KEYS/ARGV"
		script := pick(p.rng,
			"return KEYS[1]", "return ARGV[1]", "return {KEYS[1], ARGV[1]}", "return {{ARGV[1]}}",
			"return redis.error_reply('bad '..KEYS[1])",
			"return redis.error_reply('bad '..string.sub(KEYS[1],1,24))",
			"return redis.error_reply(string.upper(ARGV[1]))",
			"return string.sub(ARGV[1], 4)", "return string.lower(KEYS[1])",
			"return string.sub(KEYS[1], -12)", "return redis.status_reply(ARGV[1])",
			"error(ARGV[1])", "return redis.call('SET', KEYS[1], ARGV[1])", "return 1")
		keys, argv := []string{p.keyName(s)}, []any{p.carrier(s)}
		if p.rng.Intn(2) == 0 { // a sensitive key: its ARGV is a value
			keys, argv = []string{pick(p.rng, "password:bob", "user:1:token")}, []any{s}
		}
		if p.rng.Intn(4) == 0 { // no KEYS: the name travels as ARGV only
			script, keys = strings.ReplaceAll(script, "KEYS[1]", "ARGV[1]"), nil
			argv = []any{p.carrier(s)}
		}
		if p.rng.Intn(2) == 0 {
			p.run("eval", func(ctx context.Context) { p.eval(ctx, script, keys, argv...) })
		} else {
			args := []any{"eval", script, len(keys)}
			for _, k := range keys {
				args = append(args, k)
			}
			p.run("eval", func(ctx context.Context) { p.do(ctx, append(args, argv...)...) })
		}
	}
	p.check(kind, s, from)
}

// check fails the test when an event recorded since from holds a piece of s.
func (p *leakProbe) check(kind, s string, from int) {
	low := strings.ToLower(s)
	leaked := false
	for _, e := range p.rec.Events()[from:] {
		p.events++
		b, err := json.Marshal(e)
		if err != nil {
			p.t.Errorf("%s, %s: the event is not JSON: %v", kind, p.what, err)
			continue
		}
		text := strings.ToLower(string(b))
		for i := 0; i+leakWindow <= len(low); i++ {
			if !strings.Contains(text, low[i:i+leakWindow]) {
				continue
			}
			leaked = true
			where := fmt.Sprintf("%s: %s", kind, strings.ToUpper(p.what))
			if p.found[where]++; p.found[where] == 1 {
				p.t.Errorf("%s records the secret %q: name=%q err=%q req=%s resp=%s mutations=%+v",
					where, s, e.Name, e.Err, e.Req.Inline, e.Resp.Inline, e.Mutations)
			}
			break
		}
	}
	if leaked {
		p.leaked++
	}
}
