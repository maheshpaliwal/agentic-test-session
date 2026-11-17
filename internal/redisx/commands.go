package redisx

import "strings"

// Spec describes one Redis command: whether it mutates state and where its
// keys live in the argument vector (args[0] is the command name). It follows
// the first/last/step convention of COMMAND INFO; last < 0 counts from the end.
type Spec struct {
	Write             bool
	First, Last, Step int
}

func r(first, last, step int) Spec { return Spec{false, first, last, step} }
func w(first, last, step int) Spec { return Spec{true, first, last, step} }

var (
	r1 = r(1, 1, 1) // read, single key at args[1]
	w1 = w(1, 1, 1) // write, single key at args[1]
	rN = r(1, -1, 1)
	wN = w(1, -1, 1)
)

// commands is the classification table. Anything absent is treated as a
// keyless read (see Lookup) so unknown commands are still traced.
var commands = map[string]Spec{
	// strings
	"get": r1, "mget": rN, "strlen": r1, "getrange": r1, "substr": r1, "getbit": r1,
	"bitcount": r1, "bitpos": r1, "lcs": r(1, 2, 1),
	"set": w1, "setex": w1, "psetex": w1, "setnx": w1, "getset": w1, "getdel": w1, "getex": w1,
	"append": w1, "incr": w1, "incrby": w1, "incrbyfloat": w1, "decr": w1, "decrby": w1,
	"setrange": w1, "setbit": w1, "mset": w(1, -1, 2), "msetnx": w(1, -1, 2),
	"bitfield": w1, "bitop": w(2, -1, 1),
	// generic keyspace
	"exists": rN, "ttl": r1, "pttl": r1, "expiretime": r1, "pexpiretime": r1, "type": r1,
	"dump": r1, "touch": rN, "object": r(2, 2, 1), "randomkey": r(0, 0, 0),
	"keys": r(0, 0, 0), "scan": r(0, 0, 0), "dbsize": r(0, 0, 0),
	"del": wN, "unlink": wN, "expire": w1, "pexpire": w1, "expireat": w1, "pexpireat": w1,
	"persist": w1, "rename": w(1, 2, 1), "renamenx": w(1, 2, 1), "copy": w(1, 2, 1),
	"move": w1, "restore": w1,
	"sort":    w1, // a write only with STORE, see Classify
	"flushdb": w(0, 0, 0), "flushall": w(0, 0, 0),
	// hashes
	"hget": r1, "hgetall": r1, "hmget": r1, "hkeys": r1, "hvals": r1, "hlen": r1, "hexists": r1,
	"hstrlen": r1, "hrandfield": r1, "hscan": r1,
	"hset": w1, "hmset": w1, "hsetnx": w1, "hdel": w1, "hincrby": w1, "hincrbyfloat": w1,
	"hexpire": w1, "hpexpire": w1, "hpersist": w1, "hgetdel": w1, "hgetex": w1, "hsetex": w1,
	// lists
	"lrange": r1, "llen": r1, "lindex": r1, "lpos": r1,
	"lpush": w1, "rpush": w1, "lpushx": w1, "rpushx": w1, "lpop": w1, "rpop": w1, "lset": w1,
	"lrem": w1, "ltrim": w1, "linsert": w1, "lmove": w(1, 2, 1), "rpoplpush": w(1, 2, 1),
	"blmove": w(1, 2, 1), "brpoplpush": w(1, 2, 1),
	"blpop": w(1, -2, 1), "brpop": w(1, -2, 1),
	"lmpop": w(0, 0, 0), "blmpop": w(0, 0, 0), // keys via numkeys, see KeysOf
	// sets
	"smembers": r1, "sismember": r1, "smismember": r1, "scard": r1, "srandmember": r1,
	"sscan": r1, "sinter": rN, "sunion": rN, "sdiff": rN,
	"sadd": w1, "srem": w1, "spop": w1, "smove": w(1, 2, 1),
	"sinterstore": wN, "sunionstore": wN, "sdiffstore": wN,
	// sorted sets
	"zrange": r1, "zrevrange": r1, "zrangebyscore": r1, "zrevrangebyscore": r1,
	"zrangebylex": r1, "zscore": r1, "zmscore": r1, "zcard": r1, "zrank": r1, "zrevrank": r1,
	"zcount": r1, "zlexcount": r1, "zscan": r1, "zrandmember": r1,
	"zadd": w1, "zrem": w1, "zincrby": w1, "zremrangebyscore": w1, "zremrangebyrank": w1,
	"zremrangebylex": w1, "zpopmin": w1, "zpopmax": w1, "zrangestore": w(1, 2, 1),
	"bzpopmin": w(1, -2, 1), "bzpopmax": w(1, -2, 1),
	"zmpop": w(0, 0, 0), "bzmpop": w(0, 0, 0), // keys via numkeys, see KeysOf
	"zunionstore": w(0, 0, 0), "zinterstore": w(0, 0, 0), "zdiffstore": w(0, 0, 0),
	// streams
	"xrange": r1, "xrevrange": r1, "xlen": r1, "xinfo": r(2, 2, 1), "xpending": r1,
	"xread": r(0, 0, 0), "xreadgroup": r(0, 0, 0),
	"xadd": w1, "xdel": w1, "xtrim": w1, "xack": w1, "xclaim": w1, "xautoclaim": w1,
	"xgroup": w(2, 2, 1),
	// geo / hll
	"geopos": r1, "geodist": r1, "geohash": r1, "geosearch": r1, "pfcount": rN,
	"geoadd": w1, "pfadd": w1, "pfmerge": wN, "geosearchstore": w(1, 2, 1),
	// pub/sub: the channel stands in for the key
	"publish": w1, "spublish": w1,
	// scripting: keys are located via numkeys, see KeysOf
	"eval": w(0, 0, 0), "evalsha": w(0, 0, 0), "eval_ro": r(0, 0, 0), "evalsha_ro": r(0, 0, 0),
	"fcall": w(0, 0, 0), "fcall_ro": r(0, 0, 0),
	// RedisJSON
	"json.get": r1, "json.mget": r(1, -2, 1), "json.type": r1, "json.objkeys": r1, "json.arrlen": r1,
	"json.set": w1, "json.merge": w1, "json.del": w1, "json.forget": w1, "json.numincrby": w1,
	"json.strappend": w1, "json.arrappend": w1, "json.arrinsert": w1, "json.arrpop": w1,
	"json.mset": w(1, -1, 3), "json.toggle": w1, "json.clear": w1,
}

// Lookup returns the Spec for a command name (case-insensitive).
func Lookup(name string) (Spec, bool) {
	s, ok := commands[strings.ToLower(name)]
	return s, ok
}

// Classify is Lookup refined by the arguments: SORT only writes when it has a
// STORE destination.
func Classify(name string, args []any) Spec {
	s, _ := Lookup(name)
	if strings.EqualFold(name, "sort") {
		if storeIndex(args) < 0 {
			return r1
		}
		return w(1, -1, 1)
	}
	return s
}

// storeIndex returns the index of the destination key of SORT ... STORE dest,
// or -1.
func storeIndex(args []any) int {
	for i := 2; i+1 < len(args); i++ {
		if strings.EqualFold(Str(args[i]), "store") {
			return i + 1
		}
	}
	return -1
}

// numKeys locates the keys of commands that carry an explicit key count: the
// count is args[at] and the keys follow it. dest marks a destination key at
// args[1], ahead of the count.
var numKeys = map[string]struct {
	at   int
	dest bool
}{
	"lmpop": {at: 1}, "zmpop": {at: 1}, "blmpop": {at: 2}, "bzmpop": {at: 2},
	"zunionstore": {at: 2, dest: true}, "zinterstore": {at: 2, dest: true}, "zdiffstore": {at: 2, dest: true},
}

// KeysOf returns the indices into args that hold keys.
func KeysOf(name string, s Spec, args []any) []int {
	lname := strings.ToLower(name)
	switch lname {
	case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
		if len(args) < 3 {
			return nil
		}
		n := atoi(args[2])
		var idx []int
		for i := 0; i < n && 3+i < len(args); i++ {
			idx = append(idx, 3+i)
		}
		return idx
	case "sort":
		if len(args) < 2 {
			return nil
		}
		idx := []int{1}
		if d := storeIndex(args); d >= 0 {
			idx = append(idx, d)
		}
		return idx
	case "object", "xinfo", "xgroup":
		// subcommand forms: only the simple cases are handled.
	}
	if nk, ok := numKeys[lname]; ok {
		if nk.at >= len(args) {
			return nil
		}
		var idx []int
		if nk.dest {
			idx = append(idx, 1)
		}
		n := atoi(args[nk.at])
		for i := nk.at + 1; i <= nk.at+n && i < len(args); i++ {
			idx = append(idx, i)
		}
		return idx
	}
	if s.First <= 0 {
		return nil
	}
	last := s.Last
	if last < 0 {
		last = len(args) + last
	}
	if last >= len(args) {
		last = len(args) - 1
	}
	step := max(s.Step, 1)
	var idx []int
	for i := s.First; i <= last; i += step {
		idx = append(idx, i)
	}
	return idx
}

// destKey gives, for commands that read several keys but write one, the
// position of the written key within the command's key list (-1 = last).
var destKey = map[string]int{
	"sinterstore": 0, "sunionstore": 0, "sdiffstore": 0, "pfmerge": 0, "bitop": 0,
	"zrangestore": 0, "zunionstore": 0, "zinterstore": 0, "zdiffstore": 0,
	"geosearchstore": 0, "copy": 1, "sort": -1,
}

// popCmds pop from the first non-empty key of a list; the reply names it.
var popCmds = map[string]bool{
	"blpop": true, "brpop": true, "bzpopmin": true, "bzpopmax": true,
	"lmpop": true, "blmpop": true, "zmpop": true, "bzmpop": true,
}

// IsPop reports whether the named command pops from one of several keys and
// names that key in its reply.
func IsPop(name string) bool { return popCmds[strings.ToLower(name)] }

// WrittenKeys returns the subset of keys (indices into args, as returned by
// KeysOf) that a write command mutates: only the destination of the *STORE
// family, PFMERGE, BITOP and COPY, and only the popped key of a multi-key pop
// when popped (the key named by the reply) is known. popped == "" means
// unknown, and every candidate key is returned.
func WrittenKeys(name string, args []any, keys []int, popped string) []int {
	lname := strings.ToLower(name)
	if d, ok := destKey[lname]; ok {
		if d < 0 {
			d = len(keys) - 1
		}
		if d < 0 || d >= len(keys) {
			return nil
		}
		return keys[d : d+1]
	}
	if popped != "" && popCmds[lname] {
		for j, i := range keys {
			if i < len(args) && Str(args[i]) == popped {
				return keys[j : j+1]
			}
		}
	}
	return keys
}

// noopOnZero lists writes whose integer/boolean reply is the number of keys or
// members actually changed, so 0 / false means nothing happened.
var noopOnZero = map[string]bool{
	"set": true, "setnx": true, "msetnx": true, "renamenx": true,
	"del": true, "unlink": true, "hdel": true, "srem": true, "zrem": true,
	"expire": true, "pexpire": true, "expireat": true, "pexpireat": true, "persist": true,
}

// NoOp reports whether the reply of a successful write shows that it changed
// nothing: SET NX|XX / SETNX / MSETNX / RENAMENX whose condition failed, or
// DEL, UNLINK, HDEL, SREM, ZREM, EXPIRE*, PERSIST that affected 0 keys or
// members. Such commands yield no mutation.
func NoOp(name string, reply any) bool {
	if !noopOnZero[strings.ToLower(name)] {
		return false
	}
	switch t := reply.(type) {
	case int64:
		return t == 0
	case bool:
		return !t
	}
	return false
}
func atoi(v any) int {
	n := 0
	for _, c := range Str(v) {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
