package redisx

import (
	"encoding/json"
	"path"
	"strings"

	"tsession/core"
)

const masked = core.RedactedValue

// Redactor masks command arguments and replies. The zero value redacts by
// core's sensitive-key list only and captures nothing.
type Redactor struct {
	Keys    []string // WithRedactKeys globs forcing a key to be treated as sensitive
	Capture bool     // record values at all; false drops non-scalar replies
}

// KeySensitive reports whether the key holds sensitive data: forced by a
// WithRedactKeys glob, or any ':' '.' '/' '_' '-' delimited segment (or the
// whole key) is a sensitive name per core.IsSensitiveKey.
func (h *Redactor) KeySensitive(key string) bool {
	for _, p := range h.Keys {
		if ok, _ := path.Match(p, key); ok {
			return true
		}
	}
	return NameSensitive(key)
}

// NameSensitive checks a key or field name and its delimited segments.
func NameSensitive(name string) bool {
	if core.IsSensitiveKey(name) {
		return true
	}
	coarse := strings.FieldsFunc(name, func(r rune) bool { return r == ':' || r == '.' || r == '/' })
	for _, seg := range coarse {
		if core.IsSensitiveKey(seg) {
			return true
		}
		for _, f := range strings.FieldsFunc(seg, func(r rune) bool { return r == '_' || r == '-' }) {
			if core.IsSensitiveKey(f) {
				return true
			}
		}
	}
	return false
}

// RedactStr scrubs one string value: JSON documents are redacted structurally
// (as core.Payload does), anything else with the text scrubber.
func RedactStr(s string) string {
	t := strings.TrimSpace(s)
	if len(t) > 1 && (t[0] == '{' || t[0] == '[') && json.Valid([]byte(t)) {
		if b, changed := core.RedactJSON([]byte(t)); changed {
			return string(b)
		}
		return s
	}
	return core.RedactText(s)
}

// ScrubAny redacts a decoded reply value.
func ScrubAny(v any) any {
	switch t := v.(type) {
	case string:
		return RedactStr(t)
	case map[string]any, []any:
		return scrubDeep(t)
	}
	return v
}

func scrubDeep(v any) any {
	switch t := core.RedactAny(v).(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = ScrubAny(x)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = ScrubAny(x)
		}
		return t
	default:
		return t
	}
}

var noValueCmds = map[string]bool{
	"del": true, "unlink": true, "expire": true, "pexpire": true, "expireat": true, "pexpireat": true,
	"persist": true, "rename": true, "renamenx": true, "copy": true, "move": true,
	"incr": true, "incrby": true, "decr": true, "decrby": true, "incrbyfloat": true,
	"flushdb": true, "flushall": true, "lpop": true, "rpop": true, "spop": true,
	"zpopmin": true, "zpopmax": true, "ltrim": true, "zremrangebyscore": true,
	"zremrangebyrank": true, "zremrangebylex": true, "hdel": true, "hincrby": true,
	"hincrbyfloat": true, "xdel": true, "xtrim": true, "xack": true, "hexpire": true,
	"hpexpire": true, "hpersist": true, "getdel": true, "lmove": true, "rpoplpush": true,
	"blpop": true, "brpop": true, "bzpopmin": true, "bzpopmax": true, "sort": true,
	"bitop": true, "sinterstore": true, "sunionstore": true, "sdiffstore": true,
	"pfmerge": true, "smove": true, "getex": true, "lmpop": true, "xgroup": true,
	"zrangestore": true, "bitfield": true, "setbit": true, "hgetdel": true,
}

// MaskArgs returns the argument vector (index-aligned with args) with
// credentials and value arguments masked according to command semantics.
func (h *Redactor) MaskArgs(name string, args []any, keys []int) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = Str(a)
	}
	n := len(out)
	mask := func(i int) {
		if i >= 0 && i < n {
			out[i] = masked
		}
	}
	lname := strings.ToLower(name)
	up := func(i int) string {
		if i >= 0 && i < n {
			return strings.ToUpper(out[i])
		}
		return ""
	}

	// credentials
	switch lname {
	case "auth":
		for i := 1; i < n; i++ {
			mask(i)
		}
		return out
	case "hello":
		for i := 1; i < n; i++ {
			if up(i) == "AUTH" {
				mask(i + 1)
				mask(i + 2)
			}
		}
		return out
	case "migrate":
		for i := 1; i < n; i++ {
			switch up(i) {
			case "AUTH":
				mask(i + 1)
			case "AUTH2":
				mask(i + 1)
				mask(i + 2)
			}
		}
	case "config":
		if up(1) == "SET" {
			for i := 2; i+1 < n; i += 2 {
				switch strings.ToLower(out[i]) {
				case "requirepass", "masterauth", "masteruser":
					mask(i + 1)
				}
			}
		}
		return out
	case "acl":
		if up(1) == "SETUSER" {
			for i := 3; i < n; i++ {
				if strings.HasPrefix(out[i], ">") || strings.HasPrefix(out[i], "#") || strings.HasPrefix(out[i], "<") || strings.HasPrefix(out[i], "!") {
					mask(i)
				}
			}
		}
		return out
	}

	// value arguments
	drop := !h.Capture
	val := func(i int, sens bool) {
		if i < 0 || i >= n {
			return
		}
		if sens || drop {
			out[i] = masked
		} else {
			out[i] = RedactStr(out[i])
		}
	}
	rng := func(from, to int, sens bool) {
		for i := from; i < to && i < n; i++ {
			val(i, sens)
		}
	}
	anySens := false
	for _, k := range keys {
		if k < n && h.KeySensitive(out[k]) {
			anySens = true
		}
	}
	if len(keys) == 0 {
		switch lname {
		case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
			// no KEYS: ARGV still follows numkeys
		default:
			return out
		}
	}
	switch lname {
	case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
		if n >= 3 {
			rng(3+atoi(args[2]), n, anySens)
		}
		return out
	}
	for _, k := range keys {
		if k >= n {
			continue
		}
		key := out[k]
		sens := h.KeySensitive(key)
		switch lname {
		case "set", "setnx", "getset", "append", "publish", "spublish":
			val(k+1, sens)
		case "setex", "psetex", "setrange", "lset", "lrem":
			val(k+2, sens)
		case "linsert":
			val(k+2, sens)
			val(k+3, sens)
		case "lpush", "rpush", "lpushx", "rpushx", "sadd", "srem", "pfadd":
			rng(k+1, n, sens)
		case "mset", "msetnx":
			val(k+1, sens)
		case "zadd":
			i := k + 1
			for i < n && (up(i) == "NX" || up(i) == "XX" || up(i) == "GT" || up(i) == "LT" || up(i) == "CH" || up(i) == "INCR") {
				i++
			}
			for ; i+1 < n; i += 2 {
				val(i+1, sens)
			}
		case "zincrby":
			val(k+2, sens)
		case "hset", "hmset", "hsetnx":
			for i := k + 1; i+1 < n; i += 2 {
				val(i+1, sens || NameSensitive(out[i]))
			}
		case "xadd":
			i := k + 1
		opts:
			for i < n {
				switch up(i) {
				case "NOMKSTREAM":
					i++
				case "MAXLEN", "MINID":
					i++
					if up(i) == "=" || up(i) == "~" {
						i++
					}
					i++
					if up(i) == "LIMIT" {
						i += 2
					}
				default:
					break opts
				}
			}
			for i += 1; i+1 < n; i += 2 { // skip the entry id
				val(i+1, sens || NameSensitive(out[i]))
			}
		case "json.set", "json.merge", "json.mset":
			ps := sens || pathSensitive(out, k+1)
			val(k+2, ps)
		case "json.arrappend", "json.arrinsert", "json.strappend", "json.numincrby":
			rng(k+2, n, sens || pathSensitive(out, k+1))
		default:
			if (sens || drop) && !noValueCmds[lname] && h.IsWrite(lname) {
				rng(k+1, n, true)
				return out
			}
		}
	}
	return out
}

// IsWrite reports whether the named command mutates state.
func (h *Redactor) IsWrite(lname string) bool { s, _ := Lookup(lname); return s.Write }

// pathSensitive checks the last segment of a JSONPath argument.
func pathSensitive(out []string, i int) bool {
	if i >= len(out) {
		return false
	}
	p := strings.TrimRight(out[i], "]'\"")
	if j := strings.LastIndexAny(p, ".['\""); j >= 0 {
		p = p[j+1:]
	}
	return core.IsSensitiveKey(p)
}

// MaskReply returns the printable, redacted form of a decoded reply v; scalar
// marks ints, bools and status replies. ok=false means the reply is dropped.
func (h *Redactor) MaskReply(name string, args []any, keys []int, v any, scalar bool) (any, bool) {
	if scalar { // ints, bools, status: never carry user text
		if len(keys) > 0 && h.KeySensitive(Str(args[keys[0]])) {
			switch strings.ToLower(name) {
			case "incr", "incrby", "decr", "decrby", "hincrby", "setrange":
				return masked, true
			}
		}
		return v, true
	}
	if !h.Capture {
		return nil, false
	}
	lname := strings.ToLower(name)
	keySens := len(keys) > 0 && h.KeySensitive(Str(args[keys[0]]))
	if keySens {
		if m, isMap := v.(map[string]string); isMap {
			out := make(map[string]string, len(m))
			for k := range m {
				out[k] = masked
			}
			return out, true
		}
		if lname == "mget" {
			if s, isSl := v.([]any); isSl {
				return h.maskMget(args, keys, s), true
			}
		}
		return masked, true
	}
	field := func(i int) bool { return i < len(args) && NameSensitive(Str(args[i])) }
	switch t := v.(type) {
	case string:
		if lname == "hget" && field(2) {
			return masked, true
		}
		return RedactStr(t), true
	case []string:
		out := make([]string, len(t))
		for i, s := range t {
			out[i] = RedactStr(s)
		}
		return out, true
	case []any:
		if lname == "mget" {
			return h.maskMget(args, keys, t), true
		}
		out := make([]any, len(t))
		for i, x := range t {
			if lname == "hmget" && field(2+i) {
				out[i] = masked
			} else {
				out[i] = ScrubAny(x)
			}
		}
		return out, true
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, s := range t {
			if NameSensitive(k) {
				out[k] = masked
			} else {
				out[k] = RedactStr(s)
			}
		}
		return out, true
	default:
		return ScrubAny(v), true
	}
}

func (h *Redactor) maskMget(args []any, keys []int, vals []any) []any {
	out := make([]any, len(vals))
	for i, x := range vals {
		if i < len(keys) && h.KeySensitive(Str(args[keys[i]])) {
			out[i] = masked
		} else {
			out[i] = ScrubAny(x)
		}
	}
	return out
}
