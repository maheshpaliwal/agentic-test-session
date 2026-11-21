package redisx

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

const masked = core.RedactedValue

// Redactor masks command arguments and replies. The zero value redacts by
// core's sensitive-key list only and captures nothing.
type Redactor struct {
	Keys     []string // WithRedactKeys globs forcing a key to be treated as sensitive
	Capture  bool     // record values at all; false drops non-scalar replies
	MaskKeys []string // WithMaskKeys globs whose wildcard segments hold a secret
	KeyMask  KeyMask  // how the secret part of a key name is recorded
}

// KeySensitive reports whether the key holds sensitive data: forced by a
// WithRedactKeys glob, or any ':' '.' '/' '_' '-' delimited segment (or the
// whole key) is a sensitive name per core.IsSensitiveKey, or a ':' or '/'
// delimited segment is the plural of one ("tokens:user:1").
func (h *Redactor) KeySensitive(key string) bool {
	if matchAny(h.Keys, key) {
		return true
	}
	return NameSensitive(key) || pluralSensitive(key)
}

// NameSensitive checks a key or field name and its delimited segments.
func NameSensitive(name string) bool {
	if core.IsSensitiveKey(name) {
		return true
	}
	if !strings.ContainsAny(name, ":./_-") {
		return false
	}
	return anySegment(name, ":./", func(seg string) bool {
		return core.IsSensitiveKey(seg) || anySegment(seg, "_-", core.IsSensitiveKey)
	})
}

// pluralSensitive reports whether key, or a ':' or '/' delimited segment of
// it, is a sensitive name in the plural. Only whole segments count:
// "max_tokens" as a field of a hash is a number, a key "tokens:<user>" holds
// tokens.
func pluralSensitive(key string) bool {
	return pluralName(key) || anySegment(key, ":/", pluralName)
}

func pluralName(seg string) bool {
	n := len(seg) - 1
	return n > 1 && (seg[n] == 's' || seg[n] == 'S') && nameWord(seg) && NameSensitive(seg[:n])
}

// anySegment reports whether is holds for a non-empty proper part of s cut at
// the bytes of seps.
func anySegment(s, seps string, is func(string) bool) bool {
	start := 0
	for i := 0; i <= len(s); i++ {
		if i < len(s) && strings.IndexByte(seps, s[i]) < 0 {
			continue
		}
		if seg := s[start:i]; seg != "" && len(seg) < len(s) && is(seg) {
			return true
		}
		start = i + 1
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
	"pfmerge": true, "getex": true, "lmpop": true, "xgroup": true,
	"zrangestore": true, "bitfield": true, "setbit": true, "hgetdel": true,
	"blmove": true, "brpoplpush": true, "blmpop": true, "zmpop": true, "bzmpop": true,
	"zunionstore": true, "zinterstore": true, "zdiffstore": true, "geosearchstore": true,
}

// memberReads are read commands whose arguments after the key are stored
// values (members / elements), not options.
var memberReads = map[string]bool{
	"sismember": true, "smismember": true, "zscore": true, "zmscore": true, "zrank": true,
	"zrevrank": true, "lpos": true, "zrangebylex": true, "zlexcount": true,
	"geopos": true, "geodist": true, "geohash": true,
}

// MaskArgs returns the argument vector (index-aligned with args) with
// credentials and value arguments masked according to command semantics.
// sensKey reports that one of the keys holds sensitive data.
func (h *Redactor) MaskArgs(name string, args []any, keys []int) (out []string, sensKey bool) {
	out = make([]string, len(args))
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
		return out, false
	case "hello":
		for i := 1; i < n; i++ {
			if up(i) == "AUTH" {
				mask(i + 1)
				mask(i + 2)
			}
		}
		return out, false
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
		return out, false
	case "acl":
		if up(1) == "SETUSER" {
			for i := 3; i < n; i++ {
				if !aclRule(out[i]) { // a password, or a rule too malformed to tell
					mask(i)
				}
			}
		}
		return out, false
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
			if _, known := Lookup(lname); !known {
				// A command we cannot classify: any argument may be a value.
				rng(1, n, false)
			}
			return out, anySens
		}
	}
	switch lname {
	case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
		if n >= 3 {
			rng(3+atoi(args[2]), n, anySens)
		}
		return out, anySens
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
			// Everything after the last key is a value: masked when a key is
			// sensitive, scrubbed otherwise.
			if !noValueCmds[lname] && (h.IsWrite(lname) || memberReads[lname]) {
				rng(keys[len(keys)-1]+1, n, anySens)
			}
			return out, anySens
		}
	}
	return out, anySens
}

// aclRule reports whether an ACL SETUSER argument is a rule that holds no
// password: a flag, or a key, channel, command or selector pattern.
func aclRule(arg string) bool {
	switch strings.ToLower(arg) {
	case "on", "off", "nopass", "resetpass", "allkeys", "resetkeys", "allchannels", "resetchannels",
		"allcommands", "nocommands", "clearselectors", "reset", "sanitize-payload", "skip-sanitize-payload":
		return true
	}
	return arg != "" && strings.IndexByte("~%&+-(", arg[0]) >= 0
}

// IsWrite reports whether the named command mutates state.
func (h *Redactor) IsWrite(lname string) bool { s, _ := Lookup(lname); return s.Write }

// pathSensitive checks the last segment of a JSONPath argument.
func pathSensitive[T any](out []T, i int) bool {
	if i >= len(out) {
		return false
	}
	p := strings.TrimRight(Str(any(out[i])), "]'\"")
	if j := strings.LastIndexAny(p, ".['\""); j >= 0 {
		p = p[j+1:]
	}
	return core.IsSensitiveKey(p)
}

// opaqueReplies are commands whose reply is recorded as the redaction marker
// whatever it holds: it carries credentials (ACL), other clients' commands
// (MONITOR, SLOWLOG) or a value in a form that cannot be scrubbed (DUMP,
// DEBUG).
var opaqueReplies = map[string]bool{
	"acl": true, "dump": true, "debug": true, "migrate": true, "monitor": true, "slowlog": true,
}

// IsScript reports whether the named command runs a script or a function,
// which can hand back any part of its KEYS and ARGV.
func IsScript(lname string) bool {
	switch lname {
	case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
		return true
	}
	return false
}

// hasOption reports whether one of args[from:] is the option, in any case.
func hasOption(args []any, from int, option string) bool {
	for i := from; i < len(args); i++ {
		if strings.EqualFold(Str(args[i]), option) {
			return true
		}
	}
	return false
}

// configSecret reports whether a CONFIG parameter holds a credential.
func configSecret(name string) bool {
	l := strings.ToLower(name)
	return strings.Contains(l, "pass") || strings.Contains(l, "auth") || strings.Contains(l, "secret") ||
		l == "masteruser" || NameSensitive(name)
}

// MaskReply returns the printable, redacted form of a decoded reply v; scalar
// marks ints, bools and status replies. sensitive says that an argument of the
// command was recorded masked or one of its keys is sensitive. ok=false means
// the reply is dropped.
//
// The rules, in order:
//
//   - an integer or a boolean is recorded, except the new value of a counter
//     under a sensitive key (INCR, DECR, HINCRBY, ...); a status is recorded,
//     except that of SET ... GET, which is the old value;
//   - ACL, DUMP, DEBUG, MIGRATE, MONITOR, SLOWLOG: masked;
//   - a script, or a command the tracer does not know, with a sensitive
//     argument: masked, because the reply can be any part of it;
//   - HVALS: every value masked. The reply has no field names to tell which
//     values are sensitive, and reading them would cost another round trip;
//   - any sensitive key: masked (MGET: per key; a hash as a map: its values);
//   - HGET, HMGET, HGETDEL, HGETEX, JSON.GET, SORT ... GET: masked by the
//     field, path or pattern argument;
//   - anything else is walked through: a string is recorded as Text
//     records it, and a name (a key of KEYS, a field of HKEYS or HGETALL) as
//     ShowText does; in a map, the value of a sensitive name is masked; in an
//     array, the element after one that reads as a sensitive name is masked,
//     which covers the flat field, value, ... replies of HGETALL, HSCAN,
//     XRANGE and CONFIG GET through Do.
func (h *Redactor) MaskReply(session, name string, args []any, keys []int, v any, scalar, sensitive bool) (any, bool) {
	lname := strings.ToLower(name)
	if scalar {
		if _, status := v.(string); !status || lname != "set" || !hasOption(args, 3, "GET") {
			if !status && len(keys) > 0 && h.KeySensitive(Str(args[keys[0]])) {
				switch lname {
				case "incr", "incrby", "decr", "decrby", "hincrby", "setrange", "append":
					return masked, true
				}
			}
			return v, true
		}
	}
	if !h.Capture {
		return nil, false
	}
	w := replyWalk{h: h, session: session, sens: NameSensitive,
		names: lname == "keys" || lname == "hkeys", pairs: lname == "hgetall"}
	_, known := Lookup(lname)
	switch {
	case opaqueReplies[lname], (!known || IsScript(lname)) && sensitive:
		return masked, true
	case lname == "hvals":
		return maskEach(v), true
	case lname == "mget":
		if l, ok := v.([]any); ok {
			out := make([]any, len(l))
			for i, x := range l {
				if i < len(keys) && h.KeySensitive(Str(args[keys[i]])) {
					out[i] = masked
				} else {
					out[i] = w.value(x)
				}
			}
			return out, true
		}
	case lname == "config":
		w.sens, w.pairs = configSecret, true
		return w.value(v), true
	}
	for _, k := range keys {
		if k >= len(args) || !h.KeySensitive(Str(args[k])) {
			continue
		}
		if m, isMap := v.(map[string]string); isMap { // field names, values masked
			out := make(map[string]string, len(m))
			for f := range m {
				out[uniqueKey(out, h.ShowText(session, f))] = masked
			}
			return out, true
		}
		return masked, true
	}
	field := func(i int) bool { return i < len(args) && NameSensitive(Str(args[i])) }
	switch lname {
	case "hget":
		if field(2) {
			return masked, true
		}
	case "hmget", "hgetdel", "hgetex":
		first := 2
		if lname != "hmget" {
			for first < len(args) && !strings.EqualFold(Str(args[first]), "FIELDS") {
				first++
			}
			first += 2 // FIELDS numfields
		}
		if l, ok := v.([]any); ok {
			out := make([]any, len(l))
			for i, x := range l {
				if field(first + i) {
					out[i] = masked
				} else {
					out[i] = w.value(x)
				}
			}
			return out, true
		}
	case "json.get", "json.mget":
		for i := 2; i < len(args); i++ {
			if pathSensitive(args, i) {
				return masked, true
			}
		}
	case "sort", "sort_ro": // BY and GET patterns name the keys whose values are returned
		for i := 2; i < len(args); i++ {
			if h.KeySensitive(Str(args[i])) {
				return masked, true
			}
		}
	}
	return w.value(v), true
}

// replyWalk redacts a reply of any shape.
type replyWalk struct {
	h       *Redactor
	session string
	sens    func(name string) bool // names whose value is masked
	names   bool                   // the reply is a list of names (KEYS, HKEYS)
	pairs   bool                   // a flat reply is name, value, name, value, ... (HGETALL, CONFIG GET)
}

func (w replyWalk) text(s string) string { return w.h.Text(w.session, s) }

// names reports whether an element of a reply reads as a sensitive name, so
// that the element after it is its value.
func (w replyWalk) named(v any) bool {
	s, ok := v.(string)
	return ok && s != "" && len(s) <= 64 && w.sens(s)
}

func (w replyWalk) value(v any) any {
	switch t := v.(type) {
	case nil, bool, int, int64, uint64, float32, float64:
		return v
	case string:
		return w.text(t)
	case []byte:
		return w.text(string(t))
	case []string:
		out := make([]string, len(t))
		for i, s := range t {
			switch {
			case w.names:
				out[i] = w.h.ShowText(w.session, s)
			case i > 0 && w.named(t[i-1]):
				out[i] = masked
			default:
				out[i] = w.text(s)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			s, isText := x.(string)
			switch {
			case isText && (w.names || w.pairs && len(t)%2 == 0 && i%2 == 0):
				out[i] = w.h.ShowText(w.session, s) // a name, as it is recorded everywhere
			case i > 0 && !w.names && w.named(t[i-1]):
				out[i] = masked
			default:
				out[i] = w.value(x)
			}
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(t))
		for k, s := range t {
			name := uniqueKey(out, w.h.ShowText(w.session, k))
			if w.sens(k) {
				out[name] = masked
			} else {
				out[name] = w.text(s)
			}
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			w.entry(out, k, x)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			w.entry(out, Str(k), x)
		}
		return out
	}
	return masked // a shape the tracer does not know
}

func (w replyWalk) entry(out map[string]any, k string, x any) {
	name := uniqueKey(out, w.h.ShowText(w.session, k))
	if w.sens(k) {
		out[name] = masked
	} else {
		out[name] = w.value(x)
	}
}

// maskEach masks every element of a list reply and keeps their number.
func maskEach(v any) any {
	n := 0
	switch t := v.(type) {
	case []string:
		n = len(t)
	case []any:
		n = len(t)
	default:
		return masked
	}
	out := make([]string, n)
	for i := range out {
		out[i] = masked
	}
	return out
}

// uniqueKey returns k, or k with a number when m already holds it: names that
// are recorded alike stay apart.
func uniqueKey[V any](m map[string]V, k string) string {
	if _, taken := m[k]; !taken {
		return k
	}
	for n := 2; ; n++ {
		c := k + " (" + strconv.Itoa(n) + ")"
		if _, taken := m[c]; !taken {
			return c
		}
	}
}
