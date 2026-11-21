package redisx

import (
	"cmp"
	"fmt"
	"hash/maphash"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
)

const (
	maxDiff      = 50  // changes listed per mutation, and members per added/removed list
	maxDiffValue = 256 // bytes kept of one value
)

// item is one element of a key as an image read saw it: the value of a
// string, a hash field, a list element, a set member or a sorted-set member.
type item struct {
	id    uint64  // fingerprint of what identifies it: the hash field, the member; 0 for a string
	name  string  // hash field or sorted-set member as recorded; "" otherwise
	shown string  // the value as recorded, redaction applied
	score float64 // sorted sets only
	sum   uint64  // fingerprint of the value as stored, see sumOf
}

var sumSeed = maphash.MakeSeed()

// sumOf fingerprints a stored value or name. Redaction can make two different
// values look the same ("[REDACTED]"), and two different names too, so the
// diff tells elements apart and compares them by fingerprint: a changed
// secret is reported as changed, and two members recorded alike stay two,
// without the tracer holding the stored text while the command runs. The seed
// is per process and the fingerprint is never recorded.
func sumOf(raw string) uint64 { return maphash.String(sumSeed, raw) }

// snap is what one image read saw of a key. It lives from the before-read to
// the after-read of one command and is only used to compute the diff.
type snap struct {
	typ    string // TYPE reply: none, string, hash, list, set, zset, ...
	items  []item
	capped bool // a collection read stopped at ImageLimit: items is a part
	hidden bool // a sensitive key: it exists, its value was not read
}

func (s *snap) absent() bool { return s.typ == "none" }

// same reports whether two items hold the same value. NaN is a score like any
// other here: a member whose score stays NaN did not change.
func (a item) same(b item) bool {
	return a.sum == b.sum && a.shown == b.shown &&
		(a.score == b.score || math.IsNaN(a.score) && math.IsNaN(b.score))
}

// value is the item as it appears in a diff.
func (a item) value(typ string) any {
	if typ == "zset" {
		return scoreValue(a.score)
	}
	return clip(a.shown)
}

// scoreValue is a sorted-set score as it is recorded: a number, or the text
// Redis prints for one that JSON has no number for ("inf", "-inf", "nan").
// Such a score would otherwise fail the encoding of the whole event, or be
// written as null, which reads as "no value".
func scoreValue(f float64) any {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	return f
}

func clip(s string) string {
	if len(s) <= maxDiffValue {
		return s
	}
	return strings.ToValidUTF8(s[:maxDiffValue], "") + "...(" + strconv.Itoa(len(s)-maxDiffValue) + " more bytes)"
}

// diffSnaps lists what changed between two reads of one key:
//
//   - string: one change, field "value";
//   - hash: one change per field added, removed or changed;
//   - sorted set: the same per member, the values being the scores;
//   - set: the members added (field "added") and removed (field "removed");
//   - list: one change per index "[i]" between the unchanged head and the
//     unchanged tail of the list;
//   - a key that changed type: one change, field "type".
//
// A value that changed under redaction is listed with the redacted text on
// both sides: being listed is what says it changed. Fields and members are
// told apart by what is stored, not by how they are recorded: two members
// recorded alike are two changes with the same field text. A created key has
// only new values and a deleted key only old ones. truncated
// reports that the list is incomplete: it was cut at maxDiff, or a collection
// was read up to ImageLimit only. Two partial reads of one collection cannot
// tell an added element from one the other read did not reach, so then only
// hash fields and sorted-set members present in both are compared, and lists
// and sets get no changes at all.
func diffSnaps(b, a *snap) (out []core.FieldChange, truncated bool) {
	if b == nil || a == nil || b.hidden || a.hidden || b.absent() && a.absent() {
		return nil, false
	}
	typ := a.typ
	if a.absent() {
		typ = b.typ
	}
	both := !b.absent() && !a.absent()
	if both && b.typ != a.typ {
		return []core.FieldChange{{Field: "type", Old: b.typ, New: a.typ}}, false
	}
	partial := b.capped || a.capped
	switch typ {
	case "string":
		out = diffNamed(typ, b, a, "value", false)
	case "hash", "zset":
		out = diffNamed(typ, b, a, "", both && partial)
	case "set":
		if !both || !partial {
			out = diffSet(b, a)
		}
	case "list":
		if !both || !partial {
			out = diffList(b, a)
		}
	default:
		return nil, false // a type whose content is not imaged (stream, ...)
	}
	if len(out) > maxDiff {
		out, truncated = out[:maxDiff], true
	}
	return out, truncated || partial
}

// byID indexes the items of s by what identifies them.
func byID(s *snap) map[uint64]item {
	m := make(map[uint64]item, len(s.items))
	for _, it := range s.items {
		m[it.id] = it
	}
	return m
}

// diffNamed compares two sets of named values. name, when set, names the only
// item of each side. With common only items on both sides are compared.
func diffNamed(typ string, b, a *snap, name string, common bool) []core.FieldChange {
	old, cur := byID(b), byID(a)
	ids := set.Set[uint64]{}
	for id := range old {
		ids.Add(id)
	}
	for id := range cur {
		ids.Add(id)
	}
	var out []core.FieldChange
	for id := range ids {
		o, oOK := old[id]
		w, wOK := cur[id]
		if oOK && wOK && o.same(w) || common && oOK != wOK {
			continue
		}
		fc := core.FieldChange{Field: clip(cmp.Or(name, o.name, w.name))}
		if oOK {
			fc.Old = o.value(typ)
		}
		if wOK {
			fc.New = w.value(typ)
		}
		out = append(out, fc)
	}
	// By field, then by value: fields recorded alike keep a stable order.
	slices.SortFunc(out, func(x, y core.FieldChange) int {
		return cmp.Or(cmp.Compare(x.Field, y.Field), cmp.Compare(fmt.Sprint(x.Old, x.New), fmt.Sprint(y.Old, y.New)))
	})
	return out
}

// diffSet lists the members only one side has.
func diffSet(b, a *snap) []core.FieldChange {
	only := func(x, y *snap) []string {
		in := set.Set[uint64]{}
		for _, it := range y.items {
			in.Add(it.sum)
		}
		var out []string
		for _, it := range x.items {
			if !in.Has(it.sum) {
				out = append(out, clip(it.shown))
			}
		}
		slices.Sort(out)
		if len(out) > maxDiff {
			out = append(out[:maxDiff], "...("+strconv.Itoa(len(out)-maxDiff)+" more)")
		}
		return out
	}
	var out []core.FieldChange
	if added := only(a, b); len(added) > 0 {
		out = append(out, core.FieldChange{Field: "added", New: added})
	}
	if removed := only(b, a); len(removed) > 0 {
		out = append(out, core.FieldChange{Field: "removed", Old: removed})
	}
	return out
}

// diffList skips the elements both lists start and end with and lists the
// rest by index: what the old list held there and what the new one holds.
func diffList(b, a *snap) []core.FieldChange {
	x, y := b.items, a.items
	head := 0
	for head < len(x) && head < len(y) && x[head].same(y[head]) {
		head++
	}
	x, y = x[head:], y[head:]
	for len(x) > 0 && len(y) > 0 && x[len(x)-1].same(y[len(y)-1]) {
		x, y = x[:len(x)-1], y[:len(y)-1]
	}
	var out []core.FieldChange
	for i := range max(len(x), len(y)) {
		fc := core.FieldChange{Field: "[" + strconv.Itoa(head+i) + "]"}
		if i < len(x) {
			fc.Old = x[i].value("list")
		}
		if i < len(y) {
			fc.New = y[i].value("list")
		}
		out = append(out, fc)
	}
	return out
}

// namedWrites change only the fields or members they name.
var namedWrites = map[string]string{
	"hset": "hash", "hmset": "hash", "hsetnx": "hash", "hdel": "hash", "hincrby": "hash", "hincrbyfloat": "hash",
	"zadd": "zset", "zrem": "zset", "zincrby": "zset", "sadd": "set", "srem": "set",
}

// removals take elements away and add none.
var removals = map[string]bool{
	"del": true, "unlink": true, "getdel": true, "hdel": true, "srem": true, "zrem": true, "spop": true,
	"lpop": true, "rpop": true, "blpop": true, "brpop": true, "lmpop": true, "blmpop": true, "ltrim": true,
	"lrem": true, "zpopmin": true, "zpopmax": true, "bzpopmin": true, "bzpopmax": true, "zmpop": true,
	"bzmpop": true, "zremrangebyscore": true, "zremrangebyrank": true, "zremrangebylex": true,
}

// valueAt gives, for writes that store their argument as the whole value of a
// string key, the position of that argument after the key.
var valueAt = map[string]int{"set": 1, "getset": 1, "setex": 2, "psetex": 2}

// foreignChange reports whether the two reads around a command show a change
// the command cannot have made, which means another client wrote the key in
// between. The before and after images are two reads, not a transaction with
// the command, so the diff then holds that client's change as well. The check
// is one-sided: it never blames a command for its own effect, and it cannot
// see another client changing the very field or member the command names.
//
//   - HSET, HDEL, HINCRBY, ZADD, ZREM, ZINCRBY, SADD, SREM: a field or member
//     that changed is not among the command's arguments;
//   - DEL, the pops, the trims and the other removals: the key holds an
//     element afterwards that it did not hold before;
//   - SET, GETSET, SETEX: the key does not hold the argument afterwards.
//
// key is the index of the key in args.
func foreignChange(lname string, args []any, key int, b, a *snap) bool {
	if b == nil || a == nil || b.hidden || a.hidden || b.capped || a.capped {
		return false
	}
	if typ, ok := namedWrites[lname]; ok {
		if !b.absent() && b.typ != typ || !a.absent() && a.typ != typ {
			return !b.absent() // the key changed type under a command that cannot do that
		}
		named := set.Set[uint64]{}
		for _, arg := range namedArgs(lname, args[min(key+1, len(args)):]) {
			text, ok := wireText(arg)
			if !ok {
				return false // a name whose text on the wire is not certain: no verdict
			}
			named.Add(sumOf(text))
		}
		old, cur := byID(b), byID(a)
		for id, o := range old {
			if w, ok := cur[id]; (!ok || !o.same(w)) && !named.Has(id) {
				return true
			}
		}
		for id := range cur {
			if _, ok := old[id]; !ok && !named.Has(id) {
				return true
			}
		}
		return false
	}
	if removals[lname] {
		type element struct{ id, sum, score uint64 }
		of := func(it item) element {
			e := element{id: it.id, sum: it.sum}
			if !math.IsNaN(it.score) { // every NaN is the same score here
				e.score = math.Float64bits(it.score)
			}
			return e
		}
		had := map[element]int{}
		for _, it := range b.items {
			had[of(it)]++
		}
		for _, it := range a.items {
			if e := of(it); had[e] == 0 {
				return true
			} else {
				had[e]--
			}
		}
		return false
	}
	if at, ok := valueAt[lname]; ok && key+at < len(args) && !a.absent() {
		text, ok := wireText(args[key+at])
		return ok && (a.typ != "string" || len(a.items) != 1 || a.items[0].sum != sumOf(text))
	}
	return false
}

// namedArgs returns the fields or members a write names; rest is what follows
// its key. The values and scores between them can be of any type.
func namedArgs(lname string, rest []any) []any {
	every := func(from, step int) []any {
		var out []any
		for i := from; i < len(rest); i += step {
			out = append(out, rest[i])
		}
		return out
	}
	switch lname {
	case "hset", "hmset": // field value [field value ...]
		return every(0, 2)
	case "hsetnx", "hincrby", "hincrbyfloat": // field value
		return rest[:min(1, len(rest))]
	case "zincrby": // increment member
		return rest[min(1, len(rest)):min(2, len(rest))]
	case "zadd": // [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
		i := 0
		for i < len(rest) {
			s, _ := rest[i].(string)
			switch strings.ToUpper(s) {
			case "NX", "XX", "GT", "LT", "CH", "INCR":
				i++
				continue
			}
			break
		}
		return every(i+1, 2)
	}
	return rest // HDEL, ZREM, SADD, SREM: fields or members only
}

// wireText returns an argument as the client sends it, for the kinds of
// argument whose text is certain. Str formats the others for the record,
// which need not be what Redis stored.
func wireText(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	}
	return "", false
}
