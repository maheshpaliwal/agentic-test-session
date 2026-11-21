package redisx

import (
	"cmp"
	"maps"
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
	name  string  // hash field or sorted-set member; "" otherwise
	shown string  // the value as recorded, redaction applied
	score float64 // sorted sets only
}

// snap is what one image read saw of a key. It lives from the before-read to
// the after-read of one command and is only used to compute the diff.
type snap struct {
	typ    string // TYPE reply: none, string, hash, list, set, zset, ...
	items  []item
	capped bool // a collection read stopped at ImageLimit: items is a part
}

func (s *snap) absent() bool { return s.typ == "none" }

// same reports whether two items hold the same value.
func (a item) same(b item) bool { return a.shown == b.shown && a.score == b.score }

// value is the item as it appears in a diff.
func (a item) value(typ string) any {
	if typ == "zset" {
		return a.score
	}
	return clip(a.shown)
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
// A created key has only new values and a deleted key only old ones. truncated
// reports that the list is incomplete: it was cut at maxDiff, or a collection
// was read up to ImageLimit only. Two partial reads of one collection cannot
// tell an added element from one the other read did not reach, so then only
// hash fields and sorted-set members present in both are compared, and lists
// and sets get no changes at all.
func diffSnaps(b, a *snap) (out []core.FieldChange, truncated bool) {
	if b == nil || a == nil || b.absent() && a.absent() {
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
		out = diffNamed(typ, named(b, "value"), named(a, "value"), false)
	case "hash", "zset":
		out = diffNamed(typ, named(b, ""), named(a, ""), both && partial)
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

// named indexes the items of s by name; name, when set, names its only item.
func named(s *snap, name string) map[string]item {
	m := make(map[string]item, len(s.items))
	for _, it := range s.items {
		m[cmp.Or(name, it.name)] = it
	}
	return m
}

// diffNamed compares two sets of named values. With common only names on
// both sides are compared.
func diffNamed(typ string, b, a map[string]item, common bool) []core.FieldChange {
	var out []core.FieldChange
	names := set.Set[string]{}
	names.AddSeq(maps.Keys(b))
	names.AddSeq(maps.Keys(a))
	for _, n := range set.Sorted(names) {
		o, oOK := b[n]
		w, wOK := a[n]
		if oOK && wOK && o.same(w) || common && oOK != wOK {
			continue
		}
		fc := core.FieldChange{Field: clip(n)}
		if oOK {
			fc.Old = o.value(typ)
		}
		if wOK {
			fc.New = w.value(typ)
		}
		out = append(out, fc)
	}
	return out
}

// diffSet lists the members only one side has.
func diffSet(b, a *snap) []core.FieldChange {
	only := func(x, y *snap) []string {
		in := named(y, "")
		var out []string
		for _, it := range x.items {
			if _, ok := in[it.name]; !ok {
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
