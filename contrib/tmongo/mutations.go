package tmongo

import (
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

const store = "mongo"

var writeCmds = map[string]bool{
	"insert": true, "update": true, "delete": true, "findandmodify": true, "bulkwrite": true,
	"create": true, "drop": true, "dropdatabase": true, "createindexes": true, "dropindexes": true,
	"renamecollection": true, "collmod": true,
}

// isWrite classifies a command; aggregate with $out/$merge writes too.
func isWrite(name string, body doc) bool {
	if writeCmds[name] {
		return true
	}
	if name == "aggregate" {
		return outTarget(body) != ""
	}
	return false
}

func outTarget(body doc) string {
	for _, st := range asArr(body["pipeline"]) {
		d := asDoc(st)
		if o, ok := d["$out"]; ok {
			if s, ok := o.(string); ok {
				return s
			}
			return str(asDoc(o)["coll"])
		}
		if m, ok := d["$merge"]; ok {
			if s, ok := m.(string); ok {
				return s
			}
			into := asDoc(m)["into"]
			if s, ok := into.(string); ok {
				return s
			}
			return str(asDoc(into)["coll"])
		}
	}
	return ""
}

func pkFrom(q doc) map[string]any {
	if id, ok := q["_id"]; ok {
		if d, isDoc := id.(doc); !isDoc || !hasOperator(d) {
			return map[string]any{"_id": id}
		}
	}
	return map[string]any{"filter": q}
}

func hasOperator(d doc) bool {
	for k := range d {
		if strings.HasPrefix(k, "$") {
			return true
		}
	}
	return false
}

func failedIdx(reply doc) map[int64]bool {
	out := map[int64]bool{}
	for _, e := range asArr(reply["writeErrors"]) {
		out[num(asDoc(e)["index"])] = true
	}
	return out
}

func mutationsFor(in *inflight, reply doc) []core.Mutation {
	mk := func(entity, op string, pk map[string]any) core.Mutation {
		return core.Mutation{Store: store, Entity: entity, PK: pk, Op: op, Source: "sdk"}
	}
	var out []core.Mutation
	failed := failedIdx(reply)
	switch in.cmd {
	case "insert":
		for i, d := range asArr(in.body["documents"]) {
			if failed[int64(i)] {
				continue
			}
			m := mk(in.ent, "insert", nil)
			if dd := asDoc(d); dd != nil {
				if id, ok := dd["_id"]; ok {
					m.PK = map[string]any{"_id": id}
				}
			}
			m.After = core.Payload(d)
			out = append(out, m)
		}
	case "update":
		if num(reply["n"]) == 0 {
			return nil
		}
		upserted := map[int64]any{}
		for _, u := range asArr(reply["upserted"]) {
			ud := asDoc(u)
			upserted[num(ud["index"])] = ud["_id"]
		}
		for i, s := range asArr(in.body["updates"]) {
			if failed[int64(i)] {
				continue
			}
			sd := asDoc(s)
			var m core.Mutation
			if id, ok := upserted[int64(i)]; ok {
				m = mk(in.ent, "upsert", map[string]any{"_id": id})
			} else {
				m = mk(in.ent, "update", pkFrom(asDoc(sd["q"])))
				if truthy(sd["multi"]) {
					m.Op = "update_many"
				}
			}
			m.After = core.Payload(sd["u"]) // the update spec; the post-image is not in the reply
			out = append(out, m)
		}
	case "delete":
		if num(reply["n"]) == 0 {
			return nil
		}
		for i, s := range asArr(in.body["deletes"]) {
			if failed[int64(i)] {
				continue
			}
			sd := asDoc(s)
			op := "delete"
			if num(sd["limit"]) == 0 {
				op = "delete_many"
			}
			out = append(out, mk(in.ent, op, pkFrom(asDoc(sd["q"]))))
		}
	case "findandmodify":
		out = append(out, findAndModify(in, reply, mk)...)
	case "bulkwrite":
		out = append(out, bulk(in, reply, mk)...)
	case "aggregate":
		out = append(out, mk(outTarget(in.body), "aggregate_out", nil))
	default: // DDL: create, drop, createIndexes, ...
		out = append(out, mk(in.ent, in.cmd, nil))
	}
	return out
}

func findAndModify(in *inflight, reply doc, mk func(string, string, map[string]any) core.Mutation) []core.Mutation {
	val := asDoc(reply["value"])
	leo := asDoc(reply["lastErrorObject"])
	upserted := leo["upserted"]
	if val == nil && upserted == nil {
		return nil // nothing matched
	}
	b := in.body
	pk := pkFrom(asDoc(b["query"]))
	if id, ok := val["_id"]; ok {
		pk = map[string]any{"_id": id}
	} else if upserted != nil {
		pk = map[string]any{"_id": upserted}
	}
	m := mk(in.ent, "update", pk)
	switch {
	case truthy(b["remove"]):
		m.Op = "delete"
		m.Before = core.Payload(val)
	case truthy(b["new"]):
		m.After = core.Payload(val)
	default:
		m.Before = core.Payload(val)
		m.After = core.Payload(b["update"]) // update spec; server returned only the pre-image
	}
	if upserted != nil {
		m.Op = "upsert"
	}
	return []core.Mutation{m}
}

func bulk(in *inflight, reply doc, mk func(string, string, map[string]any) core.Mutation) []core.Mutation {
	var nss []string
	for _, n := range asArr(in.body["nsInfo"]) {
		s := str(asDoc(n)["ns"])
		nss = append(nss, s[strings.Index(s, ".")+1:])
	}
	var out []core.Mutation
	for _, o := range asArr(in.body["ops"]) {
		od := asDoc(o)
		var kind string
		var idx int64
		for _, k := range []string{"insert", "update", "delete"} {
			if v, ok := od[k]; ok {
				kind, idx = k, num(v)
			}
		}
		if kind == "" || int(idx) >= len(nss) {
			continue
		}
		switch kind {
		case "insert":
			d := asDoc(od["document"])
			m := mk(nss[idx], "insert", map[string]any{"_id": d["_id"]})
			m.After = core.Payload(d)
			out = append(out, m)
		case "update":
			m := mk(nss[idx], "update", pkFrom(asDoc(od["filter"])))
			m.After = core.Payload(od["updateMods"])
			out = append(out, m)
		case "delete":
			out = append(out, mk(nss[idx], "delete", pkFrom(asDoc(od["filter"]))))
		}
	}
	return out
}

// summarize reduces a server reply to a small, useful object.
func summarize(reply doc) doc {
	out := doc{}
	for _, k := range []string{"ok", "n", "nModified", "upserted", "writeErrors", "errmsg", "code",
		"lastErrorObject", "value"} {
		if v, ok := reply[k]; ok {
			out[k] = v
		}
	}
	if c := asDoc(reply["cursor"]); c != nil {
		out["cursor"] = doc{"id": c["id"], "count": len(batch(c))}
	}
	if v := asArr(reply["values"]); v != nil {
		out["values"] = len(v)
	}
	return out
}

func batch(cursor doc) []any {
	if b := asArr(cursor["firstBatch"]); b != nil {
		return b
	}
	return asArr(cursor["nextBatch"])
}

func readSummary(in *inflight, reply doc) (core.ReadSummary, bool) {
	rs := core.ReadSummary{Store: store, Entity: in.ent, Statement: in.cmd}
	switch {
	case in.body["filter"] != nil:
		rs.Args = core.Payload(in.body["filter"])
	case in.body["query"] != nil:
		rs.Args = core.Payload(in.body["query"])
	case in.body["pipeline"] != nil:
		rs.Args = core.Payload(in.body["pipeline"])
	}
	if c := asDoc(reply["cursor"]); c != nil {
		docs := batch(c)
		rs.RowCount = len(docs)
		for i, d := range docs {
			if id, ok := asDoc(d)["_id"]; ok && i < 20 {
				rs.PKs = append(rs.PKs, fmtID(id))
			}
		}
		if len(docs) > 0 {
			rs.Sample = core.Payload(docs[0])
		}
		return rs, true
	}
	if _, ok := reply["n"]; ok {
		rs.RowCount = int(num(reply["n"]))
		return rs, true
	}
	if v := asArr(reply["values"]); v != nil {
		rs.RowCount = len(v)
		return rs, true
	}
	return rs, false
}

func fmtID(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	p := core.Payload(v)
	return string(p.Inline)
}
