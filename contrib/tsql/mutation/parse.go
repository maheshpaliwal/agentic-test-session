package mutation

import ()

type parser struct {
	sql string
	t   []token
	d   Dialect
}

// Parse classifies sql. It never fails; unknown input yields Kind Other.
// For multi-statement text the first write statement (else the first
// statement) is described and Multi/Complex are set so no image is replayed.
func Parse(sql string, d Dialect) Info {
	toks, named := tokenize(sql, d)
	for len(toks) > 0 && toks[len(toks)-1].isPunct(";") {
		toks = toks[:len(toks)-1]
	}
	p := &parser{sql: sql, t: toks, d: d}
	segs := splitStatements(toks)
	var in Info
	pick := 0
	infos := make([]Info, len(segs))
	for i, s := range segs {
		infos[i] = p.stmt(s)
		if i > 0 && !infos[pick].Kind.IsWrite() && infos[i].Kind.IsWrite() {
			pick = i
		}
	}
	if len(segs) > 0 {
		in = infos[pick]
	}
	if len(toks) > 0 {
		in.End = toks[len(toks)-1].end
	}
	if len(segs) > 1 {
		in.Multi = true
		in.Complex = true
	}
	in.Named = named
	for _, t := range toks {
		if t.typ == tPlaceholder {
			if t.dollar {
				in.Dollar = true
			} else if t.arg >= 0 {
				in.QMark = true
			}
		}
	}
	in.ArgCols = map[int][]string{}
	seen := map[string]bool{}
	for i, s := range segs {
		r := p.mapCols(infos[i].Columns, s)
		for a, cs := range r.args {
			for _, c := range cs {
				in.ArgCols[a] = appendUniq(in.ArgCols[a], c)
			}
		}
		in.Literals = append(in.Literals, r.lits...)
		for _, id := range r.idents {
			if !seen[id] {
				seen[id] = true
				in.Idents = append(in.Idents, id)
			}
		}
	}
	return in
}

// splitStatements splits on ';' tokens, dropping empty statements.
func splitStatements(t []token) [][]token {
	var out [][]token
	start := 0
	for i, tk := range t {
		if tk.isPunct(";") {
			if i > start {
				out = append(out, t[start:i])
			}
			start = i + 1
		}
	}
	if start < len(t) {
		out = append(out, t[start:])
	}
	return out
}

func (p *parser) stmt(t []token) Info {
	i := 0
	for i < len(t) && t[i].isPunct("(") {
		i++
	}
	if i >= len(t) {
		return Info{}
	}
	if t[i].isWord("WITH") {
		return p.with(t, i+1)
	}
	return p.verb(t[i:])
}

func (p *parser) with(t []token, i int) Info {
	if i < len(t) && t[i].isWord("RECURSIVE") {
		i++
	}
	var cte *Info
	for i < len(t) {
		// name [(cols)] AS [[NOT] MATERIALIZED] ( body )
		i++ // name
		for i < len(t) && !t[i].isWord("AS") {
			if t[i].isPunct("(") {
				i = closeParen(t, i)
			}
			i++
		}
		i++ // AS
		for i < len(t) && !t[i].isPunct("(") {
			i++
		}
		if i >= len(t) {
			break
		}
		end := closeParen(t, i)
		body := t[i+1 : min(end, len(t))]
		if cte == nil && len(body) > 0 {
			if in := p.verb(body); in.Kind.IsMutation() {
				cte = &in
			}
		}
		i = end + 1
		if i < len(t) && t[i].isPunct(",") {
			i++
			continue
		}
		break
	}
	main := Info{}
	if i < len(t) {
		main = p.verb(t[i:])
	}
	if main.Kind.IsMutation() {
		return main
	}
	if cte != nil {
		cte.Complex = true
		cte.WritesInCTE = true
		return *cte
	}
	return main
}

// closeParen returns the index of the ')' matching the '(' at i (or len(t)).
func closeParen(t []token, i int) int {
	d := t[i].depth
	for j := i + 1; j < len(t); j++ {
		if t[j].isPunct(")") && t[j].depth == d {
			return j
		}
	}
	return len(t)
}

func (p *parser) verb(t []token) Info {
	for len(t) > 0 && t[0].isPunct("(") {
		t = t[1:]
	}
	if len(t) == 0 || t[0].typ != tWord {
		return Info{}
	}
	in := Info{Verb: t[0].val}
	switch t[0].val {
	case "SELECT", "VALUES", "TABLE":
		in.Kind = Select
		p.selectStmt(&in, t)
	case "INSERT", "REPLACE":
		p.insertStmt(&in, t)
	case "UPDATE":
		in.Kind = Update
		p.updateStmt(&in, t)
	case "DELETE":
		in.Kind = Delete
		p.deleteStmt(&in, t)
	case "TRUNCATE":
		in.Kind = Truncate
		i := 1
		if i < len(t) && t[i].isWord("TABLE") {
			i++
		}
		i = skipWords(t, i, "ONLY")
		i = p.tableName(&in, t, i)
		for i+1 < len(t) && t[i].isPunct(",") {
			var tmp Info
			i = p.tableName(&tmp, t, i+1)
			in.Tables = append(in.Tables, tmp.Table)
			in.Complex = true
		}
	case "MERGE":
		// MERGE INTO target [AS a] USING ...: a write whose rows we cannot replay.
		in.Kind = Upsert
		in.Complex = true
		i := 1
		if i < len(t) && t[i].isWord("INTO") {
			i++
		}
		i = skipWords(t, i, "ONLY")
		p.tableName(&in, t, i)
		for j := range t {
			if t[j].isWord("RETURNING") && t[j].depth == t[0].depth {
				p.returning(&in, t, j)
			}
		}
	case "COPY":
		// COPY tbl [(cols)] FROM ... loads rows; COPY ... TO and COPY (query) TO read.
		if len(t) > 1 && t[1].isIdent() {
			var tmp Info
			p.tableName(&tmp, t, 1)
			for j := 1; j < len(t); j++ {
				if t[j].depth == t[0].depth && t[j].isWord("FROM") {
					in = Info{Verb: "COPY", Kind: Insert, Complex: true, Bulk: true,
						Schema: tmp.Schema, Table: tmp.Table, TableSQL: tmp.TableSQL, TableRef: tmp.TableRef, Tables: tmp.Tables}
					break
				}
				if t[j].depth == t[0].depth && t[j].isWord("TO") {
					break
				}
			}
		}
	case "CREATE", "ALTER", "DROP", "RENAME", "GRANT", "REVOKE", "COMMENT":
		in.Kind = DDL
		for i := 1; i < len(t) && i < 8; i++ {
			if t[i].isWord("TABLE") {
				i++
				for i < len(t) && (t[i].isWord("IF") || t[i].isWord("NOT") || t[i].isWord("EXISTS") || t[i].isWord("ONLY")) {
					i++
				}
				p.tableName(&in, t, i)
				break
			}
		}
	}
	return in
}

// tableName parses [db.][schema.]table at t[i:], filling Schema/Table/TableSQL.
// It returns the index of the first token after the name.
func (p *parser) tableName(in *Info, t []token, i int) int {
	if i >= len(t) || !t[i].isIdent() {
		return i
	}
	st := i
	parts := []string{t[i].name(p.d)}
	i++
	for i+1 < len(t) && t[i].isPunct(".") && t[i+1].isIdent() {
		parts = append(parts, t[i+1].name(p.d))
		i += 2
	}
	in.Table = parts[len(parts)-1]
	if len(parts) > 1 {
		in.Schema = parts[len(parts)-2]
	}
	in.TableSQL = p.sql[t[st].start:t[i-1].end]
	in.TableRef = in.TableSQL
	in.Tables = append(in.Tables, in.Table)
	return i
}

var aliasStop = map[string]bool{
	"SET": true, "WHERE": true, "USING": true, "FROM": true, "JOIN": true, "INNER": true, "LEFT": true,
	"RIGHT": true, "CROSS": true, "FULL": true, "NATURAL": true, "ON": true, "VALUES": true,
	"SELECT": true, "RETURNING": true, "ORDER": true, "LIMIT": true, "GROUP": true, "HAVING": true,
	"UNION": true, "DEFAULT": true, "PARTITION": true, "STRAIGHT_JOIN": true, "FOR": true, "OFFSET": true,
	"WINDOW": true, "EXCEPT": true, "INTERSECT": true, "LATERAL": true, "OVERRIDING": true, "OUTER": true,
}

// alias consumes an optional [AS] alias after the table and extends TableRef.
func (p *parser) alias(in *Info, t []token, i int, first int) int {
	if i < len(t) && t[i].isPunct("*") { // UPDATE tbl *
		i++
	}
	switch {
	case i+1 < len(t) && t[i].isWord("AS") && t[i+1].isIdent():
		i += 2
	case i < len(t) && t[i].isIdent() && !(t[i].typ == tWord && aliasStop[t[i].val]):
		i++
	default:
		return i
	}
	in.TableRef = p.sql[t[first].start:t[i-1].end]
	return i
}

func skipWords(t []token, i int, words ...string) int {
outer:
	for i < len(t) {
		for _, w := range words {
			if t[i].isWord(w) {
				i++
				continue outer
			}
		}
		break
	}
	return i
}
