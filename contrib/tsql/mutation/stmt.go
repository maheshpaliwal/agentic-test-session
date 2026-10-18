package mutation

import (
	"strconv"
	"strings"
)

func (p *parser) selectStmt(in *Info, t []token) {
	base := 0
	if len(t) > 0 {
		base = t[0].depth
	}
	seenFrom := false
	for i := 1; i < len(t); i++ {
		if t[i].depth != base {
			continue
		}
		switch {
		case t[i].isWord("FROM") && !seenFrom:
			seenFrom = true
			p.fromTable(in, t, i+1)
		case t[i].isWord("JOIN") && seenFrom:
			p.fromTable(in, t, i+1)
		}
	}
}

func (p *parser) fromTable(in *Info, t []token, i int) {
	i = skipWords(t, i, "ONLY", "LATERAL")
	if i < len(t) && t[i].isIdent() {
		var tmp Info
		p.tableName(&tmp, t, i)
		if in.Table == "" {
			in.Schema, in.Table, in.TableSQL, in.TableRef = tmp.Schema, tmp.Table, tmp.TableSQL, tmp.TableRef
		}
		in.Tables = append(in.Tables, tmp.Table)
	}
}

func (p *parser) insertStmt(in *Info, t []token) {
	in.Kind = Insert
	if t[0].val == "REPLACE" {
		in.Kind = Upsert
	}
	i := skipWords(t, 1, "LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE")
	if i < len(t) && t[i].isWord("INTO") {
		i++
	}
	if i >= len(t) {
		return
	}
	first := i
	i = p.tableName(in, t, i)
	i = p.alias(in, t, i, first)
	// optional column list
	if i < len(t) && t[i].isPunct("(") {
		end := closeParen(t, i)
		if i+1 < len(t) && !t[i+1].isWord("SELECT") && !t[i+1].isWord("WITH") {
			for j := i + 1; j < end; j++ {
				if t[j].isIdent() && t[j].depth == t[i].depth+1 {
					in.Columns = append(in.Columns, t[j].name(p.d))
				}
			}
			i = end + 1
		}
	}
	base := t[0].depth
	if i < len(t) && t[i].isWord("VALUES") || i < len(t) && t[i].isWord("VALUE") {
		p.values(in, t, i+1)
	}
	for j := i; j < len(t); j++ {
		if t[j].depth != base {
			continue
		}
		switch {
		case t[j].isWord("ON") && j+1 < len(t) && t[j+1].isWord("CONFLICT"):
			in.Kind = Upsert
		case t[j].isWord("ON") && j+2 < len(t) && t[j+1].isWord("DUPLICATE"):
			in.Kind = Upsert
		case t[j].isWord("RETURNING"):
			p.returning(in, t, j)
		}
	}
	if in.MultiRow || len(in.Columns) == 0 {
		in.Values = nil
	}
}

func (p *parser) values(in *Info, t []token, i int) {
	if i >= len(t) || !t[i].isPunct("(") {
		return
	}
	end := closeParen(t, i)
	if end+1 < len(t) && t[end+1].isPunct(",") {
		in.MultiRow = true
		return
	}
	d := t[i].depth + 1
	in.Values = map[string]Value{}
	col, start := 0, i+1
	flush := func(stop int) {
		if col < len(in.Columns) {
			if v, ok := operand(t[start:stop]); ok {
				in.Values[in.Columns[col]] = v
			}
		}
		col++
		start = stop + 1
	}
	for j := i + 1; j < end; j++ {
		if t[j].depth == d && t[j].isPunct(",") {
			flush(j)
		}
	}
	flush(end)
}

// operand converts a token run that is exactly one placeholder/literal.
func operand(t []token) (Value, bool) {
	if len(t) == 2 && t[0].isPunct("-") && t[1].typ == tNumber {
		v, ok := operand(t[1:])
		switch x := v.Literal.(type) {
		case int64:
			v.Literal = -x
		case float64:
			v.Literal = -x
		}
		return v, ok
	}
	if len(t) != 1 {
		return Value{}, false
	}
	switch tk := t[0]; tk.typ {
	case tPlaceholder:
		if tk.arg < 0 {
			return Value{}, false
		}
		return Value{Arg: tk.arg}, true
	case tNumber:
		if n, err := strconv.ParseInt(tk.val, 10, 64); err == nil {
			return Value{Arg: -1, Literal: n}, true
		}
		if f, err := strconv.ParseFloat(tk.val, 64); err == nil {
			return Value{Arg: -1, Literal: f}, true
		}
	case tString:
		if strings.HasPrefix(tk.text, "$") {
			return Value{}, false
		}
		return Value{Arg: -1, Literal: tk.val}, true
	case tWord:
		switch tk.val {
		case "NULL":
			return Value{Arg: -1}, true
		case "TRUE":
			return Value{Arg: -1, Literal: true}, true
		case "FALSE":
			return Value{Arg: -1, Literal: false}, true
		}
	}
	return Value{}, false
}

func (p *parser) returning(in *Info, t []token, j int) {
	in.HasReturning = true
	if j+1 < len(t) && t[j+1].isPunct("*") && j+2 == len(t) {
		in.ReturningAll = true
	}
}

func (p *parser) updateStmt(in *Info, t []token) {
	i := skipWords(t, 1, "ONLY", "LOW_PRIORITY", "IGNORE")
	if i >= len(t) {
		return
	}
	first := i
	i = p.tableName(in, t, i)
	i = p.alias(in, t, i, first)
	if i < len(t) && !t[i].isWord("SET") {
		in.Complex = true // UPDATE a JOIN b ..., UPDATE a, b ...
	}
	p.tail(in, t, i, "FROM")
}

func (p *parser) deleteStmt(in *Info, t []token) {
	i := skipWords(t, 1, "LOW_PRIORITY", "QUICK", "IGNORE")
	if i < len(t) && t[i].isWord("FROM") {
		i++
		i = skipWords(t, i, "ONLY")
		if i >= len(t) {
			return
		}
		first := i
		i = p.tableName(in, t, i)
		i = p.alias(in, t, i, first)
	} else if i < len(t) {
		// MySQL multi-table form: DELETE t1 FROM t1 JOIN ...
		p.tableName(in, t, i)
		in.Complex = true
	}
	p.tail(in, t, i, "USING")
}
