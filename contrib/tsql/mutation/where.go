package mutation

import (
	"strings"
)

// tail scans the top-level remainder of UPDATE/DELETE for WHERE/RETURNING and
// flags the constructs (complexWord) that make the predicate non-replayable.
func (p *parser) tail(in *Info, t []token, i int, complexWord string) {
	base := t[0].depth
	whereTok := -1
	end := len(t)
	for j := i; j < len(t); j++ {
		if t[j].depth != base {
			continue
		}
		switch {
		case t[j].isWord(complexWord) && whereTok < 0:
			in.Complex = true
		case t[j].isWord("JOIN"):
			in.Complex = true
		case t[j].isWord("WHERE") && whereTok < 0:
			whereTok = j
		case t[j].isWord("RETURNING"):
			p.returning(in, t, j)
			end = j
			j = len(t)
		}
	}
	if whereTok < 0 {
		return
	}
	in.HasWhere = true
	wt := t[whereTok+1 : end]
	if len(wt) == 0 {
		in.HasWhere = false
		return
	}
	if len(wt) >= 2 && wt[0].isWord("CURRENT") && wt[1].isWord("OF") {
		in.CurrentOf = true
		in.Complex = true
	}
	startOff, endOff := wt[0].start, wt[len(wt)-1].end
	in.Where = p.sql[startOff:endOff]
	for _, tk := range wt {
		if tk.typ == tPlaceholder {
			in.WherePH = append(in.WherePH, Placeholder{Start: tk.start - startOff, End: tk.end - startOff, Arg: tk.arg, Dollar: tk.dollar})
		}
		if tk.depth == base && tk.isWord("LIMIT") {
			in.HasLimit = true
		}
	}
	in.Equals = equalities(wt, base)
}

// equalities extracts "col = operand" conjuncts; any top-level OR disables it.
func equalities(wt []token, base int) map[string]Value {
	var conj [][]token
	start := 0
	for j, tk := range wt {
		if tk.depth != base {
			continue
		}
		switch {
		case tk.isWord("OR"):
			return nil
		case tk.isWord("ORDER"), tk.isWord("LIMIT"):
			conj = append(conj, wt[start:j])
			start = -1
		case tk.isWord("AND"):
			conj = append(conj, wt[start:j])
			start = j + 1
		}
		if start < 0 {
			break
		}
	}
	if start >= 0 {
		conj = append(conj, wt[start:])
	}
	out := map[string]Value{}
	for _, c := range conj {
		// strip redundant parentheses around one conjunct
		for len(c) > 2 && c[0].isPunct("(") && c[len(c)-1].isPunct(")") && closeParen(c, 0) == len(c)-1 {
			c = c[1 : len(c)-1]
		}
		col, rest := column(c)
		if col != "" && len(rest) >= 2 && rest[0].isPunct("=") {
			if v, ok := operand(rest[1:]); ok {
				out[col] = v
				continue
			}
		}
		// operand = col
		for k := 1; k < len(c); k++ {
			if c[k].isPunct("=") {
				if v, ok := operand(c[:k]); ok {
					if col2, r2 := column(c[k+1:]); col2 != "" && len(r2) == 0 {
						out[col2] = v
					}
				}
				break
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// column reads [qualifier.]ident from the front of c.
func column(c []token) (string, []token) {
	if len(c) >= 3 && c[0].isIdent() && c[1].isPunct(".") && c[2].isIdent() {
		return strings.ToLower(c[2].name(Postgres)), c[3:]
	}
	if len(c) >= 1 && c[0].isIdent() {
		return strings.ToLower(c[0].name(Postgres)), c[1:]
	}
	return "", c
}
