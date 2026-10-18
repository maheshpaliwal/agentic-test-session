package mutation

import "strings"

type colMap struct {
	args   map[int][]string
	lits   []Literal
	idents []string
}

func appendUniq(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

var notColumn = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "WHERE": true, "ON": true, "WHEN": true, "THEN": true, "ELSE": true,
	"END": true, "SET": true, "VALUES": true, "SELECT": true, "BY": true, "HAVING": true, "NULL": true,
	"TRUE": true, "FALSE": true, "IN": true, "IS": true, "LIKE": true, "ILIKE": true, "BETWEEN": true,
	"ANY": true, "ALL": true, "SOME": true, "AS": true, "CASE": true, "DISTINCT": true, "FROM": true,
	"LIMIT": true, "OFFSET": true, "ESCAPE": true, "INTERVAL": true, "ARRAY": true,
}

// mapCols attributes bind arguments and literals of one statement to columns.
func (p *parser) mapCols(insertCols []string, t []token) colMap {
	n := len(t)
	cm := colMap{args: map[int][]string{}}
	litCols := map[int][]string{}
	benign := map[int]bool{}
	mapped := make([]bool, n)

	for _, tk := range t {
		if tk.typ == tWord || tk.typ == tQuoted {
			cm.idents = append(cm.idents, strings.ToLower(tk.name(Postgres)))
		}
	}

	assign := func(lo, hi int, col string) {
		for k := lo; k < hi && k < n; k++ {
			if t[k].isPunct("(") && k+1 < n && t[k+1].isWord("SELECT") {
				k = closeParen(t, k) // sub-select columns are resolved by the comparison rules
				continue
			}
			switch t[k].typ {
			case tPlaceholder:
				if t[k].arg >= 0 {
					cm.args[t[k].arg] = appendUniq(cm.args[t[k].arg], col)
					mapped[k] = true
				}
			case tString, tNumber:
				litCols[k] = appendUniq(litCols[k], col)
				mapped[k] = true
			}
		}
	}
	// items splits t[lo:hi] at commas of depth d.
	items := func(lo, hi, d int, f func(j, a, b int)) {
		j, start := 0, lo
		for k := lo; k < hi; k++ {
			if t[k].depth == d && t[k].isPunct(",") {
				f(j, start, k)
				j++
				start = k + 1
			}
		}
		f(j, start, hi)
	}

	// INSERT column list vs VALUES tuples / SELECT list.
	ins := -1
	for k, tk := range t {
		if tk.isWord("INSERT") || tk.isWord("REPLACE") {
			ins = k
			break
		}
	}
	if ins >= 0 && len(insertCols) > 0 {
		base := t[ins].depth
	scan:
		for k := ins + 1; k < n; k++ {
			if t[k].depth != base {
				continue
			}
			switch {
			case t[k].isWord("VALUES") || t[k].isWord("VALUE"):
				k++
				for k < n && t[k].isPunct("(") {
					end := closeParen(t, k)
					items(k+1, end, t[k].depth+1, func(j, a, b int) {
						if j < len(insertCols) {
							assign(a, b, insertCols[j])
						}
					})
					k = end + 1
					if k < n && t[k].isPunct(",") {
						k++
					} else {
						break
					}
				}
				break scan
			case t[k].isWord("SELECT"):
				stop := n
				for e := k + 1; e < n; e++ {
					if t[e].depth == base && (t[e].isWord("FROM") || t[e].isWord("WHERE") || t[e].isWord("RETURNING") ||
						t[e].isWord("ON") || t[e].isWord("GROUP") || t[e].isWord("ORDER") || t[e].isWord("LIMIT")) {
						stop = e
						break
					}
				}
				items(k+1, stop, base, func(j, a, b int) {
					if j < len(insertCols) {
						assign(a, b, insertCols[j])
					}
				})
				break scan
			}
		}
	}

	// SET col = expr lists (UPDATE, ON CONFLICT DO UPDATE, ON DUPLICATE KEY UPDATE).
	for k := 0; k < n; k++ {
		isSet := t[k].isWord("SET") || (t[k].isWord("UPDATE") && k > 0 && t[k-1].isWord("KEY"))
		if !isSet {
			continue
		}
		d := t[k].depth
		stop := n
		for e := k + 1; e < n; e++ {
			if t[e].depth < d {
				stop = e
				break
			}
			if t[e].depth == d && (t[e].isWord("WHERE") || t[e].isWord("FROM") || t[e].isWord("RETURNING") ||
				t[e].isWord("ORDER") || t[e].isWord("LIMIT")) {
				stop = e
				break
			}
		}
		items(k+1, stop, d, func(_, a, b int) {
			col, rest := column(t[a:b])
			if col != "" && len(rest) >= 1 && rest[0].isPunct("=") {
				assign(b-len(rest)+1, b, col)
			}
		})
	}

	// Comparison / IN / BETWEEN / LIMIT contexts for everything left.
	for k := 0; k < n; k++ {
		if mapped[k] {
			continue
		}
		isArg := t[k].typ == tPlaceholder && t[k].arg >= 0
		if !isArg && t[k].typ != tString && t[k].typ != tNumber {
			continue
		}
		col, bn := contextCol(t, k)
		switch {
		case bn:
			benign[k] = true
			if isArg {
				cm.args[t[k].arg] = appendUniq(cm.args[t[k].arg], "")
				mapped[k] = true
			}
		case col != "":
			assign(k, k+1, col)
		}
	}
	for k, tk := range t {
		if tk.typ == tString || tk.typ == tNumber {
			cm.lits = append(cm.lits, Literal{Start: tk.start, End: tk.end, Cols: litCols[k], Benign: benign[k]})
		}
	}
	return cm
}

func isOpTok(tk token) bool {
	if tk.typ == tPunct {
		return len(tk.text) == 1 && strings.Contains("=<>!~", tk.text)
	}
	return tk.typ == tWord && (tk.val == "LIKE" || tk.val == "ILIKE" || tk.val == "SIMILAR" || tk.val == "TO" || tk.val == "REGEXP" || tk.val == "RLIKE")
}

// skipOpsBack consumes comparison operator tokens ending at j; returns the new
// index and whether any were consumed.
func skipOpsBack(t []token, j int) (int, bool) {
	any := false
	for j >= 0 && isOpTok(t[j]) {
		j--
		any = true
	}
	if any && j >= 0 && (t[j].isWord("NOT") || t[j].isWord("DISTINCT") || t[j].isWord("IS")) {
		j--
		if j >= 0 && t[j].isWord("IS") {
			j--
		}
	}
	return j, any
}

// contextCol finds the column the operand at k is compared with / assigned to.
func contextCol(t []token, k int) (col string, benign bool) {
	if k > 0 {
		switch {
		case t[k-1].isWord("LIMIT"), t[k-1].isWord("OFFSET"), t[k-1].isWord("FIRST"), t[k-1].isWord("NEXT"):
			return "", true
		}
	}
	j := k - 1
	if j >= 0 && (t[j].isPunct("-") || t[j].isPunct("+")) {
		j--
	}
	if j2, ok := skipOpsBack(t, j); ok {
		return colEndingAt(t, j2), false
	}
	// col [NOT] BETWEEN a AND b
	if j >= 0 && t[j].isWord("BETWEEN") {
		j--
		if j >= 0 && t[j].isWord("NOT") {
			j--
		}
		return colEndingAt(t, j), false
	}
	if j >= 2 && t[j].isWord("AND") && t[j-2].isWord("BETWEEN") {
		b := j - 3
		if b >= 0 && t[b].isWord("NOT") {
			b--
		}
		return colEndingAt(t, b), false
	}
	// enclosing parenthesis: IN (..), = ANY(..), col = func($1)
	if open := enclosingOpen(t, k); open > 0 {
		pj := open - 1
		switch {
		case t[pj].isWord("IN"):
			pj--
			if pj >= 0 && t[pj].isWord("NOT") {
				pj--
			}
			return colEndingAt(t, pj), false
		case t[pj].isWord("ANY"), t[pj].isWord("ALL"), t[pj].isWord("SOME"):
			if j2, ok := skipOpsBack(t, pj-1); ok {
				return colEndingAt(t, j2), false
			}
		case t[pj].typ == tWord && !notColumn[t[pj].val]:
			if j2, ok := skipOpsBack(t, pj-1); ok {
				return colEndingAt(t, j2), false
			}
		}
	}
	// operand on the left: $1 [::cast] op col
	f := k + 1
	if f < len(t) && t[f].isPunct("::") {
		f += 2
	}
	if f < len(t) && isOpTok(t[f]) {
		for f < len(t) && isOpTok(t[f]) {
			f++
		}
		if f < len(t) && t[f].isIdent() && !(t[f].typ == tWord && notColumn[t[f].val]) {
			c, rest := column(t[f:])
			if c != "" && !(len(rest) > 0 && rest[0].isPunct("(")) {
				return c, false
			}
		}
	}
	return "", false
}

func enclosingOpen(t []token, k int) int {
	want := t[k].depth - 1
	for q := k - 1; q >= 0; q-- {
		if t[q].isPunct("(") && t[q].depth == want {
			return q
		}
	}
	return -1
}

// colEndingAt names the column an expression ending at t[j] refers to:
// ident, qualifier.ident, ident::cast or fn(ident).
func colEndingAt(t []token, j int) string {
	if j < 0 || j >= len(t) {
		return ""
	}
	if j >= 2 && t[j-1].isPunct("::") && t[j].isIdent() {
		return colEndingAt(t, j-2)
	}
	if t[j].isPunct(")") {
		q := -1
		for x := j - 1; x >= 0; x-- {
			if t[x].isPunct("(") && t[x].depth == t[j].depth {
				q = x
				break
			}
		}
		if q < 0 {
			return ""
		}
		name := ""
		cnt := 0
		for x := q + 1; x < j; x++ {
			if t[x].isIdent() && !(t[x].typ == tWord && notColumn[t[x].val]) &&
				!(x+1 < j && t[x+1].isPunct("(")) && !(x > 0 && t[x-1].isPunct("::")) &&
				!(x > 0 && t[x-1].isPunct(".")) {
				name = strings.ToLower(t[x].name(Postgres))
				cnt++
			}
		}
		if cnt == 1 {
			return name
		}
		return ""
	}
	if t[j].isIdent() && !(t[j].typ == tWord && notColumn[t[j].val]) {
		return strings.ToLower(t[j].name(Postgres))
	}
	return ""
}
