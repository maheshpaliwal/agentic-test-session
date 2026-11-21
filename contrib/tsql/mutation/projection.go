package mutation

import "strings"

// Projection is one item of a SELECT list or RETURNING clause whose output
// column is not simply the column it reads: an aliased column or an
// expression. Plain "col" / "t.col" / "*" items are not listed because their
// output name is the column name.
type Projection struct {
	Idents []string // lower-case identifiers the item mentions
	Out    string   // lower-case output column name; "" when it cannot be determined
}

var projStop = map[string]bool{
	"FROM": true, "WHERE": true, "GROUP": true, "HAVING": true, "WINDOW": true, "ORDER": true,
	"LIMIT": true, "OFFSET": true, "FETCH": true, "FOR": true, "UNION": true, "INTERSECT": true,
	"EXCEPT": true, "INTO": true,
}

// notAlias are trailing words that end an expression rather than name it.
var notAlias = map[string]bool{"END": true, "NULL": true, "TRUE": true, "FALSE": true, "UNKNOWN": true}

// projections lists the non-trivial items of every SELECT list and RETURNING
// clause in t, at any nesting depth.
func projections(t []token) []Projection {
	var out []Projection
	n := len(t)
	for k := 0; k < n; k++ {
		ret := t[k].isWord("RETURNING")
		if !ret && !t[k].isWord("SELECT") {
			continue
		}
		d := t[k].depth
		lo := k + 1
		if !ret {
			lo = skipWords(t, lo, "DISTINCT", "ALL")
		}
		hi := n
		for e := lo; e < n; e++ {
			if t[e].depth < d {
				hi = e
				break
			}
			if !ret && t[e].depth == d && t[e].typ == tWord && projStop[t[e].val] &&
				!(t[e].val == "FROM" && t[e-1].isWord("DISTINCT")) { // IS DISTINCT FROM
				hi = e
				break
			}
		}
		start := lo
		for e := lo; e <= hi; e++ {
			if e == hi || (t[e].depth == d && t[e].isPunct(",")) {
				if p, ok := projection(t[start:e]); ok {
					out = append(out, p)
				}
				start = e + 1
			}
		}
	}
	return out
}

// plainColumn reports whether it is [qualifier.]...column or [qualifier.]*.
func plainColumn(it []token) bool {
	if len(it) == 0 || len(it)%2 == 0 {
		return false
	}
	for i, tk := range it {
		switch {
		case i%2 == 1:
			if !tk.isPunct(".") {
				return false
			}
		case i == len(it)-1 && tk.isPunct("*"):
		case !tk.isIdent():
			return false
		}
	}
	return true
}

func projection(it []token) (Projection, bool) {
	if len(it) == 0 || plainColumn(it) {
		return Projection{}, false
	}
	p := Projection{Idents: identsOf(it)}
	if len(p.Idents) == 0 {
		return Projection{}, false
	}
	last := it[len(it)-1]
	if len(it) < 2 || !last.isIdent() || (last.typ == tWord && notAlias[last.val]) {
		return p, true
	}
	prev := it[len(it)-2]
	switch {
	case prev.isWord("AS"),
		prev.isPunct(")"), prev.typ == tString, prev.typ == tNumber, // expr alias
		plainColumn(it[:len(it)-1]): // col alias
		p.Out = strings.ToLower(last.name(Postgres))
	}
	return p, true
}
