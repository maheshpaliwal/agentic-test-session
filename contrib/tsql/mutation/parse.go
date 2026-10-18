package mutation

import (
	"strconv"
	"strings"
)

// Kind classifies a statement.
type Kind int

const (
	Other Kind = iota
	Select
	Insert
	Update
	Delete
	Upsert
	Truncate
	DDL
)

// IsWrite reports whether the statement changes data or schema.
func (k Kind) IsWrite() bool { return k >= Insert }

// IsMutation reports whether the statement produces row-level mutations.
func (k Kind) IsMutation() bool { return k >= Insert && k <= Truncate }

// Op is the core.Mutation.Op string for the kind.
func (k Kind) Op() string {
	switch k {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	case Upsert:
		return "upsert"
	case Truncate:
		return "truncate"
	}
	return ""
}

// Value is an operand: either a bind argument or a literal.
type Value struct {
	Arg     int // 0-based bind argument index; -1 for literals
	Literal any // int64, float64, string or nil (NULL) when Arg == -1
}

// Placeholder locates a bind parameter inside Info.Where.
type Placeholder struct {
	Start, End int // byte range relative to Info.Where
	Arg        int
	Dollar     bool
}

// Info is the structure extracted from one statement.
type Info struct {
	Kind         Kind
	Verb         string // upper-case leading keyword
	Schema       string // unquoted, may be empty
	Table        string // unquoted
	TableSQL     string // table name exactly as written (schema-qualified, quoted)
	TableRef     string // TableSQL plus alias, usable in FROM
	Tables       []string
	HasReturning bool
	ReturningAll bool // RETURNING *
	Columns      []string
	Values       map[string]Value // single-row INSERT: column -> operand
	MultiRow     bool
	HasWhere     bool
	Where        string
	WherePH      []Placeholder
	HasLimit     bool             // WHERE tail contains LIMIT
	Equals       map[string]Value // top-level AND-ed "col = operand" predicates
	Complex      bool             // predicate cannot be replayed (joins, FROM/USING, CTE writes)
	Named        bool             // uses :name placeholders
	WritesInCTE  bool
	Dollar       bool // uses $n placeholders
	QMark        bool // uses ? placeholders
	End          int  // byte offset just past the last non-';' token
	Multi        bool // more than one statement in the text
	CurrentOf    bool // WHERE CURRENT OF <cursor>
	Bulk         bool // COPY ... FROM (bulk load, row set unknown)

	// ArgCols maps a 0-based bind argument to the column(s) it is assigned to
	// or compared with (INSERT column list vs VALUES, SET col = $n, col op $n,
	// IN lists, ON CONFLICT DO UPDATE SET ...). A column "" marks an argument
	// that is known to be harmless (LIMIT/OFFSET). Arguments absent from the
	// map could not be attributed to a column.
	ArgCols map[int][]string
	// Idents lists every identifier/keyword word of the statement (lower
	// case); callers use it to decide whether the statement touches a
	// sensitive-named column when an argument could not be attributed.
	Idents []string
	// Literals lists every string/number literal with the column(s) it is
	// assigned to or compared with (nil when unknown).
	Literals []Literal
}

// Literal is a string or number literal located in the statement text.
type Literal struct {
	Start, End int
	Cols       []string
	Benign     bool // LIMIT/OFFSET operand
}

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

// ImageSelect builds "SELECT * FROM <table> WHERE <predicate> [LIMIT n]" that
// replays the predicate of an UPDATE/DELETE, renumbering $n placeholders so
// only referenced arguments are needed. argIdx lists, in order, the indexes
// into the original statement's arguments to pass. reason is non-empty when
// the predicate cannot be isolated.
func (in Info) ImageSelect(limit int) (query string, argIdx []int, reason string) {
	switch {
	case in.Kind != Update && in.Kind != Delete:
		return "", nil, "not_update_or_delete"
	case in.CurrentOf:
		return "", nil, "where_current_of"
	case in.Complex:
		return "", nil, "complex_statement"
	case in.Named:
		return "", nil, "named_placeholders"
	case !in.HasWhere:
		return "", nil, "no_predicate"
	case in.TableRef == "":
		return "", nil, "no_table"
	}
	var b strings.Builder
	b.WriteString("SELECT * FROM ")
	b.WriteString(in.TableRef)
	b.WriteString(" WHERE ")
	renum := map[int]int{}
	last := 0
	for _, ph := range in.WherePH {
		b.WriteString(in.Where[last:ph.Start])
		last = ph.End
		if !ph.Dollar {
			b.WriteByte('?')
			argIdx = append(argIdx, ph.Arg)
			continue
		}
		n, ok := renum[ph.Arg]
		if !ok {
			argIdx = append(argIdx, ph.Arg)
			n = len(argIdx)
			renum[ph.Arg] = n
		}
		b.WriteString("$" + strconv.Itoa(n))
	}
	b.WriteString(in.Where[last:])
	if !in.HasLimit && limit > 0 {
		b.WriteString(" LIMIT " + strconv.Itoa(limit))
	}
	return b.String(), argIdx, ""
}
