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
