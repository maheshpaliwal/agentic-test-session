package tsql

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func anySensitive(idents []string) bool { return slices.ContainsFunc(idents, core.IsSensitiveKey) }

// redactSQL masks string/number literals that are assigned to or compared
// with sensitive columns (password = 'x', INSERT (token) VALUES ('y')). A
// literal whose column cannot be determined is masked too when the statement
// mentions any sensitive-named identifier. Bind placeholders are untouched.
func redactSQL(q string, in mutation.Info) string {
	if len(in.Literals) == 0 {
		return q
	}
	sens, known := false, false
	var b strings.Builder
	last := 0
	for _, l := range in.Literals {
		hide := false
		for _, c := range l.Cols {
			if c != "" && core.IsSensitiveKey(c) {
				hide = true
			}
		}
		if !hide && len(l.Cols) == 0 && !l.Benign {
			if !known {
				sens, known = anySensitive(in.Idents), true
			}
			hide = sens
		}
		if !hide || l.Start < last || l.End > len(q) {
			continue
		}
		b.WriteString(q[last:l.Start])
		b.WriteString("'" + core.RedactedValue + "'")
		last = l.End
	}
	if last == 0 {
		return q
	}
	b.WriteString(q[last:])
	return b.String()
}

// argMask decides which positional arguments to mask. Each argument is
// attributed to its column by the parser; if any argument cannot be
// attributed and the statement mentions a sensitive-named identifier, all
// arguments are masked and conservative is true.
func argMask(in mutation.Info, args []driver.NamedValue) (mask []bool, conservative bool) {
	if len(args) == 0 {
		return nil, false
	}
	mask = make([]bool, len(args))
	unmapped := false
	for i, a := range args {
		cols, ok := in.ArgCols[i]
		if !ok || a.Name != "" {
			unmapped = true
			continue
		}
		for _, c := range cols {
			if c != "" && core.IsSensitiveKey(c) {
				mask[i] = true
			}
		}
	}
	if unmapped && anySensitive(in.Idents) {
		for i := range mask {
			mask[i] = true
		}
		return mask, true
	}
	return mask, false
}

// aliasMask finds the result columns that carry the value of a
// sensitive-named column under another name ("password AS p", also through
// subqueries). all is true when such a value ends up in an output column
// whose name cannot be determined (an expression without alias).
func aliasMask(in mutation.Info) (cols map[string]bool, all bool) {
	for pass := 0; pass < 4; pass++ { // a few rounds resolve aliases of aliases
		changed := false
		for _, p := range in.Projections {
			sens := false
			for _, id := range p.Idents {
				if cols[id] || core.IsSensitiveKey(id) {
					sens = true
					break
				}
			}
			switch {
			case !sens:
			case p.Out == "":
				return nil, true
			case !cols[p.Out]:
				if cols == nil {
					cols = map[string]bool{}
				}
				cols[p.Out] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return cols, false
}

// maskAliased masks, in place, the result-row values that field-name
// redaction would miss because the statement renames a sensitive column.
func (t *stmtTrace) maskAliased(rows []map[string]any) {
	if len(rows) == 0 || len(t.info.Projections) == 0 {
		return
	}
	cols, all := aliasMask(t.info)
	if !all && len(cols) == 0 {
		return
	}
	for _, row := range rows {
		for k := range row {
			if all || cols[strings.ToLower(k)] {
				row[k] = core.RedactedValue
			}
		}
	}
	if all {
		t.flag("rows_redacted_conservatively")
	}
}

var (
	reQuotedValue = regexp.MustCompile(`'(?:[^']|'')*'`)
	reKeyDetail   = regexp.MustCompile(`=\([^)]*\)`) // Postgres "Key (col)=(value)"
)

// redactErr returns err with its text scrubbed for recording (the
// application still receives the original error). Database errors quote row
// values ("Duplicate entry 'x' for key ..."), so when maskQuoted is set every
// quoted value is masked as well.
func redactErr(err error, maskQuoted bool) error {
	if err == nil {
		return nil
	}
	s := core.RedactText(err.Error())
	if maskQuoted {
		s = reQuotedValue.ReplaceAllString(s, "'"+core.RedactedValue+"'")
		s = reKeyDetail.ReplaceAllString(s, "=("+core.RedactedValue+")")
	}
	return errors.New(s)
}

// sensitive reports whether the statement involves data that must not be
// recorded: a skipped table, a masked literal or argument, or any
// sensitive-named identifier.
func (t *stmtTrace) sensitive() bool {
	if t.skip || t.sqlText != t.query {
		return true
	}
	return slices.Contains(t.mask, true) || anySensitive(t.info.Idents)
}

// setErr records a statement error on the span, redacted.
func (t *stmtTrace) setErr(err error) {
	if err != nil {
		t.span.SetErr(redactErr(err, t.sensitive()))
	}
}
