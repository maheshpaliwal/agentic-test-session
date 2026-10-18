package tsql

import (
	"database/sql/driver"
	"strings"

	"tsession/contrib/tsql/mutation"
	"tsession/core"
)

func anySensitive(idents []string) bool {
	for _, id := range idents {
		if core.IsSensitiveKey(id) {
			return true
		}
	}
	return false
}

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
