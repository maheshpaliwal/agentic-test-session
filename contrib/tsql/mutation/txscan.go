package mutation

import "strings"

// MayControlTx is a cheap, allocation-free pre-check for Parse(sql, d).Tx: it
// reports false only when that is certainly TxNone, so a caller that needs
// nothing but the transaction effect can skip the parse for almost every
// statement. A true answer is only a maybe ("ROLLBACK TO SAVEPOINT s",
// "COMMIT PREPARED 'x'", a keyword inside a literal after a semicolon); Parse
// then decides.
//
// Parse reports a transaction effect when some ';'-separated statement of the
// text starts with one of the keywords txEffect looks at, so the scan checks
// the first token of the text and the first token after every ';' byte. A ';'
// inside a literal or a comment is not a statement separator, but treating it
// as one can only turn the answer into a maybe, never into a wrong no.
func MayControlTx(sql string, d Dialect) bool { return anyStatementStarts(sql, d, isTxKeyword) }

// anyStatementStarts reports whether the first word of the text, or the
// first word after some ';', is one match accepts (or cannot be read
// cheaply, which is a maybe).
func anyStatementStarts(sql string, d Dialect, match func(w string) bool) bool {
	// Skipping blanks and comments after a ';' that sits inside a comment
	// can rescan the same bytes; the budget keeps the scan linear by
	// answering maybe once it has skipped more than the text is long.
	budget := len(sql) + 64
	if keywordAt(sql, 0, d, &budget, match) {
		return true
	}
	for i := 0; ; {
		j := strings.IndexByte(sql[i:], ';')
		if j < 0 {
			return false
		}
		i += j + 1
		if keywordAt(sql, i, d, &budget, match) {
			return true
		}
	}
}

// MaySetIsolation is a cheap, allocation-free pre-check for the isolation
// fields of Parse's result (TxIso, DefIso) on a text MayControlTx said no
// to. A statement that sets a level names it with the word "isolation" and
// is a SET, or calls set_config. The word alone is not enough to pay for a
// parse: it is also a column name and a word in a string, and the text it
// sits in can be a bulk insert of megabytes.
func MaySetIsolation(sql string, d Dialect) bool {
	if !MentionsIsolation(sql) {
		return false
	}
	return anyStatementStarts(sql, d, isSet) || hasWordFold(sql, "SET_CONFIG")
}

func isSet(w string) bool { return len(w) == 3 && foldEq(w, "SET") }

// hasWordFold reports whether s contains the upper-case ASCII word upper in
// any case.
func hasWordFold(s, upper string) bool {
	for i := 0; i+len(upper) <= len(s); i++ {
		if s[i]&^0x20 == upper[0] && foldEq(s[i:i+len(upper)], upper) {
			return true
		}
	}
	return false
}

// keywordAt reports whether the first token at or after s[i:] may be a word
// match accepts. Blanks and comments are skipped exactly as tokenize skips
// them.
func keywordAt(s string, i int, d Dialect, budget *int, match func(w string) bool) bool {
	n := len(s)
	st := i
skip:
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-' && (d == Postgres || i+2 >= n || s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\n' || s[i+2] == '\r'),
			c == '#' && d == MySQL:
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			lvl := 1
			i += 2
			for i < n && lvl > 0 {
				switch {
				case s[i] == '/' && i+1 < n && s[i+1] == '*' && d == Postgres:
					lvl++
					i += 2
				case s[i] == '*' && i+1 < n && s[i+1] == '/':
					lvl--
					i += 2
				default:
					i++
				}
			}
		default:
			break skip
		}
	}
	if *budget -= i - st; *budget < 0 {
		return true
	}
	if i >= n {
		return false
	}
	// The tokenizer upper-cases words with the Unicode tables (a long s or a
	// dotless i folds into an ASCII keyword), so a word with a non-ASCII
	// byte is left to Parse.
	if s[i] >= 0x80 {
		return true
	}
	if !isASCIILetter(s[i]) && s[i] != '_' {
		return false
	}
	j := i + 1
	for j < n && (isASCIILetter(s[j]) || s[j] == '_' || s[j] == '$' || s[j] >= '0' && s[j] <= '9') {
		j++
	}
	if j < n && s[j] >= 0x80 {
		return true
	}
	return match(s[i:j])
}

// isTxKeyword reports whether w is a word txEffect acts on.
func isTxKeyword(w string) bool {
	switch len(w) {
	case 3:
		return foldEq(w, "END")
	case 5:
		// RESET and DISCARD control no transaction; RESET ALL and DISCARD ALL
		// change the session's default isolation level without naming it
		// (see MentionsIsolation), so Parse looks at them too.
		return foldEq(w, "BEGIN") || foldEq(w, "START") || foldEq(w, "ABORT") || foldEq(w, "RESET")
	case 6:
		return foldEq(w, "COMMIT")
	case 7:
		return foldEq(w, "PREPARE") || foldEq(w, "DISCARD")
	case 8:
		return foldEq(w, "ROLLBACK")
	}
	return false
}

func isASCIILetter(c byte) bool { return c|0x20 >= 'a' && c|0x20 <= 'z' }

// foldEq compares an ASCII word of the same length with an upper-case keyword.
func foldEq(w, upper string) bool {
	for i := 0; i < len(w); i++ {
		if w[i]&^0x20 != upper[i] {
			return false
		}
	}
	return true
}
