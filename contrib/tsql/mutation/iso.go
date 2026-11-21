package mutation

import "strings"

// IsoLevel is what a statement text says about a transaction isolation level.
// Only SERIALIZABLE is told apart from the others: it is the level under
// which the tracing layer must not read.
type IsoLevel uint8

const (
	// IsoUnset: the text says nothing about the level.
	IsoUnset IsoLevel = iota
	// IsoSerializable: SERIALIZABLE.
	IsoSerializable
	// IsoOther: a level other than SERIALIZABLE.
	IsoOther
	// IsoUnknown: the text sets the level without showing it (DEFAULT, an
	// expression, RESET, set_config).
	IsoUnknown
)

// isoScope is what a statement sets the isolation level of.
type isoScope uint8

const (
	isoNone    isoScope = iota
	isoBegin            // the transaction BEGIN / START TRANSACTION opens (IsoUnset: the session default)
	isoTx               // the open transaction (Postgres), the next one (MySQL)
	isoSession          // the session default
	isoAll              // both, to a level the text does not show
)

// isoEffect classifies one statement by what it does to isolation levels.
// The forms are, on Postgres: the ISOLATION LEVEL mode of BEGIN and START
// TRANSACTION, SET TRANSACTION, SET SESSION CHARACTERISTICS AS TRANSACTION,
// SET / RESET of default_transaction_isolation and transaction_isolation,
// RESET ALL, DISCARD ALL and set_config; on MySQL: SET [GLOBAL | SESSION]
// TRANSACTION and SET of the transaction_isolation / tx_isolation variable
// (a GLOBAL level only reaches later sessions). What a function or procedure
// does on the server is not seen.
func isoEffect(t []token, d Dialect) (isoScope, IsoLevel) {
	word := func(i int) string {
		if i >= 0 && i < len(t) && t[i].typ == tWord {
			return t[i].val
		}
		return ""
	}
	clause := -1 // index of the level named after ISOLATION LEVEL
	for i := range t {
		if t[i].isWord("ISOLATION") && word(i+1) == "LEVEL" {
			clause = i + 2
			break
		}
	}
	switch word(0) {
	case "BEGIN", "START":
		if word(0) == "START" && word(1) != "TRANSACTION" {
			break
		}
		if clause < 0 {
			return isoBegin, IsoUnset
		}
		return isoBegin, isoWords(t, clause)
	case "SET":
		if clause >= 0 {
			lvl := isoWords(t, clause)
			switch {
			case word(1) == "TRANSACTION":
				return isoTx, lvl
			case d == Postgres && word(1) == "SESSION" && word(2) == "CHARACTERISTICS":
				return isoSession, lvl
			case d == MySQL && word(1) == "GLOBAL":
				return isoNone, IsoUnset
			case d == MySQL && (word(1) == "SESSION" || word(1) == "LOCAL") && word(2) == "TRANSACTION":
				return isoSession, lvl
			}
			return isoAll, IsoUnknown
		}
		for i := range t {
			switch word(i) {
			case "DEFAULT_TRANSACTION_ISOLATION":
				if d != Postgres {
					continue
				}
				if word(1) == "LOCAL" { // gone with the transaction, and never the open one's level
					return isoSession, IsoUnknown
				}
				return isoSession, isoValue(t, i+1)
			case "TRANSACTION_ISOLATION", "TX_ISOLATION":
				lvl := isoValue(t, i+1)
				if d == Postgres {
					return isoTx, lvl
				}
				// MySQL: "SET GLOBAL v", "SET @@GLOBAL.v" (later sessions),
				// "SET @@v" (the next transaction), anything else the session.
				scope := word(i - 1)
				if t[i-1].isPunct(".") {
					scope = word(i - 2)
				}
				switch {
				case scope == "GLOBAL" || scope == "PERSIST" || scope == "PERSIST_ONLY":
					return isoNone, IsoUnset
				case scope == "" && t[i-1].isPunct("@"):
					return isoTx, lvl
				}
				return isoSession, lvl
			}
		}
	case "RESET":
		if d != Postgres {
			break
		}
		switch word(1) {
		case "ALL", "DEFAULT_TRANSACTION_ISOLATION":
			return isoSession, IsoUnknown
		case "TRANSACTION_ISOLATION":
			return isoTx, IsoUnknown
		}
	case "DISCARD":
		if d == Postgres && word(1) == "ALL" {
			return isoSession, IsoUnknown
		}
	}
	// set_config('transaction_isolation' | 'default_transaction_isolation', ...)
	call, name := false, false
	for i := range t {
		call = call || t[i].isWord("SET_CONFIG")
		name = name || t[i].typ == tString && strings.Contains(strings.ToLower(t[i].text), "transaction_isolation")
	}
	if call && name {
		return isoAll, IsoUnknown
	}
	return isoNone, IsoUnset
}

// isoWords reads the level written as keywords at t[i:].
func isoWords(t []token, i int) IsoLevel {
	var w [2]string
	for k := range w {
		if i+k < len(t) && t[i+k].typ == tWord {
			w[k] = t[i+k].val
		}
	}
	return isoNamed(w[0] + " " + w[1])
}

// isoValue reads the level assigned at t[i:]: "= 'x'", "TO x", "= x".
func isoValue(t []token, i int) IsoLevel {
	if i < len(t) && (t[i].isPunct("=") || t[i].isWord("TO")) {
		i++
	} else if i+1 < len(t) && t[i].isPunct(":") && t[i+1].isPunct("=") {
		i += 2
	}
	if i >= len(t) {
		return IsoUnknown
	}
	if t[i].typ == tString {
		if i+1 < len(t) && !t[i+1].isPunct(",") && !t[i+1].isPunct(";") {
			return IsoUnknown // part of an expression
		}
		return isoNamed(t[i].val)
	}
	return isoWords(t, i)
}

// isoNamed maps a level's name, in any of its spellings ("READ COMMITTED",
// "read-committed"), to an IsoLevel.
func isoNamed(s string) IsoLevel {
	s = strings.Join(strings.FieldsFunc(strings.ToUpper(s), func(r rune) bool { return r == ' ' || r == '-' || r == '_' }), " ")
	switch {
	case s == "SERIALIZABLE" || strings.HasPrefix(s, "SERIALIZABLE "):
		return IsoSerializable
	case s == "READ COMMITTED", s == "READ UNCOMMITTED", s == "REPEATABLE READ":
		return IsoOther
	}
	return IsoUnknown
}

// MentionsIsolation is a cheap, allocation-free pre-check for the isolation
// fields of Parse's result (TxIso, DefIso) on a text MayControlTx said no to:
// every statement that sets a level names it with the word "isolation"
// (RESET ALL and DISCARD ALL, which do not, are a maybe of MayControlTx). The
// word is looked for in any case, anywhere in the text; the search goes from
// one "l" to the next, which is a few times the cost of MayControlTx on a
// large text.
func MentionsIsolation(sql string) bool {
	for _, l := range [2]byte{'l', 'L'} {
		for i := 0; ; i++ {
			j := strings.IndexByte(sql[i:], l)
			if j < 0 {
				break
			}
			if i += j; i >= 3 && i+6 <= len(sql) && foldEq(sql[i-3:i+6], "ISOLATION") {
				return true
			}
		}
	}
	return false
}
