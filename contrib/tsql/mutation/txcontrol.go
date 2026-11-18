package mutation

// TxControl describes what a statement text does to the session's
// transaction state.
type TxControl int

const (
	// TxNone: the text contains no transaction-control statement.
	TxNone TxControl = iota
	// TxBegin: BEGIN / START TRANSACTION.
	TxBegin
	// TxCommit: COMMIT / END.
	TxCommit
	// TxRollback: ROLLBACK / ABORT of the whole transaction.
	TxRollback
	// TxChain: COMMIT/ROLLBACK ... AND CHAIN; one transaction ends and the
	// next one is already open.
	TxChain
	// TxDetach: PREPARE TRANSACTION; the transaction leaves the session
	// without being committed or rolled back.
	TxDetach
)

// Opens reports whether a transaction is open once the text has run.
func (c TxControl) Opens() bool { return c == TxBegin || c == TxChain }

// Ends reports whether no transaction is open once the text has run.
func (c TxControl) Ends() bool { return c == TxCommit || c == TxRollback || c == TxDetach }

// txEffect classifies one statement by its leading keywords. Savepoint
// rollbacks and two-phase COMMIT/ROLLBACK PREPARED leave the session's
// transaction untouched and yield TxNone.
func txEffect(t []token) TxControl {
	word := func(i int) string {
		if i < len(t) && t[i].typ == tWord {
			return t[i].val
		}
		return ""
	}
	switch word(0) {
	case "BEGIN":
		return TxBegin
	case "START":
		if word(1) == "TRANSACTION" {
			return TxBegin
		}
	case "PREPARE":
		if word(1) == "TRANSACTION" {
			return TxDetach
		}
	case "COMMIT", "END", "ROLLBACK", "ABORT":
		if word(1) == "PREPARED" {
			return TxNone
		}
		rollback := word(0) == "ROLLBACK" || word(0) == "ABORT"
		i := 1
		if w := word(i); w == "WORK" || w == "TRANSACTION" {
			i++
		}
		switch {
		case rollback && (word(i) == "TO" || word(i) == "SAVEPOINT"):
			return TxNone
		case word(i) == "AND" && word(i+1) == "CHAIN":
			return TxChain
		case rollback:
			return TxRollback
		}
		return TxCommit
	}
	return TxNone
}
