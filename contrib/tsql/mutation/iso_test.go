package mutation

import (
	"strings"
	"testing"
)

func TestIsolationEffect(t *testing.T) {
	const (
		no  = IsoUnset
		ser = IsoSerializable
		oth = IsoOther
		unk = IsoUnknown
	)
	for _, tc := range []struct {
		d      Dialect
		sql    string
		tx     TxControl
		txIso  IsoLevel
		defIso IsoLevel
	}{
		// Postgres: the transaction a statement opens
		{Postgres, "BEGIN", TxBegin, no, no},
		{Postgres, "BEGIN ISOLATION LEVEL SERIALIZABLE", TxBegin, ser, no},
		{Postgres, "begin work isolation level serializable, read only", TxBegin, ser, no},
		{Postgres, "BEGIN TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE NOT DEFERRABLE", TxBegin, ser, no},
		{Postgres, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ", TxBegin, oth, no},
		{Postgres, "START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY", TxBegin, oth, no},
		{Postgres, "BEGIN /* c */ ISOLATION -- c\n LEVEL SERIALIZABLE;", TxBegin, ser, no},
		// the open transaction
		{Postgres, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxNone, ser, no},
		{Postgres, "SET TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE", TxNone, ser, no},
		{Postgres, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", TxNone, oth, no},
		{Postgres, "SET TRANSACTION READ ONLY", TxNone, no, no},
		{Postgres, "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'", TxNone, no, no},
		{Postgres, "SET transaction_isolation = 'serializable'", TxNone, ser, no},
		{Postgres, "SET LOCAL transaction_isolation TO 'repeatable read'", TxNone, oth, no},
		{Postgres, "SET transaction_isolation TO DEFAULT", TxNone, unk, no},
		{Postgres, "RESET transaction_isolation", TxNone, unk, no},
		// the session default
		{Postgres, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxNone, no, ser},
		{Postgres, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED", TxNone, no, oth},
		{Postgres, "SET default_transaction_isolation = 'serializable'", TxNone, no, ser},
		{Postgres, "SET default_transaction_isolation TO SERIALIZABLE", TxNone, no, ser},
		{Postgres, "SET SESSION default_transaction_isolation = 'read committed'", TxNone, no, oth},
		{Postgres, `SET "default_transaction_isolation" = 'serializable'`, TxNone, no, no}, // a quoted name is not looked at: the server is asked
		{Postgres, "SET default_transaction_isolation = DEFAULT", TxNone, no, unk},
		{Postgres, "SET LOCAL default_transaction_isolation = 'serializable'", TxNone, no, unk},
		{Postgres, "SET default_transaction_isolation = 'serializable' || ''", TxNone, no, unk},
		{Postgres, "RESET default_transaction_isolation", TxNone, no, unk},
		{Postgres, "RESET ALL", TxNone, no, unk},
		{Postgres, "DISCARD ALL", TxNone, no, unk},
		{Postgres, "SELECT set_config('default_transaction_isolation', 'serializable', false)", TxNone, unk, unk},
		{Postgres, "SELECT set_config('transaction_isolation', $1, true)", TxNone, unk, unk},
		// nothing
		{Postgres, "SHOW transaction_isolation", TxNone, no, no},
		{Postgres, "SELECT current_setting('transaction_isolation')", TxNone, no, no},
		{Postgres, "SELECT 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'", TxNone, no, no},
		{Postgres, "RESET search_path", TxNone, no, no},
		{Postgres, "DISCARD PLANS", TxNone, no, no},
		{Postgres, "UPDATE t SET isolation = 'serializable' WHERE level = 1", TxNone, no, no},
		{Postgres, "ALTER ROLE r SET default_transaction_isolation = 'serializable'", TxNone, no, no},
		// several statements: the level of the transaction left open, in order
		{Postgres, "BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxBegin, ser, no},
		{Postgres, "BEGIN ISOLATION LEVEL SERIALIZABLE; COMMIT", TxCommit, no, no},
		{Postgres, "BEGIN ISOLATION LEVEL SERIALIZABLE; COMMIT; BEGIN", TxBegin, no, no},
		{Postgres, "COMMIT; BEGIN ISOLATION LEVEL SERIALIZABLE; INSERT INTO t (id) VALUES (1)", TxBegin, ser, no},
		{Postgres, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; ROLLBACK", TxRollback, no, no},
		{Postgres, "COMMIT AND CHAIN", TxChain, no, no},
		{Postgres, "SET default_transaction_isolation = 'serializable'; BEGIN", TxBegin, no, ser},
		// MySQL
		{MySQL, "START TRANSACTION", TxBegin, no, no},
		{MySQL, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxNone, ser, no},
		{MySQL, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxNone, no, ser},
		{MySQL, "SET LOCAL TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", TxNone, no, oth},
		{MySQL, "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE", TxNone, no, no},
		{MySQL, "SET transaction_isolation = 'SERIALIZABLE'", TxNone, no, ser},
		{MySQL, "SET SESSION transaction_isolation = 'READ-COMMITTED'", TxNone, no, oth},
		{MySQL, "SET @@session.tx_isolation = 'serializable'", TxNone, no, ser},
		{MySQL, "SET @@SESSION.transaction_isolation := \"REPEATABLE-READ\"", TxNone, no, oth},
		{MySQL, "SET @@transaction_isolation = 'SERIALIZABLE'", TxNone, ser, no},
		{MySQL, "SET @@global.transaction_isolation = 'SERIALIZABLE'", TxNone, no, no},
		{MySQL, "SET GLOBAL transaction_isolation = 'SERIALIZABLE'", TxNone, no, no},
		{MySQL, "SET autocommit = 0, SESSION transaction_isolation = 'SERIALIZABLE'", TxNone, no, ser},
		{MySQL, "SET transaction_isolation = @lvl", TxNone, no, unk},
		{MySQL, "SET autocommit = 0", TxNone, no, no},
	} {
		in := Parse(tc.sql, tc.d)
		if in.Tx != tc.tx || in.TxIso != tc.txIso || in.DefIso != tc.defIso {
			t.Errorf("dialect %d: %q: tx %d, transaction level %d, session level %d; want %d, %d, %d",
				tc.d, tc.sql, in.Tx, in.TxIso, in.DefIso, tc.tx, tc.txIso, tc.defIso)
		}
		if (in.TxIso != IsoUnset || in.DefIso != IsoUnset) && !MayControlTx(tc.sql, tc.d) && !MentionsIsolation(tc.sql) {
			t.Errorf("dialect %d: %q: the scans would not have it parsed", tc.d, tc.sql)
		}
	}
}

// The scans never say no to a text Parse finds an isolation level in, as
// long as the word is in one case.
func FuzzIsolationScan(f *testing.F) {
	for _, s := range []string{
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "set session characteristics as transaction isolation level serializable",
		"SET default_transaction_isolation = 'serializable'", "RESET ALL", "discard all", "SELECT 1; reset all",
		"SELECT set_config('transaction_isolation', 'serializable', true)", "SET @@session.tx_isolation = 'x'",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for _, d := range []Dialect{Postgres, MySQL} {
			in := Parse(s, d)
			if in.TxIso == IsoUnset && in.DefIso == IsoUnset || MayControlTx(s, d) || MentionsIsolation(s) {
				continue
			}
			if !containsFold(s, "isolation") {
				t.Fatalf("dialect %d: %q sets a level without the word", d, s)
			}
		}
	})
}

func containsFold(s, word string) bool {
	for i := 0; i+len(word) <= len(s); i++ {
		j := 0
		for j < len(word) && s[i+j]|0x20 == word[j] {
			j++
		}
		if j == len(word) {
			return true
		}
	}
	return false
}

func TestMentionsIsolationDoesNotAllocate(t *testing.T) {
	q := "/* app */ INSERT INTO t (note) VALUES ('a; b'), ('c; d'); -- done"
	if MentionsIsolation(q) {
		t.Fatal("found the word")
	}
	if n := testing.AllocsPerRun(100, func() { MentionsIsolation(q) }); n != 0 {
		t.Fatalf("%v allocations", n)
	}
}

// The untraced path parses a statement for isolation levels only if it can
// set one. The word alone, in a column name or a string, must not cost a
// parse: the text can be a bulk insert of megabytes.
func TestMaySetIsolation(t *testing.T) {
	for sql, want := range map[string]bool{
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE":                                               true,
		"set session characteristics as transaction isolation level serializable":                    true,
		"SET default_transaction_isolation = 'serializable'":                                         true,
		"SELECT 1; SET default_transaction_isolation TO serializable":                                true,
		"/* x */ SET LOCAL transaction_isolation = 'serializable'":                                   true,
		"SELECT set_config('default_transaction_isolation', 'serializable', false)":                  true,
		"SET @@session.transaction_isolation = 'SERIALIZABLE'":                                       true,
		"INSERT INTO notes (body) VALUES ('isolation ward'), ('more')":                               false,
		"UPDATE wards SET isolation_ward = 1 WHERE id = 2":                                           false,
		"SELECT isolation FROM wards":                                                                false,
		"SET search_path TO app":                                                                     false,
		"INSERT INTO t (v) VALUES ('" + strings.Repeat("isolation level serializable ", 2000) + "')": false,
	} {
		if got := MaySetIsolation(sql, Postgres); got != want {
			t.Errorf("MaySetIsolation(%.60q) = %v, want %v", sql, got, want)
		}
	}
	big := "INSERT INTO t (v) VALUES ('" + strings.Repeat("isolation ", 5000) + "')"
	if n := testing.AllocsPerRun(20, func() { MaySetIsolation(big, Postgres) }); n != 0 {
		t.Errorf("MaySetIsolation allocates %v times", n)
	}
}
