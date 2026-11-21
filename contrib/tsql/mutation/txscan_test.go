package mutation

import (
	"fmt"
	"testing"
)

// MayControlTx may answer maybe for a text Parse finds no transaction control
// in, but never no for one it does.
func FuzzMayControlTx(f *testing.F) {
	for _, s := range []string{
		"BEGIN", "/* c */ begin isolation level serializable;", "START TRANSACTION READ ONLY", "START SLAVE",
		"COMMIT AND CHAIN", "END;", "ROLLBACK TO SAVEPOINT s", "abort", "PREPARE TRANSACTION 'x'", "COMMIT PREPARED 'x'",
		"SELECT 'BEGIN; COMMIT'", "BEGIN; INSERT INTO t (id) VALUES (1); COMMIT", "SELECT 1;-- c\nCOMMIT",
		"SELECT 1; /* /* n */ */ rollback", "SELECT $$ ; BEGIN $$; end", "# c\nBEGIN", "--c\nBEGIN", "- -\nBEGIN",
		"\u017ftart transaction", "comm\u0131t", "SELECT 1;\xffBEGIN", "INSERT INTO t VALUES ('a\\'; BEGIN; --')",
		"SELECT E'\\'; COMMIT", ";/*;--;/*", "(BEGIN)", "x;;; ;begin",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		for _, d := range []Dialect{Postgres, MySQL} {
			if tx := Parse(s, d).Tx; tx != TxNone && !MayControlTx(s, d) {
				panic(fmt.Sprintf("dialect %d: scan missed transaction control %d in %q", d, tx, s))
			}
		}
	})
}

func TestMayControlTxDoesNotAllocate(t *testing.T) {
	q := "/* app */ INSERT INTO t (note) VALUES ('a; b'), ('c; d'); -- done"
	if n := testing.AllocsPerRun(100, func() { MayControlTx(q, Postgres) }); n != 0 {
		panic(fmt.Sprintf("%v allocations", n))
	}
}
