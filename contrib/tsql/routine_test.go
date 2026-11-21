package tsql_test

import (
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A statement that runs code on the server (DO, CALL, EXECUTE) can write
// anything. It used to be recorded as a read, so a session in which a DO
// block changed rows looked as if it had changed nothing. It is a write
// whose effects are not recorded, and says so.
func TestRoutineIsAWriteWithUnknownEffects(t *testing.T) {
	for _, stmt := range []string{
		"DO $$ BEGIN UPDATE stock SET qty = 0; END $$",
		"CALL restock($1)",
		"EXECUTE bump_stock($1)",
		"  /* c */ call restock($1)",
	} {
		rec, ctx := setup(t)
		db := open(t, &fakeDB{}, "postgres")
		args := []any{}
		if stmt[len(stmt)-1] == ')' {
			args = append(args, 1)
		}
		if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
			panic(err)
		}
		if reads := byKind(rec.Events(), core.KindDBRead); len(reads) != 0 {
			t.Errorf("%s: recorded as a read: %+v", stmt, reads)
		}
		e := lastWrite(t, rec)
		if len(e.Mutations) != 0 || !hasFlag(e, "effects_unknown") {
			t.Errorf("%s: mutations %+v flags %v, want none and effects_unknown", stmt, e.Mutations, e.Flags)
		}
	}
	// A plain statement is not flagged.
	rec, ctx := setup(t)
	db := open(t, &fakeDB{}, "postgres")
	if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
		panic(err)
	}
	for _, e := range rec.Events() {
		if hasFlag(e, "effects_unknown") || e.Kind == core.KindDBWrite {
			t.Errorf("SELECT 1 recorded as %s with flags %v", e.Kind, e.Flags)
		}
	}
}
