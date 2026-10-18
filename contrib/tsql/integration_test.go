//go:build integration

// Real-database tests. Build with `-tags integration`, add the drivers to this
// module (`go get github.com/jackc/pgx/v5 github.com/go-sql-driver/mysql`) and
// set TSESSION_TEST_PG_DSN and/or TSESSION_TEST_MYSQL_DSN. Skipped otherwise.
package tsql_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func runLifecycle(t *testing.T, driverName, dsn, ddl, insert, update, del string) {
	rec, ctx := setup(t)
	db, err := tsql.Open(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("DROP TABLE IF EXISTS tsql_it")
	if _, err := db.Exec(ddl); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE IF EXISTS tsql_it")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{insert, update, del} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, e := range byKind(rec.Events(), core.KindDBWrite) {
		for _, m := range e.Mutations {
			ops = append(ops, m.Op)
			if e.TxID == "" {
				t.Fatal("missing TxID")
			}
		}
	}
	if len(ops) != 3 || ops[0] != "insert" || ops[1] != "update" || ops[2] != "delete" {
		t.Fatalf("ops=%v", ops)
	}
}

func TestIntegrationPostgres(t *testing.T) {
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	runLifecycle(t, "pgx", dsn,
		"CREATE TABLE tsql_it (id int PRIMARY KEY, v text)",
		"INSERT INTO tsql_it (id, v) VALUES (1, 'a')",
		"UPDATE tsql_it SET v = 'b' WHERE id = 1",
		"DELETE FROM tsql_it WHERE id = 1")
}

func TestIntegrationMySQL(t *testing.T) {
	dsn := os.Getenv("TSESSION_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_MYSQL_DSN not set")
	}
	runLifecycle(t, "mysql", dsn,
		"CREATE TABLE tsql_it (id int PRIMARY KEY, v varchar(10))",
		"INSERT INTO tsql_it (id, v) VALUES (1, 'a')",
		"UPDATE tsql_it SET v = 'b' WHERE id = 1",
		"DELETE FROM tsql_it WHERE id = 1")
}

// ---- real Postgres: capture safety, binary PKs, redaction ----

func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	return dsn
}

func pgAdmin(t *testing.T, stmts ...string) {
	t.Helper()
	db, err := sql.Open("pgx", pgDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func TestPGCapturePermissionErrorKeepsAppTxUsable(t *testing.T) {
	pgAdmin(t,
		"DROP TABLE IF EXISTS tsql_perm", "DROP ROLE IF EXISTS tsql_limited",
		"CREATE TABLE tsql_perm (id int PRIMARY KEY, v text, secret text)",
		"INSERT INTO tsql_perm VALUES (1,'a','s'),(2,'b','s')",
		"CREATE ROLE tsql_limited LOGIN",
		"GRANT SELECT (id), UPDATE (v), DELETE ON tsql_perm TO tsql_limited")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_perm", "DROP ROLE IF EXISTS tsql_limited")
	dsn := strings.Replace(pgDSN(t), "postgres@", "tsql_limited@", 1)

	// sanity: without protection a failing extra SELECT really poisons a tx
	raw, _ := sql.Open("pgx", dsn)
	defer raw.Close()
	rtx, _ := raw.Begin()
	if _, err := rtx.Exec("SELECT * FROM tsql_perm"); err == nil {
		t.Fatal("expected permission error")
	}
	if err := rtx.Commit(); err == nil {
		// pgx reports the aborted state on commit via a ROLLBACK command tag
		t.Log("commit of aborted tx returned nil (rolled back server-side)")
	}

	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"UPDATE tsql_perm SET v = 'x' WHERE id = 1",
		"UPDATE tsql_perm SET v = 'y' WHERE id = 2",
		"DELETE FROM tsql_perm WHERE id = 1",
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: app statement broke after failed capture: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
	adm, _ := sql.Open("pgx", pgDSN(t))
	defer adm.Close()
	var n int
	adm.QueryRow("SELECT count(*) FROM tsql_perm WHERE v IN ('x','y')").Scan(&n)
	if n != 1 { // id=2 updated to y; id=1 deleted
		t.Fatalf("committed state wrong: %d", n)
	}
	failed := 0
	for _, e := range byKind(rec.Events(), core.KindDBWrite) {
		if hasFlag(e, "image_failed") {
			failed++
		}
		if e.Err != "" {
			t.Fatalf("event error: %s", e.Err)
		}
	}
	if failed == 0 {
		t.Fatal("expected image_failed flags (capture was supposed to fail)")
	}
}

func TestPGCaptureTimeoutInTxRestoresSettings(t *testing.T) {
	pgAdmin(t, "DROP TABLE IF EXISTS tsql_slow", "CREATE TABLE tsql_slow (id int PRIMARY KEY, v text)", "INSERT INTO tsql_slow VALUES (1,'a')")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_slow")
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", pgDSN(t), tsql.WithCaptureTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, _ := db.BeginTx(ctx, nil)
	if _, err := tx.ExecContext(ctx, "UPDATE tsql_slow SET v = 'b' WHERE id = 1 AND (SELECT pg_sleep(0.6)) IS NOT NULL"); err != nil {
		t.Fatalf("app statement must not be limited by the capture timeout: %v", err)
	}
	var st string
	if err := tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&st); err != nil || st != "0" {
		t.Fatalf("statement_timeout leaked: %q %v", st, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_timeout") {
		t.Fatalf("flags %v", e.Flags)
	}
}

func TestPGCaptureTimeoutOutsideTxLeavesPoolUsable(t *testing.T) {
	pgAdmin(t, "DROP TABLE IF EXISTS tsql_slow2", "CREATE TABLE tsql_slow2 (id int PRIMARY KEY, v text)", "INSERT INTO tsql_slow2 VALUES (1,'a')")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_slow2")
	rec, ctx := setup(t)
	db, err := tsql.Open("pgx", pgDSN(t), tsql.WithCaptureTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "UPDATE tsql_slow2 SET v = 'b' WHERE id = 1 AND (SELECT pg_sleep(0.4)) IS NOT NULL"); err != nil {
		t.Fatalf("app statement failed after capture timeout: %v", err)
	}
	var v string
	if err := db.QueryRowContext(ctx, "SELECT v FROM tsql_slow2 WHERE id = 1").Scan(&v); err != nil || v != "b" {
		t.Fatalf("v=%q err=%v", v, err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_timeout") {
		t.Fatalf("flags %v", e.Flags)
	}
}

func TestPGByteaPrimaryKey(t *testing.T) {
	pgAdmin(t, "DROP TABLE IF EXISTS tsql_blob", "CREATE TABLE tsql_blob (id bytea PRIMARY KEY, v text)",
		"INSERT INTO tsql_blob VALUES ('\\xfffe0001','a'),('\\xfffe0002','b')")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_blob")
	for _, inTx := range []bool{false, true} {
		rec, ctx := setup(t)
		db, err := tsql.Open("pgx", pgDSN(t), tsql.WithDefaultPK("id"))
		if err != nil {
			t.Fatal(err)
		}
		var ex interface {
			ExecContext(context.Context, string, ...any) (sql.Result, error)
		} = db
		var tx *sql.Tx
		if inTx {
			tx, _ = db.BeginTx(ctx, nil)
			ex = tx
		}
		if _, err := ex.ExecContext(ctx, "UPDATE tsql_blob SET v = upper(v) WHERE v >= 'a'"); err != nil {
			t.Fatal(err)
		}
		if _, err := ex.ExecContext(ctx, "UPDATE tsql_blob SET v = lower(v) WHERE id = $1", []byte{0xff, 0xfe, 0, 1}); err != nil {
			t.Fatal(err)
		}
		if inTx {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		evs := byKind(rec.Events(), core.KindDBWrite)
		if len(evs) != 2 {
			t.Fatalf("events %d", len(evs))
		}
		for _, e := range evs {
			if hasFlag(e, "after_failed") || hasFlag(e, "image_failed") {
				t.Fatalf("inTx=%v capture failed: %v", inTx, e.Flags)
			}
		}
		ms := evs[0].Mutations
		if len(ms) != 2 {
			t.Fatalf("inTx=%v mutations %+v flags %v", inTx, ms, evs[0].Flags)
		}
		got := map[string]string{}
		for _, m := range ms {
			got[jsonMap(t, m.Before)["v"].(string)] = jsonMap(t, m.After)["v"].(string)
		}
		if got["a"] != "A" || got["b"] != "B" {
			t.Fatalf("before/after mispaired: %v", got)
		}
		if len(evs[1].Mutations) != 1 || jsonMap(t, evs[1].Mutations[0].After)["v"] != "a" {
			t.Fatalf("eq-PK update: %+v flags %v", evs[1].Mutations, evs[1].Flags)
		}
		db.Close()
		pgAdmin(t, "UPDATE tsql_blob SET v = lower(v)")
	}
}

func TestPGSerializableSkipsCapture(t *testing.T) {
	pgAdmin(t, "DROP TABLE IF EXISTS tsql_ser", "CREATE TABLE tsql_ser (id int PRIMARY KEY, v text)", "INSERT INTO tsql_ser VALUES (1,'a')")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_ser")
	rec, ctx := setup(t)
	db, _ := tsql.Open("pgx", pgDSN(t))
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE tsql_ser SET v = 'b' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	e := byKind(rec.Events(), core.KindDBWrite)[0]
	if !hasFlag(e, "image_skipped:serializable") {
		t.Fatalf("flags %v", e.Flags)
	}
}

func TestPGInsertReturningAndWhereCurrentOf(t *testing.T) {
	pgAdmin(t, "DROP TABLE IF EXISTS tsql_ins", "CREATE TABLE tsql_ins (id serial PRIMARY KEY, name text, password text)")
	defer pgAdmin(t, "DROP TABLE IF EXISTS tsql_ins")
	rec, ctx := setup(t)
	db, _ := tsql.Open("pgx", pgDSN(t))
	defer db.Close()
	tx, _ := db.BeginTx(ctx, nil)
	var id int64
	if err := tx.QueryRowContext(ctx, "INSERT INTO tsql_ins (name, password) VALUES ($1, $2) RETURNING id", "ann", "PGSECRET").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "DECLARE c1 CURSOR FOR SELECT id FROM tsql_ins FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "FETCH 1 FROM c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE tsql_ins SET name = 'bob', password = 'PGSECRET2' WHERE CURRENT OF c1"); err != nil {
		t.Fatalf("WHERE CURRENT OF: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, e := range rec.Events() {
		mustNotLeak(t, e, "PGSECRET")
	}
	evs := byKind(rec.Events(), core.KindDBWrite)
	m := evs[0].Mutations[0]
	if m.Op != "insert" || m.PK["id"] != id || jsonMap(t, m.After)["name"] != "ann" {
		t.Fatalf("%+v flags %v", m, evs[0].Flags)
	}
	if evs[1].Attrs["image_skip_reason"] != "where_current_of" {
		t.Fatalf("attrs %v flags %v", evs[1].Attrs, evs[1].Flags)
	}
}
