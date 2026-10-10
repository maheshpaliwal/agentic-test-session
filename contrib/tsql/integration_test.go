//go:build integration

// Real-database tests. Build with `-tags integration`, add the drivers to this
// module (`go get github.com/jackc/pgx/v5 github.com/go-sql-driver/mysql`) and
// set TSESSION_TEST_PG_DSN and/or TSESSION_TEST_MYSQL_DSN. Skipped otherwise.
package tsql_test

import (
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"tsession/contrib/tsql"
	"tsession/core"
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
