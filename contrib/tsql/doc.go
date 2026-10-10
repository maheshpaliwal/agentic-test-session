// Package tsql adds test-session tracing to database/sql for Postgres and
// MySQL. Replace sql.Open / sql.OpenDB with tsql.Open / tsql.OpenDB (or wrap a
// driver with Register / WrapDriver) and every statement inside an active
// session is recorded as a db.read / db.write event, with row-level
// mutations (before/after images) and db.tx.* boundary events.
//
// When the SDK is disabled the original object is returned untouched, and
// wrapping twice is a no-op. Capture failures never alter or fail the
// application's statement; they only add a flag to the event.
package tsql
