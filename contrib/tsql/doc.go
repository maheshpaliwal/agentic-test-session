// Package tsql adds test-session tracing to database/sql for Postgres and
// MySQL. Replace sql.Open / sql.OpenDB with tsql.Open / tsql.OpenDB (or wrap a
// driver with Register / WrapDriver) and every statement inside an active
// session is recorded as a db.read / db.write event, with row-level
// mutations (before/after images) and db.tx.* boundary events.
//
// When the SDK is disabled the original object is returned untouched, and
// wrapping twice is a no-op. Capture failures never alter or fail the
// application's statement; they only add a flag to the event.
//
// Only Postgres and MySQL (and databases that speak their protocols and
// dialects) are supported: the statement parser, the capture fence and the
// catalog queries are written for those two.
//
// # Primary keys
//
// A mutation is filed under the primary key of its row, and an UPDATE's row
// is read back by that key. The key columns of a table are, in this order:
// those declared with WithPKColumns; those the database catalog gives; the
// default (WithDefaultPK, "id").
//
// The catalog is asked the first time a traced INSERT, UPDATE or DELETE names
// a table on a handle (a sql.DB): pg_index on Postgres, with the table name
// exactly as the statement wrote it, so quoting, schema and search_path are
// resolved by the server as they are for the statement; KEY_COLUMN_USAGE of
// information_schema on MySQL. Composite keys are kept in key order. The
// answer is cached per handle (at most 1024 tables), "no primary key"
// included, for 30 seconds; then it is asked again. So the cost is one
// capture (three round trips on Postgres, one on MySQL) per table per handle
// every 30 seconds; several connections that
// first meet a table at the same moment may each ask once. The MySQL lookup
// has not been run against a live server. The lookup is a capture like an
// image read: fenced, bounded by the capture timeout, skipped under
// SERIALIZABLE, never run outside an active session, for reads, or with
// WithImages(false). One that fails (flag "pk_lookup_failed"; for example no
// permission on the catalog) leaves the application's transaction untouched
// and is tried again by later statements, three times in all; then the
// default applies. WithPKLookup(false) switches the lookup off.
//
// The cached keys are dropped when a traced statement on the handle is DDL
// or can change what an unqualified table name means (search_path in any
// spelling, SET ROLE, RESET, DISCARD, USE), again when the transaction such a
// statement ran in ends, and, for the tables looked up in it, when a
// transaction is rolled back. A workload that sets search_path in every
// transaction therefore pays the lookup in every transaction: use
// schema-qualified names or WithPKColumns there. What no statement on the
// handle shows (DDL through another handle or process, a search_path that
// differs between the connections of one handle or is set inside a function)
// is noticed only when the 30 seconds are over. Until then a write may be
// looked for under the old key; if those columns match more rows than were
// written, nothing is recorded as the row's new values (flag
// "pk_not_unique"). The same holds for a table without a primary key whose
// "id" does not identify a row. A table without a primary key and a view
// have nothing to find: declare their key with WithPKColumns.
//
// Rows are told apart by the recorded value of their key, so a key value is
// recorded in a form that no other value shares, the same in the mutation's
// pk and in its row images, and the same whether it was read from a row or
// taken from a bind argument. Text and numbers are recorded as they are. A
// binary key (bytes that are not UTF-8 text; a bytea, BINARY or BLOB key) is
// recorded in hex, "\xfffe0001"; bytes that are text are recorded as that
// text, unless it starts with "\x". A binary key of more than 64 bytes keeps
// its first 32 and ends in its length and a hash of the whole value,
// "...[100 bytes sha256:0123456789abcdef]"; a text key of more than 1024
// bytes is cut and ends the same way. A uuid is recorded in its text form,
// also when it is bound as 16 bytes or as a driver.Valuer (pgtype.UUID).
// Binary values of other columns are only counted ("<3 bytes>"), so a change
// that keeps the length does not show in the diff. A key column with a
// sensitive name is masked like any other.
//
// An UPDATE is recorded with the row before and after it and the fields that
// differ. When one of the two images is missing no diff is recorded, because
// what changed is not known: without the after image the event is flagged
// "after_unavailable" (below); without the before image it carries the flag
// of whatever kept the row from being read before the write
// ("image_skipped" with its reason, "image_timeout" or "image_failed"; see
// the caveats below) and the mutation has the after image only.
//
// Three flags mark a write whose key or new values are not known:
//
//   - "pk_unknown": no value for the key columns was found in the row images,
//     the predicate or the VALUES list (typically the table has no "id" and
//     no key could be looked up). The mutation has no PK, so entity views
//     group it under an unknown key.
//   - "after_unavailable": the row an UPDATE changed could not be read back
//     (no key, or the re-read failed or was skipped). The mutation carries
//     the before image only and no diff: the new values are not known, which
//     is not the same as every column having been cleared.
//   - "pk_not_unique": the key columns matched more rows than the statement
//     wrote, so they are not the table's key. The after image is dropped
//     ("after_unavailable" applies) and the key is looked up afresh.
//
// # Redaction
//
// Bind arguments and inline literals are attributed to their columns (INSERT
// column list vs VALUES, SET col = $n, col op $n, IN/ANY/BETWEEN, ON CONFLICT
// DO UPDATE SET) and masked when the column name is sensitive
// (core.IsSensitiveKey). When an argument cannot be attributed and the
// statement mentions a sensitive-named identifier, all arguments are masked
// and the event is flagged "args_redacted_conservatively". Row images, diffs
// and read samples are masked by field name as well; a result column that
// carries a sensitive column under another name ("password AS p") is masked
// too, and when its output name cannot be determined (an expression without
// alias) every value of the captured rows is masked and the event is flagged
// "rows_redacted_conservatively". Recorded error text is scrubbed, and quoted
// values in it are masked when the statement involves sensitive data; the
// application always receives the original error.
//
// # Image capture caveats
//
// Before/after images are best-effort evidence, not an atomic snapshot. The
// pre-image is an unlocked SELECT issued before the application's statement,
// so under concurrency (autocommit, or READ COMMITTED) a concurrent writer
// can make before/after include another writer's change or miss rows. The
// extra reads run on the application's own connection: on Postgres they are
// fenced with a SAVEPOINT (inside a transaction) or a short read-only
// transaction (autocommit) and bounded by a server-side statement_timeout
// (WithCaptureTimeout, default 500ms; flag "image_timeout"), so a failing
// capture cannot abort the application's transaction. A capture uses at most
// a quarter of the time the caller's deadline leaves and is skipped when that
// is too little ("image_skipped:deadline"). Transactions the
// application controls with BEGIN / COMMIT / ROLLBACK statements are tracked
// from the leading keywords of every statement in the text. Primary keys are re-bound with the exact driver values (never
// the display form); a NULL key skips the re-read ("image_skipped:pk_unbindable").
// MySQL captures are plain non-locking SELECTs bounded by a MAX_EXECUTION_TIME hint.
// Values are captured up to a length cap; "diff_truncated" marks a diff that
// found two capped values equal and so cannot see a change beyond the cap, and
// "mutations_truncated" marks a statement with more rows than were captured.
//
// # SERIALIZABLE transactions
//
// Nothing is read inside a SERIALIZABLE transaction (flag
// "image_skipped:serializable"; the key lookup is left to a later
// statement): on Postgres the extra reads take predicate locks and can make
// the transaction, or a concurrent one, fail with a serialization error it
// would not have had; on MySQL a plain SELECT is a locking read there. The
// level is known from what the connection was sent, in or outside a session:
// the isolation option of BeginTx, the ISOLATION LEVEL mode of BEGIN and
// START TRANSACTION, SET TRANSACTION, and, for a transaction that states no
// level, the session default set with SET SESSION CHARACTERISTICS AS
// TRANSACTION or default_transaction_isolation (on MySQL: SET [SESSION]
// TRANSACTION and the transaction_isolation / tx_isolation variable). In
// such a transaction tsql sends nothing of its own.
//
// A default no statement shows (set for the role or the database, in the
// connection string's options, by a function or through set_config, RESET
// or DISCARD) is asked for on Postgres: in a transaction of unknown level
// the statement that sets the first fence's timeout is "SELECT
// set_config('statement_timeout', n, true) WHERE
// current_setting('transaction_isolation') <> 'serializable'", whose row
// count says which it is. That is no extra round trip. Under SERIALIZABLE it
// sets and reads nothing, the fence is closed, and the answer holds for the
// rest of the transaction; when the transaction took the session default it
// also holds for the connection's later transactions, until a statement
// changes the default. So with a SERIALIZABLE default the first transaction
// with a traced write on each connection sees one such fence (two round
// trips, no table read, no predicate lock; it does take the transaction's
// snapshot if the application has not yet) and later ones nothing. A default
// that stops being SERIALIZABLE without a statement on the connection
// showing it is not noticed: images stay skipped on that connection.
//
// In autocommit a capture has a transaction of its own, which a SERIALIZABLE
// default makes SERIALIZABLE. That is harmless, because it is always rolled
// back and the reads of a rolled-back transaction conflict with nothing.
//
// On MySQL there is no fence to ask in. A default no statement on the
// connection shows (the global level, an init command of the driver) is not
// detected and captures run as under any other level; a level set from a
// variable or an expression counts as SERIALIZABLE. The MySQL statements have
// been checked against the manual, not against a server.
//
// # Cost of image capture
//
// Statements outside an active session, reads, and writes with WithImages(false)
// cost no extra round trip. The first traced write of a table on a handle
// costs one capture more, for its primary key (see above). Each image read of a traced write costs three on
// Postgres: one text that opens the fence and sets the timeout ("SAVEPOINT
// ts_capture; SET LOCAL statement_timeout = n", or "BEGIN READ ONLY; SET LOCAL
// ..." in autocommit), the SELECT, and one that closes the fence by rolling it
// back ("ROLLBACK TO SAVEPOINT ts_capture; RELEASE SAVEPOINT ts_capture", or
// "ROLLBACK"), which also puts statement_timeout back to what the application
// had. An UPDATE reads the rows before and after (7 round trips in all), a
// DELETE before and an INSERT with its key in VALUES after (4 each); an INSERT
// whose key the database generates is not re-read (1). These counts were
// measured with pgx on Postgres 14 only. Sending two statements
// in one text needs a driver that uses the simple query protocol for an Exec
// without arguments (pgx and lib/pq do). tsql finds out per connection, where a
// refusal cannot hurt, so the first capture on a connection costs one or two
// round trips more; with a driver that refuses, the statements are sent one by
// one (five per image read inside a transaction, four in autocommit). On MySQL
// an image read is one SELECT.
//
// # Reaching the wrapped driver
//
// sql.Conn.Raw and sql.DB.Driver hand out tsql's wrappers; UnwrapConn and
// UnwrapDriver return the driver's own objects.
package tsql
