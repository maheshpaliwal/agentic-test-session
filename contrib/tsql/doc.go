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
// Three flags mark a write whose key or new values are not known (a fourth,
// "upsert_unknown", is described under Upserts):
//
//   - "pk_unknown": no value for the key columns was found in the row images,
//     the predicate or the VALUES list (typically the table has no "id" and
//     no key could be looked up). The mutation has no PK, so entity views
//     group it under an unknown key.
//   - "after_unavailable": the row an UPDATE changed could not be read back
//     (no key, or the re-read failed or was skipped). The mutation carries
//     the before image only and no diff: the new values are not known, which
//     is not the same as every column having been cleared.
//   - "effects_unknown": the statement runs code on the server (DO, CALL,
//     EXECUTE of a prepared statement). It is recorded as a write with no
//     mutations: the rows it changed are not in its text and are not looked
//     for. No mutation on such an event does not mean nothing changed.
//   - "pk_not_unique": the key columns matched more rows than the statement
//     wrote, so they are not the table's key. The after image is dropped
//     ("after_unavailable" applies) and the key is looked up afresh.
//
// # Upserts
//
// An upsert either inserts a row or updates one that is there, and the
// statement's result does not say which. tsql finds out from the row itself
// and records each row as what happened to it:
//
//   - inserted: op "upsert", the new row as after image, every field in the
//     diff set from nothing.
//   - updated: op "update", the row before and after, and only the fields
//     that differ in the diff, like an UPDATE. A row the update moved to
//     another key has the before image only ("after_unavailable").
//   - not known: op "upsert" and the flag "upsert_unknown". The mutation has
//     no before image and no diff; it has the row after the statement where
//     that could be read, and is filed under the key in VALUES only when
//     that key is the one the statement collides on.
//
// On Postgres the row is looked for by the columns of the conflict target,
// "ON CONFLICT (a, b)", with the values in VALUES: once before the statement
// and once after it, each a capture like any other (fenced, skipped under
// SERIALIZABLE, never outside a session or with WithImages(false)). The
// target need not be the primary key; the mutation is filed under the
// primary key the row has. Both reads also take where the row version is
// stored (its table and ctid), and the read after the statement its xmax: 0
// on a version an INSERT wrote and nothing has locked since. A row that was
// not there before and is fresh after was inserted. A row that was there
// and whose version is stored elsewhere after was updated; the read before
// gives the old values. A row whose version is where it was has not been
// written by the statement, whatever its xmax says (xmax stays set on a row
// that was ever locked or updated): the WHERE of DO UPDATE left it alone,
// or the statement wrote another row than the one looked for (a trigger
// changed the key, the server rounded it), which the statement's row count
// then shows and the record says ("upsert_unknown"). A row that was not
// there and is not fresh was inserted by someone else in between, or was
// inserted by the statement and locked at once: an AFTER INSERT trigger
// that inserts a row referencing it is enough, through the foreign-key
// check. Which of the two is not known, so on such a table every inserting
// upsert is recorded as "upsert_unknown", with its after image and no diff. The statement itself is never changed (a
// RETURNING clause would need SELECT rights it may not have, fails with a
// system column on a partitioned table, and changes what the application
// receives), also when it has a RETURNING of its own.
//
// The read before the statement is not locked, so another connection can
// insert or delete the row between it and the statement. The two reads then
// disagree (no row before, yet not a fresh one after; or a row before, yet a
// fresh one after) and the row is recorded as not known rather than as
// either. What is not noticed: another writer that changes a row which is
// there before and after (the before image is then older than the values
// the statement met, as for an UPDATE), and, in autocommit, one that updates
// the row between the statement and the read after it. A view has no xmax,
// and neither has every database that speaks the Postgres protocol: there
// the first read fails inside its fence, is made again without, and for 30
// seconds the table is not asked for it; the read before the statement
// alone then says whether the row was there, and the race is not noticed.
//
// Several rows in VALUES (up to WithMaxImageRows) are read with one SELECT
// before and one after. The statement's row count has to account for them:
// rows the WHERE of DO UPDATE left as they were (they are locked all the
// same) are no mutation, and where the count cannot be matched to the rows
// found, all are recorded as not known.
//
// The outcome is not known, and nothing is read, when the statement does
// not say by which values a row is found: ON CONFLICT ON CONSTRAINT, a
// target with an expression, a collation, an operator class or the predicate
// of a partial index, a key that is not a plain placeholder or literal in
// VALUES (or is NULL), INSERT ... SELECT, MERGE. Reading by the primary key
// in VALUES instead would record a row the statement may not have touched.
// ON CONFLICT DO NOTHING writes new rows only: it is recorded as an INSERT is
// and nothing is read before it.
//
// WithUpsertBefore(false) drops the read before the statement. On Postgres
// a row that is fresh after the statement is still recorded as inserted;
// any other row was updated, or inserted and then locked, and is recorded
// as "upsert_unknown" with its after image and no diff.
//
// MySQL (INSERT ... ON DUPLICATE KEY UPDATE, REPLACE) has not been run
// against a server, and is kept to what holds in either client convention
// for affected rows. The row is looked for by the primary key in VALUES,
// before and after (one SELECT each). It is recorded as inserted when it was
// not there before, is there after, and one row was affected; as updated
// when it was there and the count is 2 (or, with CLIENT_FOUND_ROWS, 1 for a
// row set to the values it had; exactly 2 for REPLACE, which is recorded as
// an update although it deletes and inserts: columns it does not name go
// back to their defaults). Everything else is not known: a count that says
// another unique key was hit or more rows were deleted, a generated key
// (the row the server reports as inserted is read back as the after image),
// several rows in VALUES, WithUpsertBefore(false). A table with more than
// one unique key can update a row other than the one with the primary key
// in VALUES; that shows only where the count gives it away.
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
// capture cannot abort the application's transaction. The savepoint is named
// "ts_capture_" and 16 hex digits drawn at random per connection, so it is
// never a savepoint of the application. When the text that opens the fence
// fails, tsql does not know whether the savepoint was taken; it then sends
// "SHOW transaction_isolation", and rolls back to the savepoint only if that
// fails, that is, only if the transaction (or the fence's subtransaction) is
// aborted and a ROLLBACK TO can do no harm. A capture uses at most
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
// with a traced write on each connection sees one such fence (three round
// trips on a fresh connection, two afterwards, no table read, no predicate lock; it does take the transaction's
// snapshot if the application has not yet) and later ones nothing. A default
// that stops being SERIALIZABLE without a statement on the connection
// showing it is not noticed: images stay skipped on that connection.
//
// In autocommit a capture has a transaction of its own. It is opened READ
// COMMITTED whatever the session's default: a SERIALIZABLE one would take
// part in serialization checks while it is open, and a concurrent
// transaction that succeeds untraced could then fail with 40001.
//
// Under REPEATABLE READ, and for the first transaction on a connection whose
// default is SERIALIZABLE, a capture is a statement like any other: if it is
// the first of the transaction it takes the transaction's snapshot, a moment
// before the application's own statement would have. A concurrent commit
// that lands in between is then not seen, and an UPDATE of the row it
// changed fails with 40001 where the untraced run would have succeeded. Use
// WithImages(false) for tests that depend on that moment.
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
// ts_capture_x; SET LOCAL statement_timeout = n", or "BEGIN READ ONLY; SET LOCAL
// ..." in autocommit), the SELECT, and one that closes the fence by rolling it
// back ("ROLLBACK TO SAVEPOINT ts_capture_x; RELEASE SAVEPOINT ts_capture_x", or
// "ROLLBACK"), which also puts statement_timeout back to what the application
// had. An UPDATE reads the rows before and after (7 round trips in all), a
// DELETE before and an INSERT with its key in VALUES after (4 each); an INSERT
// whose key the database generates is not re-read (1). An upsert that can
// update reads the rows before and after like an UPDATE (7; 4 with
// WithUpsertBefore(false)), whatever the number of rows in VALUES and also
// when it returns its rows; one whose rows cannot be looked for is not read
// (1); ON CONFLICT DO NOTHING costs what an INSERT costs. These counts
// were measured with pgx on Postgres 14 only. Sending two statements
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
