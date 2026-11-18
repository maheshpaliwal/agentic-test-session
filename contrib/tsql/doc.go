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
// is too little ("image_skipped:deadline"). They are skipped inside
// SERIALIZABLE transactions ("image_skipped:serializable"). Transactions the
// application controls with BEGIN / COMMIT / ROLLBACK statements are tracked
// from the leading keywords of every statement in the text. Primary keys are re-bound with the exact driver values (never
// the display form); a NULL key skips the re-read ("image_skipped:pk_unbindable").
// MySQL captures are plain non-locking SELECTs bounded by a MAX_EXECUTION_TIME hint.
// Values are captured up to a length cap; "diff_truncated" marks a diff that
// found two capped values equal and so cannot see a change beyond the cap, and
// "mutations_truncated" marks a statement with more rows than were captured.
//
// # Reaching the wrapped driver
//
// sql.Conn.Raw and sql.DB.Driver hand out tsql's wrappers; UnwrapConn and
// UnwrapDriver return the driver's own objects.
package tsql
