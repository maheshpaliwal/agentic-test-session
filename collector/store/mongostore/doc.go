// Package mongostore is the MongoDB store.Store backend of the collector.
//
// # Layout
//
// Two collections in one database (defaults "events" and "seals"):
//
//   - events: one document per event. Scalar fields (session_id, seq, kind,
//     tx_id, tx_effective, start_ns, end_ns, service, ...) are stored for
//     indexing; the full event is kept as its JSON bytes in "body" (BSON
//     binary). User-controlled maps (Attrs, Mutation.PK, payloads) therefore
//     never become BSON field names, so keys such as "a.b" or "$x" are safe and
//     the event round-trips byte-for-byte like the NDJSON wire format. JSON
//     "\u0000" escapes are replaced as in the other backends (store.StripNUL).
//   - seals: one document per sealed session (_id = session id).
//
// ListSessions aggregates the events collection over a covering index
// (session_id, start_ns, end_ns) instead of maintaining per-session counters:
// counters would be updated outside the insert and drift after a crash or a
// partial batch, while the aggregation is always exact. It is O(events) index
// entries, which is fine for hundreds of sessions.
//
// # Guarantees
//
// The unique index (session_id, seq) is the concurrency guard: AppendEvents
// checks that the batch continues the head, then does an ordered InsertMany;
// a racing writer loses with a duplicate-key error, reported as an error
// wrapping store.ErrSeqConflict. A second unique index on (session_id,
// instance, local_seq) mirrors the Postgres backend's duplicate detection.
//
// Batches are NOT atomic (no transactions: they need a replica set). If an
// insert fails midway, the events before the failure stay stored. That prefix
// is always a valid gapless run, because a writer only ever inserts the Seq
// right after the head it observed and inserts in order; it is deliberately
// not rolled back, since deleting it could open a gap under a concurrent
// writer that already continued after it. The failure is recoverable: the
// caller sees an error, re-reads the tail (Events from its next Seq) and drops
// events already stored by (instance, local_seq), which is what the collector's
// session actor does on store.ErrSeqConflict. Oversized events are rejected
// (ErrEventTooLarge) before anything is written, so a poison event never
// leaves a partial batch behind.
//
// Reads and writes use the driver's default read/write concern from the URI
// (add w=majority for durability on replica sets). SealInfo.SealedAt is stored
// losslessly as RFC 3339 text. The URI is never logged or included in errors.
package mongostore
