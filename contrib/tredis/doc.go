// Package tredis records go-redis commands into the active test session.
//
//	rdb := tredis.Wrap(redis.NewClient(opts))              // one-liner
//	rdb := tredis.Wrap(redis.NewClient(opts), tredis.WithBeforeImage("user:*"))
//
// Reads become cache.read spans; writes become cache.write spans carrying a
// core.Mutation per touched key. Pipelines yield one span per command, tagged
// with the attribute pipeline=true. redis.Nil is a normal miss, not an error.
//
// Mutations name only the keys a command writes: the destination of the
// *STORE family, PFMERGE, BITOP and COPY (not their sources), and the key a
// multi-key pop (BLPOP, BZPOPMIN, LMPOP, ...) actually popped from. A write
// whose reply shows it changed nothing (SET NX|XX / SETNX / MSETNX / RENAMENX
// with a failed condition; DEL, UNLINK, HDEL, SREM, ZREM, EXPIRE*, PERSIST
// returning 0) still gets its span but no mutation.
//
// Before-images are opt-in: for keys matching WithBeforeImage globs the hook
// reads the key (GET/HSCAN/LRANGE/SSCAN/ZRANGE by TYPE, collections capped at
// about 1000 elements) before and after the write. That costs extra round
// trips, so it needs a client to read with: Wrap supplies it; with the bare
// Hook() pass WithReader. The reads are bounded per command and per pipeline
// by WithPipelineImageCap, WithImageTimeout and WithImageDeadline; a skipped
// read is flagged with the attribute image_skipped (cap, deadline or error).
// When a pipeline writes a key more than once, the first write carries the
// before-image and the last successful one the after-image.
//
// A mutation that has both images also carries a Diff, computed from the two
// reads (no further round trip) and redacted like them:
//
//   - string: one change, field "value";
//   - hash: one change per field added, removed or changed;
//   - sorted set: the same per member, the values being the scores;
//   - set: the members added (field "added") and removed (field "removed");
//   - list: one change per index "[i]" between the unchanged head and the
//     unchanged tail of the list;
//   - a key that changed type: one change, field "type".
//
// A value that changed under redaction is listed too, with the redacted
// text on both sides (password: "[REDACTED]" -> "[REDACTED]"): the diff lists
// only what changed, so being listed is what says so. The stored values are
// compared in memory by a per-process fingerprint that is never recorded.
//
// A created key has only new values, a deleted key only old ones. The diff
// lists at most 50 changes (and 50 members per added/removed list) and cuts
// values at 256 bytes. A collection of more than about 1000 elements is read
// in part, and two partial reads cannot tell an added element from one the
// other read did not reach: such a diff lists only hash fields and sorted-set
// members that both reads returned with different values, and nothing for a
// list or a set. Every cut or partial diff flags the event diff_truncated.
// The images hold no TTL, so EXPIRE and PERSIST have an empty diff, and a
// key whose value is masked as a whole (a sensitive key, see WithRedactKeys)
// is not read: it has no diff, and its span says so with the attribute
// diff_skipped=sensitive_key, because whether the value changed is unknown.
package tredis
