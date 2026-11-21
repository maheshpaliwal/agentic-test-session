// Package tredisv8 records go-redis v8 (github.com/go-redis/redis/v8)
// commands into the active test session. It is the v8 twin of contrib/tredis
// (v9): same command table, options, redaction, key-pattern before-images,
// ts:ctx:* ignore, caps and timeouts; only the Hook interface differs.
//
//	rdb := tredisv8.Wrap(redis.NewClient(opts))                       // one-liner
//	rdb := tredisv8.Wrap(redis.NewClient(opts), tredisv8.WithBeforeImage("user:*"))
//
// Reads become cache.read spans, writes cache.write spans with one
// core.Mutation per touched key; pipeline commands get pipeline=true.
// redis.Nil is a normal miss. Wrap is a pass-through when !core.Enabled() and
// idempotent per client.
//
// Mutations name only the keys a command writes: the destination of the
// *STORE family, PFMERGE, BITOP and COPY (not their sources), and the key a
// multi-key pop (BLPOP, BZPOPMIN, ...) actually popped from. A write whose
// reply shows it changed nothing (SET NX|XX / SETNX / MSETNX / RENAMENX with a
// failed condition; DEL, UNLINK, HDEL, SREM, ZREM, EXPIRE*, PERSIST returning
// 0) still gets its span but no mutation.
//
// Image reads (GET/HSCAN/LRANGE/SSCAN/ZRANGE by TYPE, collections capped at
// about 1000 elements) are bounded per command and per pipeline by
// WithPipelineImageCap, WithImageTimeout and WithImageDeadline; a skipped read
// is flagged with the attribute image_skipped (cap, deadline or error). When a
// pipeline writes a key more than once, the first write carries the
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
// A created key has only new values, a deleted key only old ones. The diff
// lists at most 50 changes (and 50 members per added/removed list) and cuts
// values at 256 bytes. A collection of more than about 1000 elements is read
// in part, and two partial reads cannot tell an added element from one the
// other read did not reach: such a diff lists only hash fields and sorted-set
// members that both reads returned with different values, and nothing for a
// list or a set. Every cut or partial diff flags the event diff_truncated.
// The images hold no TTL, so EXPIRE and PERSIST have an empty diff, and a
// key whose value is masked as a whole (a sensitive key, see WithRedactKeys)
// is not read and has none either.
//
// Clients obtained through an internal wrapper. When the *redis.Client is
// hidden behind a wrapper type (for example one that embeds a Cmdable
// interface and already installs its own hooks), hook the concrete client the
// wrapper holds:
//
//	tredisv8.WrapAny(wrapper.Cmdable)           // wrapper.Cmdable is *redis.Client / *redis.ClusterClient
//
// or equivalently
//
//	if c, ok := wrapper.Cmdable.(interface{ AddHook(redis.Hook) }); ok {
//		c.AddHook(tredisv8.Hook(tredisv8.WithReader(wrapper.Cmdable)))
//	}
//
// Do this once at construction time, after the wrapper's own AddHook calls
// (v8 runs Before* in registration order and After* in reverse). Before-images
// need a reader; WrapAny supplies the client itself, bare Hook() needs
// WithReader.
package tredisv8
