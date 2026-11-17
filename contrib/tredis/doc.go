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
package tredis
