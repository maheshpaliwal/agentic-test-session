// Package tredis records go-redis commands into the active test session.
//
//	rdb := tredis.Wrap(redis.NewClient(opts))              // one-liner
//	rdb := tredis.Wrap(redis.NewClient(opts), tredis.WithBeforeImage("user:*"))
//
// Reads become cache.read spans; writes become cache.write spans carrying a
// core.Mutation per touched key. Pipelines yield one span per command, tagged
// with the attribute pipeline=true. redis.Nil is a normal miss, not an error.
//
// Before-images are opt-in: for keys matching WithBeforeImage globs the hook
// reads the key (GET/HGETALL/LRANGE/SMEMBERS/ZRANGE by TYPE) before and after
// the write. That costs extra round trips, so it needs a client to read with:
// Wrap supplies it; with the bare Hook() pass WithReader.
package tredis
