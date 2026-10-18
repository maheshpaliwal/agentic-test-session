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
