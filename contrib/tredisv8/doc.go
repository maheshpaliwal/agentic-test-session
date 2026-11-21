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
// Entities and keys. A mutation is filed under an entity and a key, the way a
// SQL row is filed under its table and primary key. The key is the Redis key
// (Mutation.PK["key"]). The entity is the key with every id-like ':'
// separated segment replaced by "*", so the keys of one kind share an entity.
// A segment is id-like when it is all digits, or 8 or more characters of hex
// digits and '-' (a UUID, an object id, a date), or masked as a secret:
//
//	key user:1                   entity user:*
//	key order:42:items           entity order:*:items
//	key user:550e8400-e29b-41d4  entity user:*
//	key user:alice               entity user:alice     (a name is not id-like)
//	key config                   entity config
//	key session:#a1b2c3d4        entity session:*      (see Secret key names)
//
// tracectl entities lists both. history takes the entity, then the key; quote
// the "*" for the shell:
//
//	tracectl history <session> redis 'user:*' user:1
//
// "history <session> redis user:1 user:1" finds nothing: user:1 is the key of
// the entity user:*, not an entity.
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
//
// Secret key names. A key can hold a secret in its name: session:<token>,
// auth:token:<jwt>. The part that holds it is recorded as a short hash,
// "session:#a1b2c3d4", everywhere the key appears: the span name, the
// arguments, the mutation's key and the replies of KEYS, RANDOMKEY and
// multi-key pops. Redis is still sent the real key, and image reads use it. A
// ':' separated segment is masked when
//
//   - it is not a plain lower-case word (letters joined by '_', '-' or '.')
//     and comes after a segment that names a secret: a sensitive name of
//     core.SetRedactKeys (token, auth, secret, password, ...) or session,
//     sessions, sess, sid. "auth:user:42:profile" is recorded as
//     "auth:user:#a1b2c3d4:profile";
//   - it is not a plain lower-case word and holds a sensitive name itself
//     ("cache:tok_SECRET123"), or it is a JWT;
//   - it falls under a wildcard of a WithMaskKeys glob: "cart:*:items".
//
// The hash is an HMAC of the segment keyed by the session id, cut to 32 bits:
// one key reads the same in every service of a session, so its commands still
// form one entity history, and differently in another session. It hides a
// long random token. It does not hide a short or guessable one (a phone
// number, a six-digit code) from someone who can read the session and try
// candidates; for those use WithKeyMask(KeyMaskFlat), which records
// "session:[REDACTED]" and so merges all such keys into one history.
// WithKeyMask(KeyMaskOff) records key names as they are.
//
// These rules are a heuristic over key names. They mask keys that hold no
// secret ("auth:user:42") and miss secrets they cannot recognise: one under a
// prefix that is not listed, or one made of lower-case letters only. Which
// keys are sensitive in your environment is for your Security team to decide;
// list them with WithMaskKeys.
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
