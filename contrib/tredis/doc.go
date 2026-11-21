// Package tredis records go-redis commands into the active test session.
//
//	rdb := tredis.Wrap(redis.NewClient(opts))              // one-liner
//	rdb := tredis.Wrap(redis.NewClient(opts), tredis.WithBeforeImage("user:*"))
//
// Reads become cache.read spans; writes become cache.write spans carrying a
// core.Mutation per touched key. Pipelines yield one span per command, tagged
// with the attribute pipeline=true. redis.Nil is a normal miss, not an error.
//
// A MULTI/EXEC transaction (TxPipeline, TxPipelined) is a pipeline whose
// spans also carry transaction=true and transaction_outcome: committed (the
// commands ran; one can still have failed by itself, Redis does not roll
// back), aborted (a watched key changed and EXEC ran nothing: every span has
// the error redis.TxFailedErr and no mutation) or failed (EXEC was refused or
// never answered). pipeline_size and pipeline_index count the commands the
// application queued, not the MULTI and EXEC around them.
//
// A transaction the application frames itself, with Do("multi") ... Do("exec")
// on one connection, is not recognised as one: each command sent in between
// is answered QUEUED and recorded with the attribute queued=true and no
// mutation, and the effects of the EXEC are not attributed to them. Use
// TxPipeline to have them recorded.
//
// PUBLISH and SPUBLISH are recorded as a write span with the attributes
// system=redis and queue=<channel> and the number of receivers as the
// response. They carry no mutation, because a message changes no key, and
// are not queue.publish spans, because nothing reports that a subscriber
// finished: such a span would stay an unfinished job of the session.
//
// Mutations name only the keys a command writes: the destination of the
// *STORE family, PFMERGE, BITOP and COPY (not their sources), and the key a
// multi-key pop (BLPOP, BZPOPMIN, LMPOP, ...) actually popped from. A write
// whose reply shows it changed nothing (SET NX|XX / SETNX / MSETNX / RENAMENX
// with a failed condition; DEL, UNLINK, HDEL, SREM, ZREM, EXPIRE*, PERSIST
// returning 0) still gets its span but no mutation. GETSET and SET ... GET on
// a key that did not exist answer nil and are mutations all the same.
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
//	key session:#a1b2c3d4e5f6    entity session:*      (see Secret key names)
//	key session_#a1b2c3d4e5f6    entity session_*
//
// tracectl entities lists both. history takes the entity, then the key; quote
// the "*" for the shell:
//
//	tracectl history <session> redis 'user:*' user:1
//
// "history <session> redis user:1 user:1" finds nothing: user:1 is the key of
// the entity user:*, not an entity.
//
// The globs of every option are path.Match patterns, except that '*' and '?'
// match any byte, '/' included, as in a Redis KEYS pattern.
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
// Fields and members are told apart by what is stored, not by how they are
// recorded: two sorted-set members that redaction makes read alike
// ("token=[REDACTED]") are two changes with the same field text, and are
// numbered in the image ("token=[REDACTED] (2)"). An infinite score is
// recorded as the text "inf" or "-inf", which JSON has no number for.
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
// The images are two reads around the command, not a transaction with it.
// When another client writes the same key in between, the diff holds that
// client's change as well, as if this command had made it; under two steady
// writers of one hash that is every diff, and a blocking pop that waits for
// another client's push shows the push. The event is flagged diff_concurrent
// when the diff holds something the command cannot have done: HSET, HDEL,
// HINCRBY, ZADD, ZREM, ZINCRBY, SADD or SREM and a changed field or member
// that is not among its arguments; DEL, a pop, a trim or another removal and
// an element that was not there before; SET, GETSET or SETEX and a key that
// does not hold the argument afterwards. The flag is never raised for a
// command's own effect, and it cannot see another client changing the very
// field the command names, or any change next to a command it has no rule
// for (APPEND, RPUSH, scripts, ...): without the flag a diff is likely, not
// proven, to be the command's alone. Use the after-image of the next write,
// or WATCH, where that matters.
//
// Secret key names. A key can hold a secret in its name: session:<token>,
// auth:token:<jwt>. The part that holds it is recorded masked,
// "session:#a1b2c3d4e5f6", wherever a name is recorded: the span name, the
// arguments (every argument, not only those the command table knows as keys:
// the keys after STREAMS of XREAD, a MATCH, BY or GET pattern, a key handed to
// a script as ARGV), the mutation's key, hash field names in arguments,
// images and diffs, and replies (KEYS, SCAN, RANDOMKEY, multi-key pops, a
// member or a script result that reads as such a key), on the typed path and
// through Do alike. Redis is still sent the real key, and image patterns and
// image reads use it.
//
// A key is cut into segments at ':'. A segment is a name when it is made of
// letters joined by '_', '-' or '.', in any case. It is a secret name when it
// is a name that core.SetRedactKeys lists as sensitive (token, auth, secret,
// password, api_key, ...), or session, sess or sid, or the plural of one of
// those; braces around it are ignored ("{session}"). A secret name is never
// masked itself. A segment is masked when
//
//   - it comes after a secret name and is not a lower-case word:
//     "Session:<token>", "ApiKey:<key>", "tokens:<token>"; every such segment
//     after it too, so "auth:user:42:profile" is recorded as
//     "auth:user:#a1b2c3d4e5f6:profile";
//   - it starts with a secret name joined by '_', '-', '.', '/' or '=' to
//     something that is not a lower-case word; that part is masked:
//     "session_<token>", "session/<token>";
//   - it is not a name and holds a sensitive name ("cache:tok_SECRET123"), or
//     it is a JWT;
//   - it falls under a wildcard of a WithMaskKeys glob: "cart:*:items".
//
// A pattern is masked the same way ("session:Ab3d*" is recorded as
// "session:#a1b2c3d4e5f6"); a segment of wildcards only is kept.
//
// These rules are a heuristic over names: the package does not know which
// keys of your application hold secrets. Measured on real keys, they miss
//
//   - a secret after a name that is not listed: refresh:, csrf:, verify:,
//     code:, pin:, bearer:, magic_link:, oauth:state:, user:42:reset:<token>;
//   - a secret made of lower-case letters only, which reads as a name
//     ("session:abcdefgh");
//   - an e-mail address or a phone number as a segment ("user:<address>");
//   - a secret name in the middle of a '/' path ("cache/session/<token>");
//   - a name joined to its secret by something other than ':' and the
//     separators the rule knows ("session|<token>", "session%3A<token>",
//     "app.session.<token>"), and a name that is not ASCII;
//   - a secret stored under a name that says nothing ("note"): a value is
//     masked only under a sensitive name, as "key=value" with one, as a
//     bearer token, or when it is a JSON Web Token on its own;
//   - the text of a script (EVAL, FUNCTION LOAD), which is recorded as
//     written, with any key or secret that is typed into it, and what a
//     script returns after reading a sensitive field of a key whose name
//     says nothing;
//   - numbers about a sensitive value: its length (STRLEN, HSTRLEN) and its
//     bits (GETBIT, BITCOUNT).
//
// So a recording can still hold a credential. Treat an export as test data
// that may contain secrets, and keep real credentials out of the
// environments you record.
//
// and they mask segments that hold no secret: the 42 of "auth:user:42", the
// address of "rate:session:10.0.0.1". List the keys of the first kind with
// WithMaskKeys; it panics on a malformed glob, which would otherwise protect
// nothing without saying so.
//
// How a masked part is recorded is a trade-off between what a recording
// reveals and what it is good for. WithKeyMask chooses; each mode's exact
// property:
//
//   - KeyMaskHash (the default): "#" and 12 hex digits of an HMAC-SHA256 of
//     the session id and the masked part, keyed by 32 random bytes that the
//     process draws at its first use and never writes anywhere. One key reads
//     the same for as long as the process runs, so its commands form one
//     entity history, and differently in every session. Nobody can recompute
//     the hash from an export, so a secret cannot be found by trying
//     candidates. The price: another process hashes the same key differently,
//     so a key written by two services, or by one service before and after a
//     restart, is recorded as two keys with two histories.
//   - KeyMaskSession: the same, keyed by the session id alone. One key reads
//     the same in every service of the session. The session id is in every
//     exported event, so whoever can read an export can hash candidates: a
//     six-digit code or a phone number is found by trying all of them, and a
//     token known from elsewhere is confirmed. It hides a long random token
//     from a reader, and nothing that can be guessed.
//   - KeyMaskFlat: "[REDACTED]". Nothing is derived from the secret. All such
//     keys read alike, so their histories merge into one: "session:[REDACTED]"
//     is every session key of the run.
//   - KeyMaskOff: names are recorded as they are.
//
// With either hash, two different masked parts get the same 48-bit hash, and
// their histories merge without a flag, with a chance of about 1% once a
// process (KeyMaskSession: a session) has masked 2.4 million of them, and of
// 50% at 20 million.
//
// Which names are secrets, and whether a hash of one may be recorded at all,
// depends on what the recordings of your environment may hold and who reads
// them. That is a question for your Security team, not one this package can
// settle: the default is the mode that does not let a reader of an export
// test guesses, not a statement that it meets your policy.
//
// What else is kept out of a recording. Values are redacted by name: a key
// with a sensitive name segment (or its plural as a whole segment,
// "tokens:<user>"; or matching a WithRedactKeys glob) has its values masked
// in arguments, replies and images, and so has a hash or stream field with a
// sensitive name; other values are scrubbed for JSON fields, key=value pairs
// and bearer tokens with sensitive names. On top of that:
//
//   - Replies. HVALS has no field names to tell which values are sensitive
//     and learning them would cost a round trip, so its values are always
//     masked (their number is kept); use HGETALL where the values matter.
//     SET ... GET answers with the old value, which is redacted like one.
//     CONFIG GET masks the parameters that hold credentials (requirepass,
//     masterauth, masteruser, ...). The replies of ACL, DUMP, DEBUG, MIGRATE,
//     MONITOR and SLOWLOG are masked whole. A reply that is a list of field,
//     value, ... (HGETALL, HSCAN, HRANDFIELD WITHVALUES, XRANGE, XREAD
//     through Do) has the value after a sensitive name masked; so has any
//     other array, which can mask the element after a list element that
//     happens to read as a sensitive name. CLIENT, AUTH and HELLO are not
//     recorded at all.
//   - Scripts and commands the tracer does not know (modules, anything not
//     in its table). They can answer with any part of their arguments, so
//     when one of them is recorded masked, or a key is sensitive, the whole
//     reply is masked. The source of a script is recorded as it is: do not
//     write secrets into it.
//   - Errors. Redis quotes the start of the arguments in some errors and a
//     script raises whatever it builds, so no replacement of arguments in
//     the text is safe. When a command has a masked argument or a sensitive
//     key, only the error code (ERR, WRONGTYPE, NOSCRIPT, ...) and a phrase
//     from a fixed list ("unknown command", "wrong number of arguments", ...)
//     are kept, followed by "[text withheld: the command has masked
//     arguments]". Other errors are recorded with their text, scrubbed like
//     a value.
//   - Images. A sensitive key is asked for its type only: its image is
//     "[REDACTED]" when it exists and absent when it does not. The same goes
//     for every key of a command that has a sensitive key (RENAME, COPY,
//     LMOVE, SMOVE, the *STORE family and scripts can move its value to the
//     other key). Once a value sits under a key whose name is not sensitive,
//     later commands on that key record it.
package tredis
