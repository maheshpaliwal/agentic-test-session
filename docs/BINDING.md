# Attribute binding (per-user sessions without sending a session id)

Goal: a tester's session captures only that tester's traffic, even when the client (e.g. the Android app)
sends no `x-tsession-id`, by matching an attribute the client already sends (user id, device id, JWT claim).

## Model
A session may carry `bindings`: a list of `{source, name, value}`.
- `source`: `header` | `jwt_claim` | `cookie` | `query`.
  - `jwt_claim`: `name` is the claim (e.g. `sub`), read from the `Authorization: Bearer <jwt>` header. The JWT
    payload is **base64-decoded only; the signature is NOT verified.** This attributes traffic, it does not
    authenticate it. A caller can spoof a binding value and have their requests land in someone else's
    session. Acceptable for a test env; never a security boundary. Documented in every API/UI surface.
  - `header`/`cookie`/`query`: exact-match (header names case-insensitive; values case-sensitive).
- A request matches a session if ANY of its bindings match (OR), unless it matches bindings of two different
  sessions, which is ambiguous (see Ambiguity). Optionally an ordered priority list changes this (see Priority and
  fallback). Option `match: "all"` for AND is v2.

## Rules
1. **Precedence at the first instrumented hop:** explicit `x-tsession-id` header > binding match > exclusive mode > untraced.
2. **Uniqueness:** a `(source,name,value)` may be bound by at most one session in ACTIVE state; starting a
   second returns 409 (like exclusive conflicts). Bindings and `exclusive` mode are mutually exclusive on one session.
3. **Lifetime:** bindings are live only while the session is ACTIVE. After stop (DRAINING) new requests are
   no longer attributed by binding; in-flight work continues via propagated context.
4. **Only the first hop needs the attribute.** Once matched, the SDK stamps the session into the context and
   normal propagation (`traceparent`, `x-tsession-id`, queue carriers) carries it downstream.
5. **Limits:** max 8 bindings per session, value <= 256 bytes, name <= 64 bytes; JWT header larger than 8 KB ignored.

## Priority and fallback
Optional local config `BindPriority []core.BindingKey` (`runtime.Config.BindPriority`), env
`TSESSION_BIND_PRIORITY`: comma-separated `source:name`, e.g. `header:x-user-id,header:name`. `jwt` is accepted as
an alias of `jwt_claim`; header names are lower-cased; invalid or duplicate entries are ignored (one log line).

With a list set, for each request:
1. Walk the priority keys in order. The FIRST key that has a non-empty value on the request DECIDES: look up the
   ACTIVE session bound to exactly `(key, value)`. Found -> attribute to it. Not found -> the request is
   **untraced**; later keys are NOT consulted (otherwise a caller could be attributed through someone else's
   header while carrying an unknown id in the primary one).
2. If no priority key is present, the remaining bound keys (not in the list) are tried in the plain OR manner.
3. List unset -> plain OR behaviour, unchanged.

Only keys that some active binding uses are extracted from the request, so with no active bindings nothing is
extracted (0-alloc fast path) regardless of the list. An explicit `x-tsession-id` still beats everything.

`api.in` attrs: `bound_by=<source>:<name>` is the key that decided (never the value). `bound_fallback=true` is added
when the deciding key is not the first in the priority list, or the list is set and a non-listed key decided.
Keys in the list are masked in recorded headers/query whenever they appear, even if no active binding uses them.

The list is **local process config, not distributed by the control plane**. All services in one environment must set
the same `TSESSION_BIND_PRIORITY`: the entry hop decides attribution, and differing lists would let two services
attribute the same request differently (or one trace one and the other drop it). Treat it like other env config.

Example: `TSESSION_BIND_PRIORITY=header:x-user-id,header:name`. Note a backend may rewrite `name` from
`X-User-Id` only AFTER the outer handler runs, so at the tracing hop `name` is only useful when the client itself
sends it; `x-user-id` is the reliable key.

## Ambiguity
In the OR path, if a request matches bindings of two DIFFERENT sessions the SDK does not pick one: the request is
untraced (exclusive mode is not used either), `core.BindingAmbiguous()` (atomic counter, also
`runtime.GetStats().BindingAmbiguous`) is incremented and a one-line log is emitted at most every 30s. Matching
several keys of the SAME session is fine. The priority path cannot be ambiguous (exact lookup); the control plane
guarantees `(source,name,value)` is unique among ACTIVE sessions.

## Wire changes
- Session JSON: `"bindings":[{"source":"header","name":"x-user-id","value":"u_123"}]` (request + response).
- `POST /v1/sessions` accepts `bindings`; invalid -> 400; conflicting -> 409 with the conflicting session id.
- Active set (`GET /v1/active`, SSE): add
  `"bindings":[{"session_id":"ts_…","source":"header","name":"x-user-id","value":"u_123"}]` for ACTIVE sessions only.
  SDKs derive the set of attribute keys to extract from this list, so they extract nothing when it is empty.
- `core.SessionSet` stays backward compatible; add optional interface
  `Binder{ MatchAttrs(a Attributes) (sessionID string, ok bool) }` and
  `Attributes{ Get(source, name string) string }`.
- `core.BeginIngressAttrs(ctx, kind, name, carrier, attrs)` = BeginIngress + binding lookup (same precedence).
  `core.BeginIngress` keeps its signature (no attrs = no binding lookup).
- Transports pass attributes: `thttp` (headers, cookies, query), `tgrpc` (metadata; cookie/query unsupported),
  `tqueue` consumers never bind (they carry propagated context).
- JWT decode is cached (token hash -> claims) for 60s, bounded size.

## Surfaces
- `sdk/client`: `Options.Bindings`; `tracectl start --bind header:x-user-id=u_123 --bind jwt:sub=u_123`.
- Dashboard: Start form gets repeatable binding rows (source, name, value) with a spoofing warning;
  session page lists bindings; sessions list shows a "bound" badge.
- Collector: no change (events already carry session_id).

## Tests (must exist)
- Two users, same endpoints, concurrent: each session sees only its own events (zero cross-session).
- No attribute -> untraced; header session id beats a conflicting binding; conflicting active binding -> 409.
- After Stop, new requests with the attribute are untraced; in-flight worker still completes into the session.
- JWT: valid, malformed, huge, `alg:none`, missing claim; unverified-signature behaviour documented by test.
- Downstream service with no binding config still joins the trace via propagation.
- Active-set with no bindings => no attribute extraction (no JWT decode, no cookie parse) — benchmark/alloc test.
