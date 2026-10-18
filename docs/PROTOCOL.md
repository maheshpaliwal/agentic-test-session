# tsession wire protocol & ownership (v1)

All servers bind **127.0.0.1** by default (never 0.0.0.0 unless explicitly configured).
Ports: control plane `7070`, collector `7071`, dashboard `7072`. Wire format is JSON over HTTP
(NDJSON for batches, SSE for the active-set stream). No protobuf/codegen in v1.

## Modules (go.work)
`tsession` (core, thttp, sdk/*, controlplane, dashboard, cmd/*; stdlib only) ·
`tsession/collector` (pgx allowed) · `tsession/contrib/{tsql,tredis,tmongo,tdynamo,tgrpc,tqueue}`
(each may pull its own third-party libs). Only touch your own module's go.mod.

## Session model (control plane owns the FSM)
States: `CREATED → ACTIVE → DRAINING → SEALED`, plus `ABORTED`, `EXPIRED`.
```json
{"id":"ts_<ulid-ish>","name":"","mode":"header|exclusive","state":"ACTIVE",
 "initiated_by":"dashboard|client|docker|api","created_at":"RFC3339","started_at":"","ended_at":"",
 "ttl_seconds":3600,"quiesce_seconds":3,"max_drain_seconds":60,
 "incomplete":false,"dropped_events":0,"bundle_uri":""}
```
Extra optional fields: `reason` (`"ttl"` when ended by TTL) and `seal_error` (set when the collector seal failed).
TTL expiry is not a shortcut: an ACTIVE session past `ttl_seconds` moves to DRAINING (reason=ttl) and goes through
the normal drain + seal path. A clean seal records `EXPIRED`; if the seal call keeps failing (retried with
exponential backoff, default budget 30s) the session is recorded `SEALED` with `incomplete=true` and `seal_error`.
The same retry/`seal_error` rule applies to a normal stop.

`exclusive`: Start fails (409) if another exclusive session is ACTIVE/DRAINING.

## Control plane (`:7070`)
- `POST /v1/sessions` {name,mode,ttl_seconds,quiesce_seconds,max_drain_seconds,initiated_by} -> Session (ACTIVE)
- `GET /v1/sessions[?state=]`, `GET /v1/sessions/{id}`
- `PATCH /v1/sessions/{id}` {"name":"..."} -> Session; relabel in any state incl. SEALED/archived (404 unknown id,
  400 empty name after sanitising; sets `name_auto=false`; same loopback guard, needs `Content-Type: application/json`)
- `POST /v1/sessions/{id}/stop` -> DRAINING; CP then polls collector drain-status and calls collector seal
- `POST /v1/sessions/{id}/abort`
- `GET /v1/active` -> `{"active":["id",...],"exclusive":"id|","kill":false}` (active = ACTIVE+DRAINING)
- `GET /v1/active/stream` SSE; sends the same JSON as `data:` on connect and on every change (+ heartbeat comment every 15s)
- `POST /v1/kill` {"kill":true|false} global kill switch (included in active-set)

### Session names
- Session JSON has `name_auto` (bool, always present): true when the server generated the name.
- Empty `name` on start -> auto name from bindings: `<name>=<value>` of the first binding (`x-user-id=act_bind_9`; non-headers are
  prefixed `jwt:sub=`, `cookie:sid=`, `query:uid=`); up to 2 joined with ` + ` then ` +N` for the rest; no bindings ->
  `<initiated_by> HH:MM:SS` (UTC), e.g. `dashboard 14:32:07`.
- Templates in a supplied name (start and rename), single pass: `{bind}` (first binding value), `{bind.<name>}` (case-insensitive
  for headers; `jwt:sub` style also matches), `{initiated_by}`, `{time}` (HH:MM:SS UTC), `{date}`; unknown placeholders are left
  as-is, a missing binding expands to `?`.
- Every name is sanitised: control/bidi-override chars stripped, whitespace collapsed and trimmed, max 120 runes (truncated with `…`).
- `tracectl rename <id|name> <new name>`; `watch-docker` label `tsession.name` (templates allowed) sets the name, else the
  `tsession.id` value is used.

### Control plane hardening
- Every endpoint except `GET /healthz` (static `ok`, any Host) passes `core.LoopbackGuard`: Host must be loopback or in
  `--allowed-hosts`/`TSESSION_CP_ALLOWED_HOSTS` (e.g. `controlplane:7070`); a request with an `Origin` header is rejected
  unless listed in `--allowed-origins`/`TSESSION_CP_ALLOWED_ORIGINS` (default none; includes `Origin: null`); a request with a
  body needs `Content-Type: application/json`. Non-browser clients (SDK, tracectl, SSE subscriber) send no Origin and are unaffected.
- `ttl_seconds`, `quiesce_seconds`, `max_drain_seconds`: 0/omitted = default, otherwise 1..604800 (7 days); anything else is 400.
- Drain quiescence uses only the control plane's monotonic clock: drain-status is quiet once `pending==0` and none of
  `event_count`, `last_event_unix_ns`, `received_unix_ns`, `dropped_events` changed for `quiesce_seconds`. Collector timestamps are
  opaque change tokens, never compared with local time.
- Seal is retried (backoff, bounded by a 30s budget, each attempt capped by the remaining budget); on exhaustion the session ends
  `SEALED` with `incomplete=true` and `seal_error` (also for TTL expiry).
- Finished sessions leave the in-memory index after `--retention`/`TSESSION_CP_RETENTION` (default 24h); with `--store-file` they
  are archived to `<file>.archive` and stay readable via GET.

## Collector (`:7071`)
- `POST /v1/ingest` NDJSON of `core.Event` (header `X-TSession-Instance`). Idempotent on
  `(session_id, instance, local_seq)`. Returns `{"accepted":n,"duplicates":m}`. Events for sessions the
  collector doesn't know are accepted (CP/collector may race) but still stored.
- Ordering: collector keeps a short reorder window (default 500ms) per session and assigns `Seq`
  (1..N, gapless, single writer per session) ordered by `(EndHLC, Service, LocalSeq)`.
  **Seq = completion order.** Tree/nesting comes from `ParentID`/`LinkIDs`, not Seq. Events arriving
  after their window closed get the next Seq (approximation; documented).
- Read API:
  - `GET /v1/sessions/{id}/events?from_seq=0&limit=1000` -> `{"events":[...],"next_seq":n,"sealed":bool}`
  - `GET /v1/sessions/{id}/trace` -> tree: `{"session_id","roots":[Node]}` where
    `Node={"event":Event,"children":[Node],"linked":[Node]}` (`linked` = worker spans whose LinkIDs contain this span)
  - `GET /v1/sessions/{id}/entities` -> list of `{store,entity,pk,mutation_count}`
  - `GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}` -> ordered mutations with before/after/diff + seq
  - `GET /v1/sessions/{id}/summary` -> counts by kind/service, mutations by store/entity, failures,
    pending_async, dropped_events, flags, duration
  - `GET /v1/sessions` -> sessions the collector holds events for `[{id,event_count,first_ns,last_ns,sealed}]`
- Internal (control plane -> collector):
  - `GET /v1/internal/sessions/{id}/drain-status` -> `{"pending":n,"last_event_unix_ns":n,"event_count":n,"dropped_events":n}`
  - `POST /v1/internal/sessions/{id}/seal` -> flushes window, resolves tx `effective`, marks orphans, writes bundle -> `{"incomplete":bool,"orphaned":[spanIDs],"bundle_uri":""}`
- Pending-async ledger: a `queue.publish` event opens an entry keyed by its `SpanID`;
  a `worker.end` event whose `LinkIDs` contains that id closes it. Seal marks unclosed entries orphaned.
- Tx resolution: `db.write` events carry `TxID`; `db.tx.commit`/`db.tx.rollback` with the same TxID set
  `Mutation.Effective` (true/false) on all that tx's mutations at seal/read time. No TxID => effective.
- Postgres store: tables live in a dedicated schema (`--pg-schema` / `TSESSION_COLLECTOR_PG_SCHEMA`, default `tsession`;
  `[a-z_][a-z0-9_]{0,62}` only), created if missing and set as the pool `search_path`.
- Storage interface (collector/store): in-memory impl (default, tests) + Postgres impl (pgx) with the
  schema from the design doc; large payloads stay inline-truncated in v1 (blob store is a stub, fs impl optional).

### Collector hardening (v1.1)
- **Ingest response**: always `200 {"accepted","duplicates","rejected","rejected_reasons":{reason:n}}` unless the whole
  body exceeds `MaxBodyBytes` (413, default 32MB) or storage fails (500; 503 when a session's buffer is full). A line
  larger than `--max-line-bytes` (`TSESSION_COLLECTOR_MAX_LINE_BYTES`, default 4MB) is rejected alone
  (`oversize_line`); other reasons: `invalid_json`, `invalid_session_id`. Exporters must not discard a batch on 200.
- **Poison events**: `\u0000` escapes in event JSON are replaced by `\ufffd` at ingest (Postgres jsonb/text rejects NUL).
  Any other append failure is retried; after 3 consecutive failures the batch is bisected and a single event that
  still fails while the store answers reads is **quarantined** (dropped, counted per session, logged without payload).
  If the store itself is down nothing is quarantined. `quarantined` appears in drain-status, summary (also
  `flags.quarantined`) and the seal result; a session with quarantined events seals `incomplete=true`.
- **Reorder window** defaults to **500ms** (was 200ms): the SDK exporter flushes every 100ms, the POST adds network
  latency, and a cross-service child span can reach the collector in an earlier batch than its parent (which ends,
  and is exported, later). Shorter windows regularly invert parent/child `Seq` order. Override with `--window`.
- **Memory**: a sealed session's actor (goroutine + dedupe state) is evicted after `--idle-evict`
  (`TSESSION_COLLECTOR_IDLE_EVICT`, default 5m; unsealed idle sessions after 12x that); state reloads from the
  store on demand. Read endpoints never create actors; unknown ids return empty documents (200). Resolved views
  (`/trace`, `/entities`, `/summary`) are cached per session and invalidated by a new `Seq` or the seal.
- **Tx resolution**: a `db.tx.commit` with `Err != ""` or `Attrs["commit_failed"]=="true"` is NOT effective
  (as is any `db.tx.rollback`, including tsql's failed-commit rollback with `commit_failed=true`). Any negative
  outcome for a TxID is sticky regardless of arrival order. Applies to the live, paged and sealed views and to
  `store.TxOutcomes`.
- **Pending-async ledger**: a `queue.publish` entry is closed by a `worker.end` whose `LinkIDs` contain its span id
  *unless* `Attrs["retry_pending"]=="true"` (that end only records the attempt), or by `worker.dlq`.
  `worker.retry` never closes. Order of arrival does not matter. At seal, entries still open whose attempts carried
  `retry_pending` (or a `worker.retry`) are listed in the seal result's `retry_pending` (subset of `orphaned`) and the
  publisher event gets flag `retry_pending` next to `orphaned`.
- **Drain-status**: `last_event_unix_ns` (and the explicit alias `received_unix_ns`) is the collector's *receive*
  time of the latest ingest request for the session, updated on arrival (before the window flushes), independent
  of SDK clocks. `pending` = events buffered in the reorder window + open async entries.
- **Seal** is serialised on the session (concurrent calls write the bundle once; files are written via temp+rename)
  and idempotent.
- **Access control**: the whole API is wrapped in `core.LoopbackGuard` (Host must be loopback or listed in
  `--allowed-hosts` / `TSESSION_COLLECTOR_ALLOWED_HOSTS`, comma-separated; browser `Origin` must be listed in
  `--allowed-origins` / `TSESSION_COLLECTOR_ALLOWED_ORIGINS`; POST bodies must be `application/x-ndjson` or
  `application/json`). `/v1/internal/*` additionally requires a loopback peer or one inside
  `--internal-allow` / `TSESSION_COLLECTOR_INTERNAL_ALLOW` (CIDRs/IPs, default loopback only). Callers that proxy
  (dashboard, control plane in docker) must send a Host the collector allows and must not forward browser `Origin`.
- **Postgres**: on `ErrSeqConflict` (a second collector wrote the session) the actor catches up from the store
  (events past its `next`), drops buffered events the other writer already stored, and retries. `entity_mutations` is a
  write-only side index for ad-hoc SQL; the entities endpoints read the cached event view, not that table.

## Dashboard (`:7072`)
Go service embedding a static UI (vanilla JS, no build step, no CDN/external requests). Proxies
`/api/cp/*` -> control plane, `/api/col/*` -> collector (strips prefix). Shows per session: timeline,
call tree (api -> db/cache/queue -> worker), entity mutation history with before/after diffs, summary.

## SDK runtime (in every service)
Env: `TSESSION_ENABLED=true`, `TSESSION_SERVICE`, `TSESSION_CONTROLPLANE_URL` (default http://127.0.0.1:7070),
`TSESSION_COLLECTOR_URL` (default http://127.0.0.1:7071). `import _ "tsession/sdk/autoinit"` is a no-op unless
enabled. Exporter: bounded ring, batched NDJSON POST to collector, non-blocking; on overflow drop and emit
`sdk.dropped` event with `Attrs["count"]`. Active-set subscriber implements `core.SessionSet`.

### Bindings
Implements `docs/BINDING.md` (control plane half).
- `POST /v1/sessions` accepts and Session responses carry `"bindings":[{"source","name","value"}]` (omitted when none).
  Sources `header|jwt_claim|cookie|query`; header names are lowercased; at most 8 bindings, name <= 64 B, value <= 256 B,
  all non-empty; exact duplicates are collapsed. Invalid, or combined with `mode=exclusive` -> 400.
- Uniqueness is checked atomically against ACTIVE sessions only: conflict -> 409
  `{"error":"binding_conflict","conflicting_session":"ts_...","binding":{"source","name","value"}}`.
- Active set (`GET /v1/active`, every SSE message) adds `"bindings":[{"session_id","source","name","value"}]` for ACTIVE
  sessions only (omitted when empty). A binding leaves the set, and becomes reusable, as soon as its session leaves ACTIVE
  (stop, abort, TTL expiry), while the session itself stays in `active` while DRAINING.
- `jwt_claim` is read from an unverified JWT payload: attribution only, spoofable, never a security boundary.
- tracectl: `start --bind source:name=value` (repeatable; `jwt` aliases `jwt_claim`); `watch-docker` label
  `tsession.bind=header:x-user-id=u_1,jwt:sub=u_1` (comma-separated; containers share a session only if
  `tsession.id` and `tsession.bind` both match).

### SDK runtime hardening notes
- `Instance` is always made unique per process start: a user-supplied `TSESSION_INSTANCE` is kept as a prefix and `-<pid>-<6 hex>` is appended (collector dedupes on `(Instance, LocalSeq)`).
- Default exporter flush interval is 100ms (collector reorder window default: 500ms). A 413 from `/v1/ingest` bisects the batch; a single oversize event is replaced by a stub (ids/kind/name kept, `Flags=["oversize_dropped"]`). `rejected` in the ingest response is added to the `sdk.dropped` counters.
- Remote HLC values more than 10 minutes ahead of local physical time are ignored (not merged).
- Header propagation by the patched default transport: `TSESSION_PROPAGATE_HOSTS` (comma-separated host suffixes; empty = all) and `TSESSION_NO_PROPAGATE_HOSTS` (always wins). Excluded hosts are still recorded as `api.out`, with attr `propagated=false`.
- SSE (`text/event-stream`) and hijacked `api.in` spans end at first write/hijack with attr `stream=true`.

## Wrapper contract (contrib + thttp)
Mirror the wrapped library's signature; one-line adoption; if `!core.Enabled()` return the original
object unchanged; idempotent (wrapping twice is a no-op); never panic/block the host; use only
`core.Begin / BeginIngress / Inject / Span` (nil-safe) — never edit `core/` (report needed changes instead).
Mutations: set `core.Mutation` (Store/Entity/PK/Op/Before/After/Diff, Source="sdk"); reads via `core.ReadSummary`.
Queue carriers: Kafka headers, SQS message attributes, AMQP headers; opaque-payload queues use a
side-channel key `ts:ctx:{msgID}` (TTL) holding the injected carrier map. Producer span = `queue.publish`
(Req = full job params); consumer = `worker.start` (BeginIngress adds the link) ... `worker.end`.

Additional wrappers: `contrib/tredisv8` (go-redis v8 `Hook()` / `Wrap` / `WrapAny` for clients held behind an
internal wrapper; same behaviour as tredis) and `contrib/tkafkaconfluent` (confluent-kafka-go `Produce` /
`Consume` over `tqueue`; headers carrier, 1 KiB / 8-header injection cap).
