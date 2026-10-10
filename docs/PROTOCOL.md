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
`exclusive`: Start fails (409) if another exclusive session is ACTIVE/DRAINING.

## Control plane (`:7070`)
- `POST /v1/sessions` {name,mode,ttl_seconds,quiesce_seconds,max_drain_seconds,initiated_by} -> Session (ACTIVE)
- `GET /v1/sessions[?state=]`, `GET /v1/sessions/{id}`
- `POST /v1/sessions/{id}/stop` -> DRAINING; CP then polls collector drain-status and calls collector seal
- `POST /v1/sessions/{id}/abort`
- `GET /v1/active` -> `{"active":["id",...],"exclusive":"id|","kill":false}` (active = ACTIVE+DRAINING)
- `GET /v1/active/stream` SSE; sends the same JSON as `data:` on connect and on every change (+ heartbeat comment every 15s)
- `POST /v1/kill` {"kill":true|false} global kill switch (included in active-set)

## Collector (`:7071`)
- `POST /v1/ingest` NDJSON of `core.Event` (header `X-TSession-Instance`). Idempotent on
  `(session_id, instance, local_seq)`. Returns `{"accepted":n,"duplicates":m}`. Events for sessions the
  collector doesn't know are accepted (CP/collector may race) but still stored.
- Ordering: collector keeps a short reorder window (default 200ms) per session and assigns `Seq`
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
- Storage interface (collector/store): in-memory impl (default, tests) + Postgres impl (pgx) with the
  schema from the design doc; large payloads stay inline-truncated in v1 (blob store is a stub, fs impl optional).

## Dashboard (`:7072`)
Go service embedding a static UI (vanilla JS, no build step, no CDN/external requests). Proxies
`/api/cp/*` -> control plane, `/api/col/*` -> collector (strips prefix). Shows per session: timeline,
call tree (api -> db/cache/queue -> worker), entity mutation history with before/after diffs, summary.

## SDK runtime (in every service)
Env: `TSESSION_ENABLED=true`, `TSESSION_SERVICE`, `TSESSION_CONTROLPLANE_URL` (default http://127.0.0.1:7070),
`TSESSION_COLLECTOR_URL` (default http://127.0.0.1:7071). `import _ "tsession/sdk/autoinit"` is a no-op unless
enabled. Exporter: bounded ring, batched NDJSON POST to collector, non-blocking; on overflow drop and emit
`sdk.dropped` event with `Attrs["count"]`. Active-set subscriber implements `core.SessionSet`.

## Wrapper contract (contrib + thttp)
Mirror the wrapped library's signature; one-line adoption; if `!core.Enabled()` return the original
object unchanged; idempotent (wrapping twice is a no-op); never panic/block the host; use only
`core.Begin / BeginIngress / Inject / Span` (nil-safe) — never edit `core/` (report needed changes instead).
Mutations: set `core.Mutation` (Store/Entity/PK/Op/Before/After/Diff, Source="sdk"); reads via `core.ReadSummary`.
Queue carriers: Kafka headers, SQS message attributes, AMQP headers; opaque-payload queues use a
side-channel key `ts:ctx:{msgID}` (TTL) holding the injected carrier map. Producer span = `queue.publish`
(Req = full job params); consumer = `worker.start` (BeginIngress adds the link) ... `worker.end`.
