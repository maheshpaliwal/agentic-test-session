# tsession wire protocol & ownership (v1)

All servers bind **127.0.0.1** by default (never 0.0.0.0 unless explicitly configured).
Ports: control plane `7070`, collector `7071`, dashboard `7072`. Wire format is JSON over HTTP
(SSE for the active-set stream), except `POST /v1/ingest`, which is protobuf only (see "Protobuf ingest"; NDJSON
ingest was removed). The SDK ships a hand-written encoder so the root module stays stdlib-only.

## Modules (go.work)
root module (core, thttp, sdk/*, controlplane, dashboard, cmd/*; stdlib only) ·
`collector` (pgx allowed) · `proto` (protobuf schema, generated code, codec) · `contrib/{tsql,tredis,tmongo,tdynamo,tgrpc,tqueue}`
(each may pull its own third-party libs). Only touch your own module's go.mod.

## Session model (control plane owns the FSM)
States: `CREATED → ACTIVE → DRAINING → SEALED`, plus `ABORTED`, `EXPIRED`.
```json
{"id":"ts_<ulid-ish>","name":"","name_auto":false,"mode":"header|exclusive","state":"ACTIVE",
 "initiated_by":"dashboard|client|docker|api","created_at":"RFC3339","started_at":"RFC3339","ended_at":"RFC3339",
 "ttl_seconds":3600,"quiesce_seconds":3,"max_drain_seconds":60,
 "incomplete":false,"dropped_events":0,"bundle_uri":""}
```
Extra optional fields (omitted when empty/false):
- `reason`: `"ttl"` when ended by TTL.
- `seal_error`: set when the collector seal failed; cleared if a later retry seals the session.
- `seal_pending`: `true` while a seal that failed past its budget is still being retried in the background. The
  session is already terminal (`SEALED`, out of the active set, so SDKs have stopped recording), but the collector
  has not sealed it yet: `bundle_uri` is empty and `incomplete` is `true`. When a retry succeeds the field
  disappears together with `seal_error`, and `bundle_uri`/`incomplete` are filled in from the seal. When the retries
  are given up (see "Control plane hardening") the field disappears and `seal_error` stays.
- `drain_timed_out`: `true` when the drain ended at `max_drain_seconds` rather than on a quiet collector (such a
  session is `incomplete` even if the seal itself succeeds).

TTL expiry is not a shortcut: an ACTIVE session past `ttl_seconds` moves to DRAINING (reason=ttl) and goes through
the normal drain + seal path. A clean seal records `EXPIRED`; if the seal call keeps failing (retried with
exponential backoff, default budget 30s) the session is recorded `SEALED` with `incomplete=true`, `seal_error` and
`seal_pending`. The same retry/`seal_error` rule applies to a normal stop. A session sealed by a later background
retry stays `SEALED` (a terminal state never changes), also when it was ended by TTL.

`exclusive`: Start fails (409) if another exclusive session is ACTIVE/DRAINING.

## Control plane (`:7070`)
- `POST /v1/sessions` {name,mode,ttl_seconds,quiesce_seconds,max_drain_seconds,initiated_by} -> Session (ACTIVE)
- `GET /v1/sessions[?state=&name=&limit=&before=]` -> JSON array of Sessions; `GET /v1/sessions/{id}`
  - Without `limit`: every session, oldest first (by `created_at`, then `id`), optionally only those in `state`
    and/or named exactly `name`. This is what the endpoint has always answered; its cost and size grow with the
    number of sessions ever recorded, so clients that show or search sessions should page. Three things differ
    from before for a request without `limit`: `name` now filters (it was ignored), `before` is refused with
    `400` (it was ignored), and the response can carry `X-Tsession-Unreadable`.
  - `limit=N` (1..1000): the newest N matching sessions, **newest first**. `before=<cursor>` (only with `limit`)
    continues after an earlier page. The body stays a plain array; the rest is in response headers:
    `X-Tsession-Next: <cursor>` when older sessions exist (absent on the last page), `X-Tsession-Total: <n>` the
    number of sessions matching `state`/`name` whatever the page (absent when the store cannot tell cheaply: the
    file store for a query by `name`). `400` for a `limit` out of range, a `before` that is not a cursor from this
    API, or `before` without `limit`.
  - The cursor is opaque. It names a place in the order (creation time and id of the last session of the page),
    not a count, so it stays valid when that session is deleted and across restarts. While sessions are created,
    archived, changed and deleted, a run of pages never returns a session twice; a session that exists and
    matches the filters during the whole run is returned exactly once; one that is created, deleted, or starts or
    stops matching during the run may or may not be returned (sessions created after the first page are not: they
    are newer than it). A page can hold fewer than `limit` sessions, even none, and still carry `X-Tsession-Next`.
  - `name` is an exact match (names are not unique). With `--store-file` a lookup by name reads only the archived
    sessions whose name has the same 16-bit hash, newest first, until the page is full.
  - `X-Tsession-Unreadable: <n>` on any list response (with or without `limit`) from which n sessions are missing
    because the store has them and could not read them (see hardening below); absent when the list is complete.
  - A control plane older than these parameters ignores them: it answers with every session (in `state`), oldest
    first, without the headers. A client that sends `name` should check the names in the reply, and one that sends
    `limit` should be ready for more sessions than it asked for.
- `PATCH /v1/sessions/{id}` {"name":"..."} -> Session; relabel in any state incl. SEALED/archived (404 unknown id,
  400 empty name after sanitising; sets `name_auto=false`; same loopback guard, needs `Content-Type: application/json`)
- `POST /v1/sessions/{id}/stop` -> DRAINING; CP then polls collector drain-status and calls collector seal
- `POST /v1/sessions/{id}/abort`
- `DELETE /v1/sessions/{id}` -> `{"deleted":true,"id":"..."}`; finished sessions only (`409` while CREATED/ACTIVE/
  DRAINING, `404` unknown). The collector is asked first to drop the session's events, seal and bundle; if that
  fails the answer is `502` and the session stays, so the call can be repeated. A session with `seal_pending` can
  be deleted: its seal retries stop first (an attempt in flight is cancelled and waited for), and resume if the
  collector delete fails.
- `GET /metrics` Prometheus text format (sessions by state, started/finished/deleted totals, seal failures,
  `tsession_controlplane_seals_pending`, `tsession_controlplane_seal_retries_total{result="success|failure"}`,
  `tsession_controlplane_seals_abandoned_total`, subscribers, kill switch, `tsession_build_info`)
- `GET /v1/active` -> `{"active":["id",...],"exclusive":"id|","kill":false,"generation":n}` (active = ACTIVE+DRAINING)
- `GET /v1/active/stream` SSE; sends the same JSON as `data:` on connect and on every change (+ heartbeat comment every 15s)
  The SDK sends `X-Tsession-Service` and `X-Tsession-Instance` with the request (optional; shown to operators, not trusted).
- `GET /v1/subscribers` -> `[{"service","instance","connected_at","generation"}]`: the current followers of the
  stream, i.e. the connected SDKs. `service`/`instance` are omitted for followers that did not send them.
  `generation` is the newest active-set `generation` (also on `GET /v1/active` and in every stream message)
  written to that follower: a follower at or past the generation in which a session became active has been sent
  it (sent, not necessarily applied yet).
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
  `SEALED` with `incomplete=true`, `seal_error` and `seal_pending` (also for TTL expiry).
- A pending seal keeps being retried in the background (the collector seal is idempotent): from the periodic sweep and
  right after a restart (the marker is stored with the session, archived sessions included), with exponential backoff
  from the poll interval (500ms) up to 1 minute, each attempt bounded by 30s. Success clears `seal_error` and
  `seal_pending` and records `bundle_uri`/`incomplete`. The retries end when the session is deleted, or 24h after
  `ended_at` (`Config.SealPendingBudget`; at least one attempt is made after a restart), when `seal_pending` is
  cleared and `seal_error` stays.
- Finished sessions leave the in-memory index after `--retention`/`TSESSION_CP_RETENTION` (default 24h); with `--store-file` they
  are archived to `<file>.archive` and stay readable via GET.
- `--store-file` files: `<file>` is a snapshot of the unarchived sessions (a JSON array), `<file>.journal` holds the
  changes since that snapshot (one JSON object per line: `{"op":"put","session":{...}}`, `{"op":"evict","ids":[...]}`,
  `{"op":"delete","ids":[...]}`) and `<file>.archive` the archived sessions (one JSON session per line, last line of
  an id wins; the line of a deleted session, and the old line of a session archived again, is overwritten with
  spaces and fsynced, and such blank lines are dropped at startup). The journal is folded into the snapshot at startup, on a clean shutdown, and in the background when
  it passes 4 MiB (or the snapshot size, if larger); read `<file>` together with the journal while the control plane
  runs. A change is written without fsync: it survives a crash of the process, and a power failure can take back the
  last changes (a session archived just before is then unarchived again, not lost) but not more: snapshots are
  fsynced before they replace the old one. One process per store file.
- An archived session whose line in `<file>.archive` cannot be read (I/O error, file cut or changed by something
  else) is not reported as unknown: `GET`, `PATCH`, stop and abort of it answer `500` and can be repeated. `DELETE`
  still works, here and in the collector: an archived session is finished, and refusing would leave it
  undeletable. It is missing from `GET /v1/sessions`, whose response then carries `X-Tsession-Unreadable: <n>`
  with the number of sessions missing from that response. A failed read of one session is logged, a list logs one
  line per call, and `tsession_controlplane_store_read_errors_total` counts failed reads (not distinct sessions).
  At start, archive lines that do not parse are saved to `<file>.archive.corrupt`, logged and counted, instead of
  being dropped when the archive is tidied; their sessions are no longer known, so later lists do not report them.
- The file store keeps about 100 bytes in memory per archived session (its id and a 24-byte index entry: line
  offset and length, creation time, state, seal marker, name hash). A list with `limit` picks its page from that
  index and reads only the lines of the sessions it returns.

## Collector (`:7071`)
- `POST /v1/ingest` framed protobuf `Event` messages (`Content-Type: application/x-protobuf`, required; anything
  else, including a missing type or NDJSON, gets `415` naming it; see "Protobuf ingest") (header
  `X-TSession-Instance`). Idempotent on `(session_id, instance, local_seq)`. Returns
  `{"accepted":n,"duplicates":m,"rejected":r,"rejected_reasons":{...}}` (the last two are omitted when zero). Events for sessions the
  collector doesn't know are accepted (CP/collector may race) but still stored.
- Ordering: the collector holds each event for a reorder window (default 500ms) after it arrives and assigns `Seq`
  (1..N, gapless, single writer per session) ordered by `(EndHLC, Service, LocalSeq)`.
  **Seq = completion order.** Tree/nesting comes from `ParentID`/`LinkIDs`, not Seq. Each event is held for one
  reorder window after it arrives, so a straggler that sorts before it can still take its place; an event held
  back by younger ones is released after two windows at the latest. Events arriving after that get the next Seq
  (approximation; documented).
- Read API:
  - `GET /v1/sessions/{id}/events?from_seq=1&limit=1000` -> `{"events":[...],"next_seq":n,"sealed":bool}`
    (`from_seq` is inclusive; `limit` is at most 10000)
  - `GET /v1/sessions/{id}/trace` -> tree: `{"session_id","roots":[Node]}` where
    `Node={"event":Event,"children":[Node],"linked":[Node]}` (`linked` = worker spans whose LinkIDs contain this span)
  - `GET /v1/sessions/{id}/entities` -> list of `{store,entity,pk,mutation_count}`
  - `GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}` ->
    `{"store","entity","pk","mutations":[{seq,span_id,service,kind,tx_id,end_ns,op,before,after,diff,source,effective}]}`
    in Seq order; `{pk}` is the canonical JSON key or, for a single-column key, the bare value
  - `GET /v1/sessions/{id}/summary` -> `{session_id,event_count,by_kind,by_service,mutations,failures,pending_async,
    pending_async_ids,dropped_events,flags,duration_ns,sealed,incomplete,quarantined}`
  - `GET /v1/sessions` -> sessions the collector holds events for `[{id,event_count,first_ns,last_ns,sealed}]`,
    ordered by id. It lists every session: its cost grows with everything stored (a table scan on DynamoDB), so
    do not poll it.
  - `GET /v1/sessions?ids=a,b,c` -> the same array for the named sessions only: `[{id,event_count,sealed}]`, one
    element per requested id the collector holds events for, in the order asked. An id without stored events is
    left out. `first_ns` and `last_ns` are **not** in an `ids` answer (they need the session's events). The answer
    costs one head lookup per id plus one seal lookup per id that has events, at most 8 at a time; it never lists
    sessions and never reads events. `event_count` counts stored events, as in the full list (events still in the
    reorder window are not counted).
    - `ids` is a comma-separated list of at most 1000 distinct session ids (empty elements are skipped, repeats
      count once, several `ids=` parameters are one list). More than 1000, or an id that is not a valid session
      id, is `400`. `ids=` with no id answers `[]`, not the full list.
    - Every `ids` answer carries `X-Tsession-Unanswered: <n>`: the number of requested ids left out because the
      store failed for them (or the request ended first). A store failure is not a `500` here. An id missing from
      the body means "no events held" only when the header is `0`. The header's presence also tells a client that
      the collector understood `ids`: a collector older than this parameter ignores it and answers with the full
      list and no header.
    - The ids travel in the URL: 200 ids of 30 characters are about 6 KB, 1000 are about 31 KB. The collector and
      the dashboard proxy accept that (Go's 1 MB request-header limit); a reverse proxy in front of them may not
      (nginx allows 8 KB by default), so raise its limit or ask for fewer ids per request.
- Internal (control plane -> collector):
  - `GET /v1/internal/sessions/{id}/drain-status` -> `{"pending":n,"last_event_unix_ns":n,"event_count":n,"dropped_events":n}`
  - `POST /v1/internal/sessions/{id}/seal` -> flushes window, resolves tx `effective`, marks orphans, writes bundle -> `{"incomplete":bool,"orphaned":[spanIDs],"bundle_uri":""}`
  - `DELETE /v1/internal/sessions/{id}` -> `{"deleted":true,"session_id":"..."}`; drops buffered and stored events,
    the seal, cached views and the bundle directory. Idempotent (unknown id is `200`). Events arriving for the id
    afterwards start a new session at Seq 1.
- `GET /metrics` Prometheus text format (`tsession_collector_*`, `tsession_build_info`).
- Retention: `--retention D` / `TSESSION_COLLECTOR_RETENTION` (default `0` = keep forever) deletes a session `D`
  after its seal. A never-sealed session is deleted once its newest event is older than `D` and its in-memory actor
  has been evicted (see `--idle-evict`).
- Pending-async ledger: a `queue.publish` event opens an entry keyed by its `SpanID`;
  a `worker.end` event whose `LinkIDs` contains that id closes it. Seal marks unclosed entries orphaned.
- Tx resolution: `db.write` events carry `TxID`; `db.tx.commit`/`db.tx.rollback` with the same TxID set
  `Mutation.Effective` (true/false) on all that tx's mutations at seal/read time. No TxID => effective.
- Postgres store: tables live in a dedicated schema (`--pg-schema` / `TSESSION_COLLECTOR_PG_SCHEMA`, default `tsession`;
  `[a-z_][a-z0-9_]{0,62}` only), created if missing and set as the pool `search_path`.
- Storage interface (collector/store): in-memory impl (default, tests), Postgres (pgx), MongoDB (`store/mongostore`) and DynamoDB (`store/dynamostore`) impls, selected with `--store mem|pg|mongo|dynamodb`. The Postgres impl uses pgx with the
  large payloads are truncated inline; there is no blob store.

### Protobuf ingest
Mandatory: `POST /v1/ingest` requires `Content-Type: application/x-protobuf` (parameters ignored); any other or missing
type is `415 {"error": "...requires application/x-protobuf"}`. NDJSON ingest was removed (breaking change, pre-release).
- Schema: `proto/tsession/v1/event.proto` (package `tsession.v1`), a field-for-field mirror of `core.Event`
  (`Event`, `PayloadRef`, `Mutation`, `ReadSummary`, `FieldChange`). Field numbers are stable, never reuse them.
  `PayloadRef.inline` is JSON bytes and must be valid JSON; `any` values (mutation `pk`, `FieldChange.old/new`) are
  `google.protobuf.Value`, so numbers are doubles, i.e. what the JSON path yields (`float64`);
  `Mutation.effective` is `optional bool` (unset = unknown). Generated Go is committed at `proto/tsessionpb/event.pb.go`;
  regenerate with `make proto` (protoc + protoc-gen-go v1.36.9).
- Framing: the body is a stream of varint-length-delimited `Event` messages (the `protodelim` format, same as Java's
  `writeDelimitedTo`): `<uvarint len><Event bytes>` repeated, no trailer. An empty body is an empty batch.
- Errors: a message longer than `--max-message-bytes` is skipped and counted `oversize_message`; a well-framed message that
  does not decode (or has invalid inline JSON / invalid UTF-8) is counted `invalid_proto` and the next one is read.
  If the framing itself is broken (bad or truncated length prefix, truncated message) the collector cannot
  resynchronise: it counts one `invalid_proto`, stops reading, and ingests the events decoded so far. The response is
  still `200` with the usual JSON counters; `MaxBodyBytes` still yields `413`.
- NUL characters in strings (and `\u0000` escapes in inline JSON) are replaced by U+FFFD (Postgres rejects NUL).
- SDK: the exporter's default encoder is a hand-written protobuf encoder in `sdk/runtime` (stdlib only, conformance-tested
  against the generated code); `runtime.BatchEncoder` (`ContentType()`, `Encode([]core.Event)`) remains the extension point.
- **curl is not enough any more.** A test batch must be framed protobuf, e.g. a throwaway Go program in the workspace
  (`tracectl` has no ingest command):
  ```go
  body, _ := tsessionproto.Encode([]core.Event{{SessionID: "ts_demo", SpanID: "a", Kind: core.KindAPIIn,
      Instance: "i1", LocalSeq: 1, EndHLC: 1}}) // github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto
  req, _ := http.NewRequest("POST", "http://127.0.0.1:7071/v1/ingest", bytes.NewReader(body))
  req.Header.Set("Content-Type", tsessionproto.ContentType)
  ```
  (`AppendFramed` appends one event at a time.) The collector tests use the same helper pattern (`protoBody`).
- Measured once, when NDJSON ingest still existed (Apple M3 Pro, one 256-event batch of synthetic events with
  mutations, inline payloads and attrs): wire size 307,090 B NDJSON vs 134,526 B protobuf; SDK encode 0.98 ms vs
  1.53 ms; decode 3.29 ms vs 1.60 ms. Historical figures: the NDJSON benchmarks were removed with NDJSON ingest.

### Collector hardening (v1.1)
- **Ingest response**: `200 {"accepted","duplicates","rejected","rejected_reasons":{reason:n}}` (`rejected*` omitted when zero) unless the whole
  body exceeds `MaxBodyBytes` (413, default 32MB) or storage fails (500; 503 when a session's buffer is full). A message
  larger than `--max-message-bytes` (`TSESSION_COLLECTOR_MAX_MESSAGE_BYTES`, default 4MB, applies to each encoded
  protobuf message) is rejected alone (`oversize_message`); other reasons: `invalid_proto`, `invalid_session_id`.
  The old `--max-line-bytes` / `TSESSION_COLLECTOR_MAX_LINE_BYTES` still work as deprecated aliases and log a warning.
  Exporters must not discard a batch on 200.
- **Poison events**: NUL characters and `\u0000` escapes in event strings/inline JSON are replaced by U+FFFD at ingest (Postgres jsonb/text rejects NUL).
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
  (`/trace`, `/entities`, `/summary`) are cached per session, extended with newly stored events, and rebuilt on seal or delete.
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
- **Shutdown**: `Close` flushes every session's reorder window for at most `Config.ShutdownTimeout` (15s, not a
  flag); events still unwritten then are dropped and logged; an ingest or delete that races with
  it gets `503`, a seal `500`. Stop the HTTP server first (the binary does).
- **Slow store**: each store call made outside a request (window flush, catch-up, poison probe) is bounded by 30s.
  A timed-out append is retried and is never treated as a poison event. An actor whose session disappeared from the
  store (deleted by another process) rebuilds its state from the store instead of failing on Seq conflicts.
- **Seal** is serialised on the session (concurrent calls write the bundle once; files are written via temp+rename)
  and idempotent.
- **Access control**: the whole API except `GET /healthz` is wrapped in `core.LoopbackGuard` (Host must be loopback or listed in
  `--allowed-hosts` / `TSESSION_COLLECTOR_ALLOWED_HOSTS`, comma-separated; browser `Origin` must be listed in
  `--allowed-origins` / `TSESSION_COLLECTOR_ALLOWED_ORIGINS`; the only POST body the collector accepts is
  `application/x-protobuf` (ingest; seal sends no body)). `/v1/internal/*` additionally requires a loopback peer or one inside
  `--internal-allow` / `TSESSION_COLLECTOR_INTERNAL_ALLOW` (CIDRs/IPs, default loopback only). Callers that proxy
  (dashboard, control plane in docker) must send a Host the collector allows and must not forward browser `Origin`.
- **Postgres**: on `ErrSeqConflict` (a second collector wrote the session) the actor catches up from the store
  (events past its `next`), drops buffered events the other writer already stored, and retries. `entity_mutations` is a
  write-only side index for ad-hoc SQL; the entities endpoints read the cached event view, not that table.

## Dashboard (`:7072`)
Go service embedding a static UI (vanilla JS, no build step, no CDN/external requests). Proxies
`/api/cp/*` -> control plane, `/api/col/*` -> collector (strips prefix). Shows per session: timeline,
call tree (api -> db/cache/queue -> worker), entity mutation history with before/after diffs, summary.
The session list asks the control plane for the newest 200 sessions (`limit=200`; `#/?page=N` sets another page size,
1..1000) and loads older ones on request ("Load more", `before=<cursor>`). The state filter is sent to the control
plane; the search box filters the sessions that are loaded, and the page says so while older ones are not. The
auto-refresh (every 3 s) fetches the newest page only and merges it into what is loaded; if that page no longer
reaches the newest loaded session (more new sessions than a page holds), the list starts over from it. A response
with `X-Tsession-Unreadable` shows a notice. The proxy passes the query string and these response headers through;
the demo backend (`--demo`, `--open`) implements the same parameters and headers.

## SDK runtime (in every service)
Env: `TSESSION_ENABLED=true`, `TSESSION_SERVICE`, `TSESSION_CONTROLPLANE_URL` (default http://127.0.0.1:7070),
`TSESSION_COLLECTOR_URL` (default http://127.0.0.1:7071). `import _ "github.com/maheshpaliwal/agentic-test-session/sdk/autoinit"` is a no-op unless
enabled. Exporter: bounded ring, batched POST to collector (protobuf), non-blocking; on overflow drop and emit
`sdk.dropped` event with `Attrs["count"]`. Active-set subscriber implements `core.SessionSet`.

### Trace context propagation
A request or message that belongs to a session carries these keys (HTTP headers, gRPC metadata, Kafka and AMQP
headers, SQS message attributes, side-channel record):

| Key | Value | Written by |
|---|---|---|
| `x-tsession-id` | session id | tsession |
| `x-tsession-traceparent` | `00-<32 hex trace id>-<16 hex span id>-01;<clock>`: the trace and the span that is the parent of whatever the receiver starts, then `;` and the `x-tsession-hlc` value written with it | tsession, always |
| `x-tsession-hlc` | sender's hybrid logical clock, decimal | tsession |
| `traceparent` | `00-<32 hex trace id>-<16 hex span id>-01` | tsession only where the application has not set one; see below |

`traceparent` is the W3C Trace Context header, which the application's own tracing (OpenTelemetry, for example) uses
too. tsession never replaces a `traceparent` the application set.

**Writing** (`core.Inject`, `core.WriteTraceparent`). `x-tsession-traceparent` and `x-tsession-hlc` are always set
together, replacing values already there. `traceparent` is set to the same trace context only when
1. the carrier has no `traceparent`, or
2. the carrier's `traceparent` equals the trace context in the `x-tsession-traceparent` it held before this write.
   tsession wrote both, and they arrived here together: a consumer that publishes the message it received with its
   headers, a proxy that forwards headers, a message value produced twice. Or
3. `TSESSION_OVERWRITE_TRACEPARENT=true` (`runtime.Config.OverwriteTraceparent`); see the compatibility note.

"Has no `traceparent`" is a question about the key, not its value. A `traceparent` that is empty or not text (an SQS
attribute of type Binary, an AMQP header holding a number) is the application's and is left. So is an HTTP header
stored under a key that is not in canonical form (`req.Header["traceparent"] = ...`) and an AMQP header named
`Traceparent`: tsession does not add a second one beside it. SQS attribute names are compared exactly.

Rule 2 cannot tell who made the copy. A `traceparent` that a proxy filled from `x-tsession-traceparent`, or that the
application's tracing passed on unchanged from a request on which tsession had written it (a propagator without a
tracer does that), equals tsession's own value and is replaced on the next write like tsession's own. A
`traceparent` with ids of the application's own is never equal to it.

So a system without application tracing has the wire it had before, plus one key. The test client
(`sdk/client`, `Session.Inject`) follows the same rule.

**Reading** (`core.ExtractSpanContext`). The session comes from `x-tsession-id`; without it nothing is read. The trace
and parent span come from `x-tsession-traceparent` when that key is current: the clock value after its `;` equals
the carrier's `x-tsession-hlc`, or the carrier has no `x-tsession-hlc`. Otherwise, and when the key is absent or
its trace context does not parse, they come from `traceparent`. A trace context parses when it has four
`-`-separated fields of which the second is a trace id of 32 hex digits and the third a span id of 16, neither all
zeros. If neither key parses, the event still joins the session, with a new trace id and no parent.

The clock value is what ties `x-tsession-traceparent` to the hop that wrote it. A service on an SDK that predates
the key and passes a carrier on with its keys copied (a retry or dead-letter topic, a forwarded request) writes its
own span into `traceparent` and a new `x-tsession-hlc`, and copies the `x-tsession-traceparent` it does not know.
The receiver sees that the two clock values differ and reads `traceparent`, which names that service's span. A
service that received the carrier through an instrumented entry point has moved its clock past the value it
received before it writes its own, so the two differ. A service that passes the keys on without having received
them that way (an outbox relay, a batch re-sender) has not: if its clock reads the same millisecond as the
sender's, the values can be equal and the receiver then links to the first sender. Two further cases link to the
first sender instead of the forwarder, both behind a forwarder on an SDK that predates the key: a gateway that
drops `x-tsession-hlc` but passes `x-tsession-traceparent` (see the table below), and an SQS forwarder that adds
the compact attribute beside the full-form attributes it copied.

**Order of instrumentation.** On an HTTP client the application's tracing transport may wrap tsession's (`thttp`
or the patched default transport) or be wrapped by it; on a gRPC client its interceptor or stats handler may run
before or after tsession's. Run first, it has set `traceparent` and tsession leaves it. Run last, it replaces the
`traceparent` tsession wrote. Either way the receiver gets the application's `traceparent` and tsession's parent in
`x-tsession-traceparent`.

**Compatibility with SDKs that predate `x-tsession-traceparent`** ("old": they write and read `traceparent` only,
and overwrite the application's). The tables below are produced by `core.TestPropagationMatrix`, which runs every
combination against the current code and a copy of the old logic, and fails when this file differs from its output.
"Application tracing writes before tsession" means the application set `traceparent` on the request or message
before tsession's instrumentation ran; "after" means its instrumentation ran last.

<!-- matrix:begin (written by core.TestPropagationMatrix; do not edit) -->
Sender calls receiver:

| Sender SDK | Application tracing on the sender | Receiver SDK | Receiver's tsession parent | Application's `traceparent` |
|---|---|---|---|---|
| old | none | old | correct | (none) |
| old | none | new | correct | (none) |
| old | writes before tsession | old | correct | replaced |
| old | writes before tsession | new | correct | replaced |
| old | writes after tsession | old | the application's span | intact |
| old | writes after tsession | new | the application's span | intact |
| new | none | old | correct | (none) |
| new | none | new | correct | (none) |
| new | writes before tsession | old | the application's span | intact |
| new | writes before tsession | new | correct | intact |
| new | writes after tsession | old | the application's span | intact |
| new | writes after tsession | new | correct | intact |
| new, overwrite setting on | none | old | correct | (none) |
| new, overwrite setting on | none | new | correct | (none) |
| new, overwrite setting on | writes before tsession | old | correct | replaced |
| new, overwrite setting on | writes before tsession | new | correct | replaced |
| new, overwrite setting on | writes after tsession | old | the application's span | intact |
| new, overwrite setting on | writes after tsession | new | correct | intact |

Sender calls a forwarder, which passes the keys it received on to the receiver. The receiver's parent should be
the forwarder's span. Combinations that are not listed link correctly:

| Forwarder SDK | Application tracing on the forwarder | Receiver SDK | First sender (SDK / application tracing) | Receiver's tsession parent |
|---|---|---|---|---|
| old | writes after tsession | old | any | the application's span |
| old | writes after tsession | new | any | the application's span |
| new | none | old | old / none; old / writes before tsession | the first sender's span |
| new | none | old | old / writes after tsession; new / writes before tsession; new / writes after tsession; new, overwrite setting on / writes after tsession | the span of the first sender's application tracing |
| new | writes before tsession | old | any | the application's span |
| new | writes after tsession | old | any | the application's span |
| new, overwrite setting on | writes after tsession | old | any | the application's span |

111 of the 162 forwarded combinations link correctly.
<!-- matrix:end -->

Where the parent is not correct, the receiver's events are still recorded in the session (`x-tsession-id` arrives as
before), but they hang from the wrong place:
- "the application's span": the receiver read the application's ids from `traceparent`. Its events carry the
  application's trace id, and their parent (for a worker, also its link to the publish span) is the application's
  span id, which no recorded event has. Everything downstream of the receiver inherits that trace id.
- "the first sender's span": the events are in the right trace, as children of the first sender's span instead of
  the forwarder's.

What follows from the tables:
- **A new receiver never does worse than an old one.** The only rows in which a new receiver has the wrong parent
  are those with an old SDK on the last hop and application tracing writing after it, which were wrong before.
  Upgrade receivers (servers, consumers) before the services that call them, and no link gets worse at any step.
- **A new sender in front of an old receiver** loses the link when the sender runs application tracing: the old
  receiver reads the application's `traceparent`, which tsession no longer replaces. The same two services had
  correct tsession links and a broken application trace before. If such a sender must be upgraded first, set
  `TSESSION_OVERWRITE_TRACEPARENT=true` on it until its receivers are upgraded. With the setting a new SDK writes
  `traceparent` as an old one does (and `x-tsession-traceparent` as well), with the same cost to the application's
  trace. Where the application's tracing writes after tsession, the setting changes nothing, as before.
- **A new forwarder in front of an old receiver** leaves a `traceparent` it received and passes on, unless that is a
  copy of the `x-tsession-traceparent` that came with it: it cannot tell an old sender's `traceparent` from an
  application's. The old receiver then links to the first sender, or reads the ids of the first sender's
  application tracing. The setting on the forwarder restores these links too.

**Software between sender and receiver** must deliver `x-tsession-traceparent` as it was sent. When it does not,
the receiver falls back to `traceparent`, which is right only where no application tracing wrote it:

| What happens on the way | What the receiver reads |
|---|---|
| The key is dropped: a gateway, load balancer or service mesh that passes only listed request headers | `traceparent`, as from an old sender: correct without application tracing on the sender, otherwise the application's trace id and a parent that no recorded event has |
| The value is changed: two `x-tsession-traceparent` headers joined into one with a comma, a truncated value | the same; a value that does not parse is never used in part |
| `x-tsession-hlc` is dropped and `x-tsession-traceparent` is not | `x-tsession-traceparent`; the receiver's clock is not advanced by the sender's. Behind a forwarder on an SDK that predates the key this is the stale value: pass `x-tsession-hlc` too |
| `x-tsession-hlc` is changed | `traceparent`: the key is taken for one copied by an old SDK |
| `x-tsession-id` is dropped | nothing; the request is not part of the session, as before |

What to configure:
- **Gateways, load balancers and proxies that allow-list request headers**: add `x-tsession-traceparent` next to
  `x-tsession-id` and `x-tsession-hlc`.
- **Browsers.** A page that sends these headers on a cross-origin request needs `x-tsession-traceparent` in the
  server's `Access-Control-Allow-Headers`, with the other three. Without it the preflight fails and the browser
  does not send the request at all.
- **SQS consumers that call `ReceiveMessage` themselves** with a list of `MessageAttributeNames` (instead of
  `tqueue.ReceiveMessage`, which adds them, or `All`) must add `x-tsession-traceparent` to the list, next to
  `traceparent`, `x-tsession-id`, `x-tsession-hlc`, `x-tsession-ctx` and `ts`. SQS does not return an attribute
  that is not asked for, and the consumer then reads `traceparent`.

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

### Redaction
Payloads are redacted in the SDK, before they leave the process. A field, column, header or parameter name is
sensitive when (case-insensitive) it equals a configured key ignoring `-`, `_` and spaces, ends with a configured key
of five or more characters, or contains the words of a configured key as whole words (split on punctuation and
camelCase): `user[password]`, `password_confirmation`, `currentPasswordValue`, `card.cvv`, `X-Auth-Key` are masked;
`classname`, `tokenizer`, `max_tokens` are not. Whole-word matching is generous on purpose (`token_count` is masked);
adjust the list with `core.SetRedactKeys`. Defaults: see `core.DefaultRedactKeys`. In query strings and form bodies
`key`, `code`, `sig`, `pin` and `session` are masked as well. `x-tsession-id` is never masked.
HTTP bodies that have no structural redactor (multipart, XML, binary types) are recorded as `<media/type, N bytes>`.

### SDK runtime hardening notes
- `Instance` is always made unique per process start: a user-supplied `TSESSION_INSTANCE` is kept as a prefix and `-<pid>-<6 hex>` is appended (collector dedupes on `(Instance, LocalSeq)`).
- Default exporter flush interval is 100ms (collector reorder window default: 500ms). A 413 from `/v1/ingest` bisects the batch; a single oversize event is replaced by a stub (ids/kind/name kept, `Flags=["oversize_dropped"]`). `rejected` in the ingest response is added to the `sdk.dropped` counters.
- A batch leaves the exporter ring only when the collector answered 2xx or refused it for good (a 4xx other than 413/429, or an encode error: dropped and counted in `sdk.dropped`). Connection errors, 5xx and 429 keep the batch buffered; it is resent unchanged (same `local_seq`, so the collector deduplicates) with a growing pause (`FlushInterval` doubling up to `MaxCooldown`, default 5s). During an outage events are lost only when the ring is full: new events are dropped and counted. `Flush` returns false if events are still buffered when its timeout ends.
- Remote HLC values more than 10 minutes ahead of local physical time are ignored (not merged).
- Header propagation by the patched default transport: `TSESSION_PROPAGATE_HOSTS` (comma-separated host suffixes; empty = all) and `TSESSION_NO_PROPAGATE_HOSTS` (always wins). Excluded hosts are still recorded as `api.out`, with attr `propagated=false`.
- SSE (`text/event-stream`) and hijacked `api.in` spans end at first write/hijack with attr `stream=true`.

## Wrapper contract (contrib + thttp)
Mirror the wrapped library's signature; one-line adoption; if `!core.Enabled()` return the original
object unchanged; idempotent (wrapping twice is a no-op; except `tgrpc`: install each interceptor once); never panic/block the host; use only
`core.Begin / BeginIngress / Inject / Span` (nil-safe) — never edit `core/` (report needed changes instead).
Mutations: set `core.Mutation` (Store/Entity/PK/Op/Before/After/Diff, Source="sdk"); reads via `core.ReadSummary`.
Queue carriers: Kafka headers, SQS message attributes, AMQP headers; opaque-payload queues use a
side-channel key `ts:ctx:{msgID}` (TTL) holding the injected carrier map. Producer span = `queue.publish`
(Req = full job params); consumer = `worker.start` (BeginIngress adds the link) ... `worker.end`.
Every carrier follows "Trace context propagation" above: an application `traceparent` header or attribute on the
message is never replaced, and carrier keys copied from a received message are.

SQS allows ten message attributes. The carrier uses the first form that fits:
1. Full: `x-tsession-traceparent`, `x-tsession-id`, `x-tsession-hlc`, and `traceparent` when the message has none.
   Four free slots, or three when the application set `traceparent`.
2. Compact: one `x-tsession-ctx` attribute holding all four keys (`k=v&k=v`, URL-encoded), when at least one slot
   is free. No other attribute is written; a `traceparent` attribute next to it is the application's, and is
   replaced only with `TSESSION_OVERWRITE_TRACEPARENT=true`. An application attribute that happens to be named
   `x-tsession-ctx` is not overwritten.
3. The side channel, when `SQSOptions.SideChannel` is set; otherwise propagation is skipped and the publish span is
   flagged `propagation_skipped:attr_limit`.

A message with seven application attributes used the full form (three attributes) before `x-tsession-traceparent`
existed and uses the compact form now. A consumer reads the context from where `x-tsession-id` is: the attributes
alone when it is among them, or else the compact attribute (the older name `ts` is still read), or else the side
channel.

Carrier attributes on a message that is sent again with the attributes it was received with are replaced, or
removed when this publish uses another form or none, and do not count against the limit: the three `x-tsession-`
attributes, a `traceparent` that equals the trace context in `x-tsession-traceparent`, `x-tsession-ctx`, and a `ts`
written by an older SDK. `x-tsession-ctx` and `ts` are recognised by their value (a compact context that holds a
session id); an application attribute of either name is left as it is.

Additional wrappers: `contrib/tredisv8` (go-redis v8 `Hook()` / `Wrap` / `WrapAny` for clients held behind an
internal wrapper; same behaviour as tredis) and `contrib/tkafkaconfluent` (confluent-kafka-go `Produce` /
`Consume` over `tqueue`; headers carrier, 1 KiB / 8-header injection cap).
