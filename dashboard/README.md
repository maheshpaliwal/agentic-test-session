# tsession dashboard

Per-session view of what happened during a test session: timeline, call tree, entity mutation history, summary.
Vanilla JS/CSS embedded in the Go binary (`web/`), no build step, no external requests (CSP `default-src 'none'`).

## Run
```
go run ./cmd/dashboard -demo                    # synthetic data, no other services needed
go run ./cmd/dashboard                          # proxy to control plane :7070 and collector :7071
```
Then open http://127.0.0.1:7072.

| flag | env | default |
|---|---|---|
| `-listen` | `TSESSION_DASHBOARD_ADDR` | `127.0.0.1:7072` (0.0.0.0/:: refused) |
| `-control-plane` | `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` |
| `-collector` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` |
| `-demo` | `TSESSION_DASHBOARD_DEMO=true` | off |
| `-allowed-hosts` | `TSESSION_DASHBOARD_ALLOWED_HOSTS` | loopback Host headers only |
| `-open FILE` | | serve a session written by `tracectl export`, read-only, without other services; repeatable; not with `-demo` |
| `-version` | | print the version and exit |

## Server behaviour
- `/api/cp/*` -> control plane, `/api/col/*` -> collector (prefix stripped). Cookie/Authorization/Origin/Referer/X-Forwarded-* are not forwarded; Set-Cookie and CORS headers are dropped from responses; no CORS headers are added.
- Only GET/HEAD/POST, plus PATCH (rename) and DELETE (delete a finished session) for exactly `/v1/sessions/{id}` on
  the control plane. `/v1/internal/*` and `/v1/kill` are blocked (403). Cross-origin POSTs and non-loopback Host headers are refused.
- Demo mode serves the same endpoints from `demo.go`/`demodata.go`: sealed session `ts_demo0001` (POST /orders -> db/redis, rolled-back tx, queue publishes -> worker retry + DLQ), `ts_demo0002` (ACTIVE, events trickle in every 1.5s so live tail can be seen; Stop it to seal), `ts_demo0003` (incomplete, dropped events, orphaned publishes). `ts_demo0004` is bound by JWT sub, `ts_demo0005` is the multi-trigger flow. Start/Stop/rename work in memory.

## One contract, three backends
`/api/cp/*` and `/api/col/*` are answered by the real services (proxy), by the demo (`-demo`) or from exported
files (`-open`). All three answer every read endpoint in the shapes of docs/PROTOCOL.md, so whatever reads the
services reads the other two: the UI, the Go client, and `tracectl`. The dashboard prints the flags when it starts:
```
tracectl export my-run -o run.ndjson            # while the services are up
go run ./cmd/dashboard -open run.ndjson          # later, anywhere
tracectl --cp http://127.0.0.1:7072/api/cp --collector http://127.0.0.1:7072/api/col digest run
```
`summary`, `events`, `trace`, `entities`, `history`, `show`, `report`, `digest`, `diff`, `export`, `list`, `status`
and `doctor` work; the opened session keeps its id and is named after its file.
- How: the control-plane side uses the control plane's own types and in-memory store (`controlplane.Session`,
  `MemStore.ListPage`), so the session JSON, list order, paging and errors are its own. The collector cannot be
  imported (its module depends on this one), so `views.go` is a port of the collector's views that answers with
  the `sdk/client` result types. `integration/parity_test.go` serves one session from the real services, from an
  export of it and from the demo, and compares every answer; change a view in the collector and it fails.
- What an opened file cannot know, and answers with instead: `initiated_by` is `file`; `created_at`/`started_at`/
  `ended_at` are the first and last event time; `ttl_seconds`, `quiesce_seconds`, `max_drain_seconds` are 0 and
  `bundle_uri` is empty; `drain_timed_out` and `quarantined` are never set; the session is always `SEALED`, also
  when it was exported while running (its open transactions then stay undecided); `/v1/active` has generation 0
  and `/v1/subscribers` is empty. `incomplete` is worked out as the seal does it (jobs no worker finished, events
  dropped by an SDK) or set because the file ends mid-line.
- Files are read-only: every POST, PATCH and DELETE gets `405` and a message that names the file
  (`read-only: session ts_… is a file (run.ndjson, opened with dashboard -open); …`). The demo, like the proxy,
  answers `POST /v1/kill` with 403. The demo's Start/Stop/rename/delete are a simulation for the UI and are not
  held to the control plane's error bodies.

## Views (hash routes: `#/`, `#/s/{id}/{timeline|tree|entities|summary}`)
- Sessions: filter, Start/Stop, Delete (finished sessions; two clicks, the first arms the button), auto-refresh (3s),
  incomplete/dropped badges.
- Timeline: filters (service, kind, errors, entity, text), click row -> drawer with req/resp (redaction markers highlighted), mutations before/after/diff, reads, ids. Live tail polls `events?from_seq=` every 1.5s while ACTIVE/DRAINING.
- Call tree: from `/trace`; `linked` worker spans shown in a dashed "async" group, attempts/DLQ badges, duration bars, errors red, publishes without a worker flagged pending (live) or orphaned (sealed).
- Entities: `/entities` list and per-pk history (seq, op, effective vs rolled back, diff, the flags of the event). An entity with changes whose key or row image is incomplete shows "N uncertain".
- Summary: counts computed from events plus the raw collector `/summary`.

## Trigger-grouped timeline
The Timeline groups events by TRIGGER (pure logic in `web/util.js` `groupEvents`, unit-tested in `jstest/`). A trigger is an `api.in` whose parent is not in the session, a `worker.start` / `queue.consume` (async, own group, labelled "async - triggered by <publisher group> (queue.publish <job>)" with jump buttons both ways), or an orphan root (no `api.in`/`worker.start` ancestor) which becomes an "untriggered" group; events whose parent has not arrived yet (live tail) are grouped under the missing parent id, so the group keeps its key once the trigger arrives. Children follow `parent_id` to the nearest trigger (cycles / missing parents tolerated, depth capped). Groups sort by trigger start (hlc, start_ns), newest-last unless "Newest first"; rows inside a group are depth-first by start, with Seq (completion order) still shown. Headers show status, start, duration, kind counts, mutation summary (`mysql: table +1 ~1`) and flags.
- Controls: `Group by Trigger | None (flat)` (localStorage `tsession.group`, deep link `?group=trigger|none`), Newest first, Expand/Collapse all, `?collapse=all`. Errors-only collapses healthy groups; other filters hide non-matching children behind "+N hidden" (click to show dimmed). Per-group open state is keyed by trigger span id and survives live re-fetches; scroll is anchored.
- Keys: `j`/`k` over headers and rows, `Enter` toggles a header / opens a row, `[` `]` previous/next group, `e`/`c` expand/collapse all, `Esc` closes the drawer. Windowed rendering with variable row heights; collapsed groups cost one row.
- Call tree roots show the same group status and counts.

## Session naming
Sessions show their name prominently, bindings as chips (`x-user-id: act_bind_9`; jwt claims marked unverified), an `auto` tag when `name_auto` is true, and a derived subtitle `entry: <first trigger> (+N more triggers)`. List entries are derived lazily from the first 400 events of the first 10 visible sessions (cached by event count, "+N+" when truncated); the collector summary has no trigger titles. The pencil edits inline (Enter saves via `PATCH /api/cp/v1/sessions/{id}` with `{"name": ...}`, Esc cancels, optimistic with rollback toast). The proxy allows PATCH only for exactly `/v1/sessions/{id}` on the control plane (same Host/Origin/Content-Type hardening); demo mode implements it (1-120 chars, 400 otherwise). Search also matches binding values and the entry point. Demo session `ts_demo0005` ("multi-trigger activity flow") has 3 API triggers, one worker and an orphan cron root.

## Response shapes
The UI reads the shapes of docs/PROTOCOL.md. An entity history is the collector's `{store, entity, pk, mutations:[…]}`
with flat items; the bare-list and `{seq, mutation:{…}}` forms the UI used to accept were only ever produced by the
old demo backend and are gone. Still tolerated, because nothing proves no deployment needs them: a session or
entity list wrapped as `{sessions:[]}` / `{entities:[]}`, `pk` in the entity list as an object, `pending_async` as a
list. `events?from_seq=N` is inclusive; the UI dedupes by seq, so re-reading the last event is harmless. `worker.start` is a span (root, `LinkIDs` -> publisher) whose children carry `ParentID`; `worker.end`/`worker.retry`/`worker.dlq` are its children.

## Tests
`go test -race ./dashboard` covers proxy routing/stripping, demo endpoint shapes (decoded into `core.Event`), content types/headers, no external URLs or HTML-injection APIs in assets, and tree/ordering invariants of the demo data.
`go test ./cmd/tracectl -run TestReadCommands` runs every read command of tracectl on the demo and on an opened file.
`cd integration && go test -run 'Offline|Demo|Tracectl' .` compares both backends with the real control plane and
collector (mem store; no Postgres or Redis needed) and runs the `tracectl` and `dashboard` binaries; it is not part of
`make race`, which skips the integration module.
