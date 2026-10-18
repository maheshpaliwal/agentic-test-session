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

## Server behaviour
- `/api/cp/*` -> control plane, `/api/col/*` -> collector (prefix stripped). Cookie/Authorization/Origin/Referer/X-Forwarded-* are not forwarded; Set-Cookie and CORS headers are dropped from responses; no CORS headers are added.
- Only GET/HEAD/POST. `/v1/internal/*` and `/v1/kill` are blocked (403). Cross-origin POSTs and non-loopback Host headers are refused.
- Demo mode serves the same endpoints from `demo.go`/`demodata.go`: sealed session `ts_demo0001` (POST /orders -> db/redis, rolled-back tx, queue publishes -> worker retry + DLQ), `ts_demo0002` (ACTIVE, events trickle in every 1.5s so live tail can be seen; Stop it to seal), `ts_demo0003` (incomplete, dropped events, orphaned publishes). `ts_demo0004` is bound by JWT sub, `ts_demo0005` is the multi-trigger flow. Start/Stop/rename work in memory.

## Views (hash routes: `#/`, `#/s/{id}/{timeline|tree|entities|summary}`)
- Sessions: filter, Start/Stop, auto-refresh (3s), incomplete/dropped badges.
- Timeline: filters (service, kind, errors, entity, text), click row -> drawer with req/resp (redaction markers highlighted), mutations before/after/diff, reads, ids. Live tail polls `events?from_seq=` every 1.5s while ACTIVE/DRAINING.
- Call tree: from `/trace`; `linked` worker spans shown in a dashed "async" group, attempts/DLQ badges, duration bars, errors red, publishes without a worker flagged pending (live) or orphaned (sealed).
- Entities: `/entities` list and per-pk history (seq, op, effective vs rolled back, diff).
- Summary: counts computed from events plus the raw collector `/summary`.

## Trigger-grouped timeline
The Timeline groups events by TRIGGER (pure logic in `web/util.js` `groupEvents`, unit-tested in `jstest/`). A trigger is an `api.in` whose parent is not in the session, a `worker.start` / `queue.consume` (async, own group, labelled "async - triggered by <publisher group> (queue.publish <job>)" with jump buttons both ways), or an orphan root (no `api.in`/`worker.start` ancestor) which becomes an "untriggered" group; events whose parent has not arrived yet (live tail) are grouped under the missing parent id, so the group keeps its key once the trigger arrives. Children follow `parent_id` to the nearest trigger (cycles / missing parents tolerated, depth capped). Groups sort by trigger start (hlc, start_ns), newest-last unless "Newest first"; rows inside a group are depth-first by start, with Seq (completion order) still shown. Headers show status, start, duration, kind counts, mutation summary (`mysql: table +1 ~1`) and flags.
- Controls: `Group by Trigger | None (flat)` (localStorage `tsession.group`, deep link `?group=trigger|none`), Newest first, Expand/Collapse all, `?collapse=all`. Errors-only collapses healthy groups; other filters hide non-matching children behind "+N hidden" (click to show dimmed). Per-group open state is keyed by trigger span id and survives live re-fetches; scroll is anchored.
- Keys: `j`/`k` over headers and rows, `Enter` toggles a header / opens a row, `[` `]` previous/next group, `e`/`c` expand/collapse all, `Esc` closes the drawer. Windowed rendering with variable row heights; collapsed groups cost one row.
- Call tree roots show the same group status and counts.

## Session naming
Sessions show their name prominently, bindings as chips (`x-user-id: act_bind_9`; jwt claims marked unverified), an `auto` tag when `name_auto` is true, and a derived subtitle `entry: <first trigger> (+N more triggers)`. List entries are derived lazily from the first 400 events of the first 10 visible sessions (cached by event count, "+N+" when truncated); the collector summary has no trigger titles. The pencil edits inline (Enter saves via `PATCH /api/cp/v1/sessions/{id}` with `{"name": ...}`, Esc cancels, optimistic with rollback toast). The proxy allows PATCH only for exactly `/v1/sessions/{id}` on the control plane (same Host/Origin/Content-Type hardening); demo mode implements it (1-120 chars, 400 otherwise). Search also matches binding values and the entry point. Demo session `ts_demo0005` ("multi-trigger activity flow") has 3 API triggers, one worker and an orphan cron root.

## Response-shape assumptions (PROTOCOL.md leaves them open)
Session list may be an array or `{sessions:[]}`; entity detail may be an array or `{mutations:[]}`, items either flat Mutation fields + `seq` or `{seq, mutation:{...}}`; `pk` in the entity list may be a string or object. `events?from_seq=N` is treated as exclusive-or-inclusive (client dedupes by seq). `worker.start` is a span (root, `LinkIDs` -> publisher) whose children carry `ParentID`; `worker.end`/`worker.retry`/`worker.dlq` are its children.

## Tests
`go test -race ./dashboard` covers proxy routing/stripping, demo endpoint shapes (decoded into `core.Event`), content types/headers, no external URLs or HTML-injection APIs in assets, and tree/ordering invariants of the demo data.
