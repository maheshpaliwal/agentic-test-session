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
- Demo mode serves the same endpoints from `demo.go`/`demodata.go`: sealed session `ts_demo0001` (POST /orders -> db/redis, rolled-back tx, queue publishes -> worker retry + DLQ), `ts_demo0002` (ACTIVE, events trickle in every 1.5s so live tail can be seen; Stop it to seal), `ts_demo0003` (incomplete, dropped events, orphaned publishes). Start/Stop work in memory.

## Views (hash routes: `#/`, `#/s/{id}/{timeline|tree|entities|summary}`)
- Sessions: filter, Start/Stop, auto-refresh (3s), incomplete/dropped badges.
- Timeline: filters (service, kind, errors, entity, text), click row -> drawer with req/resp (redaction markers highlighted), mutations before/after/diff, reads, ids. Live tail polls `events?from_seq=` every 1.5s while ACTIVE/DRAINING.
- Call tree: from `/trace`; `linked` worker spans shown in a dashed "async" group, attempts/DLQ badges, duration bars, errors red, publishes without a worker flagged pending (live) or orphaned (sealed).
- Entities: `/entities` list and per-pk history (seq, op, effective vs rolled back, diff).
- Summary: counts computed from events plus the raw collector `/summary`.

## Response-shape assumptions (PROTOCOL.md leaves them open)
Session list may be an array or `{sessions:[]}`; entity detail may be an array or `{mutations:[]}`, items either flat Mutation fields + `seq` or `{seq, mutation:{...}}`; `pk` in the entity list may be a string or object. `events?from_seq=N` is treated as exclusive-or-inclusive (client dedupes by seq). `worker.start` is a span (root, `LinkIDs` -> publisher) whose children carry `ParentID`; `worker.end`/`worker.retry`/`worker.dlq` are its children.

## Tests
`go test -race ./dashboard` covers proxy routing/stripping, demo endpoint shapes (decoded into `core.Event`), content types/headers, no external URLs or HTML-injection APIs in assets, and tree/ordering invariants of the demo data.
