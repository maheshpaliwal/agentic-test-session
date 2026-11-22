# What tsession can miss

One list of the things the tool does not see or cannot tell, each with where the detail is. A session that is
`SEALED`, complete and passes every gate is a statement about what reached the collector from the services that
were instrumented and connected. It is not a proof that nothing else happened.

## What is not recorded

- **Traffic outside a session.** Nothing is recorded while no session is active, and in header mode only what
  carries the session's id (or matches a binding) is recorded.
- **Services that are not instrumented, not connected, or built with an older SDK.** They are not listed and not
  counted. `tracectl doctor --require-services N` is the check that the ones you expect are connected.
- **Go only.** The SDK and the wrappers are Go packages.
- **SQL that changes rows where the wrapper cannot see them.** A `DO` or `CALL` is flagged `effects_unknown` and
  its rows are not recorded; an upsert whose kind cannot be told is `upsert_unknown` with no diff; an updated row
  that cannot be read back has `after_unavailable`; rows without a usable key have `pk_unknown` or
  `pk_not_unique`. See README "Set up" and the `contrib/tsql` package doc. Triggers, cascades and anything a
  database does on its own are not recorded.
- **Requests that were refused are not failures.** An HTTP 4xx (and gRPC `NotFound`, `Canceled` and similar) is
  not a failed event: `--fail-on-error` and `RequireNoFailures` pass on it. The event carries `http.status` /
  `grpc.code`, and the digest shows it as `code`.
- **Large bodies are cut.** A payload over the size limit is recorded truncated, with a flag.

## What a verdict can miss

`incomplete` means events are known to be lost; `delivery unconfirmed` means it could not be told. Neither is
raised in these cases (details: [`PROTOCOL.md`](PROTOCOL.md), "Limits" under delivery accounting):

- **A service killed before its first delivery report, or after a report that was in order**, with events that
  reached nobody: the session is complete and wrong.
- **A fresh outage of the report endpoint shorter than about 12 seconds** is mostly not detected (3 of 8 sessions
  at 5 s, 1 of 8 at 9 s, 8 of 8 at 15 s in one measured run).
- **A process cut off from both its reports and its stream, or frozen**, that left neither a report nor events
  for the session is not named.
- **A service that stopped recording before its first event of a session** (its subscriber panicked) is
  "unconfirmed" there, not "incomplete": only `--fail-on-unconfirmed` fails.
- **Against a control plane older than the SDK's wait for propagation**, `tsessiontest.Run` cannot guarantee
  that a test's first request is recorded.
- **Nothing is authenticated.** Whoever can reach the control plane can stop a session, make one incomplete or
  forge a delivery report; see "What a caller without credentials can do" in `PROTOCOL.md` and
  [`SECURITY.md`](../SECURITY.md). Whether that is acceptable in your environment is a question for your own
  security review.

The other direction also happens: a session can be `incomplete` although nothing is missing (a service whose
reports are blocked while its events arrive). `PROTOCOL.md`, "Incomplete with nothing missing".

Timing to expect: a stop waits for the session to go quiet (3 s by default), up to 10 s more for a service that
is silent or cannot deliver, and 12 to 18 s while a service's delivery reports are failing.

## The preflight (`tracectl doctor`)

- A service checks its collector's `/healthz` when it starts and every 30 seconds while idle: **a collector
  outage shorter than that can pass unseen**.
- **Any HTTP answer but a 5xx counts as reached.** A wrong URL at which another HTTP server answers (a 404) is
  not detected; a proxy that accepts ingest and answers `/healthz` with 502 or 503 is a false alarm.
  [`CONFIG.md`](CONFIG.md), "The SDK's collector check".
- `CANNOT DELIVER` and `NO DELIVERY REPORTS` show only for sessions that are running.

## Reading an export (`tracectl --from`, `dashboard -open`)

- **A file that lost whole lines is refused only if its first line is intact** and was written by a `tracectl`
  that stores the event count (`events` in `tsession_session`). A file that lost its first line, or an older
  export that lost lines at the end, is read and judged on what is left.
- A session exported while it ran holds the events that had arrived, and is shown as running.
- An export from before the name was stored is named after its file.

## Masking

- Payloads are masked **by field name only**; a secret under an innocent name, or inside free text, is recorded.
  Redis key names are masked by a heuristic. Run it against test data, and treat exports like test data. See
  `SECURITY.md`; whether recordings may be stored or shared is a question for your own data-handling policy.

## Not exercised against real servers

- `make it` and CI run Postgres and Redis only. The `mongo` store, the `dynamodb` store (tested against an
  in-process fake), `tsql` on MySQL (including its primary-key lookup), and the Mongo and DynamoDB wrappers have
  not run against live servers there.
- The Compose file's restart policy has not been tried with Docker.

## Operations

- The kill switch is kept in memory: a control-plane restart turns it off.
- The collector and the control plane exit after repeated panics of their background loops; they need a
  supervisor that starts them again. `CONFIG.md`, "Health and panics".
