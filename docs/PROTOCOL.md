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
- `delivery`: the last delivery report of every SDK instance that sent one, sorted by service and instance:
  `[{"service","instance","recorded","delivered","dropped","buffered","delivered_total","last_error","final","at"}]`
  (see "Delivery accounting"). Instances that never reported are not listed. Only `GET /v1/sessions/{id}` and the
  answers of start, stop, abort and rename carry it; the list `GET /v1/sessions` leaves it out (up to 256
  instances per session) and keeps the two fields below.
- `incomplete_reasons`: why the control plane itself marked the session incomplete, one line per instance whose
  events did not all reach the collector, or whose delivery nothing accounts for (at most 16, then "and N more").
  A session with a reason is `incomplete` whatever the collector's seal says, also when a seal succeeds later on a
  background retry. The collector's own findings (dropped and quarantined events, orphaned async work) are not
  repeated here.
- `delivery_unconfirmed`: what the control plane could not tell about the delivery of the session's events, one
  line each (at most 16, then "and N more"; see the verdict in "Delivery accounting"). A warning: the session is
  not `incomplete` because of it, and it is not known to be complete. `--fail-on-unconfirmed` and
  `tsessiontest.RequireDeliveryConfirmed` fail on it.
- `stopped_instances`: `[{"service","instance","why","at","connected"}]`, the SDK instances that said while the
  session ran that they stopped recording after an internal error (`stopped` of `POST /v1/delivery`): what each
  said last, when the control plane was first told, and whether an instance of that name followed the stream then.
  At most 64; `stopped_instances_more` is `true` when more said so. The two lists above say what the control plane
  made of it. Omitted when there are none, by a control plane before this field, and (the list, not the flag) by
  `GET /v1/sessions`, like `delivery`.

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
- `POST /v1/delivery` {service,instance,delivered_total,reports:[{session,recorded,delivered,dropped,buffered,last_error,final}]} ->
  `{"results":[{"session","status","error"}]}`, one result per report, in order. Sent by SDKs: the delivery reports
  of one instance for up to 256 sessions, see "Delivery accounting". `status` is `accepted`; `stale` (counters
  behind the stored ones: ignored); `closed` (the session is finished, or its drain has stopped looking); `unknown`
  (no such session); `full` (the session already keeps reports of 256 instances and this is another one: it is
  judged by what it says, see the verdict, so the SDK keeps reporting changes); `invalid` (not a report, a counter outside 0..2^53, `recorded != delivered + dropped + buffered`, or
  more than 4 KiB), with `error`; `failed` (it could not be stored: send it again). The request itself is `400`
  without `instance` or with more than 256 reports, and `413` over 1 MiB. `reports` may be empty: the request then
  says only that the instance's reports arrive.
  An optional `stopped` (text, at most 256 bytes are kept) says that the instance stopped recording after an internal
  error: a loop of its SDK panicked, and the text says whether it records again or stays stopped. It is noted on
  every session that is `ACTIVE` or `DRAINING` when the request arrives (`stopped_instances` of the session, stored
  with it, so a restart of the control plane does not lose it). What the note means for a session is decided when
  the session ends:
  - something ties the instance to the session, a delivery report of it for the session or events of it in the
    collector's counts: the session is `incomplete`, with an `incomplete_reasons` entry that names the service and
    the instance (`... stopped recording after an internal error (...), so events of this session it would have
    recorded since are missing`);
  - nothing does: one line under `delivery_unconfirmed` (`... stopped recording after an internal error (...) while
    this session ran; ... whether it took part is unknown: it may have missed events it should have recorded`). An
    environment has many services that take no part in a given session, and the note is the caller's own word, so
    it alone does not fail a session; the price is that a service that stopped before it recorded its first event
    of a session is a warning there, which only the strict gate fails on (see "Which gate").

  The SDK sends it at the panic, every second while its subscriber is being restarted, and once when recording has
  resumed; when the subscriber stays stopped it sends it once. Sessions started after the last such request are not
  marked. A session keeps the notes of at most 64 instances (what an instance said last replaces what it said
  before); the note of an instance that follows the stream or has a report for the session takes the place of one
  that does neither, and that notes were left out is one more `delivery_unconfirmed` line. A control plane that does
  not know the field ignores it. Like the reports it is the caller's own word: anyone who can reach the control
  plane can put such a line into `delivery_unconfirmed` of every running session (at most the 16 lines the list
  holds, after the lines the control plane found itself), and cannot make a session `incomplete` with it that a
  report in the same name would not.
  An optional `collector` `{"reachable":true|false,"error":"..."}` says whether the instance could reach its
  collector when it last looked. The SDK looks while it has nothing to send: `GET /healthz` on its collector URL
  when it starts, every 30 s after an answer and every 5 s after none, one request at a time with the timeout of
  its ingest requests (2 s), from a goroutine that lives for that one request; never while events wait (the
  requests that carry them say the same) and never on the path of a request of the application. The collector
  counts as reached when anything answers that is not a server's failure: `200`, a `503` whose body starts with
  `degraded:` (a collector that is there and says so), and any `3xx`/`4xx` (a proxy that passes only the ingest).
  No answer, another `5xx`, or a failed request with events counts as not reached, with the error (the URL
  without userinfo, query and fragment). So it finds an address where nothing listens, a name that does not
  resolve and a gateway without its upstream; it does **not** find an address where something else answers HTTP.
  The SDK sends the field with every batch once it has looked, and sends a batch without reports when what it
  found changes, also while no session runs (not while the control plane refuses the requests of an idle
  instance). The control plane keeps it with the instance's connection to the stream (`collector` of
  `GET /v1/subscribers`) and judges no session by it. A control plane that does not know the field ignores it; an
  SDK that does not send it is not known to reach its collector, nor known not to.
- `POST /v1/sessions/{id}/delivery` {service,instance,recorded,delivered,dropped,buffered,delivered_total,last_error,final} ->
  `{"accepted":true|false}`: one report, for clients that send one session at a time (SDKs use the batch).
  `400` for a body that is not such a report, `413` for one over 4 KiB, `404` unknown session, `409` when the
  session takes no reports (any more): `closed` or `full` above.
- `DELETE /v1/sessions/{id}` -> `{"deleted":true,"id":"..."}`; finished sessions only (`409` while CREATED/ACTIVE/
  DRAINING, `404` unknown). The collector is asked first to drop the session's events, seal and bundle; if that
  fails the answer is `502` and the session stays, so the call can be repeated. A session with `seal_pending` can
  be deleted: its seal retries stop first (an attempt in flight is cancelled and waited for), and resume if the
  collector delete fails.
- `GET /metrics` Prometheus text format (sessions by state, started/finished/deleted totals, seal failures,
  `tsession_controlplane_seals_pending`, `tsession_controlplane_seal_retries_total{result="success|failure"}`,
  `tsession_controlplane_seals_abandoned_total`, subscribers, kill switch, `tsession_build_info`)
- `GET /v1/active` -> `{"active":["id",...],"exclusive":"id|","kill":false,"generation":n}` (active = ACTIVE+DRAINING)
  plus `"draining":["id",...]`, the ids in `active` that are DRAINING (omitted when there are none). An SDK that
  knows the field flushes the events of such a session and sends its final delivery report; one that does not
  ignores it. And `"delivery_reports":true`: this control plane takes delivery reports. An SDK that reads it does
  not take a `404` to a report for the answer of a control plane without the endpoint (see "Delivery
  accounting"); one that does not know the field ignores it. With it, `"delivery_heartbeat_ms":5000`: how often
  the control plane wants to hear from an SDK that follows the stream while `active` is not empty (half of
  `--delivery-grace`; omitted when the delivery check is off).
- `GET /v1/active/stream` SSE; sends the same JSON as `data:` on connect and on every change (+ heartbeat comment every 15s)
  The SDK sends `X-Tsession-Service` and `X-Tsession-Instance` with the request (optional; shown to operators, not trusted),
  and `X-Tsession-Delivery-Instance` when it sends delivery reports: the instance name of those reports.
- `GET /v1/subscribers` -> `[{"service","instance","delivery_instance","connected_at","last_delivery_report","delivery_unheard","delivery_pending","collector","generation"}]`:
  the current followers of the stream, i.e. the connected SDKs. `service`/`instance`/`delivery_instance` are
  omitted for followers that did not send them. `last_delivery_report` is when a request with delivery reports
  of the follower last arrived (for any session, or one without reports: its heartbeat), omitted when none has
  since it connected. `delivery_unheard` is `true` (omitted otherwise) for a follower with a `delivery_instance`
  of which nothing has arrived for too long, by the rule the verdict of a session goes by (see "Delivery
  accounting"): it cannot get its reports through. `tracectl doctor` marks it.
  `delivery_pending` (optional; omitted when there is nothing to say, and by a control plane before this field)
  is `{"sessions","undelivered","dropped","last_error","at"}`: what the follower's own delivery reports say of
  the sessions that are ACTIVE or DRAINING now. `undelivered` and `dropped` are the sums of `buffered` and
  `dropped` over those sessions, `sessions` how many of them have either, `last_error` and `at` the
  `last_error` (omitted when empty) and arrival time of its newest report among them. It is present when
  `undelivered` or `dropped` is above zero or `last_error` is set. Events held without a `last_error` are on
  their way; held or dropped WITH one means that the follower cannot deliver (a wrong
  `TSESSION_COLLECTOR_URL`, typically), which `tracectl doctor` shows and `doctor --require-delivery` fails on
  (exit 3, as for `delivery_unheard`). What it cannot say: anything about a follower that sends no reports (no
  `delivery_instance`) or whose reports are refused (`delivery_unheard` covers the latter), anything while no
  session runs (an SDK reports only sessions it recorded events for; `collector` below is what shows a wrong
  URL then), and nothing of sessions that have ended (their verdict is `incomplete_reasons`). It is the
  follower's word, as unauthenticated as every delivery report (see "Limits"). A reader that does not know the
  field ignores it.
  `collector` (optional) is `{"reachable","error","at"}`: what the follower last said, on this connection, about
  whether it can reach its collector (`collector` of `POST /v1/delivery`), `error` why not (omitted when it can;
  cleaned, at most 256 bytes) and `at` when the control plane was told. It is there with no session running,
  which is how a wrong `TSESSION_COLLECTOR_URL` shows before a run: `tracectl doctor` prints
  `CANNOT REACH COLLECTOR: <error>` and `doctor --require-delivery` fails on it (exit 3). **Omitted means not
  known, not unreachable**: an SDK from before the field, one that has not looked yet (it looks when it starts and
  says what it found within a few seconds), or a control plane from before it; none of those fails a check. It goes with the
  connection: a follower that connects anew says it again. What it cannot say: that what answers at the URL is a
  tsession collector (see the SDK's check below), and anything about a follower without a `delivery_instance`.
  No session's verdict looks at it, and like `delivery_pending` it is the follower's own word: whoever reaches the
  control plane can say it in a connected instance's name, and so fail or pass that check.
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

### Delivery accounting
Events that never reach the collector are invisible to it, and so to every check made on its data: a service with
a wrong `TSESSION_COLLECTOR_URL` used to leave a session that sealed complete without its events. Each SDK
instance therefore tells the control plane, which it is known to reach (it follows the active set), what it
recorded for a session and what became of it; and the control plane holds that against what the collector has
from the instance.

**Report.** One entry of `reports` in `POST /v1/delivery`, with the batch's `service`, `instance` and
`delivered_total` (`Content-Type: application/json`):
```json
{"service":"orders-api","instance":"host-4711-a1b2c3","delivered_total":412,"reports":[
 {"session":"ts_1a2b...","recorded":7,"delivered":5,"dropped":0,"buffered":2,
  "last_error":"Post http://collector:7071/v1/ingest: dial tcp: connection refused","final":true}]}
```
- `instance` is the per-process id the events carry (see "SDK runtime hardening notes"), not the name sent with
  the stream: a restarted process is a new instance, and the old one keeps its last report.
- The counters are cumulative for the life of the process and `recorded = delivered + dropped + buffered` always.
  `recorded` counts every event of the session handed to the SDK's ring, also one a full ring dropped at once.
  `delivered` counts events the collector answered 2xx for (an event it calls a duplicate is delivered: it has
  it; a resend is counted once, when it is answered). `dropped` counts events lost for good: ring overflow, a
  refusal by the collector (4xx, or `rejected` in a 2xx answer), and what an exporter still held when the process
  shut down. `buffered` is what the instance still holds. `sdk.dropped` events are not counted.
- `delivered_total` (optional) is what the process delivered for all its sessions together since it started. It
  rises while the instance gets events of any session through, which is how a drain tells an instance working
  through a backlog from one that is stuck.
- `last_error` (optional) is why the instance's last request to the collector failed, empty once one succeeded;
  sent only while events are buffered or dropped. `final` marks a report sent after the instance saw the session
  in `draining` and had delivered everything or had 2s to try, or sent while the process shuts down.
- A later report replaces the earlier one of the same service and instance. Which of two is the later one is
  read from `recorded`, which rises with every event: a report with a lower `recorded` than the one kept is an
  old one and is ignored (`stale`), and so is one with the same `recorded` and a lower `delivered` or `dropped`.
  A report with a higher `recorded` replaces the one kept whatever its other counters are. `final` is kept when
  the report that replaces it names the same `recorded` (a report sent before the instance knew of the drain,
  arriving late); one that names more events speaks for itself. The control plane adds `at`, the time it received
  the report. A report with `recorded` 0 is accepted and not stored.
- What is stored is self-reported and cleaned: `service` and `instance` printable ASCII without leading or
  trailing spaces, at most 128 bytes, the same way on the stream's headers, in a report and for the names the
  collector counts events by, so that the three compare (a service named `"orders-api "` is `orders-api`
  everywhere); `last_error` printable ASCII, at most 256 bytes, with every URL in it cut to scheme, host and path
  (no user name, password, query or fragment; the userinfo ends at the last `@`, so a password that holds `@` or
  `/` goes whole). The SDK already sends them that way.

**When an SDK reports.** Only sessions it recorded at least one event for: once the first events are delivered,
or after 1s if they are not; then when the counts or the error changed, at most every 5s; when the session turns
`draining`, at once if anything changed, and from then on at every change, so events recorded during the drain (a
worker that is still running) produce a new `final` report that replaces the last; and from `runtime.Stop`, after
the exporter was closed, with what it could not deliver counted as `dropped`. While a draining session has
undelivered events the SDK retries the collector every second instead of waiting out the exporter's pause, sends
the events of draining sessions before those of the others, and sends the session's report again, at most once a
second, when only `delivered_total` moved. The kill switch does not stop the reports. An account is dropped when
the session leaves the active set.

- **One request for everything that is due**, every 250ms at most: up to 256 reports, the draining sessions
  first and of those the ones reported longest ago; what does not fit follows at once. 500 sessions that stop
  together cost each service that took part two requests.
- **A session it recorded nothing for is not reported**, running or draining. The control plane does not expect
  to hear of it (see the drain).
- **When it has opened the stream** (and read the first active set from it) it sends one request at once,
  without reports if nothing is due: its reports arrive.
- **Heartbeat.** While the active set names a session, its own or not, an SDK that follows the stream sends
  such a request without reports whenever the control plane has not answered a batch of it for the heartbeat
  period: `delivery_heartbeat_ms` of the active set, 5s when the control plane does not say. The first is sent
  when a session begins after none. A batch with reports counts as one, so a process that reports that often
  sends none; a process that follows no session, or that polls, sends none. A control plane that has not heard
  from an instance that follows it and announced reports (`X-Tsession-Delivery-Instance`) for two periods lists
  it as unheard (see the verdict for what that costs a session).
- **Without the stream** (the SDK polls `GET /v1/active`) it learns of a drain late or not at all, so its reports
  do not wait: the first is sent at once, a change at most once a second.
- **Failures.** No answer, `5xx`, and any `4xx` but those below (what a proxy in between answers when it refuses
  the POST) are failures: the request is retried after 1s, 2s, then every 4s (a refused one: doubling up to 32s;
  never longer than the heartbeat period, and at once when a session begins after none),
  but every 250ms while the report of a draining session waits, so that an outage of the endpoint that ends
  within the drain's grace costs no session its report. The outage is logged once, at the first refusal or the
  second failure in a row, with the control plane's URL without credentials and the status, never the body; at
  most one outage and its recovery per minute. A refused request (`4xx`) is not sent again while the active set
  names no session: it is tried when a session begins, and once on every stream that is opened.
- **Reports that keep failing.** When its reports have failed for two heartbeat periods (the control plane's
  grace; it asks once more when that time is up) on a stream on which the control plane may have heard of it,
  the SDK opens the stream anew. To the control plane it is then a connection of which nothing has ever
  arrived, which every session names (see **unheard** below); an instance that was heard of and is merely
  silent is not named. On the new stream it asks at once, whatever its pause has grown to, so an outage that
  is just over names nothing. A time without a session in which nothing was tried does not count as time in
  which the reports failed; the failures before it do.
  - *Which streams.* One on which a report was answered; and one on which none was, but a request got no
    answer or a `5xx`: it may have arrived with only its answer lost (a gateway that timed out), and the
    control plane then takes the instance for one it has heard of.
  - *Which not.* One on which every request was refused (`4xx`): nothing arrived on it, and the control plane
    names it as it is. And one the SDK opened anew itself, until a report is answered on it: one new stream
    per outage.
  - *How often at most.* Never two within the control plane's grace (two heartbeat periods), nor within 5s
    when the grace is shorter, whatever the outage looks like; and not while the stream it opened anew has
    not delivered its first active set. One outage, however long, is one stream; streams that break by
    themselves during it add at most one per grace.

  Two costs, both for a session the instance took part in whose drain is under way at
  that moment: the new connection is a sign of life, so the drain waits for the instance's report for one more
  grace (drain rule 2); and for the milliseconds between the two streams the instance is not connected, so a
  drain that ends in them does not look at it.
- **Older control planes.** A control plane that takes reports says so in the active set (`delivery_reports`,
  see `GET /v1/active`). From one that does not say so, `404`/`405` to the batch means that it does not know the
  endpoint: the SDK sends one report per request to `POST /v1/sessions/{id}/delivery` (at most 32 per 250ms) and
  opens the stream anew without `X-Tsession-Delivery-Instance`; `404`/`405` there as well turns reporting off,
  silently. Both last until the stream is opened again and has delivered the active set, when the batch is tried
  first. `409` from the per-session endpoint ends reporting for that session. From a control plane that said
  `delivery_reports`, `404`/`405` is not its answer but that of something on the way (a proxy without a route
  for the POST): a failure like `403`, logged and retried as above, and the stream keeps announcing the instance,
  so its sessions end `delivery_unconfirmed` or `incomplete` and not silently complete.
- After a reconnect of the stream every report is sent again, changed or not: the control plane behind it may
  have restarted.

**Drain.** The quiet condition of a drain (see hardening below) additionally requires that

1. no instance that reported still *owes* events: with a collector count for the instance (see below),
   `collector count < recorded - dropped`; without one, `buffered > 0`. An instance that owes events is waited for
   as long as it gets anywhere, on this session or any other: a report with a higher `delivered` for this session,
   a higher `delivered_total` in any of its reports, or a higher collector count. One that got nowhere for
   `--delivery-grace` (default 10s) is no longer waited for. So a session whose events wait behind a backlog of
   other sessions in the same instance is not sealed while that instance delivers; and
2. no instance that follows the stream and announced that it reports has events of the session at the collector
   that its reports do not cover: no report for the session at all, or a last report that is not `final` and
   names fewer events than the collector has. Such an instance took part and is expected to say so. It is waited
   for until it has had the grace since its **last sign of life**: the stop, its connecting, or the arrival of a
   report of it for any session, whichever is latest. A request without reports (its heartbeat) is not one.

A connected instance of which the collector has no events of the session is not waited for and not looked at,
whether a report of it arrived or not: nothing shows that it took part, and an environment has many connected
instances that did not. Instances that do not follow the stream are waited for under rule 1 only. The grace counts
from the stop at the earliest, on the control plane's monotonic clock, and `max_drain_seconds` bounds everything
as before. When the drain ends (quiet, or timed out) the reports are closed: later ones are `closed` (`409`).

**Verdict.** The collector is then asked once more what it has, and every instance's last report `R` is held
against the collector's count `have` for that `service` and `instance` (the events it stored or holds in its
reorder window, each counted once, `sdk.dropped` events not counted). The count comes from drain-status
`instances`; when that list is cut short (more than 512 instances sent events) the control plane asks the
collector by name about the instances that reported and those that are connected (`instance-counts`). The
instance's own word does not condemn it and does not acquit it:

| Last report `R` and collector count `have` | Verdict |
|---|---|
| `have >= R.recorded`, `R.final` | nothing: complete, whatever the instance believed (it may count as undelivered, or as dropped at shutdown, events the collector stored without the answer getting back) |
| `have >= R.recorded`, not `final`, `R.dropped = 0`, `have = R.recorded` | nothing |
| `have > R.recorded`, not `final`, `R.dropped = 0` | `delivery_unconfirmed`: the instance recorded after its last report and stopped reporting; nothing says how much more |
| otherwise `R.dropped > 0` | `incomplete`, with a reason |
| otherwise (`have < R.recorded`) | `incomplete`, with a reason, whether `R` is `final` or the instance went silent |
| no report for the session, `have > 0`, the instance follows the stream, announced that it reports, had the grace (drain rule 2) and is still connected | `incomplete`, with the reason `the collector has N events from <service>/<instance>, which did not report how many it recorded`: it took part and its report did not arrive, so nothing says whether what the collector has is all it recorded (see "Incomplete with nothing missing" below) |
| no report for the session, `have = 0` (or no count for the instance) | nothing, with one exception: an instance that follows the stream, announced that it reports, and of which **nothing has arrived since it connected** (see **unheard** below) is `delivery_unconfirmed`. Something refuses its reports, and whether it took part cannot be known. One that was heard of on its connection and is silent now is not named |
| no report for the session, `have > 0`, the instance is unheard, and the drain ended (timed out) before it had the grace of drain rule 2 | `delivery_unconfirmed`: the collector has `have` events from it, and whether that is all it recorded is unknown |
| `have = R.recorded`, not `final`, `R.dropped = 0`, and the instance follows the stream and is unheard | `delivery_unconfirmed`: its last report was in order, and nothing has arrived of it since; whether it recorded more cannot be known |
| the collector has events from an instance that never reported and does not announce reports (an older SDK), or that is no longer connected | nothing |
| a report answered `full` (`409` from the per-session endpoint), because the session already stores the reports of 256 instances | the same as for a stored report, by every line above: the control plane keeps the last report of up to 1024 such instances for the verdict (replaced by later ones, waited for in the drain, held against the collector's count) without storing it with the session, so it is not in `delivery`. Of the instances beyond those nothing is kept: one `delivery_unconfirmed` line, "more than 1280 instances reported; the reports of the ones beyond that are not accounted for", and `incomplete` when one of their reports said `dropped > 0`, or was `final` with `buffered > 0` |

An instance that follows the stream and announced that it reports is **unheard** when nothing has arrived of
it for too long, neither a report for any session nor a heartbeat: none at all since it connected, a tenth of
the grace (1s) ago or longer; or none for the grace (10s, two heartbeat periods), in all of which the control
plane had a session. Without a session an instance sends no heartbeat, so its silence counts from the moment
there is one. The grace of the second case counts from the last report or heartbeat that **arrived**, not from
the moment the instance stopped: a healthy instance's last word is up to a heartbeat period (5s) old, so an
instance that is frozen, or whose reports fail, for little more than half the grace (6s) can be unheard at
the end of it. `GET /v1/subscribers` and `tracectl doctor` show both cases (`delivery_unheard`), per
connection.

An instance with more than one connection (the stream it ended is kept open for a while by a proxy or load
balancer between it and the control plane, and it has opened another) is unheard when every connection is,
and is judged by the **newest**: a request of the instance is noted on every connection it has at that
moment, so the ones that were heard of say the same, and one that says something else is a later one on
which nothing has arrived. A healthy instance with a stream that lingers is therefore not named, and one
that opened its stream anew because its reports fail is, whatever becomes of the old stream.

What it costs a session differs:

- **Never heard of on its connection**: named under `delivery_unconfirmed` of every session that ends
  meanwhile, whether it took part or not (the table above). This is an instance whose reports are refused
  from the start, and one whose reports failed for the grace and that opened its stream anew (see
  "Reports that keep failing").
- **Heard of, and silent since**: named only in a session something ties it to, a report of it for the session
  or events of it at the collector (the table above). In a session it has neither a report nor events for, it
  says nothing: such an instance is frozen, or its report endpoint is down for a moment, far more often than
  it is cut off, and naming it put a line into 15 to 84 of 200 sessions of a shared environment in which one
  idle instance of several froze for 8 to 20 seconds, or the report endpoint was down for 6. The price is the
  limit "cut off from the control plane altogether" below.

**Incomplete with nothing missing.** Two cases end `incomplete` although the collector may have every event,
and stay that way by decision, because the control plane cannot know that nothing is missing without the
report:

- an instance that took part, all of whose events arrived, and whose reports are refused on the way (a proxy
  answering `403` to the POST);
- an instance that took part, delivered everything, and was frozen (stopped in a debugger, a suspended
  container, a host that lost its network without the connection closing) from before its first report for
  the session arrived until the grace of drain rule 2 was over: ten seconds after the stop, at the earliest.

The reason says exactly what is known: `the collector has N events from <service>/<instance>, which did not
report how many it recorded`. The instance's log names the refusal in the first case. An instance in either
state that took no part costs a session nothing but, when its reports are refused, a line under
`delivery_unconfirmed`.

Without a collector count for an instance that reported, its word is all there is, and a word alone never makes a
session complete: `dropped > 0`, or `final` with `buffered > 0`, is `incomplete`; everything else that recorded
events is `delivery_unconfirmed`. Two cases:

- the collector gives no counts at all (one that does not send `instances`, or no drain-status answer): one line
  for the session, "the collector does not count events per instance (an older collector), so the delivery
  reports of N instance(s) could not be checked against it";
- the collector counts, its list was cut short, and it did not answer for this instance by name (one without
  `instance-counts`, or a failed request): one line per instance; and `incomplete` when an earlier `final` report
  of the instance held undelivered events and only a later report says that they arrived. For the connected
  instances without a report the session says once how many the collector did not answer for.

A reason reads `service orders-api (instance host-4711-a1b2c3): 0 of 7 recorded events reached the collector (last
error: ...)`, with `, N dropped by the SDK` and `; the instance stopped reporting before the session ended` where
they apply; the first number is the collector's. Both lists hold at most 16 lines, then "and N more".
`tracectl summary`/`run` (header line and `delivery` lines), `report` (Markdown, and JUnit properties `incomplete`,
`incomplete_reason`, `delivery_unconfirmed`), `digest` and the dashboard show both, and so does an export (see
"Export file"). An ABORTED session is not judged. With `--delivery-grace 0` nothing is waited for, the connected
instances are not looked at, and every shortfall is listed as `delivery_unconfirmed`.

**Which gate.** `--fail-on-incomplete` and `tsessiontest.RequireNoFailures` fail on `incomplete`: events are
known to be lost, or an instance that took part is unaccounted for. `--fail-on-unconfirmed` (`summary`, `report`,
`digest`, `run`) and `tsessiontest.RequireDeliveryConfirmed` also fail on `delivery_unconfirmed`. That is the
strict form, and the only one that catches a service whose collector URL is wrong **and** whose reports are
refused on the way: nothing of such a service reaches anybody, and the control plane cannot tell it from an idle
one behind the same proxy. Use the strict gate where every connected service is yours and expected to work (a
test environment of your own, a CI job that starts its services). Do not use it in a large shared environment:
one service of another team whose reports are blocked, or an older collector, then fails every run of yours, and
whoever can reach the control plane can make a session unconfirmed (see the limits).

**Compatibility.** Every field is optional in both directions, in the API and in the `--store-file` files.

| | Behaviour |
|---|---|
| SDK without reports | sessions drain and seal as before; its undelivered events stay invisible |
| SDK that reports but does not send `X-Tsession-Delivery-Instance` | its reports are judged against the collector like any other; its silence is not noticed |
| control plane without `POST /v1/delivery` but with the per-session endpoint | the SDK sends one report per request and does not announce itself on the stream |
| control plane without either endpoint | answers `404`, never sends `draining` or `delivery_reports`: the SDK records and exports as before, and sends no reports |
| SDK that does not know `delivery_reports` | ignores it; a `404` to its reports turns them off, as before |
| collector without `instances` in drain-status | every session with reports says so under `delivery_unconfirmed` (see above); a report that itself states a loss still makes the session `incomplete`. Upgrade the collector with, or before, the control plane |
| collector without `instance-counts` | matters only for a session with more than 512 instances: see above |
| control plane that does not know `instances` | ignores it |
| SDK that does not send `collector` (before the check of the collector) | `GET /v1/subscribers` lists it without `collector`: not known. `tracectl doctor` shows it as connected and `--require-delivery` does not fail on it before a session runs |
| control plane that does not know `collector` | ignores it; `tracectl doctor` shows what it showed before |
| `tracectl` before `collector` | ignores it: connected, as before |
| control plane that does not know `stopped` | ignores it: a session during which a service stopped recording seals as if nothing had happened |
| `--store-file` with `stopped_instances`, read by an older control plane and written back | the notes are dropped from what it rewrites; a session that ends after that does not name the instance |
| `--store-file` read by an older control plane and written back | the session stays `incomplete`; `incomplete_reasons`, `delivery_unconfirmed` and `delivery` are dropped from what it rewrites |

**Limits.** What the verdict does not see:

- An instance that is killed (no `runtime.Stop`) after a report that was in order, with events recorded since
  that reached nobody: its last report and the collector agree, and no count shows the loss. A report is at most
  5s old, 1s while the session drains. If some of the later events did arrive, the session is
  `delivery_unconfirmed`; if none did, it is complete and wrong. The same for an instance killed before its
  first report.
- A service that never connected to the control plane, or whose SDK is older, cannot be seen.
- A service whose events reach no collector and whose reports reach no control plane (a wrong collector URL
  behind a proxy that refuses the reports, from the start or from some moment on): `delivery_unconfirmed` while
  it stays connected, as above, and only the strict gate fails. Three cases in which nothing is known: the
  reports began to fail in the middle, and the session ends before the SDK has seen them fail for the grace,
  opened its stream anew, and been connected for a tenth of the grace (about 12s of failing requests with the
  defaults; a time without a session, in which nothing is tried, does not count); the service polls instead
  of following the stream; the service is not connected when the session ends.
- So a **fresh outage of the report endpoint shorter than about 12s is mostly not detected**: reports that
  arrived before, fail from some moment of the session on, with the events lost as well. Measured by the fifth
  review, with the endpoint healthy between runs: 3 of 8 sessions named the instance when the endpoint had
  been down for 5s at the stop, 1 of 8 at 9s, 8 of 8 at 15s. That is the price of not naming every instance
  that is silent for the grace in sessions it took no part in.
- **Stop to sealed takes 12 to 18s** for a session an instance took part in whose reports are failing at the
  time, where it took 10s before the SDK opened its stream anew and takes 1.2s when everything is healthy
  (the fifth review's measurement): the drain waits the grace for the report (drain rule 2), and the stream
  the SDK opens anew meanwhile is a sign of life that starts the grace over, once.
- A request that arrives **on the stream the SDK opened anew**, with its answer lost, followed by nothing: the
  control plane has heard of the instance on its newest connection, and the SDK does not open a third stream
  until a report is answered (it would otherwise open one per grace for as long as an outage lasts, each a
  new grace for a drain that waits for it). The instance is then "heard of, and silent since" again, and a
  session of which it left neither a report nor events does not name it. The same for a path that loses the
  answers and passes the requests for good: the control plane hears the instance and has its reports, which
  is the truth, and the SDK opens one stream more than it needed.
- A second outage that begins within the grace (or 5s) of a stream the SDK opened anew is answered with the
  next stream only when that time is over: up to a grace later than otherwise.
- A stream on which the reports are lost from the start (no answer, `5xx`), of an instance that delivers its
  events: the stream is opened anew once, like any other on which a request was lost, and if that falls into
  the drain of a session it took part in, the drain waits one more grace for it before the session ends
  `incomplete` (see "Incomplete with nothing missing"); a drain that times out first ends
  `delivery_unconfirmed` for it instead. With reports that are refused (`4xx`) from the start this does not
  happen.
- A process that is cut off from the control plane altogether after its first word arrived, its reports and
  its stream (a half-open connection, a partition that drops packets without closing anything), or that is
  frozen: it cannot open its stream anew, so it stays "heard of, and silent since", and a session of which it
  left neither a report nor events at the collector does not name it. If it recorded for that session (a wrong
  collector URL as well, or frozen holding the events), the session is complete and wrong. This is accepted:
  the same state without the loss, an idle instance that is frozen or behind a dead connection, is far more
  common, and used to be named in every session. `delivery_unheard` of `GET /v1/subscribers` and
  `tracectl doctor` still show it.
- An instance that took part and then cannot run or cannot be reached. If no report of it for the session had
  arrived, and it stays that way until the grace of drain rule 2 is over (ten seconds after the stop at the
  earliest), the session is `incomplete` although the collector may have all its events (see "Incomplete with
  nothing missing"). If a report of it had arrived that was in order and not `final`, the session is
  `delivery_unconfirmed` when nothing has arrived of the instance for the grace, **counted from its last report
  or heartbeat that arrived**: that word is up to 5s old when the instance stops, so six seconds of silence on
  its side can be enough, not ten. The same six seconds under a session that ends less than the grace after the
  control plane's first session following a time without any began are not seen.
- The verdict is about delivery to the collector, not about what the collector did with the events.

**What a caller without credentials can do.** Nothing here is authenticated, like stop, abort and kill: whoever
can reach the control plane (see hardening below: loopback or an allowed Host, no cross-site browser request)
can post reports and open the stream, and whoever can reach the collector's ingest can send events under any
name. The collector's `/v1/internal/` endpoints (drain-status, instance-counts, seal, delete) answer loopback and
`--internal-allow` peers only. Each line below is pinned by `TestDeliveryForgery` (integration); that a report
for another session holds a drain as a new connection does, by `TestDrainSilentParticipant` (controlplane); the
two lines about taking a line out of `delivery_unconfirmed`, by `TestDeliveryUnconfirmedLinesACallerCanRemove`
(controlplane):

| A caller that can reach | can | cannot |
|---|---|---|
| the control plane | make any running session `incomplete`: a report in a real instance's name with `recorded` ahead of the real one (the real reports are then `stale`), or of a made-up instance that owes events | make a session complete that is incomplete for an instance **whose own reports arrive**. A report in its name is held against the collector's count for the instance (asked for by name when the collector's list is crowded with other instances), so it must name no more events than the collector has; and the instance's own report then names more and replaces it, whenever the forged one was sent and whatever its other counters say (`{recorded 1, dropped 1, final}` sent first does not make the real reports stale) |
| the control plane | **make a session complete that is incomplete for an instance whose own reports do not arrive** (it took part, the collector has some of its events, something refuses its reports): one `final` report in its name that names as many events as the collector has. The report and the count agree, and nothing says that the instance recorded more | |
| the control plane | hold a drain up to `max_drain_seconds`, by reporting a rising `delivered` or `delivered_total` for an instance that owes events (the session then ends `incomplete`, timed out) | hold a drain by opening the stream and announcing reports: an instance without events at the collector is not waited for |
| the control plane | put a line into `delivery_unconfirmed` of every session, by following the stream with `X-Tsession-Delivery-Instance` and never reporting, and so fail every run that uses the strict gate | fail a session that way with the default gate |
| the control plane | take a line out of `delivery_unconfirmed`: one request in the name of a connected instance whose own reports do not arrive makes it one that was heard of on its connection, which is not named where it left neither a report nor events | hide a shortfall, or turn it into `delivery_unconfirmed`, by filling the 256 instances of a session before the real ones report: a report answered `full` is judged like a stored one, `final` or not |
| the control plane | take the line "the collector has M events, more than the N of the instance's last report" out of `delivery_unconfirmed`: a `final` report in the instance's name with the same `recorded` as its last one (and no lower `delivered` or `dropped`) is not stale and makes the kept report `final`, and `have >= recorded` with `final` is complete. The instance recorded after its last report and stopped reporting (killed, say), and nothing says so any more | |
| the control plane | with 1280 reports of made-up instances (as many requests), keep the report of an instance that reports for the first time after them from being judged. The session then says "more than 1280 instances reported" under `delivery_unconfirmed`, where it would be `incomplete` for an instance that is killed after a report that was not `final` and held events. (A report that names dropped events, or is `final` and holds some, makes the session `incomplete` all the same; and an instance on the stream with events at the collector is `incomplete` as one that did not report) | touch a finished session: reports are `closed` once the drain stops looking |
| the collector's ingest | **make an incomplete session complete**: events sent in the name (`service`, `instance`) of an instance that lost its own make the collector's count match the instance's report. A count cannot tell forged events from real ones | |
| the collector's ingest and the control plane's stream | make a session `incomplete` and hold its drain: events in the name of an instance it announces on the stream and never reports for. The drain is held for the grace; and up to `max_drain_seconds` when the caller opens a new stream in that name, before or after it closes the old one, or posts a report for any other session in that name, more often than the grace is long (each is a sign of life; a request without reports is not) | |
| the collector's ingest | crowd the collector's list of instances (more than 512 per session): the control plane then asks by name | make the control plane miss an instance that reported or is connected that way |

A report is at most 4 KiB, a batch 256 reports, and a session keeps the reports of at most 1024 instances beside
the 256 it stores. Whether running without authentication is acceptable for an environment is not something this document
decides.

### Export file
`tracectl export <id> -o FILE` (`client.Session.Export`) writes the session's events, one JSON object per line in
`seq` order, as `GET /v1/sessions/{id}/events` returns them. The first line carries one more member besides the
event's own:

```json
{"session_id":"ts_1a2b...","seq":1,"kind":"api.in", "...": "...",
 "tsession_session":{"state":"SEALED","incomplete":true,
  "incomplete_reasons":["service orders-api (instance host-4711-a1b2c3): 0 of 7 recorded events reached the collector"],
  "delivery_unconfirmed":["..."],"drain_timed_out":true,"max_drain_seconds":60,"seal_error":"..."}}
```
(one line in the file). `tsession_session` is the session's verdict as the control plane had it when the export
was written, which no event holds: `state`, `incomplete`, `incomplete_reasons`, `delivery_unconfirmed`,
`drain_timed_out` (with `max_drain_seconds`, the limit it ran into) and `seal_error`, each omitted when empty. It
is final for a session that has ended. `dashboard -open FILE` gives the opened session that verdict, so
`tracectl summary`, `report` and `digest` against it print what they print for the live session and their
`--fail-on-*` flags exit the same. The delivery reports themselves (`delivery`) are not in the file.

A reader that does not know the member skips it like any unknown field of an event: an older `dashboard -open`
reads the file as before, without the verdict. An export without the member (written by an older tracectl, or of
a session the control plane no longer knew) is judged on its events alone: incomplete for dropped events and
unfinished async work, or when its last line is cut short.

**A session without events** has no event to carry the verdict, and is the session the verdict matters most
for: its only service could not deliver anything (a wrong collector URL). Its export is one line that is not an
event, the session's id and the verdict:

```json
{"session_id":"ts_1a2b...","tsession_session":{"state":"SEALED","incomplete":true,
 "incomplete_reasons":["service orders-api (instance host-4711-a1b2c3): 0 of 6 recorded events reached the collector (last error: ...)"],
 "started_at":"2026-11-21T10:00:00Z","ended_at":"2026-11-21T10:00:12Z"}}
```
(one line in the file). `started_at` and `ended_at` are written in this form only; the events say when the
session ran otherwise. `dashboard -open` opens it as a sealed session with no events and that verdict, and every
`tracectl` read command answers for it as for the live session: `summary`, `report` and `digest` exit `3` with
`--fail-on-incomplete` and with `--fail-on-empty`, as they do live. A `dashboard -open` from before this form
refuses the file with `not an exported event (no session_id or seq)`; it refused the empty file such a session
used to export as with `no events`. Only a session without events that the control plane does not know either
still exports as an empty file, which does not open (`no events, and no session`).

### Control plane hardening
- Every endpoint except `GET /healthz` (any Host; `ok`, or `503` and `degraded: <reason>` while a background goroutine
  that panicked waits for its restart, see "Health and panics" in [`CONFIG.md`](CONFIG.md)) passes `core.LoopbackGuard`: Host must be loopback or in
  `--allowed-hosts`/`TSESSION_CP_ALLOWED_HOSTS` (e.g. `controlplane:7070`); a request with an `Origin` header is rejected
  unless listed in `--allowed-origins`/`TSESSION_CP_ALLOWED_ORIGINS` (default none; includes `Origin: null`); a request with a
  body needs `Content-Type: application/json`. Non-browser clients (SDK, tracectl, SSE subscriber) send no Origin and are unaffected.
- `ttl_seconds`, `quiesce_seconds`, `max_drain_seconds`: 0/omitted = default, otherwise 1..604800 (7 days); anything else is 400.
- Drain quiescence uses only the control plane's monotonic clock: drain-status is quiet once `pending==0` and none of
  `event_count`, `last_event_unix_ns`, `received_unix_ns`, `dropped_events` changed for `quiesce_seconds`. Collector timestamps are
  opaque change tokens, never compared with local time. A drain also waits for SDK instances that still owe the
  collector events and are getting somewhere, and for the report of a connected instance whose events of the
  session the collector has (see "Delivery accounting"), on the same clock.
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
- A request can hold events of several sessions, and each session is taken or failed on its own. When at least one
  session fails, the status is that of the first failed session in id order (`500` storage, `503` buffer full or
  shutting down), whether or not other sessions were taken, and the body is
  `{"error":"...","accepted":n,"duplicates":m,"failed_sessions":{"<session id>":"storage"|"backpressure"|"shutdown"},"failed_sessions_truncated":true}`
  (plus `rejected*` as above). A failed session counts nothing: `accepted` and `duplicates` are the events of the
  sessions that did not fail, so they are zero when every session failed. A client may therefore conclude from
  such an answer, and only from one with `accepted + duplicates > 0` and without `failed_sessions_truncated`, that
  the events of every session the answer does not name are done (taken, or counted in `rejected`); those of the
  named sessions must be sent again. Sending the whole request again is always correct: what was taken comes back
  as `duplicates`. The list holds at most 32 sessions; when more failed, `failed_sessions_truncated` is `true`
  (omitted otherwise) and a session that is not named may have failed too. The body carries session ids and
  counts, never event data. A collector older than this field answers the same statuses with `{"error":"..."}`
  only, and a client that does not know the field loses nothing by ignoring it.
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
  - `GET /v1/sessions/{id}/entities` -> list of `{store,entity,pk,mutation_count,uncertain,services}`; `pk` is the key as
    canonical JSON (`{}` when it is not known). `uncertain` (optional, omitted when 0) counts the mutations whose
    event carries a flag saying the recorded key or row image is incomplete or uncertain: `pk_unknown`,
    `pk_not_unique`, `pk_lookup_failed`, `upsert_unknown`, `after_unavailable`, `after_from_args`, `after_failed`,
    `image_skipped`, `image_timeout`, `image_failed`, `image_truncated`, `diff_truncated`, `diff_concurrent` (a flag with a reason,
    `image_skipped:deadline`, counts as its base name; `core.UncertainFlag`). `services` (optional, omitted when no
    event names one) are the services whose events changed the entity, sorted: two services that mask one Redis key
    each with its own secret record two entities, and this tells them apart
  - `GET /v1/sessions/{id}/entities/{store}/{entity}/{pk}` ->
    `{"store","entity","pk","mutations":[{seq,span_id,service,kind,tx_id,end_ns,op,before,after,diff,source,effective,flags}]}`
    in Seq order; `{pk}` is the canonical JSON key (`{}` for the unknown key; an empty segment is not a route) or,
    for a single-column key, the bare value. `flags` (optional, omitted when empty) are the flags of the event that
    made the change, so a history that merges rows under a non-unique key says so. An entity that is not there
    answers `200` with no mutations. Both fields are additive: older collectors and exports written before them
    omit them, and readers treat a missing field as none
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
  - `GET /v1/internal/sessions/{id}/drain-status` -> `{"pending":n,"last_event_unix_ns":n,"event_count":n,"dropped_events":n,"instances":[{"service","instance","events":n}]}`.
    `instances` is what the collector has of the session from every SDK instance, by the `service` and `instance`
    of the events, sorted: events it stored and events still in the reorder window, each counted once however often
    it was sent, `sdk.dropped` events not counted. It is always present (`[]` for a session without events); at
    most 512 instances are listed, the first in order, and `"instances_truncated":true` says that there were
    more. A control plane that does not know the field ignores it.
  - `POST /v1/internal/sessions/{id}/instance-counts` {"instances":[{"service","instance"}]} (`application/json`,
    at most 512 instances and 512 KiB) -> `{"instances":[{"service","instance","events":n}]}`: the same counts
    for the instances asked about, in the order asked, `0` for one without events. The control plane asks when
    `instances_truncated` is set: a list can be crowded with instances nobody asked about, an answer cannot.
    Names are compared cleaned (printable ASCII, trimmed), as the control plane knows them; instances whose
    names clean to the same are added up. It creates no state; an unknown session has nothing from anybody.
  - `POST /v1/internal/sessions/{id}/seal` -> flushes window, resolves tx `effective`, marks orphans, writes bundle -> `{"incomplete":bool,"orphaned":[spanIDs],"bundle_uri":""}`
  - `DELETE /v1/internal/sessions/{id}` -> `{"deleted":true,"session_id":"..."}`; drops buffered and stored events,
    the seal, cached views and the bundle directory. Idempotent (unknown id is `200`). Events arriving for the id
    afterwards start a new session at Seq 1.
- `GET /metrics` Prometheus text format (`tsession_collector_*`, `tsession_build_info`).
- Retention: `--retention D` / `TSESSION_COLLECTOR_RETENTION` (default `0` = keep forever) deletes a session `D`
  after its seal. A never-sealed session is deleted once its newest event is older than `D` and its in-memory actor
  has been evicted (see `--idle-evict`). Every sweep deletes all the sessions that are due. On the in-memory and
  Postgres stores a sweep reads only the sealed sessions that may be due, plus the events of every never-sealed
  session: its cost grows with the number of never-sealed sessions, not with the sealed ones. On the MongoDB and
  DynamoDB stores it still lists every session (an aggregation over all events; a `Scan` of the whole table) and
  reads every seal. A sweep that runs out of time is carried on by the next one from where it stopped. On
  Postgres, events whose session has no row in `sessions` (the collector never writes that state) are not swept.
- Pending-async ledger: a `queue.publish` event opens an entry keyed by its `SpanID`;
  a `worker.end` event whose `LinkIDs` contains that id closes it. Seal marks unclosed entries orphaned.
- Tx resolution: `db.write` events carry `TxID`; `db.tx.commit`/`db.tx.rollback` with the same TxID set
  `Mutation.Effective` (true/false) on all that tx's mutations at seal/read time. No TxID => effective.
- Postgres store: tables live in a dedicated schema (`--pg-schema` / `TSESSION_COLLECTOR_PG_SCHEMA`, default `tsession`;
  `[a-z_][a-z0-9_]{0,62}` only), created if missing and set as the pool `search_path`.
- Storage interface (collector/store): in-memory impl (default, tests), Postgres (pgx), MongoDB (`store/mongostore`) and DynamoDB (`store/dynamostore`) impls, selected with `--store mem|pg|mongo|dynamodb`.
  Large payloads are truncated inline; there is no blob store.

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
  body exceeds `MaxBodyBytes` (413, default 32MB) or storage fails for at least one session (500; 503 when a session's
  buffer is full; the body then names the failed sessions, see `POST /v1/ingest` above). A message
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
- **Access control**: the whole API except `GET /healthz` (`ok`, or `503` and `degraded: <reason>`, see "Health and
  panics" in [`CONFIG.md`](CONFIG.md)) is wrapped in `core.LoopbackGuard` (Host must be loopback or listed in
  `--allowed-hosts` / `TSESSION_COLLECTOR_ALLOWED_HOSTS`, comma-separated; browser `Origin` must be listed in
  `--allowed-origins` / `TSESSION_COLLECTOR_ALLOWED_ORIGINS`; the POST bodies the collector accepts are
  `application/x-protobuf` (ingest, which refuses anything else) and `application/json` (the internal
  instance-counts; seal sends no body)). `/v1/internal/*` additionally requires a loopback peer or one inside
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
- An event leaves the exporter ring only when the collector answered 2xx or refused it for good (a 4xx other than 413/429, or an encode error: dropped and counted in `sdk.dropped`). Connection errors, 5xx and 429 keep it buffered; it is resent with the same `local_seq` (so the collector deduplicates) after a growing pause (`FlushInterval` doubling up to `MaxCooldown`, default 5s). During an outage events are lost only when the ring is full: new events are dropped and counted. `Flush` returns false if events are still buffered when its timeout ends.
- One failing session does not hold up the others of the same process. A failed answer that names sessions of the request (`failed_sessions`) is not an outage: the named sessions are left out of the following requests and retried on their own pause (the same doubling up to `MaxCooldown`), oldest events first, so the events of one session are always sent in the order they were recorded; the other sessions are sent first and without a pause. The events of the sessions such an answer does not name are resolved only when it says `accepted + duplicates > 0` and is not `failed_sessions_truncated`; otherwise they are sent again. Everything stays in the ring meanwhile: there is no second buffer and no new limit, so a failing session costs ring space and nothing else. A failed answer that names no session of the request (an older collector, a 429, no answer) is an outage as before, and so are two requests in a row for sessions that were not failing of which the collector took nothing: the pass ends and the next one waits.
- A session that kept failing while other sessions were accepted over at least `MaxCooldown` is held until the collector accepts it again. Held changes one thing: when the ring is full and an event of another session arrives, the newest buffered event of a held session (looked for among the newest 1024 events, and never one that was already sent, since the collector may have it) is dropped and counted for that session instead of the new event. A full ring still loses exactly one event per new event, and without a held session it drops the new event as before. `Flush` returns false while a failing session has events buffered, and `Close` discards them with the ring.
- Every process reports to the control plane what it recorded per session and what became of it (delivered, dropped, still buffered), see "Delivery accounting". The counts are kept in the ring under the lock `Record` already takes; nothing is counted for a process that records no event, and such a process sends one small report per session
  that drains while it follows the stream. An account whose session left the active set while the ring still
  held its events is kept until they are gone, so an event recorded later cannot make `delivered` exceed
  `recorded`.
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
