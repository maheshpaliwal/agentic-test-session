# Configuration reference

Every setting is an environment variable; the binaries also take the flag shown. A flag wins over its variable.
Run a binary with `-h` for the authoritative list.

## Instrumented service (SDK, read by `sdk/autoinit`)

| Variable | Default | Meaning |
|---|---|---|
| `TSESSION_ENABLED` | off | Only the exact value `true` starts the SDK. |
| `TSESSION_SERVICE` | empty | Service name on every event; set it. |
| `TSESSION_INSTANCE` | empty | Instance name (pod, host); distinguishes replicas. |
| `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` | Where the SDK follows the active sessions. |
| `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Where the SDK sends events. |
| `TSESSION_PROPAGATE_HOSTS` | all hosts | Comma-separated host suffixes the patched default HTTP transport sends session headers to. |
| `TSESSION_NO_PROPAGATE_HOSTS` | none | Host suffixes it never sends them to; wins over the list above. |
| `TSESSION_BIND_PRIORITY` | none | Order in which binding attributes are tried; see [`BINDING.md`](BINDING.md). |
| `TSESSION_OVERWRITE_TRACEPARENT` | off | Only the exact value `true` switches it on. Replace a `traceparent` the application set on an outgoing request or message with tsession's. For a rollout only; see below. |

The two propagation lists apply to `http.DefaultTransport` as patched by `autoinit`, not to `thttp.Client`.

`TSESSION_OVERWRITE_TRACEPARENT` applies to every carrier that puts the context in the request or message: HTTP,
gRPC, Kafka, AMQP, and SQS in both its attribute forms. (With the SQS side channel the context is not in the
message and nothing is replaced.) By default tsession sends its trace context in `x-tsession-traceparent` and leaves
the application's `traceparent` alone. A service downstream whose SDK predates that key reads `traceparent`, so
behind an upstream that runs application tracing (OpenTelemetry, for example) its events are recorded in the
session with a wrong parent. Setting the variable on the upstream restores those links and breaks the application's
own trace on that hop, as before. It cannot help where the application's tracing writes `traceparent` after
tsession. Unset it once the downstream services are upgraded. Services that start the SDK with `runtime.Start` set
`Config.OverwriteTraceparent`.

The setting is not needed when receivers (servers, consumers) are upgraded before the services that call them:
behind any sender, old or new, an upgraded receiver links at least as well as the old one did. The full table of version combinations, and
what gateways, CORS configurations and hand-written SQS consumers must let through (`x-tsession-traceparent`), are
in [`PROTOCOL.md`](PROTOCOL.md), "Trace context propagation".

## Collector (`collector/cmd/collector`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--listen` | `TSESSION_COLLECTOR_LISTEN` | `127.0.0.1:7071` | Listen address. |
| `--store` | `TSESSION_COLLECTOR_STORE` | `mem` | `mem`, `pg`, `mongo` or `dynamodb`. |
| `--pg-dsn` | `TSESSION_COLLECTOR_PG_DSN` | | Postgres connection string (`pg`). |
| `--pg-schema` | `TSESSION_COLLECTOR_PG_SCHEMA` | see `-h` | Schema of the trace tables (`pg`). |
| `--mongo-uri` | `TSESSION_COLLECTOR_MONGO_URI` | | MongoDB URI (`mongo`). |
| `--mongo-db` | `TSESSION_COLLECTOR_MONGO_DB` | `tsession` | Database (`mongo`). |
| `--dynamo-table` | `TSESSION_COLLECTOR_DYNAMO_TABLE` | see `-h` | Table (`dynamodb`). |
| `--dynamo-region` | `TSESSION_COLLECTOR_DYNAMO_REGION` | from the AWS environment | Region (`dynamodb`). |
| `--dynamo-endpoint` | `TSESSION_COLLECTOR_DYNAMO_ENDPOINT` | | Endpoint override, for an emulator (`dynamodb`). |
| `--dynamo-create-table` | `TSESSION_COLLECTOR_DYNAMO_CREATE_TABLE` | off (`true` enables) | Create the table if missing. |
| `--window` | `TSESSION_COLLECTOR_WINDOW` | `500ms` | Reorder window per event; `0` sequences immediately. |
| `--retention` | `TSESSION_COLLECTOR_RETENTION` | `0` (keep forever) | Delete a session this long after its seal, or after its last event if it never sealed. |
| `--bundle-dir` | `TSESSION_COLLECTOR_BUNDLE_DIR` | `./bundles` | Directory for sealed bundles; empty disables. Holds recorded payloads. |
| `--idle-evict` | `TSESSION_COLLECTOR_IDLE_EVICT` | `5m` | Release a session's in-memory state after this idle time. |
| `--max-message-bytes` | `TSESSION_COLLECTOR_MAX_MESSAGE_BYTES` | 4 MiB | Largest single event accepted. |
| `--allowed-hosts` | `TSESSION_COLLECTOR_ALLOWED_HOSTS` | loopback only | Extra Host values accepted, comma-separated. |
| `--allowed-origins` | `TSESSION_COLLECTOR_ALLOWED_ORIGINS` | none | Browser origins allowed to call the API. |
| `--internal-allow` | `TSESSION_COLLECTOR_INTERNAL_ALLOW` | loopback only | CIDRs or IPs allowed to call `/v1/internal/*` (the control plane). |

Prefer the variables for connection strings: flag values show up in the process list.

## Control plane (`cmd/controlplane`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--addr`, `--listen` | `TSESSION_CP_ADDR` | `127.0.0.1:7070` | Listen address. |
| `--collector-url` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Collector it drains and seals through. |
| `--store-file` | `TSESSION_CP_STORE` | none (memory only) | File for session persistence; `FILE.journal` and `FILE.archive` appear next to it as sessions change and age out. |
| `--retention` | `TSESSION_CP_RETENTION` | `24h` | Keep finished sessions in memory this long; without `--store-file` they are then forgotten. |
| `--default-ttl` | `TSESSION_CP_DEFAULT_TTL` | `3600` | Default session time limit, in seconds. |
| `--allowed-hosts` | `TSESSION_CP_ALLOWED_HOSTS` | loopback only | Extra Host values accepted. |
| `--allowed-origins` | `TSESSION_CP_ALLOWED_ORIGINS` | none | Browser origins allowed to call the API. |
| `--delivery-grace` | `TSESSION_CP_DELIVERY_GRACE` | `10s` | How long a drain waits for a service instance that still owes the collector events and gets nowhere (it delivers nothing for any session, and the collector receives nothing from it), and for the delivery report of a connected instance whose events of the session the collector has, counted from its last sign of life, before the session is sealed incomplete (see "Delivery accounting" in PROTOCOL.md). A connected instance that took no part is never waited for. An instance that is working through a backlog is waited for up to `max_drain_seconds`. While a session runs, every connected instance is asked to send a heartbeat every half of it (5s); one of which nothing has arrived for the whole of it, or nothing at all a tenth of it (1s) after it connected, is named in `delivery_unconfirmed`. Keep it above the SDK's request timeout plus retry interval (3s with the defaults). `0` turns the check off: reports are still stored and shown, and no session is marked incomplete because of them. |

## Dashboard (`cmd/dashboard`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `-listen` | `TSESSION_DASHBOARD_ADDR` | `127.0.0.1:7072` | Listen address; wildcard addresses are refused. |
| `-control-plane` | `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` | Control plane it proxies. |
| `-collector` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Collector it proxies. |
| `-demo` | `TSESSION_DASHBOARD_DEMO` | off (`true` enables) | Serve built-in synthetic data. |
| `-open FILE` | | | Serve an exported session read-only, without other services; repeatable; not with `-demo`. |
| `-allowed-hosts` | `TSESSION_DASHBOARD_ALLOWED_HOSTS` | loopback only | Extra Host values accepted. |

## tracectl and the Go client

| Flag | Variable | Default |
|---|---|---|
| `--cp` | `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` |
| `--collector` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` |

The gates of `tracectl summary`, `report`, `digest` and `run` exit 3 when they fail:

| Flag | Fails when |
|---|---|
| `--fail-on-error` | the session recorded failed events. |
| `--fail-on-incomplete` | the session is incomplete: dropped or quarantined events, orphaned async work, a failed seal, a drain that timed out, or events a service recorded that did not reach the collector (`incomplete_reasons`). |
| `--fail-on-empty` | the session recorded no events. |
| `--fail-on-unconfirmed` | the control plane could not tell whether every event of every service instance reached the collector (`delivery_unconfirmed`): the strict form of the delivery check, like `tsessiontest.RequireDeliveryConfirmed`. For environments where every connected service is yours; in a large shared one a single service whose delivery reports are blocked fails every run. See "Which gate" in PROTOCOL.md. |

The gates give an export opened with `dashboard -open` the exit status they give the live session: the export
carries the session's verdict (see "Export file" in PROTOCOL.md).

`tracectl run -- <command>` sets `TSESSION_ID` to the id of the session it started, in the environment of the
command only. Nothing joins that session by itself: the command sends the id as the `x-tsession-id` header (or
uses a binding or `--mode exclusive`, which need no id). In Go, `client.AttachEnv` and `tsessiontest.Ambient`
read `TSESSION_ID` and return that session for tagging requests; `tsessiontest.Run` does not read it and starts
a session per test, as it does without `tracectl run`. When `--cp` or `--collector` were given on the command line, the
command also gets them as `TSESSION_CONTROLPLANE_URL` and `TSESSION_COLLECTOR_URL`. The session's TTL (`--ttl`,
default one hour) must be longer than the command takes, or the session expires under it.

`TSESSION_ARTIFACT_DIR` makes `sdk/client/tsessiontest` save the session of a failed test into that directory;
use an absolute path.

## Health and panics

`GET /healthz` of the collector and the control plane answers `200 ok`. A panic in one of their background
goroutines does not end the service: it is logged at error level with its stack, counted
(`tsession_*_goroutine_panics_total`), and the goroutine is started again after a backoff (100 ms, doubling up to
30 s). While a goroutine waits for that restart `/healthz` answers `503` with `degraded: ` and the name of the
goroutine; `tracectl doctor` prints it as `DEGRADED` and fails. The Docker health checks of `docker/docker-compose.yml`
tolerate it: a container is unhealthy only after a minute of failed checks.

A loop that panics every time must not leave a service that looks alive and does nothing. The loops a service cannot
do without (collector: `janitor`, `retention`; control plane: `sweep`) share a **panic budget**: 5 panics within 10
minutes (`gosafe.PanicBudget`, `gosafe.PanicBudgetWindow`; constants, not settings; the window is six retention
sweeps when those are more than 100 s apart, so a sweep that panics every time is found out). The panic that uses it up is
logged, `/healthz` answers `503 degraded: the panic budget is spent: ...`, and the service shuts down in order (as on
SIGTERM, within 20 s) and **exits with status 1**, so that whatever supervises it starts it again with clean state.
Run the services under something that restarts them (`restart: on-failure` in Compose, a Deployment, systemd).

The drain of one session is not part of that budget: after 3 panics it is given up, and the session ends `SEALED` and
incomplete with an `incomplete_reasons` entry that says that the control plane failed. A pending seal whose attempts
panic is retried with backoff until the 24 h after which a failing seal is given up.

The SDK inside an instrumented service restarts each of its three loops at most 5 times and then leaves it stopped;
the application is never ended. When its subscriber panics it stops recording at once and tells the control plane,
again every second while the subscriber is being restarted and once more when recording has resumed; the control
plane marks the sessions that run when it is told incomplete (`stopped recording after an internal error`). A
subscriber that stays stopped says so once. From then on the service records nothing and is no longer listed by
`tracectl doctor`, like a service that is not running; sessions started later do not name it, so use
`tracectl doctor --require-services N` before a run to notice it.

## Metrics

The collector and the control plane serve `GET /metrics` in the Prometheus text format. The dashboard, tracectl
and the SDK export no metrics. A scraper on another host needs its Host value in `--allowed-hosts`. Counters
start at zero when the process starts. A labelled counter has no sample until the first event with that label
value.

### Collector

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `tsession_build_info` | gauge | `service` (`collector`), `version`, `goversion` | Build information; the value is always 1. |
| `tsession_collector_buffered_events` | gauge | none | Events waiting in reorder windows, not yet in the store. |
| `tsession_collector_deleted_sessions_total` | counter | `cause`: `api`, `retention` | Sessions deleted, by cause (api or retention). |
| `tsession_collector_goroutine_panics_total` | counter | `goroutine`: `janitor`, `retention`, `retention_stop` | Panics recovered in a background loop, by goroutine (each is logged with its stack, and the loop is restarted with backoff; /healthz answers 503 meanwhile). |
| `tsession_collector_ingest_errors_total` | counter | `cause`: `backpressure`, `shutdown`, `storage` | Ingest requests that failed, by cause (backpressure, shutdown or storage). |
| `tsession_collector_ingest_failed_sessions_total` | counter | `cause`: `backpressure`, `shutdown`, `storage` | Sessions whose events an ingest request could not take, by cause (backpressure, shutdown or storage). |
| `tsession_collector_ingest_partial_failures_total` | counter | none | Ingest requests that failed for some of their sessions and were accepted for the others. |
| `tsession_collector_ingested_events_total` | counter | `result`: `accepted`, `duplicate` | Events received by ingest, by result (accepted or duplicate). |
| `tsession_collector_quarantined_events_total` | counter | none | Poison events isolated and dropped because the store kept rejecting them. |
| `tsession_collector_rejected_events_total` | counter | `reason`: `invalid_session_id`, `invalid_proto`, `oversize_message` | Events refused by ingest, by reason. |
| `tsession_collector_seals_total` | counter | `result`: `complete`, `incomplete`, `error` | Seal requests, by result (complete, incomplete or error). |
| `tsession_collector_sessions_in_memory` | gauge | none | Sessions that currently have an in-memory actor. |
| `tsession_collector_store_append_failures_total` | counter | none | Failed attempts to append a batch of events to the store (each is retried). |

### Control plane

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `tsession_build_info` | gauge | `service` (`controlplane`), `version`, `goversion` | Build information; the value is always 1. |
| `tsession_controlplane_goroutine_panics_total` | counter | `goroutine`: `sweep`, `drain`, `seal_retry`, `compaction` | Panics recovered in a background goroutine, by goroutine (each is logged with its stack; sweep and drain are restarted with backoff, and /healthz answers 503 meanwhile). |
| `tsession_controlplane_kill_switch` | gauge | none | 1 while the global kill switch is on. |
| `tsession_controlplane_seal_failures_total` | counter | none | Sessions whose collector seal failed after all retries (recorded incomplete). |
| `tsession_controlplane_seal_retries_total` | counter | `result`: `success`, `failure` | Background attempts to seal a session whose seal failed past its budget, by result. |
| `tsession_controlplane_seals_abandoned_total` | counter | none | Sessions whose pending seal was given up after the pending budget (seal_error stays). |
| `tsession_controlplane_seals_pending` | gauge | none | Finished sessions whose collector seal failed and is still being retried. |
| `tsession_controlplane_sessions` | gauge | `state`: `ACTIVE`, `DRAINING` | Sessions that have not finished, by state. |
| `tsession_controlplane_sessions_deleted_total` | counter | none | Sessions deleted through the API. |
| `tsession_controlplane_sessions_finished_total` | counter | `state`: `SEALED`, `EXPIRED`, `ABORTED` | Sessions that reached a terminal state, by state. |
| `tsession_controlplane_sessions_started_total` | counter | none | Sessions started. |
| `tsession_controlplane_store_read_errors_total` | counter | none | Stored sessions that could not be read (I/O error or damaged archive file); they are missing from lists. |
| `tsession_controlplane_subscribers` | gauge | none | Open active-set streams (one per connected SDK). |
