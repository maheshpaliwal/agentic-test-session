# Changelog

Everything below is relative to the last state of `main` before this work; part of it has since been merged
there, so someone upgrading from a recent `main` has already met the older entries. Benchmark figures were measured once
on a development laptop, mostly against in-process fakes; treat them as indications, not guarantees.

## Unreleased

### Upgrading

Before deploying:

- `GET /healthz` of the collector and the control plane can answer `503 degraded: <reason>`: while a background
  loop of theirs restarts after a panic, and once the panic budget is spent. `tracectl doctor` then prints
  `DEGRADED <reason>` and exits 1. A health check that restarts on the first non-200 will restart sooner than
  the service would have exited by itself.
- Both services exit 1 after 5 panics of their background loops (sweep, janitor, retention) within 10 minutes,
  after an orderly shutdown of at most 30 seconds (longer than the collector's own stop, see "Give the collector
  at least 25 seconds" below). Run them under a supervisor that restarts them; the Compose file has
  `restart: on-failure:5` (not tried with Docker).
- Embedders of `controlplane.Manager`: start it with `m.RunInBackground(ctx)` where you had `go m.Run(ctx)`; a
  bare `Run` has no panic recovery and no panic budget.
- A custom `controlplane.Collector` must fill `DrainStatus.Instances` (and may implement `InstanceCounter`);
  without them every session with delivery reports ends `delivery_unconfirmed`.
- SDKs started with `runtime.Start` ask their collector's `/healthz` every 30 seconds while idle. Behind a proxy
  that answers that path with a 5xx the service shows as `CANNOT REACH COLLECTOR` in `tracectl doctor`, and
  `doctor --require-delivery` fails; a negative `runtime.Config.Exporter.ReachCheck` turns the check off.
- Stopping a session can take up to 10 seconds longer, while the control plane waits for a service that is
  silent or cannot deliver (`--delivery-grace`, `TSESSION_CP_DELIVERY_GRACE`; 0 turns the check off), and 12 to
  18 seconds while a service's delivery reports are failing.
- The first line of every export carries a `tsession_session` object next to the event. Decoders that refuse
  unknown fields, and golden files of exports, change.
- `tsession.digest/v1` changed within the version: a running session has `recording: unknown` (it was
  `complete`), failures that did the same thing are one entry, `omitted.section` can be `verdict`, and fields
  were added (`code`, `first_error`, `rolled_back_by`, `drill_down_note`, `hidden`). A reader that only adds
  what it knows is unaffected; one that compared the text is not.
- HTTP events answered with 400 or above carry the attribute `http.status`. Recordings compared across versions
  differ in it, and the digest no longer merges requests with different statuses into one line.
- `tracectl list --state` refuses a word that is not a state, and `--cp` / `--collector` (and their environment
  variables) are refused without a scheme; both exited 0 or failed later. The dashboard binary exits 2 with a
  stack trace on a start-up error (it exited 1 with one line). Ten log lines of the services moved from warn to
  error level.
- Custom implementations need new methods: `collector/store.Store.DeleteSession(ctx, sessionID) error` (a missing
  id is not an error), `controlplane.Store.Delete(id string) error`, `controlplane.Collector.Delete(ctx, id) error`.
  `controlplane.Evictor.Evict` is now `Evict(ids ...string)`. A store that keeps the old `Evict(id string)` still
  compiles, because the interface is optional, but is then silently not used for eviction.
- These structs gained fields, so unkeyed literals of them no longer compile: `collector.Config` and
  `controlplane.Config` (`OnPanicBudgetSpent` among them), `runtime.ExporterConfig` (`ReachCheck`),
  `controlplane.Session`, `Subscriber`, `ActiveSet` and `DrainStatus`, `collector.DrainStatus`, `Status` and
  `Entity`, `client.Info`, `Subscriber` and `Entity`.
- `tsql` records rows under the primary key the database catalog names, where it used `id`, and records no diff
  for an `UPDATE` whose row cannot be read back. Tests that assert on recorded keys or diffs of tables keyed by
  something other than `id` will see different values, and the database user needs to read the catalog
  (`pg_index`, `information_schema`); `WithPKLookup(false)` restores the old key choice.
- `tsql` records binary primary-key values in hex (`\xfffe0001`; beyond 64 bytes a prefix plus
  `...[N bytes sha256:…]`), so that different rows no longer share a key; text keys are kept whole up to 1024
  bytes. Events recorded earlier show a binary key as `<3 bytes>` and file under a different key. Note that the
  bytes of a binary key are now in the recording, where before only their length was.
- `controlplane.NewFileStore` takes options (`NewFileStore(path, opts ...FileStoreOption)`); code that holds it
  as a `func(string)` value no longer compiles. In package `dashboard`, `Session` and `Binding` are now the
  `controlplane` types (`CreatedAt` is a `time.Time`, not a string), and `BuildTrace`, `Node`, `EntityRef` and
  `EntityMutation` are gone.
- Recorded inbound request headers now include `x-tsession-traceparent`, which changes payload sizes and
  saved exports.
- The SDK exporter keeps events until the collector accepts them: `Flush` returns false while events are
  buffered, and `runtime.Stop` with an unreachable collector takes its full 2s. Events reach the collector's
  store 0.5 to 1s after arrival at the default window (it was up to 0.5s). Test teardowns and tests that read
  results at once may need to wait (`Session.StopAndWait` does).
- `tsql` records an upsert that updated a row with op `update`, where it recorded `upsert`. An assertion that
  filters on `Op: "upsert"` (for example `tsessiontest.RequireMutation`) no longer matches those rows, and
  `ON CONSTRAINT` upserts lose their image and carry `upsert_unknown`.
- Redis recordings change: secret-looking key parts are masked by default (entities and keys read
  `session:#...`; `tredis.WithKeyMask(tredis.KeyMaskOff)` restores the names), `PUBLISH` no longer appears as a
  changed key, `pipeline_size` and `pipeline_index` no longer count `MULTI`/`EXEC`, `HVALS` replies are always
  masked, and `*` in `WithBeforeImage`, `WithIgnoreKeys` and the other key patterns now also matches `/`. A
  malformed `WithMaskKeys` or `WithRedactKeys` pattern panics when the option is applied.
- gRPC calls that end with a status the caller causes or expects (`NotFound`, `InvalidArgument`,
  `AlreadyExists`, `PermissionDenied`, `Unauthenticated`, `FailedPrecondition`, `Aborted`, `OutOfRange`,
  `ResourceExhausted`, `Canceled`) are no longer failed events: a session whose only "failures" were such calls
  now passes `--fail-on-error` and `RequireNoFailures`. Assert on the attribute `grpc.code` if a test relied on
  them failing.
- `sdk/client/tsessiontest` panics where it called `t.Fatalf` for a broken set-up (a session that cannot be
  started or stopped, `Ambient` without a surrounding `tracectl run`, `Stop` or a `Require` function on the
  ambient session, a summary, mutations or events that cannot be read, `RequireMutation` on a session that was
  not sealed): the message is the same, but a panic ends
  the whole test binary, so the package's later tests do not run and `t.Cleanup` functions are skipped. An
  assertion that does not hold (`RequireNoFailures`, `RequireDeliveryConfirmed`, `RequireMutation`,
  `RequireNoMutation`) reports with `t.Errorf` and stops with `t.FailNow`: it fails its test only, and the
  cleanup of `Run` names the session and saves it to `$TSESSION_ARTIFACT_DIR`.
- Delivery accounting: upgrade the collector with or before the control plane, otherwise every session with
  reports is `delivery_unconfirmed`. A proxy between services and control plane must pass `POST /v1/delivery`;
  if it answers 404 or 403 a session the service took part in ends `incomplete`, and one it did not take part in
  `delivery_unconfirmed`. Sessions whose services recorded events
  the collector lacks are now `incomplete`, so a gate that passed may fail. An older control plane rewriting
  `--store-file` keeps `incomplete` but drops the reasons. The export of a session without events is one line,
  not an empty file, and an older `dashboard -open` refuses it. `tracectl summary` prints `(SEALED, incomplete)`
  for a timed-out drain or a failed seal.
- `GET /v1/sessions` on the control plane without `limit`: `name` now filters and `before` is refused with `400`;
  both were ignored.
- A custom collector store that does not implement the optional `store.ExpiredLister` is still swept for
  retention by listing every session.
- Collector on Postgres: the first start of this version builds two indexes on `sessions`
  (`sessions_sealed_at`, `sessions_unsealed`), not `CONCURRENTLY`. A session's first event waits while they
  build; measured at 0.84 s for 2 million sessions. An older collector keeps working against the new schema.
- `client.ErrConflict` has a new message (`errors.Is` still works). `thttp`'s `Hijack` on a writer that does not
  support it returns an error that wraps `http.ErrNotSupported` instead of the bare value.
- Scripts: `tracectl` exits 2 for an unknown command (was 1) and 0 for `-h`; `--fail-on-*` gates exit 3. The 403
  body for a rejected Host header is now a sentence naming the host.
- More values are redacted (see "Changed"). If a test asserts on recorded payloads, check it; `core.SetRedactKeys`
  restores a narrower list.
- `GET /metrics` sits behind the Host guard: a scraper that addresses the service by a non-loopback name needs
  that name in `--allowed-hosts`.
- The DynamoDB store now calls `BatchWriteItem` (for delete); the collector's IAM policy must allow it.
- Gateways and load balancers that allow-list request headers, CORS `Access-Control-Allow-Headers`, and SQS
  consumers that list `MessageAttributeNames` themselves must add `x-tsession-traceparent`. Without it receivers
  fall back to `traceparent`.
- Give the collector at least 25 seconds to stop: it flushes for up to 15s after up to 5s of HTTP shutdown.

Roll in this order, so that mixed versions keep working:

1. Collector. (A new control plane cannot delete sessions through an old collector: every delete answers `502`.)
2. Control plane.
3. SDKs in services, receivers (servers, queue consumers) before the services that call them. Two reasons:
   the SQS compact carrier attribute is now `x-tsession-ctx` (was `ts`); new consumers read both names, old
   consumers lose the trace of a new producer in compact mode. And tsession's trace context now travels in
   `x-tsession-traceparent`: a receiver on an older SDK behind an upgraded sender that runs application tracing
   (OpenTelemetry) still records its events in the session, but with the application's trace id and a wrong
   parent. Where the order cannot be kept, set `TSESSION_OVERWRITE_TRACEPARENT=true` on the upgraded sender until
   its receivers are upgraded; that keeps the old behaviour, including the overwritten application header. The
   full version table, generated by a test, is in `docs/PROTOCOL.md` under "Trace context propagation".
4. Dashboard and `tracectl`.

Old and new SDKs work with old and new servers. Against an old control plane `Session.WaitPropagated` returns at
once, so `tsessiontest.Run` cannot guarantee that a test's first request is recorded.

Rolling back:

- Collector, SDKs, dashboard: no data format changed (no table, column or protobuf change; Postgres gains two indexes that an
  older collector ignores).
  Sessions deleted by the API or by retention stay deleted.
- Control plane with `--store-file`: stop it cleanly first. A clean stop folds `<file>.journal` into `<file>`,
  which the old binary reads. After an unclean stop the old binary ignores the journal: changes since the last
  compaction are missing and sessions deleted since then reappear. If a journal file is left behind, delete it
  before starting the new binary again, or it is replayed over the old binary's newer data. Pending seal retries
  are forgotten by the old binary either way.

### Added

- `tracectl run --require-services N` does not run the command with fewer than N services connected (exit 3),
  and exits 3 after it when a service that was connected at the start is gone when the command ends, naming it.
  A service that is killed during a run could leave a session that reads as complete; this is the check for it
  where the run is driven by `tracectl run`.
- `tracectl run --export` writes the exit status of its command into the file (`command_status` in
  `tsession_session`), and `tracectl --from FILE ... --fail-on-error` fails on a status other than 0: a run whose
  tests failed without a failed event passed a gate that had the artifact alone. `client.Session.Export` takes
  functions that edit the verdict before it is written. The README has a two-job CI example and the exit
  statuses of `--from` in one place.
- `docs/LIMITS.md`: what the tool does not record, what a verdict can miss, and what the preflight and an
  export cannot tell, in one place; the README links it from its first screen and says what adopting it costs.
  `docs/PROTOCOL.md` documents `name`, `name_auto` and `events` of an export's `tsession_session`.
- `tracectl --from` and `dashboard -open` read a gzip'ed export as it is, and `-` for standard input
  (`gunzip -c run.ndjson.gz | tracectl --from - digest`). `--from` after the command is refused with the order
  that works, where the command reported an unknown flag.
- `tracectl run --export FILE` writes the session as `export -o FILE` does, when the run ends: the artifact a
  CI job keeps. The `--digest` and `--report` files of that run then name commands that read it
  (`tracectl --from run.ndjson show <seq>`), which still work when the services of the run are gone; without
  `--export` they name the session, as before.
- `tracectl --from FILE` reads a session saved with `tracectl export -o FILE`, with no service running: `digest`,
  `summary`, `report`, `events`, `trace`, `show`, `entities`, `history`, `diff`, `status` and `list`, with the
  output and the exit status of the live session. The session argument can be left out. `--from` given twice is
  `diff`'s baseline and candidate. A digest read from a file names the file in its drill-down commands
  (`tracectl --from run.ndjson show <seq>`). Commands that start, stop or change a session, and `--from` with
  `--cp` or `--collector`, exit 2; a missing, empty, cut-short or foreign file is one line and exit 1. A CI job
  or an agent that holds the artifact needs nothing else. `tracectl` links the dashboard's offline backend for
  it and is about 0.5 MB larger. Go API: `(*dashboard.Demo).Files()` and `dashboard.OpenedFile`.
- `tracectl run --digest FILE --max-bytes N`, as `digest --max-bytes`.
- `ExporterConfig.ReachCheck` (zero, off, for `NewExporter`; `runtime.Start` sets 30 s),
  `client.Info.StoppedInstances`, `client.Subscriber.Collector`; a session has `stopped_instances` (at most 64)
  and `stopped_instances_more`, left out of `GET /v1/sessions`.
- A collector URL that leads nowhere shows before any session: the SDK checks its collector's `/healthz` while
  idle (every 30 s, not per event) and tells the control plane; `GET /v1/subscribers` gains `collector`, and
  `tracectl doctor` prints `CANNOT REACH COLLECTOR: <error>` for such a service and exits 3 under
  `--require-delivery`, also with no session running. Any HTTP answer but a 5xx counts as reached, so a wrong
  URL at which another HTTP server answers is not detected.
- `tsessiontest.RequireEvent` and `tsessiontest.RequireFailure` with `tsessiontest.EventFilter` (service, kind,
  part of the name): assert that a call, a query or a job was recorded, or that a failure the test provoked was
  met where it should be. Both return the matching events.
- `thttp` sets the attribute `http.status` (`thttp.StatusAttr`) on a request that was answered with 400 or
  above, on both sides. The status was only in the recorded response, which is cut when the body is large; the
  digest's `code` reads the attribute. Requests that went well carry no such attribute.
- `tracectl digest`: failures merged into one entry keep the first one's error as it was recorded
  (`(the first: "dial tcp 10.0.0.7:8080: connect: connection refused")`, `first_error` in JSON), so the host or
  key that normalising takes out is still there. Left out when the budget is tight.
- `tracectl digest`: a change that was rolled back names the first event of its transaction that failed after it
  (`ROLLED BACK (a later statement of its transaction failed: #12)`, `rolled_back_by` in JSON); that this
  failure caused the rollback is likely, not recorded. A digest read with `--cp` or `--collector` on the
  command line says that its drill-down commands need the same flags (`drill_down_note`; left out when the
  budget is tight).
- The SDK tells the control plane when it stops recording (`stopped` in `POST /v1/delivery`): the sessions
  running at that moment that it took part in end incomplete, naming the service and instance; for the others
  it is a `delivery_unconfirmed` line. `Manager.Health`, `Collector.Health`;
  label `compaction` on `tsession_controlplane_goroutine_panics_total`; "Health and panics" in `docs/CONFIG.md`.
- `dashboard -demo`: the incomplete session carries a delivery report and the reason drawn from it, and one
  update changes a masked column, so the delivery table and the "changed (value hidden)" row can be seen without
  any service running.
- `tracectl list` ends a session's line with `INCOMPLETE` when its record is known to have gaps and with
  `DELIVERY-UNCONFIRMED` when the delivery of some instance could not be confirmed. Lines of other sessions are
  unchanged.
- `tracectl doctor` marks a connected service that reports events it cannot deliver, with the count, the last
  error and what to check; `doctor --require-delivery` exits 3 for it and for a service whose delivery reports
  do not arrive (a preflight to run while a session is active: it only sees services that report). `GET
  /v1/subscribers` carries an optional `delivery_pending` per follower.
- The dashboard's entity view follows the diff the SDK recorded, so a field that changed under redaction reads
  "changed · value hidden" (it read "no field changes"); `tracectl show`, `history` and `digest` say
  `changed (value hidden)`. The session page lists the delivery reports per service instance, and every event
  flag has help text.
- An entity names the services that changed it (optional `services` on the collector's entity rows), shown by
  `tracectl entities`, the `history` header and the dashboard: the same Redis key masked by two services is
  two rows, and now says which is whose.
- **Delivery accounting: a service that cannot deliver its events no longer passes the gates.** Each SDK
  instance reports to the control plane how many events it recorded per session (`POST /v1/delivery`, batched,
  plus one small request every 5 s while a session runs). When a drain ends, the control plane compares that
  with what the collector stored for the instance, and a shortfall makes the session `incomplete` with a reason
  that names the service, the instance and both numbers. A drain also waits for an instance that still owes
  events while it makes progress. Before, a service with a wrong `TSESSION_COLLECTOR_URL` lost its events and
  `--fail-on-incomplete` still passed.
  - Where the control plane cannot tell (a connected service whose reports do not arrive, an old collector) the
    session is `delivery_unconfirmed`: shown by `summary`, `report`, `digest`, `run` and the dashboard, and
    failed only by the new `--fail-on-unconfirmed` (and `tsessiontest.RequireDeliveryConfirmed`). Use that gate
    where every connected service is yours; in a shared environment another team's service can trip it.
    The strict gate includes the plain one: on its own it also fails a session that is known to be incomplete.
  - Measured with real binaries, 50 to 2,000 runs per scenario: no false `incomplete` on healthy systems
    (parallel sessions, 500 sessions stopping together, slow or briefly unreachable collector and report
    endpoint, idle services frozen or half-open); healthy drain time unchanged.
  - Not detected: a service killed before it ever reports, a service on an older SDK, and a session that ends
    sooner after a service's last word than the grace (10 s, `--delivery-grace`; 0 turns the check off), or less
    than about 10 s after the control plane's first session following a time without any began; a process cut
    off from both its reports and its stream.
  - Reported as `incomplete` although nothing is missing: a service whose reports are blocked while its events
    all arrived, and one frozen until the grace after the stop is over, after delivering and before reporting. The control
    plane cannot know that without the report, and the reason says so.
  - The endpoint is unauthenticated, like stop and abort. `docs/PROTOCOL.md` lists what a caller who can reach
    the control plane or the collector can do: make a session incomplete, hold a drain up to `max_drain`, and,
    by ingesting events in a service's name or forging a matching final report for a service whose own reports
    are blocked, make an incomplete session look complete. Whether that is acceptable is for whoever owns the
    environment's security to decide.
  - `tracectl export` carries the verdict, also for a session without events, so the gates exit for an
    exported file as for the live session.
- `client.StatusError` and `client.StatusCode(err)`: the error of a request the service answered with a non-2xx
  status now carries the code, so callers no longer match its text (it reads the same as before).
- **Redis writes carry a diff** when before-images are on (`tredis.WithBeforeImage`): the value of a string,
  per field of a hash, per member of a sorted set, `added`/`removed` for a set, by index for a list. It adds no
  round trips. Collections beyond the image cap get a partial diff or none, flagged `diff_truncated`; a diff
  that holds something the command cannot have done (another client wrote in between) is flagged
  `diff_concurrent`. A redacted value that changed is listed as `[REDACTED] -> [REDACTED]`. `MULTI`/`EXEC`
  spans carry `transaction` and `transaction_outcome`.
- **Secret-looking parts of Redis key names are masked** (`tredis`, `tredisv8`) in arguments, replies, images,
  diffs, span names and errors: `session:<token>` is recorded as `session:#<12 hex>`. The default
  (`KeyMaskHash`) is a hash keyed by a random secret of the process that is never written anywhere, so it
  cannot be recomputed from an export; the price is that the same key reads differently in another service or
  after a restart. `KeyMaskFlat` records `[REDACTED]`, `KeyMaskSession` a hash that is the same across services
  but can be guessed for short tokens, `KeyMaskOff` nothing; `WithMaskKeys` adds patterns. Which names count as
  secret is a heuristic (all-lower-case tokens, e-mail addresses and phone numbers as key parts are not
  caught): the list for your environment is a decision for whoever owns its data policy. A test puts a random
  secret into random commands and checks that no part of it reaches a recorded event. An independent review
  then found nine leaks outside what that test generates (a value stored through `BinaryMarshaler`, sensitive
  keys read through `XREAD` and commands missing from the table, the replies of a hand-typed `MULTI`/`EXEC`,
  names such as `oauth2_token:` and `PHPREDIS_SESSION:`, names longer than 1 KB); eight are fixed and pinned by
  tests, and the package doc lists what is still recorded as written (script source among it). A recording can
  still hold a credential: treat exports as test data that may contain secrets.
- `tracectl start --id-only` prints just the session id, for scripts; `tracectl doctor --require-services N`
  exits 3 with fewer connected SDK instances (a CI preflight).
- **`tracectl digest <id|name>`**: a whole session in one bounded answer for an AI agent or a person: the
  verdict, the failures with where they sit in the call tree, what changed (with uncertain records marked) and
  the call tree with repeated siblings collapsed (`40x`). It never exceeds `--max-bytes` (default 16384) and
  says what it left out and which command prints it. Markdown, or JSON with schema `tsession.digest/v1`
  (`docs/DIGEST.md`); takes the `--fail-on-*` gates; `tracectl run --digest FILE` writes it after the seal.
  Recorded text is quoted as data and stripped of control characters; that protects the structure of the
  output, not a model's judgement of what the text says.
  Failures that did the same thing in the same place are listed once with a count, so 300 requests failing
  the same way take one line, not the whole budget.
  The call tree merges them the same way, keeps apart calls that ended differently (a gRPC `NotFound` is not
  folded into the `OK` calls next to it) and shows a call's status code and an event's flags on its line.
- **Uncertain records are visible in entity history.** Each history entry carries its event's flags and each
  entity an `uncertain` count (optional fields); `tracectl history` and `entities` and the dashboard show them.
  `tracectl history` accepts a key as `k=v,k2=v2`, loosely typed JSON or the bare value of a one-column key;
  `''` or `--unknown-key` reads changes recorded without a key. `tracectl rename|stop|abort|status|wait --short`
  print `<id> <STATE>` instead of JSON.
- **`tracectl run [flags] -- <command>`** starts a session, runs the command with `TSESSION_ID` in its
  environment, stops and seals the session, prints the summary to stderr and can write a report (`--report FILE`).
  It exits with the command's status if that failed, else 1 if the session could not be sealed or read, else 3 if
  a `--fail-on-*` gate failed. The command must send the id itself as `x-tsession-id`; nothing reads the variable
  automatically. The command runs in a process group of its own, so a terminal's Ctrl-C reaches it once; it
  cannot read from the terminal. SIGINT, SIGTERM, SIGHUP and SIGQUIT are passed on, and the session is stopped
  and sealed after them; a control plane that cannot be reached is asked again until `--timeout`. Signal
  handling is untested on Windows.
- **Go tests under `tracectl run`.** `client.AttachEnv` returns the session named in `TSESSION_ID`, and
  `tsessiontest.Ambient(t, o)` joins it from a test without stopping it. `tsessiontest.Run` still starts one
  session per test and ignores the variable. There are no assertions on the shared session: its verdict exists
  only once `tracectl run` has sealed it, so `tsessiontest.Stop` and `Require*` panic when given that
  session and never stop it. An empty header-mode session makes `tracectl run` say why.
- **Delete and retention.** `DELETE /v1/sessions/{id}` on the control plane, `tracectl delete` and a two-click
  Delete in the dashboard remove a finished session with its events, seal and bundle. The collector's
  `--retention` (`TSESSION_COLLECTOR_RETENTION`, off by default) deletes a session that long after its seal, or
  after its last event if it never sealed. Events that arrive late for a deleted id start it again at Seq 1.
- **Results from the CLI and from Go.** `tracectl summary | events | entities | history | trace | show | diff |
  export | report`, `tracectl abort`, `tracectl version`. `sdk/client` gained `Session.Summary`, `Events`,
  `EventsPage`, `Entities`, `History`, `Trace`, `Mutations`, `Export`, `Delete`, `Detached`, and more fields on
  `Info`. `tracectl` can read results by id for a session the control plane has forgotten.
- **CI gates and reports.** `summary` and `report` take `--fail-on-error`, `--fail-on-incomplete` and
  `--fail-on-empty` (exit 3). `report` writes Markdown for a job summary (each failed event with the request it
  came from, why the session is incomplete, mutation counts by outcome) or JUnit. `diff --fail-on-diff` compares
  two runs with ids and literals in names normalised.
- **Go test helpers.** `sdk/client/tsessiontest`: `Run` ties a session to a test and waits (up to 2s) until the
  connected services were sent it; `RequireNoFailures`, `RequireMutation`, `RequireNoMutation`. With
  `TSESSION_ARTIFACT_DIR` set, a failed test's session is saved as a file (mode 0600).
- **Open a run offline.** `dashboard -open run.ndjson` (repeatable) serves sessions written by `tracectl export`,
  read-only, without the control plane or collector.
- **Finding out why a session is empty.** `tracectl doctor` checks both services, lists the services connected
  to the control plane (`GET /v1/subscribers`; the SDK reports its service and instance name) and shows the kill
  switch. The dashboard's empty state names the connected services. `sdk/autoinit` logs at start whether the SDK
  records; the SDK logs once per outage when it cannot deliver to the collector, once when the collector refuses
  events, and when `thttp.Handler` was called before the SDK was enabled.
- **Operations.** `GET /metrics` (Prometheus text format) on the collector and the control plane, listed in
  `docs/CONFIG.md` with every setting. `-version` on the collector, control plane and dashboard; container
  images are stamped with `git describe`. `tracectl kill on|off|status` for the control plane's kill switch. The
  services bind their port before logging that they listen, and name the flag to change when it is taken; the
  control plane accepts `--listen` as well as `--addr`.
- **Paged session list.** `GET /v1/sessions` takes `limit` (1..1000), `before` (a cursor) and `name` (exact
  match) next to `state`. With `limit` the newest sessions come first, with `X-Tsession-Next` and
  `X-Tsession-Total` headers. Without it the response is as before, except that `name` now filters (it was
  ignored) and `before` is refused with `400` (it was ignored). `X-Tsession-Unreadable` says how many stored
  sessions could not be read into a response. `client.ListPage`; `tracectl list --limit N`; `tracectl` resolves
  a name with the filter instead of listing everything; the dashboard loads the newest 200 sessions with
  "Load more", and its search covers the sessions that are loaded.
- **Session counts by id.** The collector's `GET /v1/sessions` takes `ids=<comma-separated session ids>` (at most
  1000) and answers `[{id,event_count,sealed}]` for those sessions from one head and one seal lookup each,
  instead of listing every stored session (a table scan on DynamoDB). Ids the store could not answer are counted
  in `X-Tsession-Unanswered`. Without `ids` the endpoint is unchanged. The dashboard asks only for the sessions
  it is showing; before, it fetched the collector's whole list every 3 seconds.
- **Dashboard.** "Copy as curl" on HTTP events; a link from a mutation to its entity history; the call tree
  shows each trigger's status and summary; a "seal retrying" label.
- **Control plane.** A seal that failed past its 30s budget is retried in the background (backoff up to 1m, for
  up to 24h; across restarts with `--store-file`), with session fields `seal_pending` and `drain_timed_out`. The
  active set carries a `generation` (on `GET /v1/active`, in every stream message and on `GET /v1/subscribers`).
  `FileStore.Close`, `WithStoreLogger`, and the optional `CheckedStore` and `SelectiveLister` store capabilities.
- Wrappers: `tsql.UnwrapConn` / `tsql.UnwrapDriver`; `WithImageDeadline` in `tredis` / `tredisv8`; gRPC ingress
  honours header and JWT-claim bindings, so requests that were unattributed before can now join a session.
- `make lint`, `make race`, `make fmt-check`. CI runs format check, vet, lint and the race tests (it does not
  run the integration suite).

### Changed

- `tsessiontest`: a failed `RequireMutation` or `RequireNoMutation` fails its test, not the test binary (it
  panicked), and names what was asked for (`matching entity "orders", op "update"`). The hint of a failed test
  reads `tracectl digest <id>`, where it read `tracectl summary <id>`. The docs say what a failed event is (a 404
  is not one), that a second stop is allowed, and what panics.
- `dashboard -demo`: the start of a job carries no error, as the SDK records it (`worker.start` is a point
  event; a failed job fails at its `worker.end`). The demo session has 7 failed events where it had 10.
- `tracectl diff` says when a side is incomplete (`note: b is incomplete: ...`, and `"recording": "incomplete"`
  on that side in JSON): what such a side lacks may be events that were lost, not what the run did.
- `tracectl digest` shows an HTTP status of 400 and above on a request's line (`code "HTTP 404"`, `"code"` in
  JSON). A 4xx is not a failed event, so a request the test was refused read like one that went well.
  Requests with different statuses are no longer merged into one line.
- `--fail-on-incomplete` on a session that is still running prints a note on standard error that it can only
  judge what has arrived so far; the exit status is unchanged. `docs/DIGEST.md` documents `hidden` of a changed
  field.
- A panic in a background goroutine of the SDK, the collector or the control plane no longer ends the process
  (for the SDK, the host application): the goroutine restarts with a backoff, and what was lost is reported. A
  drain that panics three times ends its session SEALED and incomplete; `max_drain` counts from the stop, also
  across restarts of the drain. The SDK stops recording at the first panic of its subscriber until it has the
  active set again. A panic under a lock no longer leaves the lock held.
  Known limits: a subscriber that stays stopped is reported once, and later sessions seal without that service's
  events and without a mark (it then counts as a service that is not connected, which `doctor --require-services`
  sees); any caller that reaches the control plane can send `stopped`; panics of the seal retry and of compaction are outside the budget.
- `tracectl list` refuses a reply whose sessions have no state (`--cp` pointing at the collector) with exit 1; it
  printed a row of blanks. `tracectl summary` of a session without events says that nothing was recorded.
- `tracectl start --name X` notes on standard error when an earlier session is also named X, with its id and
  state: the name stands for the new session from then on. An id that neither the control plane nor the
  collector knows is reported as "no session with id ..." before the HTTP status.
- `tracectl digest` of a running session says `recording: not yet known (the session is still running)` and
  carries `"recording": "unknown"`; it said `complete`, also while a service could not deliver its events.
- `tracectl list --state` accepts a state in any case and refuses a word that is not a state, naming the six; it
  printed nothing and exited 0. `--cp` and `--collector` without a scheme (`127.0.0.1:7070`) are refused with the
  URL to write. An unknown command prints one line, not the whole usage text before it.
- Collector and control plane log lines that carry an error are at error level (ten were warnings).
- The dashboard binary panics on a start-up error (a refused listen address, an unreadable `-open` file) where
  it called `log.Fatalf`: the message is the same, followed by a stack trace, and the exit status is 2, not 1.
- **gRPC failures follow the HTTP rule.** A call is a failed event when its status says the callee failed
  (`Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable`, `DataLoss`), as an HTTP 5xx is;
  the other statuses are recorded in `grpc.code` and `grpc.message` and are not failures, as an HTTP 4xx is
  not. Before, a lookup that missed failed a session over gRPC and passed it over HTTP.
- Dashboard `-demo` and `-open`: the session list without `limit` is oldest first, as on the control plane, and
  an unknown session reads as empty on the collector side instead of `404`.
- `tracectl history` for an entity the session did not change exits 1 and names what was looked up; it used to
  print `0 mutation(s)` and exit 0.
- **Redaction is stricter.** Sensitive names also match as whole words (`user[password]`,
  `password_confirmation`, `currentPasswordValue`, `card.cvv`, `X-Auth-Key`); new default keys (`passphrase`,
  `auth`, `jwt`, `signature`, `session_id`, `sessionid`, `otp`); query strings and form bodies also mask `key`,
  `code`, `sig`, `pin` and `session`. Harmless fields such as `token_count`, `auth_type` or `otp_enabled` are
  masked too.
- **Less is recorded verbatim.** An HTTP body that is not JSON, form-encoded or plain text (multipart, XML,
  YAML, binary and other application types) is recorded as `<media/type, N bytes>`. Redis writes that changed
  nothing, MongoDB statements after the first failure of an ordered write, and a DynamoDB delete that returned no
  old item no longer produce mutations.
- **The collector keeps what it accepted.** On shutdown it flushes every session's reorder window (for at most
  15s) instead of discarding it; each event is held one reorder window after it arrives (two at most), so events
  reach the store 0.5 to 1s after arrival at the default window instead of up to 0.5s.
- **The SDK exporter keeps a batch until the collector accepts it.** Connection errors, 5xx and 429 are retried
  with backoff instead of dropping events after about 300ms; up to the ring size (8192 events) stays in memory
  during an outage. `Flush` returns false while events are buffered, and `runtime.Stop` with an unreachable
  collector takes its full 2s.
- **Control plane file store** (`--store-file`). Changes are appended to `<file>.journal` instead of rewriting
  the whole file under the session lock (the journal is new; `<file>.archive` already existed); finished sessions
  past retention live in `<file>.archive` with a small
  index in memory and are read on demand (one file descriptor stays open); deleting an archived session
  overwrites its line in place. Snapshots are fsynced; torn lines and leftover temp files are tolerated; damaged
  archive lines are saved to `<file>.archive.corrupt` and reported at start; an archived session that cannot be
  read answers `500` instead of `404` and can still be deleted. Existing files load, and the first start
  rewrites them. A paged list is picked from the index and reads only its own lines (about 2 ms for the newest
  200 of 100,000 archived sessions); an unpaged list of more than about 16,000 archived sessions reads the
  archive on every call (about 0.5 s at 100,000).
- **tsession no longer overwrites the application's `traceparent`.** While a session was active the SDK replaced
  the W3C `traceparent` on outgoing requests and messages with its own ids, which broke the application's own
  distributed traces (OpenTelemetry and others). Its trace context now travels in a new key,
  `x-tsession-traceparent`, on HTTP, gRPC, Kafka, AMQP, SQS and the side channel; `traceparent` is still written
  where the request or message has none, so nothing changes on the wire for a system without application
  tracing apart from the extra key. A `traceparent` with ids that are not hex or are all zeros is ignored.
- SQS: the compact single-attribute carrier is `x-tsession-ctx` (was `ts`, which collided with application
  attributes). The full carrier is four message attributes (three when the application set `traceparent`), so a
  message with seven application attributes now uses the compact attribute. A stale tsession carrier on a
  republished message is removed; application attributes of the same names are left alone.
- kafka-go: a traced batch is written with one `WriteMessages` call instead of one per message.
- Redis: only destination keys of `*STORE`-style commands are recorded as mutated; image reads are capped and
  share a deadline.
- `tsql`: capture uses at most a quarter of the caller's remaining deadline; recorded error text is redacted;
  multi-table `TRUNCATE` yields one mutation per table; new flags `image_skipped:deadline`,
  `rows_redacted_conservatively`, `diff_truncated`.
- The dashboard proxy forwards `DELETE` for exactly `/v1/sessions/{id}` on the control plane: anyone who can
  reach the dashboard can delete finished sessions. The kill switch and the collector's internal API stay
  blocked. The proxy also serves `GET /metrics` of both services and the control plane's
  `GET /v1/subscribers` (the names of connected services) to anyone who can reach the dashboard.
- The collector has a read timeout of one minute per request and the servers close idle connections after two
  minutes.

### Fixed

- From the review of the command status and `--require-services`:
  - `summary`, `report` and `digest` say that the command of the run failed (`**Failed**: the command of the run
    failed (exit status 7); no failed events were recorded.`, `verdict.command_status` in the digest's JSON), for
    a file read with `--from` and for the files `tracectl run` writes itself. They printed "Passed" for a run
    that exited 7, and an agent that read the digest without a gate took the run for a good one;
  - the export of a run in which `--require-services` found a service gone is incomplete and says which: the
    run exited 3 and its file passed every gate;
  - `--require-services` counts replicas (of two that are called the same, one can be gone) and looks for a
    service for 6 seconds before it calls it gone (an SDK whose stream was cut reconnects within 5);
  - a `command_status` outside 0 to 255 is refused.
  Not changed: a gate asked through `dashboard -open` does not see the command's status (`docs/LIMITS.md`).
- `dashboard -open` shows a file without a verdict, or of a session that had not ended, as incomplete with the
  reason: a gate asked through the dashboard passed where `tracectl --from` on the same file did not.
- `tracectl --from` on a relative name that is not there says in which directory it looked (a command copied
  from a digest is often run elsewhere); `--from - < /dev/null` is an empty file, not "a terminal".
- `tracectl --from`, from the re-check of the fixes above: a verdict with a count and no state is read as
  unproven (it passed as a sealed session); a `tsession_session` member written twice on the first line, or a
  member of the verdict written twice, is refused (the last one was read, so a clean verdict could follow one
  that said incomplete); a drain that timed out makes the session incomplete whatever `incomplete` says; a
  session id may hold letters, digits and `_.:-` only. A file that does not say how its session ended is called
  that in the gates, the digest and the report, where the words were the ones for a session the control plane
  has forgotten (and the report read "The session is still ,"). `tracectl diff` notes a side of which it is not
  known how it ended (`"recording": "unknown"`).
- `tracectl run --export` finds a `--digest` or `--report` on the same file also when it is named by an
  absolute path or through a link, and once more after the export is written (the export is then kept and the
  other file not written).
- `tracectl --from -` with a terminal on standard input says that it reads an export from there and how to pipe
  one in; it waited for input without a word.
- `tracectl --from` passed all three gates on files that proved nothing (a review's findings). Now:
  - a verdict that does not decode (`"events":"3"`, `"incomplete":"true"`), a negative count, a `state` that is
    none, a second verdict, or another number of events than the count is refused (exit 1); such a verdict was
    dropped, and the file judged without it;
  - a file without a verdict or without the count is read as a session whose state is not known, with a note:
    `--fail-on-incomplete` and `--fail-on-unconfirmed` fail on it, as they do on a live session the control
    plane has forgotten. That is also what an export written while the control plane did not answer gives;
  - a session that had not ended when it was exported (`ACTIVE`, `DRAINING`: `run --export` of a session that
    did not seal) fails `--fail-on-incomplete`; it got a note and exit 0;
  - reasons or a seal error make the session incomplete whatever `incomplete` next to them says.
  `dashboard -open` stays a viewer: it refuses what cannot be read and shows the rest.
- `tracectl --from`: a session is named by its id first, then by its name or its file's name; a reference that
  fits two sessions is refused (a file named like another file's session id won), and a session name that begins
  with `ts_` works. Session names from a file are printed without control characters, and a session id that
  is not printable ASCII is refused. No more than 512 MB of events are read from one file (a 36 MB gzip file
  took 2.6 GB of memory).
- `tracectl run --export FILE` with `--digest` or `--report` on the same file is refused (the second overwrote
  the export and the run still said it wrote it); the export is made private (0600) also when the file was
  there; an export that fails part-way is removed. `--from - --from -` says that it was given twice.
- `tracectl --from` and `dashboard -open` given two exports of one session name both files and say that only one
  can be read at a time; a file given twice is said to be given twice. Both read "was already opened from
  another file".
- `tracectl --from FILE`, from a walkthrough of it:
  - a file that lost whole lines (a download that stopped at the end of a line) is refused: the export now says
    how many events it wrote (`events` in `tsession_session`). The first 5 of 32 lines read as "5 events, 0
    failures" and passed every gate. A file that lost its first line still cannot be told from an old export.
  - a session exported while it ran is shown as running (`**Live**`, `recording: not yet known`), not as sealed
    and complete;
  - the export stores the session's name (`name`), and the session is shown and can be named by it; it was
    named after the file;
  - the commands a Markdown report names read the file (`tracectl --from run.ndjson trace --errors`); they
    named the session, and run as printed would have asked the services at the default addresses.
- Delivery accounting, two silent misses from its fifth review: an instance with an old stream that lingers behind
  a proxy and a new one that was never heard of is judged by the newest connection, and is named again; a stream
  on which a report got no answer or a 5xx is opened anew once after the grace (never two within a grace, one
  extra stream per outage), so a service whose first report's answer was lost is named again.
- What an SDK told the control plane about reaching its collector is told again after a time: every check
  period (30 s) while it cannot, every ten periods while it can. A report from another caller that named the
  instance and said the opposite stood until the SDK's own state changed, so `tracectl doctor` could pass for a
  service whose collector did not answer.
- An instance that said it stopped recording, sent no report and is gone is asked for by name when the
  collector's list of instances is cut short: its events then make the session incomplete, where it was
  "unconfirmed".
- The `stopped` note of one instance on one session is replaced by one that says something else at most 8 times
  (`changes` on the note). A caller that varied the text made the control plane write every running session on
  every request: about 113 MB of journal from 300 small requests against 10 sessions, by the reviewer's count.
- `tracectl doctor` with `--cp` at a service that answers `/healthz` and is no control plane (the collector's
  address) says so and exits 1; it printed "ok" and "this control plane is too old". Its list of connected
  services shows the instance name, which was blank. The README's sample-service step no longer puts a
  foreground command before the `curl` it asks for.
- The SDK's `stopped` note (it stopped recording after a panic) makes a session incomplete only when a delivery
  report or events in the collector tie that instance to it; for the other running sessions it is one
  `delivery_unconfirmed` line. One note, from any caller, marked every running session incomplete. The notes are
  stored with the session and survive a restart of the control plane, which lost them. A service that stops
  recording before its first event of a session is therefore "unconfirmed" there: the default gates pass, and
  `--fail-on-unconfirmed` fails.
- `/healthz` is degraded while a seal retry or a compaction that panicked waits for its next attempt (at most
  30 s for a compaction); both stay outside the panic budget.
- Delivery accounting, from its fourth review: a connected service that was heard of and is merely silent (frozen,
  or the report endpoint briefly down) is no longer named under `delivery_unconfirmed` in sessions it left no
  report and no events in (46 to 80 of 200 sessions in the reviewer's runs, now 0); an SDK whose reports fail for
  the grace reopens its stream and is named. A refused delivery report is not retried while no session runs.
  Reports that begin to fail during a run are named after about 12 s, where it was 10.
- Panic recovery, from its review: a session that a panicking sweep left DRAINING is announced to the SDKs when
  the next sweep starts its drain (they were not told, so they did not flush or send their last delivery
  report); the restart backoff of a background loop starts over after a run longer than ten minutes, so a panic
  every few days no longer leaves `/healthz` degraded for up to 30 seconds each time; a drain that is run again
  after a panic counts its session once in `sessions_finished_total`.
- Control plane and client: a large answer from the collector was not read to its end, so its connection could
  not be reused and every poll opened a new one; repeated rounds of 500 sessions ran a machine out of ports.
- `tredis`: values that were recorded although the key or field is sensitive: the reply of `SET ... GET`,
  `HVALS`, `CONFIG GET` of credentials, `ACL`, and the value moved by `RENAME`. The error of a command with a
  masked argument keeps its code and kind only, since Redis and scripts quote arguments in error text.
  `PUBLISH` no longer records a changed key; `GETSET` on a new key is a mutation; a command queued by a raw
  `MULTI` is not. Members of a sorted set that redact alike are kept apart, and infinite scores are recorded.
- Every wrapper that records nothing when created before the SDK starts now says so (`tredis.Wrap`,
  `tredisv8.Wrap`, the `tgrpc` interceptors, `thttp.Transport`, `tsql.Open`, `tsql.OpenDB`, `tsql.WrapDriver`,
  `tmongo.Apply`, `tdynamo.Apply`); only `thttp.Handler` did.
- `tsql`: `DO`, `CALL` and `EXECUTE` were recorded as reads. They are writes whose row changes tsql cannot
  see, recorded as `db.write` with the new flag `effects_unknown` and no mutations.
- An `INSERT ... RETURNING id` (any RETURNING that lists only some columns) was shown as an uncertain record
  in `tracectl digest`, `history`, `entities` and the dashboard, although its row image is read separately and
  is complete. The flag `returning_partial` describes the statement, not the image, and no longer counts as
  uncertain.
- `tsql`: an upsert (`INSERT ... ON CONFLICT DO UPDATE`) that updated an existing row was recorded like an
  insert, every column as new; with a conflict target other than the primary key it could be filed under a row
  that was never written. It is now recorded as an `update` with the row's earlier values and a diff of the
  changed fields, a multi-row upsert as one mutation per row, and an upsert whose outcome cannot be known under
  the new flag `upsert_unknown`, with no diff. A traced Postgres upsert takes 7 round trips instead of 4
  (`WithUpsertBefore(false)` keeps 4 and records updates as `upsert_unknown`). Not flagged: another connection
  updating the same row between the reads. On a table whose AFTER INSERT trigger inserts a row referencing the
  new one, every inserting upsert is `upsert_unknown`. The MySQL path has only run against a fake.
- Dashboard: `-demo` and `-open FILE` answered several read endpoints in shapes of their own, so `tracectl` and
  the Go client could not read them. They now answer as the control plane and collector do, which makes every
  read command (`summary`, `events`, `trace`, `entities`, `history`, `show`, `report`, `digest`) work on an
  exported run: `dashboard -open run.ndjson` prints the `tracectl --cp ... --collector ...` flags to use.
  `tracectl export` through these backends no longer drops the first event. Tests in `integration/` compare
  each endpoint with the real services. A refused write on an opened file names the file.
- `tsql` on Postgres: nothing is read inside a SERIALIZABLE transaction however the level was set (`BEGIN` or
  `START TRANSACTION ... ISOLATION LEVEL`, `SET TRANSACTION`, session characteristics, a connection, role or
  database default); it was only detected through `BeginTx`, and two concurrent inserting transactions that
  commit untraced could fail with 40001 when traced. The autocommit capture's own transaction is READ COMMITTED.
  Still true: under REPEATABLE READ a capture can take the transaction's snapshot a moment before the
  application's first statement would have.
- `tsql`: an `UPDATE` whose before image is missing records no diff (it listed every column as new). In one
  failure path the capture fence rolled back to a savepoint it had not taken, which could undo the
  application's own work; the savepoint is now named per connection (`ts_capture_<random>`).
- SDK: one session the collector keeps failing no longer blocks delivery of the other sessions in the same
  process. Its events stay buffered and are retried on their own; once it has kept failing for a while as others
  were accepted, a full buffer drops its newest unsent events before another session loses any. Measured over
  30 s with three healthy sessions and one failing at 100 events/s each: healthy events lost went from 3780 to 0.
  Until that point is reached, and for a failing session that alone fills the buffer, healthy sessions can still
  lose events, fewer than before. Failures the collector does not attribute to a session (timeouts, proxy
  errors) remain whole-request outages. With the buffer full and a session held, recording an event costs up to
  about 30 µs instead of tens of nanoseconds.
- Collector: a failed ingest request names the sessions it failed (`failed_sessions`, at most 32, and
  `failed_sessions_truncated`), and a failed session counts no accepted events. New counters
  `tsession_collector_ingest_failed_sessions_total{cause}` and `tsession_collector_ingest_partial_failures_total`.
  An older SDK ignores the new fields and behaves as before.
- `sdk/client` and `tracectl`: an error or `doctor` line that names a service URL no longer shows the URL's user
  name, password or query (a token there ended up in CI logs). New `client.DisplayURL` and
  `client.ScrubURLError` for callers that print such errors themselves.
- Collector: a bare numeric key of 1,000,000 or more did not match in entity history.
- Dashboard opened on an exported file showed a duration of 0 for a recording shorter than a second.
- `tracectl diff` treated `HTTP 500` and `HTTP 503` (and differing SQLSTATE, MySQL error numbers and unnamed
  gRPC codes, `StatusCode: 500`, `error code N`, `exit status N`) as the same failure and reported no
  differences. The rule is a guess from the words around a number: `status=503` without a space is still
  normalised. `tracectl report --fail-on-incomplete` now also
  fails on a seal error alone, as `summary` already did.
- `tsql`: a table whose primary key is not `id` was recorded as if an `UPDATE` wiped the row (every column
  `-> ∅`, no key). Primary keys are now read from the database catalog (Postgres, MySQL; the MySQL query has not
  been run against a live server), once per table per handle and again after 30 seconds. `WithPKColumns` still
  wins; `WithPKLookup(false)` turns the lookup off. Cost: three more round trips on the first traced write to a
  table, and a table whose key is not `id` now takes the same 7 round trips per `UPDATE` as any other, because its row
  is read back (before, it was not).
- `tsql`: an `UPDATE` whose row cannot be read back records no diff and reads "after image unavailable" in
  `tracectl` and the dashboard; a key that matches more rows than were written is flagged `pk_not_unique`
  instead of recording another row's values. New flag `pk_lookup_failed`.
- Collector on Postgres: several collectors starting together on a database without the schema failed with a
  unique violation in `CREATE SCHEMA`; the schema is now created under the migration lock. `make it` also runs
  the Postgres store's own tests against the real server.
- Collector: a retention sweep that runs out of time resumes where it stopped, so sessions that cannot be
  deleted no longer block the ones after them.
- `tsql` on Postgres: a capture that ran longer than a second left its `statement_timeout` and savepoint in
  the application's transaction, or in autocommit a read-only transaction that made the application's write
  fail. The fence is now closed under its own deadline.
- `tracectl report`: a session with no events is headed **Empty**, not **Passed**; with `--fail-on-empty` the
  job summary used to say "Passed" on a failed job.
- `tsessiontest`: a failed event reported by `RequireNoFailures` is no longer printed a second time when the
  test ends.
- Collector: an ingest racing with shutdown could panic (send on a closed channel); it now gets `503`. A seal
  racing with shutdown gets `500`.
- Collector: a store call that timed out could get an event quarantined as "poison"; timeouts are retried, and
  background store calls are bounded (30s). `Close` could take minutes with a hung store.
- Collector: the reorder window applied to a whole flush instead of to each event, so an event could be
  sequenced ahead of a straggler that sorts before it.
- Collector: a session removed from the store behind the collector's back no longer wedges on Seq conflicts.
- Collector (DynamoDB store): every append after a session's first declared an expression attribute name it did
  not use, which DynamoDB rejects. Read from the code and tested against the in-process fake only.
- Redaction: a secret after an escaped quote in truncated JSON survived; DynamoDB expression values inside
  function calls, `IN` lists and reused placeholders leaked; Redis member arguments of read commands on
  sensitive keys leaked; SQL `lower(u.password) = $1` and aliased sensitive columns leaked; a Kafka record value
  or queue message body that is a JSON string literal was recorded unmasked; scrubbing text twice left
  `[REDACTED]]` behind.
- `thttp`: the request-body capture of the client wrapper raced with the transport when the server answered
  before the body was fully sent; `Flush` / `Hijack` / `Push` work when the underlying writer only exposes them
  through `Unwrap`; a wrapped mux mounted under a wrapped mux recorded every request twice.
- Wrappers: recording code in `thttp`, `tgrpc` and the Redis wrappers recovers from its own panics instead of
  letting them reach the host call. gRPC client streams were recorded `OK` when the RPC failed or was cancelled.
- `tsql`: parsing a statement with a very large `IN (...)` list was quadratic (about 10s for 65k placeholders).
- SDK exporter: `Close` waited out a request stuck on an unresponsive collector.
- Control plane: expanding a very long session-name template is bounded.

### Performance

- Span and trace ids come from the runtime's random generator, without a system call: a span id takes 14 ns
  where it took 310 (parallel benchmark on a laptop; the system call was more than half the CPU of starting a
  span on macOS). They identify spans and are not secrets; the instance suffix still comes from the system.
- From a measured audit of the SDK's hot paths (figures are medians of interleaved runs on a loaded laptop):
  - a request that carries no session header no longer allocates to find that out: reading the header costs 11 ns
    and no allocation, where it cost 48 ns and one (`core.HTTPHeaderCarrier.Get`); `thttp.Handler` without an
    active session takes about 120 ns per request, where it took about 160;
  - `core.RedactJSON` looks for a sensitive key before it parses, and returns a document without one as it is.
    The audit measured a recorded `tsql` insert at 4.6 µs and 66 allocations, where it was 7.2 µs and 109, and a
    Redis `GET` at 12 allocations where it was 25; documents that do hold a secret cost what they did;
  - `core.RedactText` skips the regular expressions that cannot match and runs the last one once per match: a
    64 KB request through `thttp.Handler` takes about 1.6 ms, where it took about 2.0;
  - `tgrpc`'s server interceptors read nothing while no session is active, and otherwise read the metadata key
    by key instead of copying all of it (the audit: 121 ns and 1 allocation, where it was 279 ns and 4);
  - a Redis key masked with the process secret reuses the keyed hash (the audit: 52 allocations, where it was 59).
  `core/redact_same_test.go` holds the fast paths of `RedactJSON` and `RedactText` to the plain implementations
  on 500,000 generated inputs, valid and broken.
- The control plane indexes a session's delivery reports once per judgement, not once per unheard instance: with
  1,000 streams and 1,280 reports a judgement takes 2.6 ms and 1.1 MB where it took 407 ms and 430 MB (the
  developer's benchmark, `BenchmarkJudgeDelivery`), and no longer stalls the API while it holds the lock.
- Collector retention sweep on the in-memory and Postgres stores reads only the sealed sessions that may be
  due instead of listing every session and reading every seal. With 10,000 sealed sessions and nothing due, a
  Postgres sweep went from about 0.4–1 s to about 1 ms. Its cost still grows with the number of never-sealed
  sessions, and the MongoDB and DynamoDB stores still list every session per sweep.
- `tsql` on Postgres: fewer round trips per traced write. The capture fence is closed by rolling back to its
  savepoint, and its opening and closing statement pairs go out as one text each where the driver allows it
  (detected per connection). Measured on Postgres 14 with pgx: a traced `UPDATE` takes 7 round trips instead of
  9 (autocommit) or 11 (in a transaction); `DELETE` and keyed `INSERT` take 4 instead of 5 or 6. The recorded
  mutations are unchanged. Not measured on other Postgres versions or behind pgbouncer.
- Outside an active session the wrappers do no tracing work: `tsql` neither parses nor redacts (a 54 KB literal
  INSERT: about 3 ms and 8.6 MB down to about 1 µs), `tdynamo` renders nothing, the Redis wrappers do not
  classify the command, the queue wrappers do not decode message bodies or consult the side channel (which
  delayed every job by up to 200 ms behind a slow store). `core.Active(ctx)` and `core.AnyActive()` are the
  checks; a custom session set can implement the optional `core.AnyActiver`.
- Sensitive-key matching memoises verdicts: redacting an 8 KB JSON payload went from about 590 µs / 3.7 MB to
  148 µs / 143 KB.
- Collector: a cached session view is extended with newly stored events instead of re-reading the session, and
  the status of a session with no live actor is remembered while the store is unchanged.
- Control plane file store: a write no longer costs time proportional to the number of sessions (with 10,000
  sessions: about 51 ms to 0.1 ms).

### Internal

- No code calls `Fatal` or `Fatalf`: program code and all tests panic with the same message. A failing
  assertion therefore stops its package's test binary at once; the other packages still run.
- `internal/slicex`, `internal/mapx`, `internal/set`; hand-written loops and `sort.Slice` calls replaced by them
  or by the standard `slices` / `maps` / `cmp` packages.
- `slicex.Count` and `mapx.MapKeys` join `slicex.FilterMap` and `slicex.ToMap`; 23 more loops in
  `core`, `cmd/tracectl`, `contrib/tsql`, `contrib/tqueue`, `contrib/tdynamo` and `internal/redisx` use them, the
  standard library, or one shared function (`refsOf`, `identsOf`, `digestFlags`) where the same loop stood twice.
- The go-redis v8 and v9 wrappers share one implementation (`internal/redisx.Tracer`).
