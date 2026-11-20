# Changelog

## Unreleased

### Added
- **Delete and retention.** `DELETE /v1/sessions/{id}` on the control plane and `tracectl delete` remove a finished
  session together with its events, seal and bundle. The collector's `--retention` (`TSESSION_COLLECTOR_RETENTION`)
  deletes sessions that long after their seal. `store.Store` has a new `DeleteSession` method, implemented by all
  four backends and covered by the conformance suite.
- **Results from the CLI and from Go.** `tracectl summary | events | entities | history | export`, with
  `summary --fail-on-error` / `--fail-on-incomplete` (exit status 3) for CI gates, plus `tracectl abort` and
  `tracectl version`. `sdk/client` gained `Session.Summary`, `Events`, `EventsPage`, `Entities`, `History`, `Trace`
  and `Delete`, and `Info.Reason` / `Info.SealError`.
- The SDK says whether it records: `sdk/autoinit` logs one line at start when `TSESSION_ENABLED` is set (enabled with its URLs, or why it is
  disabled), and a request rejected by the Host guard now gets a 403 that names the host and the setting to
  change. The README quick start gained the environment block and an "empty session" checklist.
- `tracectl report <id|name> [--format markdown|junit] [-o FILE]` writes a CI report of one session. Markdown (for
  a job summary) lists each failed event with the request it came from, why the session is incomplete, and
  mutation counts by outcome; JUnit gives one test case per request. `--fail-on-error` and
  `--fail-on-incomplete` exit 3 after the report is written.
- Dashboard: "Copy as curl" on the Request tab of HTTP events, with shell-safe quoting, a `$BASE_URL` placeholder
  for path-only URLs, and notes for left-out headers, redacted values and truncated bodies.
- Dashboard: an empty session now says which services are connected to the control plane, or that none is.
- The SDK logs once when it cannot deliver events to the collector, once when delivery resumes, and once when the
  collector refuses them, and at start when a wrapper (`thttp.Handler`) was called before the SDK was enabled.
  Both cases used to show up only as an empty session.
- `tracectl` reads results by session id for a session the control plane has forgotten (`client.Detached`).
- The services bind their port before logging that they listen; a taken port names the flag to change. The
  control plane accepts `--listen` as well as `--addr`.
- `tracectl summary` and `tracectl report` take `--fail-on-empty` (exit 3 when the session recorded no events): a
  run in which nothing was connected or attributed passed every other gate.
- Container images report a real version: `make up` stamps `git describe` into the binaries (they are built
  without `.git` and used to say `(devel)`).
- `tracectl kill on|off|status` sets and reads the control plane's global kill switch; `client.SetKill` and
  `client.KillSwitch` do the same from Go. `tracectl doctor` reports when the switch is on.
- `docs/CONFIG.md` lists every setting and every metric of the collector and the control plane; a test in each
  package keeps the metric tables in step with the code.
- Dashboard demo: HTTP events have the shape the HTTP wrapper records, so "Copy as curl" appears on them.
- `tracectl doctor` checks that the control plane and collector answer and lists the services connected to the
  control plane (`GET /v1/subscribers`); the SDK now reports its service and instance name when it connects.
  An empty session no longer looks the same whether the service is unconnected or simply got no traffic.
- `tracectl diff <a> <b>` compares two sessions: failures in one run only, mutation and event counts, and the
  first point where a service's event sequence diverges, with ids and literals in names normalised.
  `--fail-on-diff` exits 3; `--ignore-counts` tolerates retries; `--json`.
- **Open a run offline.** `dashboard -open run.ndjson` (repeatable) serves sessions written by `tracectl export`,
  read-only and without the control plane or collector, so a failed CI run can be attached as a file and opened
  locally.
- **The first request of a test is recorded.** The control plane tracks which version of the active set each
  connected service was sent (`generation` on `GET /v1/active` and `GET /v1/subscribers`); `Session.WaitPropagated`
  waits for all of them, and `tsessiontest.Run` calls it (up to 2s) before returning.
- **A failed test leaves an openable file.** With `TSESSION_ARTIFACT_DIR` set, `tsessiontest.Run` saves the session
  of a failed test there (mode 0600) and logs the `dashboard -open` command, so the run outlives the CI job.
  `Session.Export` writes the same format from Go.
- **Assertions for Go tests.** `sdk/client/tsessiontest` ties a session to a test (`Run`) and asserts on what it
  recorded (`RequireNoFailures`, `RequireMutation`, `RequireNoMutation`); `Session.Mutations` returns a session's
  mutations as a flat, filterable list.
- `tracectl trace` prints the call tree (`--errors`, `--depth`), and `tracectl show <session> <seq>` prints one
  event in full, so a failed run can be read from a CI log or over SSH without the dashboard.
- The dashboard can delete finished sessions (two-click Delete in the session list).
- **Metrics.** `GET /metrics` (Prometheus text format) on the collector and the control plane; `-version` on the collector,
  control plane and dashboard, and `tracectl version`.
- `tsql.UnwrapConn` / `tsql.UnwrapDriver`; `WithImageDeadline` in `tredis` / `tredisv8`; gRPC ingress now honours
  header and JWT-claim bindings.
- `make lint`, `make race`, `make fmt-check`; CI runs them.

### Changed
- **Redaction is stricter.** Sensitive names now also match as whole words (`user[password]`,
  `password_confirmation`, `currentPasswordValue`, `card.cvv`, `X-Auth-Key`), new default keys were added
  (`passphrase`, `auth`, `jwt`, `signature`, `session_id`, `otp`), and query strings / form bodies also mask `key`,
  `code`, `sig`, `pin` and `session`. Expect more `[REDACTED]` values than before, including harmless fields such as
  `token_count`; tune with `core.SetRedactKeys`.
- HTTP bodies without a structural redactor (multipart, XML, binary types) are recorded as `<media/type, N bytes>`.
- The collector flushes every session's reorder window on shutdown instead of discarding it, for at most
  `Config.ShutdownTimeout` (15s) in total; events still unwritten then are dropped and logged.
- SQS: the compact single-attribute carrier is now `x-tsession-ctx` (was `ts`, which collided with application
  attributes). Consumers read both names; an old consumer reading a new producer loses the trace in compact mode.
- kafka-go: a traced batch is written with one `WriteMessages` call (one publish span per message) instead of one
  call per message.
- Redis: only destination keys of `*STORE`-style commands are recorded as mutated; writes that changed nothing
  (`SETNX` on an existing key, `DEL` of a missing key, ...) no longer produce a mutation; before/after image reads
  are capped and share a deadline.
- MongoDB: statements after the first failure of an ordered write are no longer recorded as mutations.
- `tsql`: capture uses at most a quarter of the caller's remaining deadline; recorded error text is redacted;
  multi-table `TRUNCATE` yields one mutation per table. New flags: `image_skipped:deadline`,
  `rows_redacted_conservatively`, `diff_truncated`.
- `tracectl`: an unknown command exits 2 (was 1), like a bad global flag. Usage errors inside a command still
  exit 1.
- `tracectl summary --fail-on-incomplete` also fails for a session the control plane marked incomplete (a failed
  seal, a drain that timed out), not only for what the collector's summary reports.

### Fixed
- Collector: an ingest or seal racing with shutdown could panic (send on a closed channel); it now gets `503`.
- Collector: a store call that timed out could get an event quarantined as "poison"; timeouts are retried instead,
  and background store calls are bounded (30s).
- Collector: `Close` could take minutes when the store hung, because every session's final flush waited out several
  store timeouts; it is bounded by `ShutdownTimeout` now, also when a request is still inside the hung store, and
  a retention sweep in flight ends when the collector closes.
- Control plane file store: a delete whose journal record started a background compaction was undone by the next
  unclean restart (the snapshot was taken while the session was still in memory).
- Control plane file store: a partial line left in the archive by a failed write (disk full) made the next
  evicted session unreadable after a restart; a failed delete could bring an older archived record back.
- `dashboard -open`: a file cut short mid-line still opens (marked incomplete); duplicate or out-of-range
  sequence numbers are rejected instead of silently hiding events.
- `tkafkaconfluent`: a record value that is a JSON string literal was recorded unmasked.
- SDK log lines never print the collector or control plane URL with its userinfo or query, and a flapping
  collector logs at most one outage and one recovery per minute.
- `tracectl`: a mistyped `ts_` id is an error again (the fallback for forgotten sessions made it read as an
  empty session that passed every gate), and `--fail-on-incomplete` fails for a session the control plane no
  longer knows, since its seal state is unknown.
- Build: a tag name can no longer inject shell or linker flags through `make up`.
- Redaction: scrubbing text that was already scrubbed left `[REDACTED]]` behind and changed the recorded size;
  `core.RedactText` is idempotent now. Queue message bodies are scrubbed exactly once, and bodies nested in a
  Kafka batch that are JSON string literals are scrubbed too.
- `thttp`: the request-body capture of the HTTP client wrapper raced with the transport when the server answered
  before the body was fully sent (data race, possible garbage in the recorded body).
- Wrappers: recording code in `thttp`, `tgrpc` and the Redis wrappers now recovers from its own panics instead of
  letting them reach the host call (for example a gRPC message whose `MarshalJSON` panics).
- Control plane: a seal that failed past its 30s budget left the session `SEALED` with `seal_error` forever. It is
  now retried in the background (backoff up to 1m, for up to 24h, across restarts). New session fields
  `seal_pending` and `drain_timed_out`; new metrics `tsession_controlplane_seals_pending`,
  `tsession_controlplane_seal_retries_total` and `tsession_controlplane_seals_abandoned_total`.
- SDK exporter: a batch now stays in the ring until the collector accepts it. Connection errors, 5xx and 429 are
  retried with backoff instead of dropping events after about 300ms, so a collector restart no longer loses
  events, and `Flush` returns false while events are still buffered. `runtime.Stop` with an unreachable
  collector now uses its full timeout (2s).
- Collector: the reorder window applied to a whole flush instead of to each event, so an event could be sequenced
  a moment after it arrived and ahead of a straggler that sorts before it. Each event now gets a full window.
- Collector: a session removed from the store behind the collector's back no longer wedges on Seq conflicts.
- Redaction: a secret after an escaped quote in truncated JSON survived; DynamoDB expression values inside
  function calls, `IN` lists and reused placeholders leaked; Redis member arguments of read commands on sensitive
  keys leaked; SQL `lower(u.password) = $1` and aliased sensitive columns (`password AS p`) leaked.
- gRPC client streams were recorded `OK` when the RPC failed or was cancelled.
- `thttp`: `Flush` / `Hijack` / `Push` now work when the underlying writer only exposes them through `Unwrap`; a
  wrapped mux mounted under a wrapped mux recorded every request twice.
- `tsql`: parsing a statement with a very large `IN (...)` list was quadratic (about 10s for 65k placeholders).
- SDK exporter: `Close` waited out a request stuck on an unresponsive collector.

### Performance
- `tsql`: statements outside an active session are no longer parsed or redacted; they go straight to the driver
  with no added allocation in the benchmark (a 54 KB literal INSERT with no session active: about 3 ms and 8.6 MB down to about
  1 µs, against a fake driver). Hand-written transaction control outside a session is still tracked.
- `tqueue`: consumers no longer consult the side channel while no session is active, so a slow side-channel
  store no longer delays every job by the lookup bound (200 ms by default). `tdynamo`: calls outside a session
  no longer render the request (no allocation in the benchmark), while still learning table key schemas.
  New `core.AnyActive()` and the optional `core.AnyActiver` interface for session sets.
- Outside a session, the Redis wrappers no longer classify the command or allocate, and the queue wrappers no
  longer decode and redact every message body (a 64 KB JSON message cost about 1.7 ms and 1.4 MB per consume,
  an 8 KB binary one about 1 ms, with no session active). `core.Active(ctx)` is the check wrappers use.
- Control plane: the file store (`--store-file`) appends each change to `<file>.journal` instead of rewriting the
  whole file under the session lock (a write with 10,000 sessions: about 51 ms to 0.1 ms; a sweep evicting 500:
  about 20 s to 2 ms, on the development machine). Snapshots are fsynced, torn lines and leftover temp files are
  tolerated, and existing files load unchanged. `Evictor.Evict` now takes a batch of ids.
- Collector: a cached session view is extended with the events stored since, instead of re-reading the whole
  session whenever it grows, and the status of a session with no live actor is remembered while the store is
  unchanged. Polling a live session reads each event about once rather than once per poll.
- Sensitive-key matching no longer builds a `strings.Replacer` per call and memoises verdicts: redacting an 8 KB
  JSON payload went from about 590 µs / 3.7 MB to 148 µs / 143 KB, a traced 100-byte request through `thttp` from
  about 51 µs / 240 KB to 20 µs / 14 KB (`go test -bench` in `core/` and `thttp/` on the development machine).

### Internal
- New `internal/slicex`, `internal/mapx` and `internal/set` helpers; hand-written loops, `sort.Slice` calls and
  default-value blocks replaced by them or by the standard `slices` / `maps` / `cmp` packages.
- The go-redis v8 and v9 wrappers share one implementation (`internal/redisx.Tracer`); each keeps only its hook
  methods and client adapter.

### For implementers of the interfaces
- `collector/store.Store`: add `DeleteSession(ctx, sessionID) error`.
- `controlplane.Store`: add `Delete(id string) error`. `controlplane.Collector`: add `Delete(ctx, id) error`.
- `controlplane.Evictor`: `Evict(ids ...string)` (was a single id).
