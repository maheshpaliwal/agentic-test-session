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
- `tracectl trace` prints the call tree (`--errors`, `--depth`), and `tracectl show <session> <seq>` prints one
  event in full, so a failed run can be read from a CI log or over SSH without the dashboard.
- The dashboard can delete finished sessions (two-click Delete in the session list).
- **Metrics.** `GET /metrics` (Prometheus text format) on the collector and the control plane; `-version` on every
  binary.
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
- The collector flushes every session's reorder window on shutdown instead of discarding it.
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
- `tracectl`: an unknown command exits 2 (was 1), like other usage errors.

### Fixed
- Collector: an ingest or seal racing with shutdown could panic (send on a closed channel); it now gets `503`.
- Collector: a store call that timed out could get an event quarantined as "poison"; timeouts are retried instead,
  and background store calls are bounded (30s).
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
