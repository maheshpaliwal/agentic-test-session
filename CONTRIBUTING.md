# Contributing

## Setup
Go 1.25+ (the workspace pins 1.26). Clone, then `make build vet unit`. `make it` runs the integration suite and
starts its own throwaway Postgres and Redis from local binaries (`integration/scripts/it.sh`; set `PGBIN` /
`REDIS_BIN` if they are not at the Homebrew default paths).

## Ground rules
- A change stays inside one module's `go.mod`. The root module is stdlib-only; heavy dependencies belong in
  `collector/` or a `contrib/*` module.
- Instrumentation must be **fail-open**: bounded work, no panics, no behaviour change for the host call.
- Redaction happens before data leaves the process. Add a test for any new place that records payloads. A payload
  with no structural redactor (multipart, XML, binary) is recorded as a placeholder, never verbatim.
- Exported identifiers carry a doc comment; a new `store.Store` method goes into the shared conformance suite
  (`collector/store/storetest`).
- Wire changes are additive and documented in `docs/PROTOCOL.md`.
- Servers bind loopback by default; do not add defaults that listen on all interfaces.
- Keep Redis-client-independent logic in `internal/redisx`, not duplicated across `tredis` and `tredisv8`.
- Binaries share plumbing from `internal/svc` (env defaults, CSV lists, graceful `Serve`); wrappers guard recording code with `defer core.Recover()`.
- Use synthetic data in tests and examples: no real credentials, hostnames, customer data or employer-specific names.

## Pull requests
Run `make vet lint race` and `gofmt -l .` first (CI runs `make fmt-check vet lint race`). Describe the behaviour change and how you tested it.
