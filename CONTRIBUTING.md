# Contributing

## Setup
Go 1.25+ (the workspace pins 1.26). Clone, then `make build vet unit`. `make it` runs the integration suite and
needs throwaway Postgres and Redis (`integration/scripts/it.sh`).

## Ground rules
- A change stays inside one module's `go.mod`. The root module is stdlib-only; heavy dependencies belong in
  `collector/` or a `contrib/*` module.
- Instrumentation must be **fail-open**: bounded work, no panics, no behaviour change for the host call.
- Redaction happens before data leaves the process. Add a test for any new place that records payloads.
- Wire changes are additive and documented in `docs/PROTOCOL.md`.
- Servers bind loopback by default; do not add defaults that listen on all interfaces.
- Keep Redis-client-independent logic in `internal/redisx`, not duplicated across `tredis` and `tredisv8`.
- Use synthetic data in tests and examples: no real credentials, hostnames, customer data or employer-specific names.

## Pull requests
Run `make vet unit` and `gofmt -l .` first. Describe the behaviour change and how you tested it.
