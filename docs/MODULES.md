# Module layout & go.mod hygiene

Modules are tied together by `go.work`; no `replace tsession => ../..` lines are used
(they were removed from `contrib/*`). `go build/vet/test` work from any module dir while
the workspace is active.

## Module paths
The root module is `github.com/maheshpaliwal/agentic-test-session`; every other module lives under it
(`github.com/maheshpaliwal/agentic-test-session/collector`, `github.com/maheshpaliwal/agentic-test-session/contrib/tsql`, ...). Modules are tied together by `go.work` for local development.

Until the first tagged release, the intra-repo `require ... v0.0.0` lines resolve only through the workspace,
so `go mod tidy` inside a dependent module cannot fetch the root module. To tidy one:

    cd contrib/tredis   # or any module
    printf '\nreplace github.com/maheshpaliwal/agentic-test-session => ../..\n' >> go.mod   # collector: => ..
    GOWORK=off GOFLAGS=-mod=mod go mod tidy
    perl -ni -e 'print unless /^replace github\.com/maheshpaliwal/agentic\-test\-session =>/' go.mod

Release plan: tag the root (`v0.1.0`) and each submodule with its path prefix (`collector/v0.1.0`,
`contrib/tsql/v0.1.0`), then replace the `v0.0.0` requires with those versions.

## go directives
All modules use `go 1.25`, except `contrib/tmongo` (`go 1.25.0`, forced by mongo-driver v2.9.x)
and nothing requires 1.26 any more. `contrib/tredis` previously needed 1.26 because of
go-redis v9.23.0 (not miniredis); it is pinned to go-redis v9.14.1 + miniredis v2.35.0,
which build and pass `-race` on Go 1.25. Bumping go-redis past v9.2x requires `go 1.26`.

`contrib/tsql` is deliberately dependency-free: its `-tags integration` tests import pgx and
the MySQL driver, which `go mod tidy` would pull into go.mod; add them locally only when running
those tests (see the header of `integration_test.go`).

## Shared internals
`internal/redisx` (root module) holds the Redis-client-independent half of `contrib/tredis` (go-redis v9) and
`contrib/tredisv8` (go-redis v8): command classification, argument rendering and the `Redactor`. Each contrib
module keeps only its client-version-specific hook adapter. Go's `internal` rule is path based, so both contrib
modules (`github.com/maheshpaliwal/agentic-test-session/contrib/...`) may import it; nothing outside this repo's module paths can.
