# Module layout & go.mod hygiene

Modules are tied together by `go.work`; no `replace tsession => ../..` lines are used
(they were removed from `contrib/*`). `go build/vet/test` work from any module dir while
the workspace is active.

## Known limitation: bare root module path
The root module is named `tsession` (no dot in the first path element). Go rejects such
paths outside the standard library for anything that must resolve a module from the
network, so inside a module that depends on it **`go mod tidy` fails** with
`malformed module path "tsession": missing dot in first path element` (workspace mode
does not run tidy). Consequences: go.mod/go.sum of `collector` and `contrib/*` cannot be
re-tidied as-is, and the modules cannot be consumed from outside this workspace.

Workaround used to tidy (temporary replace, then strip it):

    cd contrib/tredis   # or any module
    printf '\nreplace tsession => ../..\n' >> go.mod   # collector: => ..
    GOWORK=off GOFLAGS=-mod=mod go mod tidy
    sed -i '' '/^replace tsession/d' go.mod            # GNU sed: drop the ''

Recommended permanent fix (NOT applied): rename the root module to a dotted path, e.g.
`tsession.local/tsession` (the `.local` path is never fetched; with a `replace` or go.work it
resolves locally and `go mod tidy` works). Verified on a scratch copy (build, vet, tests, and
tidy-with-replace all pass). BSD `sed` has no non-greedy/anchored multi-form support that is
reliable here, so the exact command uses perl (preinstalled on macOS/Linux):

    # 1) imports in Go files (only import-shaped lines, never string literals like "tsession")
    find . -type f -name '*.go' -print0 | xargs -0 perl -pi -e \
      's{^(\s*(?:[\w.]+\s+)?)"tsession(/[^"]*)?"\s*$}{$1"tsession.local/tsession$2"\n}'
    # 2) module + require lines in every go.mod
    find . -type f -name go.mod -print0 | xargs -0 perl -pi -e \
      's{^module tsession(/|$)}{module tsession.local/tsession$1}; s{^(\s*(?:require )?)tsession v0\.0\.0}{$1tsession.local/tsession v0.0.0}'

Then grep Dockerfiles/Makefile/docs for the old `tsession/` module paths, and run
`go build ./... && go vet ./... && go test ./...` in every module.

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
modules (`tsession/contrib/...`) may import it; nothing outside `tsession/...` can.
