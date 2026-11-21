COMPOSE ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo docker-compose)
# Version stamped into the container binaries (the images are built without .git). A tag name is
# attacker-influenced text: it is reduced to a safe alphabet here and reaches the compose command
# through the environment, never by being pasted into a shell command line.
TSESSION_VERSION := $(shell git describe --tags --always --dirty 2>/dev/null | tr -cd 'A-Za-z0-9._+-')
export TSESSION_VERSION
DC = DOCKER_BUILDKIT=1 $(COMPOSE) -f docker/docker-compose.yml --env-file docker/.env
CP ?= http://127.0.0.1:7070

# Every module in go.work (root, collector, contrib/*, integration), resolved dynamically.
MODDIRS = $(shell go list -m -f '{{.Dir}}')
# Integration needs throwaway Postgres/Redis on 127.0.0.1:55432/56379 (scripts/it.sh); it runs separately.
UNITDIRS = $(filter-out %/integration,$(MODDIRS))

# staticcheck runs through `go run` so everyone (and CI) uses the same version.
STATICCHECK ?= go run honnef.co/go/tools/cmd/staticcheck@v0.8.2

.PHONY: build test unit race it vet lint fmt-check up down demo env
build:
	@set -e; for d in $(MODDIRS); do echo "== build $$d"; (cd $$d && go build ./...); done
unit:
	@fail=0; for d in $(UNITDIRS); do echo "== test $$d"; (cd $$d && go test ./...) || fail=1; done; exit $$fail
race:
	@fail=0; for d in $(UNITDIRS); do echo "== test -race $$d"; (cd $$d && go test -race ./...) || fail=1; done; exit $$fail
lint:
	@fail=0; for d in $(MODDIRS); do echo "== staticcheck $$d"; (cd $$d && $(STATICCHECK) ./...) || fail=1; done; exit $$fail
fmt-check:
	@out=$$(gofmt -l $(MODDIRS) 2>&1 | grep -v '\.pb\.go$$' | grep -v '/\.claude/'); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
it:
	cd integration && ./scripts/it.sh all
test:
	@$(MAKE) --no-print-directory unit; u=$$?; $(MAKE) --no-print-directory it; i=$$?; exit $$((u || i))
vet:
	@set -e; for d in $(MODDIRS); do echo "== vet $$d"; (cd $$d && go vet ./...); done
env:
	@test -f docker/.env || cp docker/.env.example docker/.env
up: env
	$(DC) up -d --build --wait
down:
	$(DC) --profile app --profile watch down -v
demo: up
	@id=$$(curl -fsS -X POST -H 'Content-Type: application/json' $(CP)/v1/sessions -d '{"name":"make-demo","mode":"header","initiated_by":"api"}' | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'); \
	echo "session $$id"; curl -fsS $(CP)/v1/active; echo; \
	curl -fsS -X POST $(CP)/v1/sessions/$$id/stop >/dev/null; echo "stopped; dashboard: http://127.0.0.1:7072"

# Regenerate proto/tsessionpb/event.pb.go (needs protoc and protoc-gen-go v1.36.9 on PATH).
.PHONY: proto
proto:
	cd proto && PATH="$$HOME/go/bin:$$PATH" protoc -I . --go_out=. --go_opt=module=github.com/maheshpaliwal/agentic-test-session/proto tsession/v1/event.proto
