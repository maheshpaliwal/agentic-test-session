COMPOSE ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo docker-compose)
DC = DOCKER_BUILDKIT=1 $(COMPOSE) -f docker/docker-compose.yml --env-file docker/.env
CP ?= http://127.0.0.1:7070

# Every module in go.work (root, collector, contrib/*, integration), resolved dynamically.
MODDIRS = $(shell go list -m -f '{{.Dir}}')
# Integration needs throwaway Postgres/Redis on 127.0.0.1:55432/56379 (scripts/it.sh); it runs separately.
UNITDIRS = $(filter-out %/integration,$(MODDIRS))

.PHONY: build test unit it vet up down demo env
build:
	@set -e; for d in $(MODDIRS); do echo "== build $$d"; (cd $$d && go build ./...); done
unit:
	@fail=0; for d in $(UNITDIRS); do echo "== test $$d"; (cd $$d && go test ./...) || fail=1; done; exit $$fail
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
	@id=$$(curl -fsS -X POST $(CP)/v1/sessions -d '{"name":"make-demo","mode":"header","initiated_by":"api"}' | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'); \
	echo "session $$id"; curl -fsS $(CP)/v1/active; echo; \
	curl -fsS -X POST $(CP)/v1/sessions/$$id/stop >/dev/null; echo "stopped; dashboard: http://127.0.0.1:7072"

# Regenerate proto/tsessionpb/event.pb.go (needs protoc and protoc-gen-go v1.36.9 on PATH).
.PHONY: proto
proto:
	cd proto && PATH="$$HOME/go/bin:$$PATH" protoc -I . --go_out=. --go_opt=module=github.com/maheshpaliwal/agentic-test-session/proto tsession/v1/event.proto
