#!/usr/bin/env bash
# Throwaway Postgres (127.0.0.1:55432) + Redis (127.0.0.1:56379) for the e2e tests.
#   it.sh start | stop | test | all
set -euo pipefail
IT_DIR="${IT_DIR:-${TMPDIR:-/tmp}/tsession-it}"
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@14/bin}"
REDIS_BIN="${REDIS_BIN:-/opt/homebrew/bin/redis-server}"
PG_PORT=55432; REDIS_PORT=56379
HERE="$(cd "$(dirname "$0")/.." && pwd)"
export TSESSION_IT_PG_DSN="postgres://$(whoami)@127.0.0.1:${PG_PORT}/postgres?sslmode=disable"
export TSESSION_IT_REDIS_ADDR="127.0.0.1:${REDIS_PORT}"

start() {
  mkdir -p "$IT_DIR"
  if [ ! -d "$IT_DIR/pgdata" ]; then
    "$PGBIN/initdb" -D "$IT_DIR/pgdata" -A trust -U "$(whoami)" >"$IT_DIR/initdb.log" 2>&1
  fi
  "$PGBIN/pg_ctl" -D "$IT_DIR/pgdata" -l "$IT_DIR/pg.log" -w \
    -o "-c listen_addresses=127.0.0.1 -p ${PG_PORT} -c unix_socket_directories='' -c fsync=off" start
  "$REDIS_BIN" --bind 127.0.0.1 --port "$REDIS_PORT" --save "" --appendonly no \
    --dir "$IT_DIR" --pidfile "$IT_DIR/redis.pid" --logfile "$IT_DIR/redis.log" --daemonize yes
  for _ in $(seq 50); do
    "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PG_PORT" -q && break; sleep 0.2
  done
  echo "pg=$TSESSION_IT_PG_DSN redis=$TSESSION_IT_REDIS_ADDR"
}

stop() {
  if [ -f "$IT_DIR/redis.pid" ]; then kill "$(cat "$IT_DIR/redis.pid")" 2>/dev/null || true; fi
  if [ -d "$IT_DIR/pgdata" ]; then "$PGBIN/pg_ctl" -D "$IT_DIR/pgdata" -m fast stop 2>/dev/null || true; fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  test) cd "$HERE" && go test -count=1 -v -timeout 300s ./... ;;
  all) trap stop EXIT; start; (cd "$HERE" && go test -count=1 -v -timeout 300s ./...) ;;
  *) echo "usage: $0 start|stop|test|all" >&2; exit 2 ;;
esac
