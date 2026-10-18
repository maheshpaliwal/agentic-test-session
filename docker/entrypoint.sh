#!/bin/sh
# Resolve this container's own (compose-network) IP and bind the service to it.
# The services keep their loopback-only defaults; only inside the container's private
# network namespace do we pass an explicit, specific interface address (never 0.0.0.0/::).
# Host exposure is controlled solely by the 127.0.0.1-bound port mappings in compose.
set -eu
ip=$(hostname -i | awk '{print $1}')
export TSESSION_SELF_IP="$ip"
case "$TSESSION_BIN" in
  controlplane) export TSESSION_CP_ADDR="$ip:${TSESSION_PORT:-7070}" ;;
  collector)    export TSESSION_COLLECTOR_LISTEN="$ip:${TSESSION_PORT:-7071}" ;;
  dashboard)    export TSESSION_DASHBOARD_ADDR="$ip:${TSESSION_PORT:-7072}" ;;
  exampleapp)   export EXAMPLEAPP_LISTEN="$ip:${TSESSION_PORT:-8080}" ;;
esac
exec "/usr/local/bin/$TSESSION_BIN" "$@"
