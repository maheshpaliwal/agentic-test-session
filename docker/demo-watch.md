# tracectl watch-docker demo

`tracectl watch-docker` listens to `docker events` and, for every container that has the
label `tsession.id=<name>`, starts a session (`initiated_by=docker`, name = label value) on
container `start` and stops it on `die`.

1. Start the stack: `make up` (control plane on 127.0.0.1:7070).
2. Run the watcher on the host (it needs the docker CLI and the control plane URL):
   `go run ./cmd/tracectl --cp http://127.0.0.1:7070 watch-docker`
   (or `docker compose -f docker/docker-compose.yml run --rm tracectl` is NOT provided on purpose:
   mounting docker.sock into a container grants root on the host.)
3. Start a labelled container using the override file:
   `docker compose -f docker/docker-compose.yml -f docker/docker-compose.watch.yml --env-file docker/.env --profile watch up watched-job`
   or ad hoc: `docker run --rm --label tsession.id=adhoc alpine sleep 5`
4. Observe: `go run ./cmd/tracectl list` shows a session named `watch-demo` ACTIVE, then
   DRAINING/SEALED a few seconds after the container exits.

Label usage in your own compose files:
```yaml
services:
  myservice:
    labels:
      tsession.id: my-session-name
```
