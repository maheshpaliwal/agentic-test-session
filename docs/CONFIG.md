# Configuration reference

Every setting is an environment variable; the binaries also take the flag shown. A flag wins over its variable.
Run a binary with `-h` for the authoritative list.

## Instrumented service (SDK, read by `sdk/autoinit`)

| Variable | Default | Meaning |
|---|---|---|
| `TSESSION_ENABLED` | off | Only the exact value `true` starts the SDK. |
| `TSESSION_SERVICE` | empty | Service name on every event; set it. |
| `TSESSION_INSTANCE` | empty | Instance name (pod, host); distinguishes replicas. |
| `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` | Where the SDK follows the active sessions. |
| `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Where the SDK sends events. |
| `TSESSION_PROPAGATE_HOSTS` | all hosts | Comma-separated host suffixes the patched default HTTP transport sends session headers to. |
| `TSESSION_NO_PROPAGATE_HOSTS` | none | Host suffixes it never sends them to; wins over the list above. |
| `TSESSION_BIND_PRIORITY` | none | Order in which binding attributes are tried; see [`BINDING.md`](BINDING.md). |

The two propagation lists apply to `http.DefaultTransport` as patched by `autoinit`, not to `thttp.Client`.

## Collector (`collector/cmd/collector`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--listen` | `TSESSION_COLLECTOR_LISTEN` | `127.0.0.1:7071` | Listen address. |
| `--store` | `TSESSION_COLLECTOR_STORE` | `mem` | `mem`, `pg`, `mongo` or `dynamodb`. |
| `--pg-dsn` | `TSESSION_COLLECTOR_PG_DSN` | | Postgres connection string (`pg`). |
| `--pg-schema` | `TSESSION_COLLECTOR_PG_SCHEMA` | see `-h` | Schema of the trace tables (`pg`). |
| `--mongo-uri` | `TSESSION_COLLECTOR_MONGO_URI` | | MongoDB URI (`mongo`). |
| `--mongo-db` | `TSESSION_COLLECTOR_MONGO_DB` | `tsession` | Database (`mongo`). |
| `--dynamo-table` | `TSESSION_COLLECTOR_DYNAMO_TABLE` | see `-h` | Table (`dynamodb`). |
| `--dynamo-region` | `TSESSION_COLLECTOR_DYNAMO_REGION` | from the AWS environment | Region (`dynamodb`). |
| `--dynamo-endpoint` | `TSESSION_COLLECTOR_DYNAMO_ENDPOINT` | | Endpoint override, for an emulator (`dynamodb`). |
| `--dynamo-create-table` | `TSESSION_COLLECTOR_DYNAMO_CREATE_TABLE` | off (`true` enables) | Create the table if missing. |
| `--window` | `TSESSION_COLLECTOR_WINDOW` | `500ms` | Reorder window per event; `0` sequences immediately. |
| `--retention` | `TSESSION_COLLECTOR_RETENTION` | `0` (keep forever) | Delete a session this long after its seal, or after its last event if it never sealed. |
| `--bundle-dir` | `TSESSION_COLLECTOR_BUNDLE_DIR` | `./bundles` | Directory for sealed bundles; empty disables. Holds recorded payloads. |
| `--idle-evict` | `TSESSION_COLLECTOR_IDLE_EVICT` | `5m` | Release a session's in-memory state after this idle time. |
| `--max-message-bytes` | `TSESSION_COLLECTOR_MAX_MESSAGE_BYTES` | 4 MiB | Largest single event accepted. |
| `--allowed-hosts` | `TSESSION_COLLECTOR_ALLOWED_HOSTS` | loopback only | Extra Host values accepted, comma-separated. |
| `--allowed-origins` | `TSESSION_COLLECTOR_ALLOWED_ORIGINS` | none | Browser origins allowed to call the API. |
| `--internal-allow` | `TSESSION_COLLECTOR_INTERNAL_ALLOW` | loopback only | CIDRs or IPs allowed to call `/v1/internal/*` (the control plane). |

Prefer the variables for connection strings: flag values show up in the process list.

## Control plane (`cmd/controlplane`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--addr` | `TSESSION_CP_ADDR` | `127.0.0.1:7070` | Listen address. |
| `--collector-url` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Collector it drains and seals through. |
| `--store-file` | `TSESSION_CP_STORE` | none (memory only) | File for session persistence; also writes `FILE.journal` and `FILE.archive`. |
| `--retention` | `TSESSION_CP_RETENTION` | `24h` | Keep finished sessions in memory this long; without `--store-file` they are then forgotten. |
| `--default-ttl` | `TSESSION_CP_DEFAULT_TTL` | `3600` | Default session time limit, in seconds. |
| `--allowed-hosts` | `TSESSION_CP_ALLOWED_HOSTS` | loopback only | Extra Host values accepted. |
| `--allowed-origins` | `TSESSION_CP_ALLOWED_ORIGINS` | none | Browser origins allowed to call the API. |

## Dashboard (`cmd/dashboard`)

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `-listen` | `TSESSION_DASHBOARD_ADDR` | `127.0.0.1:7072` | Listen address; wildcard addresses are refused. |
| `-control-plane` | `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` | Control plane it proxies. |
| `-collector` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` | Collector it proxies. |
| `-demo` | `TSESSION_DASHBOARD_DEMO` | off (`true` enables) | Serve built-in synthetic data. |
| `-open FILE` | | | Serve an exported session read-only, without other services; repeatable; not with `-demo`. |
| `-allowed-hosts` | `TSESSION_DASHBOARD_ALLOWED_HOSTS` | loopback only | Extra Host values accepted. |

## tracectl and the Go client

| Flag | Variable | Default |
|---|---|---|
| `--cp` | `TSESSION_CONTROLPLANE_URL` | `http://127.0.0.1:7070` |
| `--collector` | `TSESSION_COLLECTOR_URL` | `http://127.0.0.1:7071` |

`TSESSION_ARTIFACT_DIR` makes `sdk/client/tsessiontest` save the session of a failed test into that directory;
use an absolute path.
