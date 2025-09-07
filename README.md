English | [简体中文](README.zh.md)

# volkit

![CI](https://img.shields.io/github/actions/workflow/status/swarmnative/volkit/publish.yml?branch=main)
![Release](https://img.shields.io/github/v/release/swarmnative/volkit)
![License](https://img.shields.io/github/license/swarmnative/volkit)
![Docker Pulls](https://img.shields.io/docker/pulls/swarmnative/volkit)
![Image Size](https://img.shields.io/docker/image-size/swarmnative/volkit/latest)
![Go Version](https://img.shields.io/github/go-mod/go-version/swarmnative/volkit)
![Last Commit](https://img.shields.io/github/last-commit/swarmnative/volkit)
![Issues](https://img.shields.io/github/issues/swarmnative/volkit)
![PRs](https://img.shields.io/github/issues-pr/swarmnative/volkit)

Cluster S3 volume controller for single Docker and Docker Swarm. Powered by rclone (MIT). Not affiliated with or endorsed by rclone.

---

## Features
- Host-level rclone FUSE mount (apps bind-mount from host path)
- Optional in-cluster HAProxy for load-balancing & failover
- Declarative "volumes" via labels (S3 bucket/prefix), K8s-like experience
- Self-healing: lazy-unmount & recreate if mount becomes unhealthy
- Optional node-local LB alias: `volkit-lb-<hostname>` for nearest access

---

## Quick Start (minimal stack)
Prereqs: Swarm initialized; FUSE enabled; label nodes that should mount.
```bash
docker node update --label-add mount_s3=true <NODE>
```
Create credentials (Swarm secrets):
```bash
docker secret create s3_access_key -
# paste AccessKey then Ctrl-D

docker secret create s3_secret_key -
# paste SecretKey then Ctrl-D
```
Deploy (single backend service, assume an existing S3-compatible service `s3-backend`; mounter reaches `tasks.s3-backend:9000` via built-in HAProxy):
```yaml
version: "3.8"

networks: { s3_net: { driver: overlay, internal: true } }

secrets:
  s3_access_key: { external: true }
  s3_secret_key: { external: true }

services:
  volkit:
    image: ghcr.io/swarmnative/volkit:latest
    networks: [s3_net]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - type: bind
        source: /mnt/s3
        target: /mnt/s3
        bind: { propagation: rshared }
    secrets: [s3_access_key, s3_secret_key]
    environment:
      - VOLKIT_PROXY_ENABLE=true
      - VOLKIT_PROXY_ENGINE=haproxy
      - VOLKIT_PROXY_LOCAL_SERVICES=s3-backend
      - VOLKIT_PROXY_BACKEND_PORT=9000
      - VOLKIT_PROXY_HEALTH_PATH=/health
      - VOLKIT_PROVIDER=
      - VOLKIT_RCLONE_REMOTE=S3:mybucket
      - VOLKIT_MOUNTPOINT=/mnt/s3
      - VOLKIT_ACCESS_KEY_FILE=/run/secrets/s3_access_key
      - VOLKIT_SECRET_KEY_FILE=/run/secrets/s3_secret_key
      - VOLKIT_RCLONE_ARGS=--vfs-cache-mode=writes --dir-cache-time=12h
      - VOLKIT_UNMOUNT_ON_EXIT=true
      - VOLKIT_AUTOCREATE_BUCKET=false
      - VOLKIT_AUTOCREATE_PREFIX=true
    deploy:
      mode: global
      placement: { constraints: [node.labels.mount_s3 == true] }
      restart_policy: { condition: any }
```
Use a named volume from application containers (created by volkit):
```yaml
volumes:
  volkit-teams-appA-vol-data: { external: true }

services:
  app:
    image: your/app:latest
    volumes:
      - type: volume
        source: volkit-teams-appA-vol-data
        target: /data
    deploy:
      placement: { constraints: [node.labels.mount_s3 == true] }
```

---

## Access modes (root / non-root)
Pick one based on your security and visibility needs:

- Mode A: Non-root apps can access (requires host `user_allow_other`)
  - Host (one-time):
    - `echo user_allow_other | sudo tee /etc/fuse.conf`
    - `nsenter -t 1 -m -- mount --make-rshared /mnt/s3`
  - volkit (example args — adjust uid/gid/umask to your app user):
```yaml
environment:
  VOLKIT_RCLONE_ARGS: >-
    --allow-other
    --uid=1000
    --gid=1000
    --umask=0022
    --vfs-cache-mode=writes
    --vfs-cache-max-size=256M
    --vfs-cache-max-age=12h
    --vfs-cache-poll-interval=1m
    --vfs-read-ahead=0
    --buffer-size=1M
    --dir-cache-time=1h
    --attr-timeout=1s
    --transfers=2
    --checkers=2
    --multi-thread-streams=0
    --s3-chunk-size=5M
    --s3-upload-concurrency=1
    --s3-max-upload-parts=10000
    --s3-force-path-style=true
    --use-server-modtime
    --no-update-modtime
```
  - App container:
```yaml
services:
  app:
    image: your/app:latest
    user: "1000:1000"
    volumes:
      - type: bind
        source: /mnt/s3
        target: /data
        bind: { propagation: rshared }
```

- Mode B: No host change (root-only access, simplest)
  - volkit:
```yaml
environment:
  VOLKIT_RCLONE_ARGS: >-
    --allow-root
    --uid=0
    --gid=0
    --umask=0022
    --vfs-cache-mode=writes
```
  - App container (hardened root):
```yaml
services:
  app:
    image: your/app:latest
    user: "0:0"
    volumes:
      - type: bind
        source: /mnt/s3
        target: /data
        bind: { propagation: rshared }
    security_opt: ["no-new-privileges:true"]
    cap_drop: ["ALL"]
```

Note: In both modes the mountpoint `/mnt/s3` must be `shared` (controller attempts to set it; if needed run the nsenter command above once per node).

---

## Configuration (env vars)

### Basic
| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_ENDPOINT` | url | yes | - | S3 endpoint (e.g. http://s3.local:9000) |
| `VOLKIT_PROVIDER` | string | no | empty | S3 provider hint: `Minio`/`AWS` |
| `VOLKIT_RCLONE_REMOTE` | string | yes | `S3:bucket` | rclone remote (e.g. `S3:bucket`) |
| `VOLKIT_MOUNTPOINT` | path | yes | `/mnt/s3` | Host mountpoint |
| `VOLKIT_ACCESS_KEY_FILE` | path | yes | `/run/secrets/s3_access_key` | AccessKey secret file |
| `VOLKIT_SECRET_KEY_FILE` | path | yes | `/run/secrets/s3_secret_key` | SecretKey secret file |
| `VOLKIT_RCLONE_ARGS` | string | no | empty | Extra rclone args |

### Unified Issuer (credentials provisioning)
Controller prefers short‑lived creds and falls back to static AK/SK automatically. Credentials are injected to the mounter via a tmpfs file; rclone runs with `--config /dev/null` (no on‑disk config).

| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_ISSUER_MODE` | enum | no | auto | `external` | `s3_admin` | `static` (auto: if endpoint set → `external`, else `static`) |
| `VOLKIT_ISSUER_ENDPOINT` | url | when external/s3_admin | empty | Issuer service or MinIO STS/Admin endpoint (can use controller’s HAProxy) |
| `VOLKIT_ISSUER_ACCESS_KEY` | string | when s3_admin | empty | Root/issuer AK (MinIO admin) |
| `VOLKIT_ISSUER_SECRET_KEY` | string | when s3_admin | empty | Root/issuer SK |
| `VOLKIT_ISSUER_TTL_SEC` | int | no | `3600` | Desired lifetime for short‑lived creds (STS/SA); typical 900–3600 |
| `VOLKIT_ALLOW_ROOT_ISSUER` | bool | when s3_admin | `false` | Explicit opt‑in to use root as issuing principal |
| `VOLKIT_S3_ADMIN_FORCE_STATIC` | bool | no | `false` | Force static AK/SK (bypass STS/SA) — not recommended |
| `VOLKIT_SA_ENABLE` | bool | no | `true` | Enable Service Account fallback (MinIO `mc`) when STS is unavailable |
| `VOLKIT_MC_IMAGE` | string | no | `minio/mc:latest` | `mc` image for SA create/prune (run as short‑lived helper) |
| `VOLKIT_ISSUER_INSECURE` | bool | no | `false` | Allow http/self‑signed in trusted overlay; prefer https/mTLS otherwise |
| TLS (`VOLKIT_TLS_CLIENT_CERT_FILE`, `VOLKIT_TLS_CLIENT_KEY_FILE`, `VOLKIT_TLS_SERVER_CA_FILE`) | paths | no | empty | Optional mTLS for issuer calls |

Fallback (static; fully compatible):
- `VOLKIT_ACCESS_KEY` / `VOLKIT_SECRET_KEY` (optional `VOLKIT_SESSION_TOKEN`)

STS/SA priority (s3_admin mode):
- Try STS (issuer=root) → if not available, try MinIO Service Account (mc) with expiry → finally static. `/creds` returns `expiresAt` and `issuer` (`s3_admin_sts` | `s3_admin_sa` | `s3_admin_static`).

### Named Volumes (experimental)
- Auto‑create: when the control object `.volkit/.ready` is present under a declared prefix and the host mount is writable, the agent creates a Docker local volume (driver `local` with bind) named `volkit-<prefix>`.
- Labels: volumes are labeled `volkit.managed=true` and `volkit.prefix=<prefix>`.
- Reclaim: enable `VOLKIT_VOLUME_RECLAIM=true` to remove stale `volkit-*` volumes; only those with managed label and whose device path is beneath the mount root are eligible (safer reclaim). Drift (unexpected device root) is detected and counted.
- Usage in services:
```yaml
services:
  app:
    volumes:
      - type: volume
        source: volkit-teams-appA-vol-data
        target: /data
```

### Readiness & placement (multi‑level)
- Controller aggregates per‑node ready prefixes reported by agent and writes `node.labels.volkit.ready.<level>=true`.
- Services can constrain placement: `node.labels.volkit.ready.<level>==true`.
- Migrations:
  - On losing readiness, controller triggers `ServiceUpdate` with `ForceUpdate++` (cooldown guarded).
  - On readiness regain, ForceUpdate is optional: global `VOLKIT_READY_MIGRATE_ON_REGAIN` or per‑service label `volkit.forceUpdateOnRegain=true|false`.
- Cooldown: `VOLKIT_READY_MIGRATE_COOLDOWN_SEC` (default 30).

### Security & resources (mounter)
| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `VOLKIT_MOUNTER_READONLY_ROOTFS` | bool | `true` | Read‑only rootfs for mounter |
| `VOLKIT_MOUNTER_NO_NEW_PRIVILEGES` | bool | `true` | Add `no-new-privileges:true` |
| `VOLKIT_MOUNTER_CAPS_ALLOW` | csv | `SYS_ADMIN` | Allowed capabilities list (others dropped) |
| `VOLKIT_SECCOMP_PROFILE` | string | empty | Optional seccomp profile |
| `VOLKIT_APPARMOR_PROFILE` | string | empty | Optional apparmor profile |
| `VOLKIT_MOUNTER_TMPFS_SIZE` | string | `16m` | /tmp tmpfs size |
| `VOLKIT_MOUNTER_PIDS_LIMIT` | int | - | PIDs limit |
| `VOLKIT_MOUNTER_MEMORY_BYTES` | int | - | Memory limit (bytes) |
| `VOLKIT_MOUNTER_NANO_CPUS` | int | - | CPU limit (NanoCPUs) |

### Backoff & polling
| Variable | Default | Description |
| --- | --- | --- |
| `VOLKIT_CREDS_BACKOFF_SEC` | 30 | Backoff seconds when `/creds` fails (per level) |
| `VOLKIT_POLL_INTERVAL` | 15s | Reconcile loop interval |
| `VOLKIT_CREDS_MIN_SEC` | 60 | Agent `/creds` poll floor (ETag‑aware) |

### Service Account pruning (MinIO)
| Variable | Default | Description |
| --- | --- | --- |
| `VOLKIT_SA_PRUNE` | false | Enable periodic pruning of expired Service Accounts created by volkit |
| `VOLKIT_SA_PRUNE_GRACE_SEC` | 600 | Delete only when expired for at least this grace |
| `VOLKIT_SA_PREFIX` | `volkit-` | Name prefix used when minting SA (for identification) |

### Metrics (extended)
`/metrics` includes (non‑exhaustive):
- Reconcile/mounter/mount health counters and timings
- Readiness: `volkit_ready_nodes{level="<lvl>"}`
- Volumes: `volkit_volume_created_total`, `volkit_volume_reclaimed_total`, `volkit_volume_drift_total`
- Credentials: `volkit_creds_fetch_ok_total`, `volkit_creds_fetch_errors_total`, `volkit_creds_backoff_total`
- Mounter backoff: `volkit_mounter_backoff_total`
- Service unblock: `volkit_services_unblocked_total`
- SA pruning: `volkit_sa_pruned_total`, `volkit_sa_prune_errors_total`
Aux endpoint: `/metrics/ready` emits only readiness counts.

### Access modes
| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_ALLOW_OTHER` | bool | no | `false` | If `true`, controller defaults to `--allow-other` (host must enable `/etc/fuse.conf` `user_allow_other`); if `false`, defaults to `--allow-root`. Your `VOLKIT_RCLONE_ARGS` can still override. |

### HAProxy / Node-local LB
| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_PROXY_ENABLE` | bool | no | `false` | Enable built-in reverse proxy |
| `VOLKIT_PROXY_ENGINE` | string | no | `haproxy` | Proxy engine |
| `VOLKIT_PROXY_LOCAL_SERVICES` | csv | when enabled | `minio-local` | Local backend service names (comma-separated) |
| `VOLKIT_PROXY_REMOTE_SERVICE` | string | no | `minio-remote` | Optional remote service name |
| `VOLKIT_PROXY_BACKEND_PORT` | int | when enabled | `9000` | Backend port |
| `VOLKIT_PROXY_HEALTH_PATH` | string | no | `/minio/health/ready` | Health check path |
| `VOLKIT_PROXY_LOCAL_LB` | bool | no | `false` | Per-node alias mode |
| `VOLKIT_PROXY_NETWORK` | string | when local LB | empty | Overlay network (attachable) |
| `VOLKIT_PROXY_PORT` | int | when enabled | `8081` | HAProxy listen port |
| `VOLKIT_PROXY_AUTO` | bool | no | `false` | Enable agent-side probing and scoring |
| `VOLKIT_PROXY_AUTO_ALLOW_COORDINATOR` | bool | no | `false` | Allow coordinator to run scoring too |

### rclone image/update
| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_DEFAULT_RCLONE_IMAGE` | string | no | `rclone/rclone:latest` | Default rclone image baked in |
| `VOLKIT_RCLONE_IMAGE` | string | no | inherits default | Override rclone image at runtime |
| `VOLKIT_RCLONE_UPDATE_MODE` | enum | no | `on_change` | `never`/`on_change` |

### Cleanup & autocreation
| Variable | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `VOLKIT_UNMOUNT_ON_EXIT` | bool | no | `true` | Lazy unmount & remove mounter on exit |
| `VOLKIT_AUTOCREATE_BUCKET` | bool | no | `false` | Autocreate bucket (if backend supports) |
| `VOLKIT_AUTOCREATE_PREFIX` | bool | no | `true` | Autocreate prefix (directory) |
| `VOLKIT_READ_ONLY` | bool | no | `false` | Enforce read-only (skips remote mkdir) |

---

## Deployment Modes
- Single backend: set `VOLKIT_PROXY_LOCAL_SERVICES=minio`, mounter reaches `tasks.minio:9000`
- Multiple services (per node): comma-separated `VOLKIT_PROXY_LOCAL_SERVICES=minio1,minio2,...`
- Node-local LB: enable `VOLKIT_PROXY_LOCAL_LB=true` and set `VOLKIT_PROXY_NETWORK`; rclone uses `volkit-lb-<hostname>`

---

## Operations
- HTTP:
  - `/ready` readiness (write-probe or RO-aware when `VOLKIT_READ_ONLY=true`)
  - `/healthz` liveness
  - `/status` JSON snapshot
  - `/validate` config validation (JSON)
  - `/metrics` Prometheus (enable `VOLKIT_ENABLE_METRICS=true`)
- Logs: JSON `slog`; configurable `VOLKIT_LOG_LEVEL=debug|info|warn|error`

### HTTP server & middleware (baseline)
- Timeouts: `ReadHeaderTimeout=5s`, `ReadTimeout=15s`, `WriteTimeout=15s`, `IdleTimeout=60s`, `MaxHeaderBytes=1MiB`.
- Middleware:
  - Request ID: every response contains `X-Request-Id` (propagates inbound value or generates one).
  - Panic recovery: any handler panic returns `500 {"error":"internal server error"}` and is logged.
  - Gzip for JSON responses with `Accept-Encoding: gzip`.
- Rate limiting (selected endpoints): on 429 includes `Retry-After: 1` to guide client backoff.
- Routing is centralized in `internal/httpserver.BuildMux`.

### WebSocket baseline
- Per-message write timeout (default `VOLKIT_WS_WRITE_TIMEOUT_SEC=5`).
- Optional per-node connection cap (`VOLKIT_WS_MAX_PER_NODE`, default 2).

---

## Security & Best Practices
- Least‑privileged S3 credentials; rotate periodically
- Controller is stateless w.r.t. secrets; it fetches/derives creds at runtime and never writes them to disk
- Mounter receives creds via a tmpfs file and runs with `--config /dev/null` (no rclone.conf on disk)
- Prefer named volumes (local+bind) with `external: true` over direct binds; precreate on target nodes to avoid accidental empty local volumes
- Container: non‑root, read‑only rootfs, `no-new-privileges:true`, drop `NET_RAW`
- Docker API: consider docker-socket-proxy with minimal endpoints

---

## FAQ
- Should MinIO start first?
  - Recommended yes. Controller retries until backend is available.
- Will `tasks.<service>` connect to other nodes’ proxies?
  - It resolves backend service replicas. For node-local LB, use `volkit-lb-<hostname>`.

---

## Security Baseline (production)
- Prefer HTTPS with optional mTLS; avoid enabling `VOLKIT_HTTP_ENABLE` in production.
- Keep `AUTH_DISABLE=false`. Even with it true, sensitive endpoints now require TLS.
- Provide least‑privileged S3 credentials; rotate periodically. Consider STS/SA short‑lived creds.
- Run with read‑only rootfs, `no-new-privileges:true`, and minimal caps for mounter (only `SYS_ADMIN`).
- Consider docker-socket-proxy and scoped permissions to Docker API.
- Monitor `/metrics` for issuer attempts/success/failures, WS backpressure drops, reconcile errors.

### Startup self-checks
On startup, process exits with error if critical env constraints are violated:
- `VOLKIT_MODE=agent` requires `VOLKIT_COORDINATOR_URL`.
- Ranges: `VOLKIT_WS_WRITE_TIMEOUT_SEC` in [1,120]; `VOLKIT_RL_SENSITIVE_CAP` and `VOLKIT_RL_SENSITIVE_RPS` in [1,10000].
- Warnings: `VOLKIT_ALLOW_ROOT_ISSUER=true` has no effect when `VOLKIT_ISSUER_MODE=static`.

---

## License
MIT (see `LICENSE`). See `THIRD_PARTY_NOTICES.md` for third-party attributions and licenses (rclone, HAProxy, Alpine, etc.).

## Contributing
PRs/Issues welcome (see `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`).

## Example: S3-compatible cluster with node-local HAProxy (nearest + LB + failover)
Node-local LB with HAProxy is recommended for Swarm: each node’s rclone connects to its own local proxy alias for lowest latency, while HAProxy performs health checks and failover. Strategy: `leastconn`.

Assumptions:
- You already have an S3-compatible cluster reachable via multiple Swarm services (e.g., `s3node1,s3node2,...`) on port 9000
- Each service exposes an HTTP readiness endpoint (e.g., `/health`)

```yaml
version: "3.8"

networks:
  s3_net:
    driver: overlay
    attachable: true

secrets:
  s3_access_key: { external: true }
  s3_secret_key: { external: true }

services:
  volkit:
    image: ghcr.io/swarmnative/volkit:latest
    networks: [s3_net]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - type: bind
        source: /mnt/s3
        target: /mnt/s3
        bind: { propagation: rshared }
    secrets: [s3_access_key, s3_secret_key]
    environment:
      # Proxy: multi-service backends, node-local alias (nearest + failover)
      - VOLKIT_PROXY_ENABLE=true
      - VOLKIT_PROXY_ENGINE=haproxy
      - VOLKIT_PROXY_LOCAL_SERVICES=s3node1,s3node2,s3node3,s3node4
      - VOLKIT_PROXY_BACKEND_PORT=9000
      - VOLKIT_PROXY_HEALTH_PATH=/health
      - VOLKIT_PROXY_LOCAL_LB=true
      - VOLKIT_PROXY_NETWORK=s3_net
      - VOLKIT_PROXY_PORT=8081
      # S3 and rclone
      - VOLKIT_RCLONE_REMOTE=S3:team-bucket
      - VOLKIT_MOUNTPOINT=/mnt/s3
      - VOLKIT_ACCESS_KEY_FILE=/run/secrets/s3_access_key
      - VOLKIT_SECRET_KEY_FILE=/run/secrets/s3_secret_key
      # Tuning: many small files & deep dirs (balanced cache/mem)
      - VOLKIT_RCLONE_ARGS=--vfs-cache-mode=full --vfs-cache-max-size=4G --vfs-cache-max-age=48h --dir-cache-time=24h --attr-timeout=2s --buffer-size=8M --s3-chunk-size=8M --s3-upload-concurrency=4 --s3-max-upload-parts=10000
      - VOLKIT_UNMOUNT_ON_EXIT=true
      - VOLKIT_AUTOCREATE_PREFIX=true
    deploy:
      mode: global
      placement: { constraints: [node.labels.mount_s3 == true] }
      restart_policy: { condition: any }
```

Notes:
- `--vfs-cache-mode=full` improves read patterns for many small files; adjust cache size/age based on disk space.
- For S3-compatible endpoints requiring path-style addressing, add `--s3-force-path-style=true`. For AWS, also add `--s3-region=<region>`.
