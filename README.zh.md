[English](README.md) | 简体中文

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

为单机 Docker 与 Docker Swarm 提供 S3 兼容对象存储“卷”的轻量控制器（Powered by rclone, MIT；与 rclone 无从属或背书关系）：
- 宿主机级 rclone FUSE 挂载（业务容器通过 bind 使用）。
- 内置 HAProxy（可选）用于负载均衡与故障转移。
- 声明式“卷”（前缀）供给，接近 K8s 体验。

---

## 快速开始（最小 Stack）
前提：已初始化 Swarm；目标节点开启 FUSE；为使用挂载的节点打标签。
```bash
docker node update --label-add mount_s3=true <NODE>
```
创建凭据（Swarm secrets）：
```bash
docker secret create s3_access_key -
# 粘贴 AccessKey 回车，Ctrl-D 结束

docker secret create s3_secret_key -
# 粘贴 SecretKey 回车，Ctrl-D 结束
```
部署（单后端 Service 示例，假设已存在可达的 S3 兼容服务 `s3-backend`，mounter 通过内置 HAProxy 均衡到 `tasks.s3-backend:9000`）：
```yaml
version: "3.8"

networks:
  s3_net:
    driver: overlay
    internal: true

secrets:
  s3_access_key:
    external: true
  s3_secret_key:
    external: true

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
      placement:
        constraints:
          - node.labels.mount_s3 == true
      restart_policy: { condition: any }
```
业务容器使用挂载：
```yaml
services:
  app:
    image: your/app:latest
    volumes:
      - type: bind
        source: /mnt/s3
        target: /data
        bind: { propagation: rshared }
    deploy:
      placement:
        constraints: [node.labels.mount_s3 == true]
```

---

## 访问模式（root / 非 root）
根据安全与可见性诉求选择其一：

- 模式 A：非 root 业务容器可访问（使用官方 rclone 镜像需宿主开启 user_allow_other；使用 `volkit-rclone` 可跳过宿主机步骤）
  - 宿主一次性：
    - 使用 `volkit-rclone` 可省略；若使用官方 rclone 镜像，则执行：`echo user_allow_other | sudo tee /etc/fuse.conf`
    - `nsenter -t 1 -m -- mount --make-rshared /mnt/s3`
  - volkit：
    - `VOLKIT_RCLONE_ARGS` 加 `--allow-other --uid=1000 --gid=1000 --umask=0022`（按业务 UID/GID 调整）
  - 业务容器：
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

- 模式 B：不改宿主（仅 root 可访问，最省事）
  - volkit：
    - `VOLKIT_RCLONE_ARGS` 用 `--allow-root --uid=0 --gid=0 --umask=0022`
  - 业务容器：
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
    security_opt: [no-new-privileges:true]
    cap_drop: ["ALL"]
```

提示：两种模式均需确保 `/mnt/s3` 为 shared（已内置自愈尝试，必要时手工执行上面的 nsenter 命令）。

---

## 配置（环境变量）

### 基本
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_ENDPOINT` | S3 端点（如 https://s3.local:9000） | 必填 |
| `VOLKIT_PROVIDER` | 可选，通用 S3 留空；或 `Minio`/`AWS` | 空 |
| `VOLKIT_RCLONE_REMOTE` | rclone 远端（如 `S3:bucket`） | `S3:bucket` |
| `VOLKIT_MOUNTPOINT` | 宿主机挂载点 | `/mnt/s3` |
| `VOLKIT_ACCESS_KEY_FILE` | AccessKey 的 secret 路径 | `/run/secrets/s3_access_key` |
| `VOLKIT_SECRET_KEY_FILE` | SecretKey 的 secret 路径 | `/run/secrets/s3_secret_key` |
| `VOLKIT_RCLONE_ARGS` | 追加 rclone 参数（唯一调优入口） | 空 |

### 统一发证（Issuer）
控制面优先签发短期凭据（STS/Service Account），失败自动回退静态 AK/SK。凭据以临时文件（宿主机 tmpfs）注入 mounter，rclone 以 `--config /dev/null` 运行（不落盘 rclone.conf）。

| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_ISSUER_MODE` | `external`（外部发证服务）/`s3_admin`（直连 MinIO Admin/STS/SA）/`static`；为空时自动：有 `VOLKIT_ISSUER_ENDPOINT` 则 `external`，否则 `static` | 自动 |
| `VOLKIT_ISSUER_ENDPOINT` | 发证服务或 STS/Admin 端点（可指向 controller 内置 HAProxy） | 空 |
| `VOLKIT_ISSUER_ACCESS_KEY` | 发证账号 AK（s3_admin 时；通常为 MinIO root） | 空 |
| `VOLKIT_ISSUER_SECRET_KEY` | 发证账号 SK（s3_admin 时） | 空 |
| `VOLKIT_ISSUER_TTL_SEC` | 短期凭据生命周期（STS/SA），推荐 900–3600 | `3600` |
| `VOLKIT_ALLOW_ROOT_ISSUER` | 显式允许使用 root 作为“发证账号”（s3_admin 模式） | `false` |
| `VOLKIT_S3_ADMIN_FORCE_STATIC` | 强制使用静态 AK/SK（绕过 STS/SA，不推荐） | `false` |
| `VOLKIT_SA_ENABLE` | 启用 Service Account 回退（无 STS 时用 mc 生成短期子钥） | `true` |
| `VOLKIT_MC_IMAGE` | mc 镜像（仅轮换/清理时短时拉起） | `minio/mc:latest` |
| `VOLKIT_ISSUER_INSECURE` | 允许 http/自签证书（仅限受信 overlay）；跨域建议 https/mTLS | `false` |
| TLS（`VOLKIT_TLS_CLIENT_CERT_FILE`、`VOLKIT_TLS_CLIENT_KEY_FILE`、`VOLKIT_TLS_SERVER_CA_FILE`） | 发证端 mTLS/CA（可选） | 空 |

行为优先级（s3_admin）：先尝试 STS（`issuer=s3_admin_sts`）→ 不可用则用 mc 创建“带过期的 Service Account”（`issuer=s3_admin_sa`）→ 再不行回退静态（`issuer=s3_admin_static`）。/creds 响应包含 `expiresAt` 与 `issuer`，agent 依据 ETag 变化自动重建 mounter 完成无感换证。

回退（全兼容）：
- `VOLKIT_ACCESS_KEY` / `VOLKIT_SECRET_KEY`（可选 `VOLKIT_SESSION_TOKEN`）

### 访问控制
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_ALLOW_OTHER` | 为 true 时默认加 `--allow-other`（需宿主 `/etc/fuse.conf` 启用 `user_allow_other`）；为 false 时用 `--allow-root` | `false` |

### 负载均衡（HAProxy）
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_PROXY_ENABLE` | 是否启用内置反代 | `false` |
| `VOLKIT_PROXY_ENGINE` | 反代引擎（仅 `haproxy`） | `haproxy` |
| `VOLKIT_PROXY_LOCAL_SERVICES` | 后端 Service 名，支持逗号分隔 | `minio-local` |
| `VOLKIT_PROXY_REMOTE_SERVICE` | 远端 Service 名（可留空） | `minio-remote` |
| `VOLKIT_PROXY_BACKEND_PORT` | 后端端口 | `9000` |
| `VOLKIT_PROXY_HEALTH_PATH` | 健康检查路径 | `/minio/health/ready` |

### 节点本地 LB（唯一别名）
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_PROXY_LOCAL_LB` | 启用“每节点本地 LB 别名”模式 | `false` |
| `VOLKIT_PROXY_NETWORK` | HAProxy/mounter 所在 overlay 网络（需 attachable） | 空 |
| `VOLKIT_PROXY_PORT` | HAProxy 监听端口 | `8081` |
- 别名规范：`volkit-lb-<hostname>`；启用后 rclone 端点将自动指向该别名。

### rclone 镜像/更新策略
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_DEFAULT_RCLONE_IMAGE` | 发布时内嵌的 rclone 镜像 | `rclone/rclone:latest` |
| `VOLKIT_RCLONE_IMAGE` | 运行时覆盖 rclone 镜像 | 继承默认 |
| `VOLKIT_RCLONE_UPDATE_MODE` | `never`/`on_change` | `on_change` |
| `VOLKIT_RCLONE_PULL_INTERVAL` | `periodic` 模式拉取间隔 | `24h` |

#### 自建镜像：volkit-rclone（推荐用于 allow_other 场景）
- 项目内置 `images/volkit-rclone/Dockerfile`，基于官方 `rclone/rclone:latest`，额外写入 `/etc/fuse.conf` 的 `user_allow_other`。
- 使用方式：
  - 构建并推送：
    ```bash
    docker build -t <your-registry>/volkit-rclone:latest images/volkit-rclone
    docker push <your-registry>/volkit-rclone:latest
    ```
  - 在 `volkit` 配置中指定：
    ```yaml
    environment:
      - VOLKIT_RCLONE_IMAGE=<your-registry>/volkit-rclone:latest
      - VOLKIT_ALLOW_OTHER=true
    ```
  - 注意：仅当需要非 root 访问挂载点时才需要 `allow_other`；root 访问无需该镜像。

### 清理与自动创建
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_UNMOUNT_ON_EXIT` | 退出时懒卸并移除本节点 mounter | `true` |
| `VOLKIT_AUTOCREATE_BUCKET` | 自动创建桶（后端需支持） | `false` |
| `VOLKIT_AUTOCREATE_PREFIX` | 自动创建前缀（目录） | `true` |

### 命名卷（实验）
- **自动创建**：agent 在本地挂载点检测到控制对象 `.volkit/.ready` 且挂载可写时，为每个声明的前缀创建 Docker 本地卷（local driver + bind），命名规则：`volkit-<prefix>`（标签：`volkit.managed=true`、`volkit.prefix=<prefix>`）。
- **回收**：`VOLKIT_VOLUME_RECLAIM=true` 时自动回收不再声明的 `volkit-*`；仅删除带受管标签且 device 路径位于挂载根下的卷；如发现漂移（device 不在挂载根下）会告警并计数。
- **消费方式（服务内）**：
```yaml
services:
  app:
    volumes:
      - type: volume
        source: volkit-teams-appA-vol-data
        target: /data
```

### 安全与资源（mounter）
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_MOUNTER_READONLY_ROOTFS` | mounter 只读 rootfs | `true` |
| `VOLKIT_MOUNTER_NO_NEW_PRIVILEGES` | `no-new-privileges:true` | `true` |
| `VOLKIT_MOUNTER_CAPS_ALLOW` | 允许的 capabilities（逗号分隔） | `SYS_ADMIN` |
| `VOLKIT_SECCOMP_PROFILE` | seccomp profile（路径或 Docker 内置名） | 空 |
| `VOLKIT_APPARMOR_PROFILE` | apparmor profile 名称 | 空 |
| `VOLKIT_MOUNTER_TMPFS_SIZE` | /tmp tmpfs 大小 | `16m` |
| `VOLKIT_MOUNTER_PIDS_LIMIT` | PIDs 限额 | 空 |
| `VOLKIT_MOUNTER_MEMORY_BYTES` | 内存上限（字节） | 空 |
| `VOLKIT_MOUNTER_NANO_CPUS` | CPU 限额（纳秒 CPU） | 空 |

### 退避与轮询
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_CREDS_BACKOFF_SEC` | `/creds` 失败后的退避秒数（分级） | `30` |
| `VOLKIT_POLL_INTERVAL` | 调谐周期 | `15s` |
| `VOLKIT_CREDS_MIN_SEC` | agent `/creds` 轮询最小间隔（ETag 友好） | `60` |

### Service Account 清理（MinIO）
| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `VOLKIT_SA_PRUNE` | 启用定期清理 volkit 创建的过期 SA | `false` |
| `VOLKIT_SA_PRUNE_GRACE_SEC` | 过期宽限期（超过才删除） | `600` |
| `VOLKIT_SA_PREFIX` | SA 命名前缀（识别用） | `volkit-` |

### 就绪与调度（多级别）
- agent 周期上报“已就绪前缀”；controller 维护 `node.labels.volkit.ready.<level>=true`。
- 服务可加约束：`placement.constraints: ["node.labels.volkit.ready.<level>==true"]`。
- 迁移（ForceUpdate）：
  - **失去就绪**：必触发 `ServiceUpdate(F++)`，受冷却保护。
  - **恢复就绪**：是否触发可由全局 `VOLKIT_READY_MIGRATE_ON_REGAIN` 或服务标签 `volkit.forceUpdateOnRegain=true|false` 控制。
- **冷却时间**：`VOLKIT_READY_MIGRATE_COOLDOWN_SEC`（默认 30）。

### 指标（扩展）
- `/metrics` 包含（节选）：
  - 就绪：`volkit_ready_nodes{level="<lvl>"}`
  - 卷：`volkit_volume_created_total`、`volkit_volume_reclaimed_total`、`volkit_volume_drift_total`
  - 凭据：`volkit_creds_fetch_ok_total`、`volkit_creds_fetch_errors_total`、`volkit_creds_backoff_total`
  - mounter：`volkit_mounter_backoff_total`
  - 服务解阻：`volkit_services_unblocked_total`
  - SA 清理：`volkit_sa_pruned_total`、`volkit_sa_prune_errors_total`
- `/metrics/ready`：仅输出每级别就绪节点数（简化版）。

### HTTP 服务器与中间件（基线）
- 超时：`ReadHeaderTimeout=5s`、`ReadTimeout=15s`、`WriteTimeout=15s`、`IdleTimeout=60s`、`MaxHeaderBytes=1MiB`。
- 中间件：
  - Request ID：每个响应包含 `X-Request-Id`（沿用来路或自动生成）。
  - Panic Recovery：处理器 `panic` 时返回 `500 {"error":"internal server error"}`，并记录日志。
  - JSON 响应自动 gzip（当客户端带 `Accept-Encoding: gzip`）。
- 限速：命中限速返回 `429` 时带 `Retry-After: 1` 指示客户端退避。
- 路由：统一由 `internal/httpserver.BuildMux` 注册。

### WebSocket 基线
- 消息写超时（默认 `VOLKIT_WS_WRITE_TIMEOUT_SEC=5`）。
- 可选每节点连接上限（`VOLKIT_WS_MAX_PER_NODE`，默认 2）。

### 启动自检
进程启动时会对关键环境进行校验，不满足将直接退出并打印错误：
- `VOLKIT_MODE=agent` 时必须设置 `VOLKIT_COORDINATOR_URL`。
- 范围：`VOLKIT_WS_WRITE_TIMEOUT_SEC` ∈ [1,120]；`VOLKIT_RL_SENSITIVE_CAP` 与 `VOLKIT_RL_SENSITIVE_RPS` ∈ [1,10000]。
- 冲突提示：当 `VOLKIT_ISSUER_MODE=static` 时，`VOLKIT_ALLOW_ROOT_ISSUER=true` 无效（仅告警）。

### 最小 Stack（含 s3_admin 发证）
```yaml
environment:
  # 反代（可选，直接将发证端点走 HAProxy，实现就近 + 故障转移）
  - VOLKIT_PROXY_ENABLE=true
  - VOLKIT_PROXY_ENGINE=haproxy
  - VOLKIT_PROXY_LOCAL_SERVICES=minio-1,minio-2
  - VOLKIT_PROXY_BACKEND_PORT=9000
  - VOLKIT_PROXY_HEALTH_PATH=/health
  - VOLKIT_PROXY_NETWORK=s3_net
  - VOLKIT_PROXY_PORT=8081
  # 发证（root 作为发证账号）
  - VOLKIT_ISSUER_MODE=s3_admin
  - VOLKIT_ALLOW_ROOT_ISSUER=true
  - VOLKIT_ISSUER_ENDPOINT=http://volkit-lb-$(hostname):8081
  - VOLKIT_ISSUER_ACCESS_KEY_FILE=/run/secrets/minio_root_user
  - VOLKIT_ISSUER_SECRET_KEY_FILE=/run/secrets/minio_root_password
  - VOLKIT_ISSUER_TTL_SEC=1800
  - VOLKIT_SA_ENABLE=true
  - VOLKIT_MC_IMAGE=minio/mc:latest
  - VOLKIT_SA_PRUNE=true
  - VOLKIT_SA_PRUNE_GRACE_SEC=600
  # rclone 与挂载
  - VOLKIT_RCLONE_REMOTE=S3:team-bucket
  - VOLKIT_MOUNTPOINT=/mnt/s3
  - VOLKIT_ALLOW_OTHER=false
```

---

## 声明式“卷”（基于标签的前缀供给）
默认使用“无前缀”键；也可选用域名前缀（前缀优先，冲突告警）。

在服务的 `labels` 中声明（无前缀示例）：
- `s3.enabled=true`
- `s3.bucket=my-bucket`（可选）
- `s3.prefix=teams/appA/vol-data`
- 预留：`s3.class=throughput|low-latency|low-mem`、`s3.reclaim=Retain|Delete`、`s3.access=rw|ro`、`s3.args=--vfs-cache-max-size=5G`

若需启用统一域前缀（示例 `your-org.io`）：设置 `VOLKIT_LABEL_PREFIX=your-org.io`，并改用：
- `your-org.io/s3.enabled=true`
- `your-org.io/s3.bucket=my-bucket`
- `your-org.io/s3.prefix=teams/appA/vol-data`

控制器会在本节点幂等创建 `/mnt/s3/<prefix>` 目录（若启用自动创建亦会尝试创建远端前缀/桶），应用 bind 到该路径即可使用。

---

## 最小 docker run 示例（默认无代理）
```bash
docker run -d --name vols3 \
  -e VOLKIT_ENDPOINT=http://s3.local:9000 \
  -e VOLKIT_RCLONE_REMOTE=S3:your-bucket \
  -e VOLKIT_MOUNTPOINT=/mnt/s3 \
  -e VOLKIT_ACCESS_KEY_FILE=/run/secrets/s3_access_key \
  -e VOLKIT_SECRET_KEY_FILE=/run/secrets/s3_secret_key \
  -e VOLKIT_ENABLE_METRICS=false \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /mnt/s3:/mnt/s3:rshared \
  --restart=always \
  ghcr.io/swarmnative/volkit:latest
```

## Prometheus 抓取示例
```yaml
scrape_configs:
  - job_name: 'volkit'
    scrape_interval: 30s
    static_configs:
      - targets: ['volkit:8080']
```

## 无代理直跑（去 supervisor）
- 当 `VOLKIT_PROXY_ENABLE=false`（默认）时，容器入口将直接 `exec volume-ops`，不再启动 supervisor。

---

## 归因与免责声明（rclone）
- 本项目的 mounter 基于 `rclone`（MIT 许可）实现，部分镜像/功能依赖官方 `rclone/rclone` 镜像。
- 我们提供了自建镜像 `volkit-rclone` 的 Dockerfile（仅写入 `/etc/fuse.conf` 的 `user_allow_other`），方便在需要 `allow_other` 的场景使用。
- 归因：
  - 源码与镜像来源：`https://github.com/rclone/rclone`
  - 许可：MIT（以上游仓库声明为准）
- 免责声明：
  - `volkit-rclone` 为社区自建镜像，非 rclone 官方发行版；仅用于与本项目集成的便利性。
  - 使用包含 “rclone” 的命名仅为说明性引用，不代表官方背书；请勿将其用于误导性的品牌传播。

---

## FAQ
- MinIO 是否应先启动？
  - 建议先部署并通过健康检查；Swarm 无严格 `depends_on`，控制器会重试直至就绪。
- `tasks.<service>` 会不会连到其他节点的代理？
  - 它解析的是后端 Service 副本 IP，通常用于直连后端而非本项目的 HAProxy。若启用节点本地 LB，请使用 `volkit-lb-<hostname>` 端点。

## 示例：连接 S3 兼容集群 + 启用节点本地 HAProxy（就近 + 负载均衡 + 故障转移）
在 Swarm 中推荐启用“节点本地 LB”：每个节点的 rclone 连接本节点 HAProxy 别名以降低延迟，HAProxy 负责健康检查与故障转移。策略：`leastconn`。

前提：
- 已有 S3 兼容集群，可通过多个 Swarm 服务访问（例如 `s3node1,s3node2,...`，端口 9000）
- 每个服务提供 HTTP 就绪检查（例如 `/health`）

```yaml
version: "3.8"

networks:
  s3_net:
    driver: overlay
    attachable: true
    internal: true

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
      # 反代：多后端 + 节点本地别名（就近 + 故障转移）
      - VOLKIT_PROXY_ENABLE=true
      - VOLKIT_PROXY_ENGINE=haproxy
      - VOLKIT_PROXY_LOCAL_SERVICES=s3node1,s3node2,s3node3,s3node4
      - VOLKIT_PROXY_BACKEND_PORT=9000
      - VOLKIT_PROXY_HEALTH_PATH=/health
      - VOLKIT_PROXY_LOCAL_LB=true
      - VOLKIT_PROXY_NETWORK=s3_net
      - VOLKIT_PROXY_PORT=8081
      # S3 与 rclone
      - VOLKIT_RCLONE_REMOTE=S3:team-bucket
      - VOLKIT_MOUNTPOINT=/mnt/s3
      - VOLKIT_ACCESS_KEY_FILE=/run/secrets/s3_access_key
      - VOLKIT_SECRET_KEY_FILE=/run/secrets/s3_secret_key
      # 调优：多小文件/多目录（平衡缓存与内存）
      - VOLKIT_RCLONE_ARGS=--vfs-cache-mode=full --vfs-cache-max-size=4G --vfs-cache-max-age=48h --dir-cache-time=24h --attr-timeout=2s --buffer-size=8M --s3-chunk-size=8M --s3-upload-concurrency=4 --s3-max-upload-parts=10000
      - VOLKIT_UNMOUNT_ON_EXIT=true
      - VOLKIT_AUTOCREATE_PREFIX=true
    deploy:
      mode: global
      placement: { constraints: [node.labels.mount_s3 == true] }
      restart_policy: { condition: any }
```

提示：
- `--vfs-cache-mode=full` 对多小文件/目录遍历更友好；根据磁盘空间调整缓存大小与期限。
- 若后端要求 path-style，可加 `--s3-force-path-style=true`；若为公有云对象存储，补 `--s3-region=<region>`。
- 默认已追加 `--allow-non-empty`，避免与 rshared 绑定冲突（目录已挂载）。
- 两种模式均需确保 `/mnt/s3` 为 shared（已内置自愈，必要时手工执行上面的 nsenter 命令）。
- `FUSE3` 上游已移除 `allow_root` 支持，`--allow-root` 会被忽略；如需跨用户访问，请使用 `--allow-other` 且在 mounter 镜像内启用 `/etc/fuse.conf` 的 `user_allow_other`。

### 节点就近（Overlay IP 模式，推荐）
- 当 `VOLKIT_PROXY_ENABLE=true` 且设置了 `VOLKIT_PROXY_NETWORK`（attachable 的 overlay）时，controller 每次调谐会查询“自身在该 overlay 的 IPv4”，并将 mounter 的端点设置为 `http://<controller-overlay-IP>:<VOLKIT_PROXY_PORT>`。
- mounter 同样加入该 overlay，实现“本节点 mounter 直连本节点 controller 的 HAProxy”，无需节点别名与 127.0.0.1 共享命名空间。
- 若 controller 重启或 IP 变化，controller 会检测端点漂移并自动重建 mounter，端点随之更新。


### 协调器 / 代理 模式（实验特性）

为了解决“服务尚未启动但需要提前创建绑定目录”“集中治理/降低并发写”等问题，支持将控制面和数据面解耦：
- 协调器（coordinator）：只读 manager API，解析服务标签并聚合为各节点的声明集合，可选在远端写占位对象。
- 代理（agent）：仅在本机执行 mkdir/rshared 自愈与 rclone 生命周期；通过 /claims 拉取本机所需声明，不直接访问 manager API。

运行模式（环境变量）
- VOLKIT_MODE=all | coordinator | agent
  - coordinator：仅做聚合与（可选）占位对象写入；不做本地 mkdir
  - agent：仅做本地 mkdir；从协调器拉取 /claims
  - all：保持单体（默认）
- VOLKIT_COORDINATOR_URL=http://coordinator:8080 （agent 模式必填）
- VOLKIT_MANAGER_DOCKER_HOST=tcp://prod-docker-socket-proxy-svc:2375（建议设置给 coordinator/all）
- VOLKIT_CREATE_PLACEHOLDER=off | coordinator（coordinator 单点在远端写 .keep 占位）

标签规范（支持多声明）
- 单声明：
  - volkit.enabled=true
  - volkit.bucket=volumes
  - volkit.prefix=openlist/data
  - 可选：volkit.mode=local|remote|both、volkit.nodes=n1.example.com,n2.example.com、volkit.uid/gid/chmod
- 多声明（索引化）：
  - volkit.claims=2
  - volkit.1.bucket=volumes
  - volkit.1.prefix=openlist/data
  - volkit.1.mode=local|remote|both
  - volkit.1.nodes=n1.example.com,n2.example.com
  - volkit.1.uid=1000、volkit.1.gid=1000、volkit.1.chmod=0755
  - volkit.2.bucket=volumes
  - volkit.2.prefix=teamA/cache

说明与行为
- 本地目录创建：agent（或 all）在 $VOLKIT_MOUNTPOINT 下执行 mkdir -p，幂等，不依赖 S3。
- 远端占位：S3 无“目录”，coordinator 可选单点写 .keep 对象以“显形”；否则首次业务写入即显形。
- 节点定向：通过 volkit.nodes 精确到主机名；（后续可加 nodeSelector）。
- 白名单：VOLKIT_CLAIM_ALLOWLIST_REGEX 可限制允许的前缀，避免误创建。

/claims 接口（coordinator 暴露）
- GET /claims?node=<hostname>
- 返回示例：
```
[
  {"bucket":"volumes","prefix":"openlist/data","mode":"local","uid":1000,"gid":1000,"chmod":"0755"}
]
```
- agent 周期或在 /reload 时拉取并创建本地目录。

示例部署（Stack）
```yaml
version: "3.8"

networks:
  prod-overlay-net:
    external: true

secrets:
  prod-minio-root-user:
    external: true
  prod-minio-root-password:
    external: true

services:
  # manager 上仅 1 副本：Docker Socket Proxy（只读）
  prod-docker-socket-proxy-svc:
    image: your-registry/docker-socket-proxy:latest
    networks:
      prod-overlay-net: {}
    environment:
      CONTAINERS: 0
      SERVICES: 1
      TASKS: 1
      EVENTS: 1
      NETWORKS: 0
      NODES: 0
      IMAGES: 0
      VOLUMES: 0
      INFO: 0
      PING: 1
      SWARM: 1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.role == manager

  # 协调器：解析 Service 标签并下发 /claims，可选单点写占位
  volkit-coordinator:
    image: ghcr.io/swarmnative/volkit:latest
    init: true
    networks:
      prod-overlay-net: {}
    environment:
      VOLKIT_MODE: coordinator
      VOLKIT_MANAGER_DOCKER_HOST: tcp://prod-docker-socket-proxy-svc:2375
      VOLKIT_CREATE_PLACEHOLDER: coordinator   # 可选：单点写 .keep
      VOLKIT_READ_SERVICE_LABELS: "true"
      VOLKIT_PROVIDER: Minio
      VOLKIT_RCLONE_REMOTE: "S3:volumes"
      VOLKIT_ACCESS_KEY_FILE: /run/secrets/prod-minio-root-user
      VOLKIT_SECRET_KEY_FILE: /run/secrets/prod-minio-root-password
      VOLKIT_AUTOCREATE_BUCKET: "true"
      VOLKIT_AUTOCREATE_PREFIX: "true"
    secrets: [prod-minio-root-user, prod-minio-root-password]
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.role == manager

  # 代理：各节点本地 mkdir/rshared，自愈与 mounter 生命周期
  volkit-agent:
    image: ghcr.io/swarmnative/volkit:latest
    init: true
    networks:
      prod-overlay-net: {}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - type: bind
        source: /mnt/s3
        target: /mnt/s3
        bind:
          propagation: rshared
    environment:
      VOLKIT_MODE: agent
      VOLKIT_COORDINATOR_URL: https://volkit-coordinator:8080
      VOLKIT_MOUNTPOINT: /mnt/s3
      VOLKIT_PROVIDER: Minio
      VOLKIT_RCLONE_REMOTE: "S3:volumes"
      VOLKIT_ALLOW_OTHER: "true"
      VOLKIT_ENROLL_TOKEN: "<enroll-token>"
    deploy:
      mode: global
      placement:
        constraints:
          - node.labels.volume.s3 == true
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://127.0.0.1:8080/ready >/dev/null"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 20s

  # 示例业务服务（使用 volkit 创建的“命名卷”）
  app:
    image: your/app:latest
    networks:
      prod-overlay-net: {}
    volumes:
      - type: volume
        source: volkit-teams-appA-vol-data
        target: /data
    deploy:
      placement:
        constraints:
          - node.labels.volume.s3 == true
```

注意事项
- 基路径：可通过 VOLKIT_MOUNTPOINT 切换（默认 /mnt/s3）；请确保设为 shared：`mount --make-rshared <路径>`。
- S3 占位：默认不写；如需立刻可见，开启 VOLKIT_CREATE_PLACEHOLDER=coordinator。
- 权限：如需控制本地目录属主/权限，使用 volkit.uid/gid/chmod；或在 agent 侧通过容器用户映射实现。



