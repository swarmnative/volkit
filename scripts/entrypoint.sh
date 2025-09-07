#!/bin/sh
set -eu

# If running as root, dynamically join docker.sock group then drop to app user
if [ "$(id -u)" = "0" ]; then
  SOCK=${DOCKER_SOCK_PATH:-/var/run/docker.sock}
  if [ -S "$SOCK" ]; then
    GID=$(stat -c %g "$SOCK" 2>/dev/null || echo "")
    if [ -n "$GID" ]; then
      if ! getent group "$GID" >/dev/null 2>&1; then
        addgroup -g "$GID" dockersock >/dev/null 2>&1 || true
      fi
      GRP_NAME=$(getent group "$GID" | cut -d: -f1 || echo "dockersock")
      # add app user into the docker socket group (BusyBox: addgroup USER GROUP)
      addgroup app "$GRP_NAME" >/dev/null 2>&1 || true
    fi
  fi
  # Prefer /var/run/docker.sock, fallback to /run/docker.sock
  if [ -S /var/run/docker.sock ]; then
    export DOCKER_HOST=unix:///var/run/docker.sock
  elif [ -S /run/docker.sock ]; then
    export DOCKER_HOST=unix:///run/docker.sock
  fi
  # re-exec as app preserving env, initialize supplementary groups
  exec setpriv --reuid app --regid app --init-groups /entrypoint.sh "$@"
fi

# Preflight checks (best-effort with clear warnings)
if [ ! -e /dev/fuse ]; then
  echo "[WARN] /dev/fuse not found. rclone FUSE mount will fail. Bind /dev/fuse into the container." >&2
fi
# Check SYS_ADMIN by attempting a harmless mount --help (non-fatal)
if ! grep -qw SYS_ADMIN /proc/1/status 2>/dev/null && ! grep -qw SYS_ADMIN /proc/self/status 2>/dev/null; then
  echo "[WARN] Container may lack CAP_SYS_ADMIN. Ensure cap_add: [SYS_ADMIN] for mounter container." >&2
fi

# Custom CA import (optional): mount PEM files in /app/etc/ca and install
if [ -d "/app/etc/ca" ]; then
  if command -v update-ca-certificates >/dev/null 2>&1; then
    cp -f /app/etc/ca/* /usr/local/share/ca-certificates/ 2>/dev/null || true
    update-ca-certificates || true
  else
    echo "[WARN] update-ca-certificates not available; custom CAs in /app/etc/ca not loaded" >&2
  fi
fi

# Pre-start self-heal for leftover mounts and ensure shared propagation
MOUNTPOINT=${VOLS3_MOUNTPOINT:-"/mnt/s3"}
UNMOUNT_ON_EXIT=${VOLS3_UNMOUNT_ON_EXIT:-"true"}

is_mounted() {
  grep -qsE "[[:space:]]$(printf %q "$MOUNTPOINT")[[:space:]]" /proc/self/mountinfo
}

test_rw() {
  touch "$MOUNTPOINT/.vols3_probe" 2>/dev/null && rm -f "$MOUNTPOINT/.vols3_probe" 2>/dev/null
}

lazy_unmount() {
  nsenter -t 1 -m -- fusermount -uz "$MOUNTPOINT" 2>/dev/null || true
  nsenter -t 1 -m -- umount -l "$MOUNTPOINT" 2>/dev/null || true
  fusermount -uz "$MOUNTPOINT" 2>/dev/null || true
  umount -l "$MOUNTPOINT" 2>/dev/null || true
}

make_shared() {
  nsenter -t 1 -m -- mount --make-rshared "$MOUNTPOINT" 2>/dev/null || mount --make-rshared "$MOUNTPOINT" 2>/dev/null || true
}

pre_start_self_heal() {
  mkdir -p "$MOUNTPOINT" 2>/dev/null || true
  if is_mounted; then
    if ! test_rw; then
      echo "[INFO] Detected stale mount at $MOUNTPOINT, attempting lazy unmount..." >&2
      lazy_unmount
    fi
  fi
  make_shared
}

cleanup() {
  if [ "$UNMOUNT_ON_EXIT" = "true" ]; then
    echo "[INFO] Unmount on exit enabled, attempting lazy unmount for $MOUNTPOINT" >&2
    lazy_unmount
  fi
}

pre_start_self_heal

ENABLE_PROXY=${VOLS3_PROXY_ENABLE:-"false"}
PROXY_ENGINE=${VOLS3_PROXY_ENGINE:-"haproxy"} # haproxy only

# tunables
BALANCE_ALG=${VOLS3_PROXY_BALANCE:-"leastconn"}
TIMEOUT_CONNECT=${VOLS3_PROXY_TIMEOUT_CONNECT:-"2s"}
TIMEOUT_CLIENT=${VOLS3_PROXY_TIMEOUT_CLIENT:-"60s"}
TIMEOUT_SERVER=${VOLS3_PROXY_TIMEOUT_SERVER:-"60s"}
RETRIES=${VOLS3_PROXY_RETRIES:-"2"}
PORT=${VOLS3_PROXY_PORT:-"8081"}

# target writable paths for non-root
APP_ETC=${APP_ETC:-"/app/etc"}
APP_LOG=${APP_LOG:-"/app/var/log"}
APP_RUN=${APP_RUN:-"/app/var/run"}
HAPROXY_ETC=${HAPROXY_ETC:-"${APP_ETC}/haproxy"}
SUPERVISOR_CONF=${SUPERVISOR_CONF:-"${APP_ETC}/supervisord.conf"}

if [ "$ENABLE_PROXY" = "true" ]; then
  H_LOCAL_SERVICE=${VOLS3_PROXY_LOCAL_SERVICES:-"minio-local"}
  H_REMOTE_SERVICE=${VOLS3_PROXY_REMOTE_SERVICE:-"minio-remote"}
  H_PORT=${VOLS3_PROXY_BACKEND_PORT:-"9000"}
  H_HEALTH_PATH=${VOLS3_PROXY_HEALTH_PATH:-"/minio/health/ready"}
  # Build multiple local service templates if comma-separated (avoid set -- pollution)
  LOC_LINES=""
  OLD_IFS="$IFS"
  IFS=','
  for svc in $H_LOCAL_SERVICE; do
    svc_trim=$(echo "$svc" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [ -z "$svc_trim" ] && continue
    # unique template name per service; sanitize for haproxy server name
    svc_id=$(echo "$svc_trim" | tr -c 'A-Za-z0-9_-' '_')
    line="  server-template loc_${svc_id} 1-8 tasks.${svc_trim}:${H_PORT} resolvers docker resolve-prefer ipv4 init-addr none weight 100"
    if [ -z "$LOC_LINES" ]; then
      LOC_LINES="$line"
    else
      LOC_LINES="$LOC_LINES
$line"
    fi
  done
  IFS="$OLD_IFS"
  RMT_LINE=""
  if [ -n "$H_REMOTE_SERVICE" ]; then
    RMT_LINE="  server-template rmt 1-8 tasks.${H_REMOTE_SERVICE}:${H_PORT} resolvers docker resolve-prefer ipv4 init-addr none backup weight 10"
  fi
  mkdir -p "$HAPROXY_ETC"
  cat > "${HAPROXY_ETC}/haproxy.cfg" <<EOF
global
  log stdout format raw local0
  tune.bufsize 32768
  stats socket /var/run/haproxy.sock mode 600 level admin expose-fd listeners

defaults
  mode http
  option  httplog
  option  http-keep-alive
  http-reuse safe
  timeout connect ${TIMEOUT_CONNECT}
  timeout client  ${TIMEOUT_CLIENT}
  timeout server  ${TIMEOUT_SERVER}
  retries ${RETRIES}

resolvers docker
  nameserver dns 127.0.0.11:53
  resolve_retries 3
  timeout retry 1s
  hold valid 10s

frontend s3_in
  bind :${PORT}
  default_backend s3_upstream

backend s3_upstream
  balance ${BALANCE_ALG}
  option httpchk GET ${H_HEALTH_PATH}
  http-check expect status 200-399
  default-server inter 2s fastinter 500ms downinter 5s fall 3 rise 2 slowstart 5s maxconn 500
${LOC_LINES}
${RMT_LINE}
EOF
fi

# Generate supervisord config based on ENABLE_PROXY
mkdir -p "${APP_LOG}/supervisor" "${APP_RUN}"
if [ "$ENABLE_PROXY" = "true" ]; then
  cat > "$SUPERVISOR_CONF" <<'SUPV'
[supervisord]
logfile=/app/var/log/supervisor/supervisord.log
pidfile=/app/var/run/supervisord.pid
loglevel=info
nodaemon=true

[program:haproxy]
command=/usr/sbin/haproxy -f /app/etc/haproxy/haproxy.cfg -db
autorestart=true
priority=10

[program:volume-ops]
command=/usr/local/bin/volume-ops
autorestart=true
priority=20
SUPV
  # Explicitly exec supervisord (do not rely on CMD and do not exec "$@")
  exec /usr/bin/supervisord -c "$SUPERVISOR_CONF"
else
  # 无代理：运行 volume-ops 并安装退出清理
  trap 'cleanup' EXIT INT TERM
  /usr/local/bin/volume-ops &
  pid=$!
  wait "$pid"
  status=$?
  cleanup || true
  exit "$status"
fi

