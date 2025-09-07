# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.22.0

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Install build deps
RUN apk add --no-cache git ca-certificates

# Cache modules first
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy rest and build
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$(go env GOARCH) \
    go build -trimpath -ldflags "-s -w" -o /out/volkit ./cmd/volkit

ARG BASE_IMAGE=gcr.io/distroless/base-debian12:nonroot
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.source="https://github.com/swarmnative/volkit"
LABEL org.opencontainers.image.title="volkit"
LABEL org.opencontainers.image.description="S3 volume controller for Docker/Swarm"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /
COPY --from=build /out/volkit /usr/local/bin/volkit

# Prepare default mountpoint
RUN ["/busybox/sh","-lc","mkdir -p /mnt/s3 || true"]

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/volkit"]
# syntax=docker/dockerfile:1.7

FROM golang:1.22-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BUILD_VERSION=dev
WORKDIR /src
COPY go.mod ./
COPY go.sum ./
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go env -w GOPROXY=https://proxy.golang.org,direct GOSUMDB=sum.golang.org && \
    rm -f go.sum || true && \
    go mod tidy && \
    go mod download && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.buildVersion=$BUILD_VERSION" -mod=mod -o /out/volume-ops ./cmd/volume-ops

FROM alpine:3.20
RUN apk add --no-cache haproxy supervisor curl ca-certificates util-linux su-exec \
    && addgroup -S app && adduser -S -G app -H -s /sbin/nologin app
WORKDIR /app
COPY --from=builder /out/volume-ops /usr/local/bin/volume-ops
# Allow baking default rclone image at build time
ARG RCLONE_IMAGE="rclone/rclone:latest"
# Export as runtime default (can be overridden by env)
ENV VOLKIT_DEFAULT_RCLONE_IMAGE=${RCLONE_IMAGE}
ENV VOLKIT_BUILD_VERSION=${BUILD_VERSION}
COPY supervisord.conf /app/etc/supervisord.default.conf
COPY THIRD_PARTY_NOTICES.md /app/THIRD_PARTY_NOTICES.md
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && mkdir -p /app/etc /app/var/log/supervisor /app/var/run \
    && chown -R app:app /app

EXPOSE 8080 8081
# OCI labels
LABEL org.opencontainers.image.source="https://github.com/swarmnative/volkit" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="volkit" \
      org.opencontainers.image.description="S3/obj storage volume controller/agent for Docker/Swarm (powered by rclone)" \
      org.opencontainers.image.version=${BUILD_VERSION}
# run as root here; entrypoint will drop privileges to app
USER root
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/usr/bin/supervisord", "-c", "/app/etc/supervisord.conf"]

