# syntax=docker/dockerfile:1.7
ARG BUILDER_IMAGE=golang:1.23.1-bookworm@sha256:d824d528994a5c92c813be5bfb3a58eeb06f190a07e868a83ee07ff5b3f11467
ARG BASE_IMAGE=debian:bookworm-slim@sha256:49195b098197775ce087dd46c7bc3a670f5e1f0217ec5e1bce9f6b5791dfae40

# ---------- Stage 1: Go Application Builder ----------
FROM ${BUILDER_IMAGE} AS builder
ARG SERVICE_PATH=.
WORKDIR /build

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://proxy.golang.org,direct

COPY ${SERVICE_PATH}/go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY ${SERVICE_PATH}/src/ ./src/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /build/platform-service ./src/main.go

# ---------- Stage 2: Minimal Hardened Runtime ----------
FROM ${BASE_IMAGE} AS runtime
ARG SERVICE_PATH=.
ARG UID=10001
ARG GID=10001

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata tini \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid ${GID} app 2>/dev/null || true \
 && useradd --uid ${UID} --gid ${GID} --no-create-home --shell /usr/sbin/nologin app 2>/dev/null || true \
 && mkdir -p /app/config /app/bin && chown -R ${UID}:${GID} /app

WORKDIR /app

COPY --from=builder --chown=${UID}:${GID} /build/platform-service /app/bin/platform-service
COPY --chown=${UID}:${GID} ${SERVICE_PATH}/config/ /app/config/

ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.source="https://github.com/Chief-Strategist-J/System-Architect" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.title="event-driven-platform"

USER ${UID}:${GID}
EXPOSE 50051 8081
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/bin/platform-service", "--config", "/app/config/default.yaml"]
