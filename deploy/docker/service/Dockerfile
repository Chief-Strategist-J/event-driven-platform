# syntax=docker/dockerfile:1.7
ARG BUILDER_IMAGE=golang:1.23-bookworm
ARG BASE_IMAGE=debian:bookworm-slim

# ---------- Stage 1: Go Application Builder ----------
FROM ${BUILDER_IMAGE} AS builder
ARG SERVICE_PATH=.
WORKDIR /build

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://proxy.golang.org,direct

COPY ${SERVICE_PATH}/go.mod ${SERVICE_PATH}/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY ${SERVICE_PATH}/src/ ./src/
COPY ${SERVICE_PATH}/messaging/ ./messaging/
COPY ${SERVICE_PATH}/database/ ./database/
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
COPY --from=builder --chown=${UID}:${GID} /build/database/ /app/database/
COPY --chown=${UID}:${GID} ${SERVICE_PATH}/config/ /app/config/

ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.source="https://github.com/Chief-Strategist-J/System-Architect" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.title="event-driven-platform"

ENV PORT=8080 \
    ADMIN_PORT=8081 \
    ADMIN_HOST=0.0.0.0

USER ${UID}:${GID}
EXPOSE 8080 8081
ENTRYPOINT ["/usr/bin/tini", "--", "/app/bin/platform-service"]
CMD ["serve"]
