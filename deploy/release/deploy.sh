#!/usr/bin/env bash
set -euo pipefail

: "${APP_DIGEST:?APP_DIGEST must be set to an immutable sha256 digest}"
: "${COMPOSE_PROJECT_NAME:=event_platform}"

echo "==> Deploying event-driven-platform with digest: ${APP_DIGEST}"

docker compose -f deploy/docker/compose/docker-compose.single.yml pull app
docker compose -f deploy/docker/compose/docker-compose.single.yml up -d --wait --wait-timeout 180

echo "${APP_DIGEST}" > /tmp/event_driven_platform_previous.env
echo "==> Deployment successful."
