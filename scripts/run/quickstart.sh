#!/usr/bin/env bash
# One-command start of the whole platform. Honors deploy/docker/compose/.env if present.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIR="$ROOT/deploy/docker/compose"
FILE="$DIR/docker-compose.all.yml"

command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 ('docker compose') is required" >&2; exit 1; }

ARGS=(-f "$FILE")
[ -f "$DIR/.env" ] && ARGS+=(--env-file "$DIR/.env") && echo "Using $DIR/.env"
[ "${EDP_OBSERVABILITY:-0}" = "1" ] && ARGS+=(--profile observability)

[ "${EDP_PULL:-1}" = "1" ] && docker compose "${ARGS[@]}" pull --ignore-buildable
docker compose "${ARGS[@]}" up -d --wait

API="${EDP_API_PORT:-8080}"; ADM="${EDP_ADMIN_PORT:-8081}"
if [ -f "$DIR/.env" ]; then
  # shellcheck disable=SC1091
  set -a; . "$DIR/.env"; set +a
  API="${EDP_API_PORT:-8080}"; ADM="${EDP_ADMIN_PORT:-8081}"
fi
cat <<MSG

Platform is up.
  API:      http://127.0.0.1:${API}/api/v1/
  Health:   curl http://127.0.0.1:${ADM}/health/ready
  Metrics:  curl http://127.0.0.1:${ADM}/metrics
  Smoke:    scripts/test/smoke.sh
  Logs:     docker compose ${ARGS[*]} logs -f app
  Stop:     docker compose ${ARGS[*]} down -v
MSG
