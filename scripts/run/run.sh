#!/usr/bin/env bash
# Local development runner
set -euo pipefail

# Resolve script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_ROOT}"

# Locate docker executable (fallback to snap if not in PATH)
DOCKER_BIN="$(command -v docker 2>/dev/null || true)"
if [ -z "${DOCKER_BIN}" ] && [ -x "/snap/bin/docker" ]; then
    DOCKER_BIN="/snap/bin/docker"
fi

if [ -z "${DOCKER_BIN}" ]; then
    echo "Error: docker binary not found in PATH or /snap/bin/docker" >&2
    exit 1
fi

# Ensure secrets directory and default db_password.txt exist if missing
COMPOSE_SECRETS_DIR="${PROJECT_ROOT}/deploy/docker/compose/secrets"
mkdir -p "${COMPOSE_SECRETS_DIR}"
if [ ! -f "${COMPOSE_SECRETS_DIR}/db_password.txt" ]; then
    echo "postgres" > "${COMPOSE_SECRETS_DIR}/db_password.txt"
    chmod 600 "${COMPOSE_SECRETS_DIR}/db_password.txt"
fi

COMPOSE_FILE="${PROJECT_ROOT}/deploy/docker/compose/docker-compose.infra.yml"

echo "==> Starting infrastructure docker services using ${COMPOSE_FILE}..."
"${DOCKER_BIN}" compose -f "${COMPOSE_FILE}" up -d "$@"

