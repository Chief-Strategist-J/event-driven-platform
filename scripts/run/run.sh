#!/usr/bin/env bash
# Flexible runner to locate and start docker compose setups from anywhere
set -euo pipefail

# Find project root dynamically (searches upward for git root or specific marker)
find_project_root() {
    local dir="${1:-$(pwd)}"
    while [ "$dir" != "/" ]; do
        if [ -d "$dir/.git" ] || [ -f "$dir/go.mod" ]; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done
    return 1
}

# Resolve script directory or project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || echo "")"
PROJECT_ROOT=""

if [ -n "$SCRIPT_DIR" ]; then
    PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR" || true)"
fi

if [ -z "$PROJECT_ROOT" ]; then
    PROJECT_ROOT="$(find_project_root "$(pwd)" || true)"
fi

if [ -z "$PROJECT_ROOT" ]; then
    echo "Error: Could not locate project root from script location or current directory" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"

# Locate docker executable dynamically
DOCKER_BIN="$(command -v docker 2>/dev/null || true)"
if [ -z "${DOCKER_BIN}" ]; then
    for candidate in /usr/bin/docker /usr/local/bin/docker /snap/bin/docker; do
        if [ -x "$candidate" ]; then
            DOCKER_BIN="$candidate"
            break
        fi
    done
fi

if [ -z "${DOCKER_BIN}" ]; then
    echo "Error: docker binary not found in PATH or standard locations" >&2
    exit 1
fi

# Locate compose files dynamically
find_compose_file() {
    local target_name="$1"
    # 1. Check direct standard location
    if [ -f "${PROJECT_ROOT}/deploy/docker/compose/${target_name}" ]; then
        echo "${PROJECT_ROOT}/deploy/docker/compose/${target_name}"
        return 0
    fi
    # 2. Search recursively in project
    local found
    found="$(find "${PROJECT_ROOT}" -type f -name "${target_name}" | head -n 1)"
    if [ -n "$found" ]; then
        echo "$found"
        return 0
    fi
    return 1
}

TARGET_COMPOSE="${COMPOSE_FILE_NAME:-docker-compose.infra.yml}"
COMPOSE_FILE="${COMPOSE_FILE:-$(find_compose_file "$TARGET_COMPOSE" || true)}"

if [ -z "${COMPOSE_FILE}" ] || [ ! -f "${COMPOSE_FILE}" ]; then
    echo "Error: Could not find compose file '${TARGET_COMPOSE}' in project root '${PROJECT_ROOT}'" >&2
    exit 1
fi

COMPOSE_DIR="$(dirname "${COMPOSE_FILE}")"

# Ensure secrets directory and default db_password.txt exist
COMPOSE_SECRETS_DIR="${COMPOSE_DIR}/secrets"
mkdir -p "${COMPOSE_SECRETS_DIR}"
if [ ! -f "${COMPOSE_SECRETS_DIR}/db_password.txt" ]; then
    echo "postgres" > "${COMPOSE_SECRETS_DIR}/db_password.txt"
    chmod 600 "${COMPOSE_SECRETS_DIR}/db_password.txt"
fi

# Enable observability profile by default so monitoring, telemetry, and main services run
COMPOSE_PROFILES="${COMPOSE_PROFILES:-observability}"
export COMPOSE_PROFILES

echo "==> Project Root: ${PROJECT_ROOT}"
echo "==> Using Compose File: ${COMPOSE_FILE}"
echo "==> Active Profiles: ${COMPOSE_PROFILES}"
echo "==> Running docker compose up -d..."

"${DOCKER_BIN}" compose -f "${COMPOSE_FILE}" up -d "$@"
