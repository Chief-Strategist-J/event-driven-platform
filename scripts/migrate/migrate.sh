#!/usr/bin/env bash
# Applies pending DDL migrations in order conforming to 05-db-writing-rules.md
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_ROOT}"

DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable}"
MIGRATIONS_DIR="${PROJECT_ROOT}/database/migrations/sql"

DOCKER_BIN="$(command -v docker 2>/dev/null || true)"
if [ -z "${DOCKER_BIN}" ] && [ -x "/snap/bin/docker" ]; then
    DOCKER_BIN="/snap/bin/docker"
fi

echo "==> Applying pending PostgreSQL migrations from ${MIGRATIONS_DIR}..."

for migration_file in $(ls "${MIGRATIONS_DIR}"/[0-9][0-9][0-9][0-9]_*.sql | grep -v '\.rollback\.sql$' | sort); do
    echo "Applying $(basename "${migration_file}")..."
    if [ -n "${DOCKER_BIN}" ] && "${DOCKER_BIN}" ps --format '{{.Names}}' | grep -q 'event_platform-db-1'; then
        "${DOCKER_BIN}" exec -i event_platform-db-1 psql -U postgres -d event_store < "${migration_file}"
    elif command -v psql >/dev/null 2>&1; then
        psql "${DATABASE_URL}" -f "${migration_file}"
    else
        echo "Error: neither running docker container 'event_platform-db-1' nor local 'psql' found." >&2
        exit 1
    fi
done

echo "==> All migrations applied successfully!"
