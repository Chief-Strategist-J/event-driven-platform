#!/usr/bin/env bash
# Pushes repositories to backup account (jaydeep-8073417202) using token
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TOKEN="${1:-}"
if [ -z "${TOKEN}" ]; then
    echo "Usage: $0 <github_token>" >&2
    exit 1
fi

BACKUP_USER="jaydeep-8073417202"

echo "========================================================================"
echo ">>> PUSHING EVENT-DRIVEN-PLATFORM TO BACKUP REPOSITORY (${BACKUP_USER})"
echo "========================================================================"
cd "${PROJECT_ROOT}"

EDP_TARGET="https://${BACKUP_USER}:${TOKEN}@github.com/${BACKUP_USER}/event-driven-platform.git"
git push "${EDP_TARGET}" main:main --force

echo "========================================================================"
echo ">>> PUSHING SYSTEM-ARCHITECT TO BACKUP REPOSITORY (${BACKUP_USER})"
echo "========================================================================"
cd "${PROJECT_ROOT}/.."

PARENT_TARGET="https://${BACKUP_USER}:${TOKEN}@github.com/${BACKUP_USER}/System-Architect.git"
git push "${PARENT_TARGET}" main:main --force

echo "========================================================================"
echo ">>> [SUCCESS] Successfully pushed all repositories to backup account!"
echo "========================================================================"
