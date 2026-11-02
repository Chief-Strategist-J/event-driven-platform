#!/usr/bin/env bash
set -euo pipefail

PREV_FILE="/tmp/event_driven_platform_previous.env"
if [[ ! -f "${PREV_FILE}" ]]; then
  echo "Error: previous deployment state file ${PREV_FILE} not found." >&2
  exit 1
fi

PREV_DIGEST=$(cat "${PREV_FILE}")
echo "==> Rolling back to previous digest: ${PREV_DIGEST}"

APP_DIGEST="${PREV_DIGEST}" docker compose -f deploy/docker/compose/docker-compose.single.yml up -d --wait
echo "==> Rollback complete."
