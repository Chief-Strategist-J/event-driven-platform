#!/usr/bin/env bash
# Runs production service binary in accordance with 12-factor configuration
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_ROOT}"

GO_BIN="$(command -v go 2>/dev/null || true)"
if [ -z "${GO_BIN}" ] && [ -x "/home/btpl-lap-22/.local/bin/go" ]; then
    GO_BIN="/home/btpl-lap-22/.local/bin/go"
fi

if [ -z "${GO_BIN}" ]; then
    echo "Error: go binary not found in PATH or /home/btpl-lap-22/.local/bin/go" >&2
    exit 1
fi

BIN_DIR="${PROJECT_ROOT}/bin"
mkdir -p "${BIN_DIR}"

echo "==> Compiling production binary..."
"${GO_BIN}" build -trimpath -ldflags="-s -w" -o "${BIN_DIR}/platform-service" ./src/main.go

echo "==> Launching production server..."
exec "${BIN_DIR}/platform-service" serve "$@"
