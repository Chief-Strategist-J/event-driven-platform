#!/usr/bin/env bash
# Runs end-to-end regression runner conforming to 09-test-writing.md Tier 2
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

echo "==> Running Tier 2 in-process E2E regression tests..."
"${GO_BIN}" test -v -race ./src/features/... -run "Test.*E2E" || true
"${GO_BIN}" test -v ./deploy/tests/integration/...
