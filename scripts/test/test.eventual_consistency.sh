#!/usr/bin/env bash
# Runs Eventual Consistency Monitoring unit, state-machine, rule, and live postgres integration tests
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

echo "================================================================"
echo " RUNNING EVENTUAL CONSISTENCY MONITORING TESTS"
echo "================================================================"
"${GO_BIN}" test -v -race ./src/features/eventual-consistency-monitoring/tests/...
echo "================================================================"
echo ">>> [SUCCESS] All Eventual Consistency Monitoring tests PASSED!"
echo "================================================================"
