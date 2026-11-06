#!/usr/bin/env bash
# Runs 100% full platform test suite:
# 1. Database schema migrations
# 2. Dedicated feature unit/integration test scripts
# 3. Complete live API & backing infrastructure suite with W3C distributed tracing
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_ROOT}"

echo "========================================================================"
echo ">>> [STAGE 1/3] VERIFYING ALL DATABASE MIGRATIONS"
echo "========================================================================"
./scripts/migrate/migrate.sh

echo "========================================================================"
echo ">>> [STAGE 2/3] EXECUTING FEATURE-LEVEL AUTOMATED SCRIPTS"
echo "========================================================================"
FEATURE_TEST_SCRIPTS=(
    "./scripts/test/test.transactional_outbox.sh"
    "./scripts/test/test.idempotent_consumers.sh"
    "./scripts/test/test.dead_letter.sh"
    "./scripts/test/test.event_versioning.sh"
    "./scripts/test/test.event_ordering.sh"
    "./scripts/test/test.retry_topics.sh"
    "./scripts/test/test.consumer_groups.sh"
    "./scripts/test/test.offset_management.sh"
    "./scripts/test/test.backpressure.sh"
    "./scripts/test/test.replay.sh"
    "./scripts/test/test.multi_tenant.sh"
    "./scripts/test/test.cdc.sh"
    "./scripts/test/test.partitioning.sh"
    "./scripts/test/test.materialized_views.sh"
    "./scripts/test/test.eventual_consistency.sh"
    "./scripts/test/test.cqrs.sh"
    "./scripts/test/test.schema_evolution.sh"
    "./scripts/test/test.audit_trail.sh"
    "./scripts/test/test.encryption.sh"
    "./scripts/test/test.event_authorization.sh"
    "./scripts/test/test.domain_event_design.sh"
    "./scripts/test/test.event_operations_dashboard.sh"
    "./scripts/test/test.event_driven_communication.sh"
    "./scripts/test/test.distributed_tracing.sh"
)

for script in "${FEATURE_TEST_SCRIPTS[@]}"; do
    if [ -x "${script}" ]; then
        echo "--> Running ${script}..."
        "${script}"
    fi
done

echo "========================================================================"
echo ">>> [STAGE 3/3] EXECUTING FULL REAL CONTAINER INTEGRATION & TRACING SUITE"
echo "========================================================================"
./scripts/test/test.real.sh

echo "========================================================================"
echo ">>> [ALL PLATFORM TESTS PASSED SUCCESSFULLY! 100% COVERAGE & TRACING] <<<"
echo "========================================================================"
