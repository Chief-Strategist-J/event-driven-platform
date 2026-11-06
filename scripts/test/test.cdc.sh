#!/usr/bin/env bash
# Runs automated CDC integration tests against real Debezium, PostgreSQL, and Kafka
# Conforming to platform rules: zero manual commands, dynamic path discovery, strict error checking.
set -euo pipefail

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || echo "")"
PROJECT_ROOT="$(find_project_root "${SCRIPT_DIR:-$(pwd)}")"
cd "${PROJECT_ROOT}"

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
    echo "Error: docker binary not found in PATH or standard paths" >&2
    exit 1
fi

DEBEZIUM_URL="${DEBEZIUM_URL:-http://127.0.0.1:8083}"
KAFKA_BOOTSTRAP="kafka:9092"
POSTGRES_DB="event_store"

echo "================================================================"
echo " 1. VERIFYING BACKING CONTAINERS HEALTH"
echo "================================================================"
for svc in event_platform-db-1 event_platform-kafka-1 event_platform-debezium-1; do
    if ! "${DOCKER_BIN}" ps --format '{{.Names}}' | grep -q "^${svc}$"; then
        echo "Container ${svc} is not running! Starting with scripts/run/run.sh..."
        COMPOSE_PROFILES="observability,cdc" ./scripts/run/run.sh
        break
    fi
done

echo "Waiting for Debezium REST endpoint (${DEBEZIUM_URL})..."
MAX_RETRIES=30
RETRY_COUNT=0
until curl -s "${DEBEZIUM_URL}/" >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ ${RETRY_COUNT} -ge ${MAX_RETRIES} ]; then
        echo "Error: Debezium at ${DEBEZIUM_URL} failed to become reachable within timeout" >&2
        exit 1
    fi
    sleep 1
done
echo ">>> Debezium is reachable: $(curl -s "${DEBEZIUM_URL}/")"

echo "================================================================"
echo " 2. REGISTERING CDC OUTBOX CONNECTOR IN DEBEZIUM"
echo "================================================================"
CONNECTOR_NAME="pg-outbox-connector"

# Delete any existing test connector to start completely fresh
curl -s -X DELETE "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1 || true
sleep 1

REG_RESP=$(curl -s -i -X POST "${DEBEZIUM_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'"${CONNECTOR_NAME}"'",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "plugin.name": "pgoutput",
      "database.hostname": "db",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "'"${POSTGRES_DB}"'",
      "topic.prefix": "edp",
      "table.include.list": "public.outbox",
      "snapshot.mode": "never",
      "slot.name": "debezium_slot_outbox",
      "publication.name": "debezium_pub_outbox",
      "publication.autocreate.mode": "filtered",
      "heartbeat.interval.ms": "5000",
      "tombstones.on.delete": "false"
    }
  }')

echo "Connector registration response:"
echo "${REG_RESP}"

if ! echo "${REG_RESP}" | grep -q -E '(201 Created|409 Conflict)'; then
    echo "FATAL: Failed to register connector in Debezium!"
    exit 1
fi

echo "Waiting for connector task to transition to RUNNING state..."
TASK_STATE=""
for i in {1..30}; do
    STATUS_JSON=$(curl -s "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}/status" || true)
    TASK_STATE=$(echo "${STATUS_JSON}" | grep -o '"state":"RUNNING"' | head -n 1 || true)
    if [ -n "${TASK_STATE}" ]; then
        break
    fi
    sleep 1
done

echo "Current Connector Status:"
curl -s "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}/status" | grep -o '"state":"[^"]*"'
echo ""

echo "================================================================"
echo " 3. VERIFYING POSTGRESQL LOGICAL REPLICATION SLOT"
echo "================================================================"
SLOT_INFO=$("${DOCKER_BIN}" exec event_platform-db-1 psql -U postgres -d "${POSTGRES_DB}" -t -A -c \
    "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots WHERE slot_name = 'debezium_slot_outbox';")
echo "PostgreSQL Slot: ${SLOT_INFO}"

if ! echo "${SLOT_INFO}" | grep -q 'debezium_slot_outbox|pgoutput|logical|t'; then
    echo "FATAL: Expected active logical replication slot 'debezium_slot_outbox' in PostgreSQL!"
    exit 1
fi
echo ">>> PostgreSQL replication slot verified active!"

echo "================================================================"
echo " 4. PRODUCING TRANSACTIONAL OUTBOX EVENT IN POSTGRESQL"
echo "================================================================"
TEST_RUN_ID=$(date +%s)
TEST_AGG_ID="ord_cdc_script_${TEST_RUN_ID}"
SUFFIX_26=$(printf "%-26s" "${TEST_RUN_ID}abcdefghijklmnopqrstuvwxyz" | cut -c1-26)
TEST_PUB_ID="otb_${SUFFIX_26}"

"${DOCKER_BIN}" exec event_platform-db-1 psql -U postgres -d "${POSTGRES_DB}" -c "
INSERT INTO outbox (
    id,
    public_id,
    tenant_id,
    source_service,
    external_id,
    correlation_id,
    causation_id,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    event_type,
    schema_version,
    payload,
    headers,
    status
) VALUES (
    gen_random_uuid(),
    '${TEST_PUB_ID}',
    'tenant_cdc_script',
    'order-service',
    'ext_cdc_${TEST_RUN_ID}',
    gen_random_uuid(),
    gen_random_uuid(),
    'Order',
    '${TEST_AGG_ID}',
    1,
    'OrderCreated',
    1,
    '{\"order_id\":\"${TEST_AGG_ID}\",\"amount\":199.99}',
    '{\"traceparent\":\"00-cdc-script-${TEST_RUN_ID}-01\"}',
    'PENDING'
);"
echo ">>> Transaction committed to outbox table."

echo "================================================================"
echo " 5. VERIFYING CDC EVENT PROPAGATION IN KAFKA"
echo "================================================================"
KAFKA_TOPIC="edp.public.outbox"
echo "Polling Kafka topic ${KAFKA_TOPIC} for aggregate_id: ${TEST_AGG_ID}..."

CONSUMED_RECORD=$("${DOCKER_BIN}" exec -i event_platform-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --topic "${KAFKA_TOPIC}" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000 2>/dev/null || true)

if echo "${CONSUMED_RECORD}" | grep -q "${TEST_AGG_ID}"; then
    echo ">>> Successfully verified CDC record captured from PostgreSQL WAL to Kafka!"
else
    echo "FATAL: Did not observe expected outbox record in Kafka topic ${KAFKA_TOPIC}!"
    exit 1
fi

echo "================================================================"
echo ">>> [SUCCESS] Debezium and CDC end-to-end integration test PASSED!"
echo "================================================================"
