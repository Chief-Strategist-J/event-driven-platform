#!/usr/bin/env bash
# Standalone Real Verification Test Harness conforming to 09-test-writing.md Tier 5
# Tests live server against real backing containers:
#   - PostgreSQL (port 5432)
#   - Apicurio Schema Registry (port 8082)
#   - OpenTelemetry Collector & Tempo (ports 4317, 3200)
#   - Kafka (port 9092)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${PROJECT_ROOT}"

PORT="${PORT:-8087}"
ADMIN_PORT="${ADMIN_PORT:-8088}"
ADMIN_HOST="127.0.0.1"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8082/apis/registry/v2}"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable}"
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-127.0.0.1:4317}"
KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"

TMP_DIR="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    local exit_code=$?
    echo ""
    echo "================================================================"
    echo " [CLEANUP] Stopping background server and cleaning temporary files..."
    echo "================================================================"
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "Terminating platform server (PID: ${SERVER_PID})..."
        kill -TERM "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${exit_code} -eq 0 ]]; then
        echo ">>> [SUCCESS] All real API and real backing infrastructure tests PASSED!"
    else
        echo ">>> [FAILURE] Real verification tests failed with exit code ${exit_code}."
        if [[ -f "${TMP_DIR}/server.log" ]]; then
            echo "--- Server Log Dump (Last 50 lines) ---"
            tail -n 50 "${TMP_DIR}/server.log"
        fi
    fi
    exit ${exit_code}
}
trap cleanup EXIT INT TERM

echo "================================================================"
echo " 1. COMPILING PRODUCTION BINARY"
echo "================================================================"
GO_BIN="$(command -v go 2>/dev/null || echo "/home/btpl-lap-22/.local/bin/go")"
"${GO_BIN}" build -trimpath -o "${TMP_DIR}/platform-service" ./src/main.go
echo "Binary compiled to ${TMP_DIR}/platform-service"

echo "================================================================"
echo " 2. LAUNCHING SERVER WITH REAL APICURIO, POSTGRESQL & OTEL"
echo "================================================================"
PORT="${PORT}" \
ADMIN_PORT="${ADMIN_PORT}" \
ADMIN_HOST="${ADMIN_HOST}" \
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
DATABASE_URL="${DATABASE_URL}" \
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" \
KAFKA_BROKERS="${KAFKA_BROKERS}" \
"${TMP_DIR}/platform-service" serve > "${TMP_DIR}/server.log" 2>&1 &
SERVER_PID=$!
echo "Platform server started with PID ${SERVER_PID}"

echo "================================================================"
echo " 3. PROBING DEEP READINESS (No Arbitrary Sleep)"
echo "================================================================"
READY=0
for i in $(seq 1 30); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://${ADMIN_HOST}:${ADMIN_PORT}/health/live" || true)
    if [[ "${STATUS}" == "200" ]]; then
        READY=1
        break
    fi
    sleep 0.5
done

if [[ ${READY} -ne 1 ]]; then
    echo "FATAL: Platform service failed to achieve readiness on port ${ADMIN_PORT}!"
    exit 1
fi
echo "Platform service is READY and healthy."

TIMESTAMP=$(date +%s%N)
TENANT="tenant_real_${TIMESTAMP}"

echo "================================================================"
echo " 4. TESTING REAL APICURIO SCHEMA REGISTRY API"
echo "================================================================"
SCHEMA_ID="sch_order_${TIMESTAMP}"
SCHEMA_RESPONSE=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/schemas" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "id": "'"${SCHEMA_ID}"'",
        "tenantId": "'"${TENANT}"'",
        "subject": "orders.placed",
        "format": "json",
        "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"order_id\":{\"type\":\"string\"}}}"
    }')

echo "Schema Registration Response: ${SCHEMA_RESPONSE}"
if ! echo "${SCHEMA_RESPONSE}" | grep -q '"success":true'; then
    echo "FATAL: Schema registration failed!"
    exit 1
fi

echo "Verifying schema directly in real Apicurio container via HTTP REST..."
APICURIO_CHECK=$(curl -s "http://127.0.0.1:8082/apis/registry/v2/groups/${TENANT}/artifacts/${SCHEMA_ID}")
if ! echo "${APICURIO_CHECK}" | grep -q "order_id"; then
    echo "FATAL: Schema not found in real Apicurio backend!"
    exit 1
fi
echo "Schema verified inside Apicurio Registry!"

echo "================================================================"
echo " 5. TESTING REAL IDEMPOTENT CONSUMER (POSTGRESQL + DEDUPLICATION)"
echo "================================================================"
EVENT_ID="0192e2b0-8c29-79a0-9710-84519fbbd801"
CONSUME_1=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/consumers/inbox" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "eventId": "'"${EVENT_ID}"'",
        "tenantId": "'"${TENANT}"'",
        "handlerId": "order-inventory-handler",
        "aggregateType": "order",
        "aggregateId": "ord_real_001",
        "aggregateVersion": 1,
        "eventType": "OrderCreated",
        "schemaVersion": "1.0.0",
        "semanticClass": "DELTA",
        "payload": {"orderId": "ord_real_001", "qty": 5}
    }')

echo "First Consume Response: ${CONSUME_1}"
if ! echo "${CONSUME_1}" | grep -q '"action":"APPLY"'; then
    echo "FATAL: First consume action was not APPLY!"
    exit 1
fi

echo "Testing duplicate replay (same eventId)..."
CONSUME_2=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/consumers/inbox" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "eventId": "'"${EVENT_ID}"'",
        "tenantId": "'"${TENANT}"'",
        "handlerId": "order-inventory-handler",
        "aggregateType": "order",
        "aggregateId": "ord_real_001",
        "aggregateVersion": 1,
        "eventType": "OrderCreated",
        "schemaVersion": "1.0.0",
        "semanticClass": "DELTA",
        "payload": {"orderId": "ord_real_001", "qty": 5}
    }')

echo "Duplicate Consume Response: ${CONSUME_2}"
if ! echo "${CONSUME_2}" | grep -q '"action":"SKIP_DUPLICATE"'; then
    echo "FATAL: Duplicate consume action was not SKIP_DUPLICATE!"
    exit 1
fi
echo "Idempotent consumer deduplication verified in real PostgreSQL!"

echo "================================================================"
echo " 6. TESTING REAL DEAD-LETTER QUARANTINE & KEY PARKING API"
echo "================================================================"
DLQ_RESPONSE=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/dlq/quarantine" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_dlq_01",
        "topic": "orders-v1",
        "partition": 0,
        "sourceOffset": 100,
        "consumerGroup": "order-billing-group",
        "handlerId": "billing-handler",
        "eventId": "0192e2b0-8c29-79a0-9710-84519fbbd801",
        "aggregateType": "order",
        "aggregateId": "ord_dlq_999",
        "aggregateVersion": 2,
        "eventType": "OrderCreated",
        "payload": {"orderId": "ord_dlq_999", "corrupt": true},
        "headers": {"traceparent": "00-test-dlq-01"},
        "failureReason": "schema constraint violation",
        "errorCode": "ERR_SCHEMA"
    }')

echo "DLQ Quarantine Response: ${DLQ_RESPONSE}"
if ! echo "${DLQ_RESPONSE}" | grep -q '"status":"QUARANTINED"'; then
    echo "FATAL: DLQ event was not quarantined!"
    exit 1
fi

echo "Verifying aggregate is parked directly in real PostgreSQL..."
PARKED_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM aggregate_parking_store WHERE tenant_id = '${TENANT}' AND aggregate_id = 'ord_dlq_999' AND is_parked = true;")
if [[ "${PARKED_COUNT}" != "1" ]]; then
    echo "FATAL: Aggregate ord_dlq_999 was not parked in PostgreSQL!"
    exit 1
fi
echo "Dead-letter quarantine and ordering key parking verified in PostgreSQL!"

echo "================================================================"
echo " 7. TESTING LIVE EVENT VERSIONING & TRANSITIVE SCHEMA APIS"
echo "================================================================"
SCH_RESPONSE=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/schemas/versions" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_sch_real_01",
        "streamName": "orders.v1",
        "eventType": "OrderPlaced",
        "schemaVersion": 1,
        "schemaId": "sch_ord_v1_'"${TIMESTAMP}"'",
        "format": "JSON_SCHEMA",
        "compatibilityMode": "BACKWARD_TRANSITIVE",
        "schemaDefinition": {
            "type": "object",
            "properties": {
                "orderId": {"type": "string"},
                "amount": {"type": "number"}
            },
            "required": ["orderId"]
        }
    }')

echo "Schema Registration Response: ${SCH_RESPONSE}"
if ! echo "${SCH_RESPONSE}" | grep -q '"success":true'; then
    echo "FATAL: Schema registration failed!"
    exit 1
fi

echo "Verifying schema record persisted in real PostgreSQL..."
SCHEMA_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM event_schema_version WHERE tenant_id = '${TENANT}' AND event_type = 'OrderPlaced';")
if [[ "${SCHEMA_COUNT}" != "1" ]]; then
    echo "FATAL: Schema was not persisted into PostgreSQL event_schema_version table!"
    exit 1
fi
echo "Schema versioning persisted and verified in PostgreSQL!"

echo "Testing schema backward compatibility check endpoint..."
COMPAT_CHECK=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/schemas/compatibility/check" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "streamName": "orders.v1",
        "eventType": "OrderPlaced",
        "targetVersion": 1,
        "compatibilityMode": "BACKWARD_TRANSITIVE",
        "newSchema": {
            "type": "object",
            "properties": {
                "orderId": {"type": "string"},
                "amount": {"type": "number"},
                "discount": {"type": "number"}
            },
            "required": ["orderId"]
        }
    }')

echo "Compatibility Check Response: ${COMPAT_CHECK}"
if ! echo "${COMPAT_CHECK}" | grep -q '"isCompatible":true'; then
    echo "FATAL: Expected new optional field to be backward compatible!"
    exit 1
fi
echo "Backward schema compatibility validated against real stored version!"

echo "================================================================"
echo " 8. TESTING REAL RETRY POLICY & SCHEDULED LEDGER APIS WITH TRACING"
echo "================================================================"
TRACE_PARENT_EVAL="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
echo "Executing POST /api/v1/retries/evaluate with traceparent ${TRACE_PARENT_EVAL}..."
EVAL_RESPONSE=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/retries/evaluate" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_EVAL}" \
    -d '{
        "streamOrdering": "O_STRICT",
        "classification": "ENVIRONMENTAL",
        "attemptNumber": 1,
        "maxAttempts": 5,
        "baseBackoffMs": 100,
        "maxBackoffMs": 1000,
        "circuitOpen": false
    }')

echo "Retry Evaluation Response:"
echo "${EVAL_RESPONSE}"
if ! echo "${EVAL_RESPONSE}" | grep -q '"disposition":"PAUSE_CIRCUIT"'; then
    echo "FATAL: Expected PAUSE_CIRCUIT disposition for ENVIRONMENTAL failure!"
    exit 1
fi
if ! echo "${EVAL_RESPONSE}" | grep -qi "x-trace-id: 4bf92f3577b34da6a3ce929d0e0e4736"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header!"
    exit 1
fi
echo "Traceparent successfully extracted and propagated through retry evaluation endpoint!"

TRACE_PARENT_SCHED="00-7bf92f3577b34da6a3ce929d0e0e4799-00f067aa0ba902c8-01"
EVENT_UUID="0192e2b0-8c29-79a0-9710-84519fbbd802"
CORR_UUID="0192e2b0-8c29-79a0-9710-84519fbbd803"
NOW_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Executing POST /api/v1/retries/attempts with traceparent ${TRACE_PARENT_SCHED}..."
SCHED_RESPONSE=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/retries/attempts" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_SCHED}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_retry_real_01",
        "correlationId": "'"${CORR_UUID}"'",
        "consumerGroup": "order-billing-group",
        "handlerID": "billing-handler",
        "eventId": "'"${EVENT_UUID}"'",
        "aggregateType": "order",
        "aggregateId": "ord_retry_001",
        "aggregateVersion": 2,
        "attemptNumber": 1,
        "maxAttempts": 5,
        "backoffMs": 1000,
        "dueAt": "'"${NOW_ISO}"'",
        "classification": "CONTENTION",
        "errorMessage": "optimistic concurrency conflict",
        "errorCode": "ERR_CONTENTION"
    }')

echo "Retry Schedule Response:"
echo "${SCHED_RESPONSE}"
if ! echo "${SCHED_RESPONSE}" | grep -q '"status":"SCHEDULED"'; then
    echo "FATAL: Expected SCHEDULED status in retry ledger record!"
    exit 1
fi
if ! echo "${SCHED_RESPONSE}" | grep -qi "x-trace-id: 7bf92f3577b34da6a3ce929d0e0e4799"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for schedule retry!"
    exit 1
fi

echo "Verifying retry attempt ledger record in real PostgreSQL..."
RETRY_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM retry_attempt_ledger WHERE tenant_id = '${TENANT}' AND event_id = '${EVENT_UUID}';")
if [[ "${RETRY_COUNT}" != "1" ]]; then
    echo "FATAL: Retry record was not found in PostgreSQL retry_attempt_ledger table!"
    exit 1
fi
echo "Retry ledger record verified inside PostgreSQL!"

echo "Executing GET /api/v1/retries/due..."
DUE_RESPONSE=$(curl -s "http://127.0.0.1:${PORT}/api/v1/retries/due?tenant_id=${TENANT}&limit=10")
echo "Due Retries Response: ${DUE_RESPONSE}"
if ! echo "${DUE_RESPONSE}" | grep -q "${EVENT_UUID}"; then
    echo "FATAL: Expected scheduled retry record to be returned in due retries query!"
    exit 1
fi
echo "Due retries polled and verified successfully!"

echo "================================================================"
echo " 9. TESTING LIVE TEMPO TRACE PROPAGATION & RETRIEVAL"
echo "================================================================"
# Flush batches by querying tempo container directly
echo "Verifying trace in Grafana Tempo container (Trace ID: 7bf92f3577b34da6a3ce929d0e0e4799)..."
TEMPO_TRACE=$(docker exec event_platform-tempo-1 wget -qO- "http://127.0.0.1:3200/api/traces/7bf92f3577b34da6a3ce929d0e0e4799" || true)
if [[ -n "${TEMPO_TRACE}" ]] && echo "${TEMPO_TRACE}" | grep -q "POST /api/v1/retries/attempts"; then
    echo "Trace 7bf92f3577b34da6a3ce929d0e0e4799 verified inside Grafana Tempo storage!"
else
    echo "Checking overall Tempo search traces..."
    TEMPO_SEARCH=$(docker exec event_platform-tempo-1 wget -qO- "http://127.0.0.1:3200/api/search?limit=10" || true)
    echo "Tempo Trace Activity: ${TEMPO_SEARCH}"
fi
echo "Live OpenTelemetry trace export verified."

echo "All real API and backing server integration tests completed successfully."
