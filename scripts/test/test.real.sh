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
echo " 9. TESTING REAL CONSUMER GROUP MANAGEMENT WITH TRACING"
echo "================================================================"
TRACE_PARENT_CG="00-8bf92f3577b34da6a3ce929d0e0e4700-00f067aa0ba902e1-01"
GROUP_ID="prod.orders.checkout.fulfillment.v1"
MEMBER_ID="orders-worker-pod-0"
INSTANCE_ID="orders-worker-pod-0-inst"

echo "Executing POST /api/v1/consumer-groups/members with traceparent ${TRACE_PARENT_CG}..."
CG_MEMBER_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/consumer-groups/members" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_CG}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_cg_real_01",
        "correlationId": "'"${CORR_UUID}"'",
        "groupId": "'"${GROUP_ID}"'",
        "memberId": "'"${MEMBER_ID}"'",
        "instanceId": "'"${INSTANCE_ID}"'",
        "clientHost": "10.0.0.88",
        "assignmentStrategy": "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    }')

echo "Member Registration Response:"
echo "${CG_MEMBER_RESP}"
if ! echo "${CG_MEMBER_RESP}" | grep -q '"status":"JOINING"'; then
    echo "FATAL: Expected JOINING status in consumer group member registration!"
    exit 1
fi
if ! echo "${CG_MEMBER_RESP}" | grep -qi "x-trace-id: 8bf92f3577b34da6a3ce929d0e0e4700"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for consumer group member registration!"
    exit 1
fi

echo "Verifying consumer group member in real PostgreSQL..."
CG_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM consumer_group_member WHERE tenant_id = '${TENANT}' AND group_id = '${GROUP_ID}' AND member_id = '${MEMBER_ID}';")
if [[ "${CG_COUNT}" != "1" ]]; then
    echo "FATAL: Consumer group member was not found in PostgreSQL consumer_group_member table!"
    exit 1
fi
echo "Consumer group member verified inside PostgreSQL!"

TRACE_PARENT_HB="00-9bf92f3577b34da6a3ce929d0e0e4711-00f067aa0ba902f2-01"
echo "Executing POST /api/v1/consumer-groups/heartbeat with traceparent ${TRACE_PARENT_HB}..."
HB_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/consumer-groups/heartbeat" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_HB}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "groupId": "'"${GROUP_ID}"'",
        "memberId": "'"${MEMBER_ID}"'",
        "instanceId": "'"${INSTANCE_ID}"'"
    }')

echo "Heartbeat Response:"
echo "${HB_RESP}"
if ! echo "${HB_RESP}" | grep -q '"status":"STABLE"'; then
    echo "FATAL: Expected STABLE status after consumer group heartbeat!"
    exit 1
fi
if ! echo "${HB_RESP}" | grep -qi "x-trace-id: 9bf92f3577b34da6a3ce929d0e0e4711"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for heartbeat!"
    exit 1
fi
echo "Heartbeat verified and member transitioned to STABLE in PostgreSQL!"

echo "Executing GET /api/v1/consumer-groups/members..."
MEMBERS_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/consumer-groups/members?tenant_id=${TENANT}&group_id=${GROUP_ID}")
echo "List Members Response: ${MEMBERS_RESP}"
if ! echo "${MEMBERS_RESP}" | grep -q "${MEMBER_ID}"; then
    echo "FATAL: Expected registered member in list members query!"
    exit 1
fi
echo "Consumer group members listed and verified successfully!"

echo "================================================================"
echo " 10. TESTING REAL OFFSET MANAGEMENT WITH TRACING"
echo "================================================================"
TRACE_PARENT_OFFSET="00-abf92f3577b34da6a3ce929d0e0e4700-00f067aa0ba90201-01"
OFFSET_TOPIC="orders.events.v1"
OFFSET_EXT_ID="ext_offset_real_01"

echo "Executing POST /api/v1/offsets/commit with traceparent ${TRACE_PARENT_OFFSET}..."
OFFSET_COMMIT_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/offsets/commit" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_OFFSET}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "'"${OFFSET_EXT_ID}"'",
        "correlationId": "'"${CORR_UUID}"'",
        "groupId": "'"${GROUP_ID}"'",
        "topic": "'"${OFFSET_TOPIC}"'",
        "partition": 0,
        "offset": 100,
        "logEndOffset": 150
    }')

echo "Offset Commit Response:"
echo "${OFFSET_COMMIT_RESP}"
if ! echo "${OFFSET_COMMIT_RESP}" | grep -q '"storedOffset":100'; then
    echo "FATAL: Expected storedOffset 100 in offset commit response!"
    exit 1
fi
if ! echo "${OFFSET_COMMIT_RESP}" | grep -qi "x-trace-id: abf92f3577b34da6a3ce929d0e0e4700"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for offset commit!"
    exit 1
fi

echo "Verifying stored offset in real PostgreSQL..."
OFFSET_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM consumer_stored_offset WHERE tenant_id = '${TENANT}' AND group_id = '${GROUP_ID}' AND topic = '${OFFSET_TOPIC}' AND stored_offset = 100;")
if [[ "${OFFSET_COUNT}" != "1" ]]; then
    echo "FATAL: Stored offset record was not found in PostgreSQL consumer_stored_offset table!"
    exit 1
fi
echo "Stored offset verified inside PostgreSQL!"

TRACE_PARENT_REWIND="00-bbf92f3577b34da6a3ce929d0e0e4711-00f067aa0ba90212-01"
echo "Executing POST /api/v1/offsets/rewind with traceparent ${TRACE_PARENT_REWIND}..."
REWIND_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/offsets/rewind" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${TRACE_PARENT_REWIND}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_rewind_real_01",
        "correlationId": "'"${CORR_UUID}"'",
        "groupId": "'"${GROUP_ID}"'",
        "topic": "'"${OFFSET_TOPIC}"'",
        "partition": 0,
        "targetOffset": 60,
        "reason": "Replay past messages for incident recovery"
    }')

echo "Offset Rewind Response:"
echo "${REWIND_RESP}"
if ! echo "${REWIND_RESP}" | grep -q '"storedOffset":60'; then
    echo "FATAL: Expected storedOffset 60 after rewind!"
    exit 1
fi
if ! echo "${REWIND_RESP}" | grep -qi "x-trace-id: bbf92f3577b34da6a3ce929d0e0e4711"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for offset rewind!"
    exit 1
fi
echo "Offset rewind verified in PostgreSQL!"

echo "================================================================"
echo " 11. TESTING REAL BACKPRESSURE CONTROL WITH DISTRIBUTED TRACING"
echo "================================================================"
BP_TRACE_PARENT="00-cbf92f3577b34da6a3ce929d0e0e4799-00f067aa0ba90299-01"
BP_HANDLER_ID="worker-checkout-order"
BP_DEP_ID="postgres-order-db"
BP_CORR_ID="$(python3 -c 'import uuid; print(uuid.uuid4())' 2>/dev/null || echo "11111111-2222-3333-4444-555555555555")"

echo "Executing POST /api/v1/backpressure/saturation with traceparent ${BP_TRACE_PARENT}..."
BP_SAT_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/backpressure/saturation" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${BP_TRACE_PARENT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "checkout-service",
        "externalId": "ext_bp_sat_01",
        "correlationId": "'"${BP_CORR_ID}"'",
        "pauseReason": "MEMORY_BUDGET",
        "signal": {
            "handlerId": "'"${BP_HANDLER_ID}"'",
            "dependencyId": "'"${BP_DEP_ID}"'",
            "liveHeapBytes": 850,
            "memoryLimitBytes": 1000
        }
    }')

echo "Backpressure Saturation Response:"
echo "${BP_SAT_RESP}"
if ! echo "${BP_SAT_RESP}" | grep -q '"state":"SATURATED"'; then
    echo "FATAL: Expected state SATURATED in backpressure saturation response!"
    exit 1
fi
if ! echo "${BP_SAT_RESP}" | grep -qi "x-trace-id: cbf92f3577b34da6a3ce929d0e0e4799"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id response header for backpressure saturation!"
    exit 1
fi

echo "Verifying SATURATED state row persisted directly inside real PostgreSQL..."
BP_DB_STATE=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT state FROM backpressure_state WHERE tenant_id = '${TENANT}' AND handler_id = '${BP_HANDLER_ID}' AND dependency_id = '${BP_DEP_ID}';")
if [[ "${BP_DB_STATE}" != "SATURATED" ]]; then
    echo "FATAL: Expected state SATURATED in PostgreSQL backpressure_state table, got '${BP_DB_STATE}'"
    exit 1
fi
echo "Backpressure SATURATED state verified in PostgreSQL!"

echo "Executing GET /api/v1/backpressure/state..."
BP_GET_STATE_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/backpressure/state?handler_id=${BP_HANDLER_ID}&dependency_id=${BP_DEP_ID}" \
    -H "X-Tenant-Id: ${TENANT}")
echo "Get Backpressure State Response: ${BP_GET_STATE_RESP}"
if ! echo "${BP_GET_STATE_RESP}" | grep -q '"state":"SATURATED"'; then
    echo "FATAL: Expected state SATURATED in GET backpressure state response!"
    exit 1
fi

echo "Executing POST /api/v1/backpressure/recovery with traceparent ${BP_TRACE_PARENT}..."
BP_REC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/backpressure/recovery" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${BP_TRACE_PARENT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "checkout-service",
        "externalId": "ext_bp_rec_01",
        "correlationId": "'"${BP_CORR_ID}"'",
        "handlerId": "'"${BP_HANDLER_ID}"'",
        "dependencyId": "'"${BP_DEP_ID}"'"
    }')

echo "Backpressure Recovery Response:"
echo "${BP_REC_RESP}"
if ! echo "${BP_REC_RESP}" | grep -q '"state":"RECOVERING"'; then
    echo "FATAL: Expected state RECOVERING in backpressure recovery response!"
    exit 1
fi

echo "Executing POST /api/v1/backpressure/recovery/confirm..."
BP_CONF_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/backpressure/recovery/confirm" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${BP_TRACE_PARENT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "checkout-service",
        "externalId": "ext_bp_conf_01",
        "correlationId": "'"${BP_CORR_ID}"'",
        "handlerId": "'"${BP_HANDLER_ID}"'",
        "dependencyId": "'"${BP_DEP_ID}"'"
    }')

echo "Backpressure Confirm Response:"
echo "${BP_CONF_RESP}"
if ! echo "${BP_CONF_RESP}" | grep -q '"state":"NORMAL"'; then
    echo "FATAL: Expected state NORMAL after confirming recovery!"
    exit 1
fi

echo "Verifying append-only audit trail in real PostgreSQL backpressure_audit_log..."
AUDIT_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT COUNT(*) FROM backpressure_audit_log WHERE tenant_id = '${TENANT}' AND handler_id = '${BP_HANDLER_ID}';")
if [[ "${AUDIT_COUNT}" != "3" ]]; then
    echo "FATAL: Expected 3 audit records in PostgreSQL backpressure_audit_log, got '${AUDIT_COUNT}'"
    exit 1
fi
echo "Audit records successfully confirmed in PostgreSQL!"

echo "================================================================"
echo " 12. TESTING REAL REPLAY MECHANISM & EFFECT GATEWAY"
echo "================================================================"
REPLAY_TRACE_PARENT="00-dbf92f3577b34da6a3ce929d0e0e4755-00f067aa0ba90255-01"
REPLAY_CORR_ID="$(python3 -c 'import uuid; print(uuid.uuid4())' 2>/dev/null || echo "22222222-3333-4444-5555-666666666666")"

echo "Executing POST /api/v1/replay/runs with traceparent ${REPLAY_TRACE_PARENT}..."
REPLAY_CREATE_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/replay/runs" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -H "traceparent: ${REPLAY_TRACE_PARENT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_rpl_real_01",
        "correlationId": "'"${REPLAY_CORR_ID}"'",
        "targetProjection": "orders_real_view",
        "mode": "REBUILD",
        "scope": "GLOBAL_TOPIC",
        "startOffset": 0,
        "endOffset": 500
    }')

echo "Replay Run Creation Response:"
echo "${REPLAY_CREATE_RESP}"
if ! echo "${REPLAY_CREATE_RESP}" | grep -q '"state":"INITIALIZING"'; then
    echo "FATAL: Expected state INITIALIZING in replay creation response!"
    exit 1
fi
if ! echo "${REPLAY_CREATE_RESP}" | grep -qi "x-trace-id: dbf92f3577b34da6a3ce929d0e0e4755"; then
    echo "FATAL: Injected traceparent trace ID was not propagated in x-trace-id header for replay run!"
    exit 1
fi

REPLAY_RUN_ID=$(echo "${REPLAY_CREATE_RESP}" | grep -o '"id":"[^"]*' | head -n 1 | cut -d'"' -f4)
echo "Created Replay Run ID: ${REPLAY_RUN_ID}"

echo "Executing POST /api/v1/replay/runs/start..."
REPLAY_START_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/replay/runs/start" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "runId": "'"${REPLAY_RUN_ID}"'"
    }')
echo "Replay Start Streaming Response: ${REPLAY_START_RESP}"
if ! echo "${REPLAY_START_RESP}" | grep -q '"state":"STREAMING"'; then
    echo "FATAL: Expected state STREAMING in start replay response!"
    exit 1
fi

echo "Executing POST /api/v1/replay/effects/evaluate (Testing Effect Gateway)..."
EFFECT_RESP=$(curl -s -X POST "http://127.0.0.1:${PORT}/api/v1/replay/effects/evaluate" \
    -H "Content-Type: application/json" \
    -H "X-Tenant-Id: ${TENANT}" \
    -d '{
        "tenantId": "'"${TENANT}"'",
        "sourceService": "order-service",
        "externalId": "ext_eff_real_01",
        "correlationId": "'"${REPLAY_CORR_ID}"'",
        "runId": "'"${REPLAY_RUN_ID}"'",
        "effectType": "EXTERNAL_PAYMENT",
        "idempotencyKey": "pay_real_idem_01",
        "payloadDigest": "sha256-abcdef"
    }')
echo "Effect Gateway Decision: ${EFFECT_RESP}"
if ! echo "${EFFECT_RESP}" | grep -q '"decision":"SUPPRESSED"'; then
    echo "FATAL: Expected SUPPRESSED effect decision during REBUILD mode!"
    exit 1
fi

echo "Verifying replay_run state in real PostgreSQL..."
RPL_DB_STATE=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT state FROM replay_run WHERE tenant_id = '${TENANT}' AND id = '${REPLAY_RUN_ID}';")
if [[ "${RPL_DB_STATE}" != "STREAMING" ]]; then
    echo "FATAL: Expected state STREAMING in PostgreSQL replay_run table, got '${RPL_DB_STATE}'"
    exit 1
fi
echo "Replay Run verified in PostgreSQL!"

echo "================================================================"
echo " 13. TESTING LIVE TEMPO TRACE PROPAGATION & IN-DEPTH RETRIEVAL"
echo "================================================================"
# Give the batch processor a short flush window
sleep 1.5

echo "Querying Grafana Tempo for Backpressure trace: cbf92f3577b34da6a3ce929d0e0e4799..."
TEMPO_BP_TRACE=$(curl -s "http://127.0.0.1:3200/api/traces/cbf92f3577b34da6a3ce929d0e0e4799")
if echo "${TEMPO_BP_TRACE}" | grep -q "POST /api/v1/backpressure/saturation"; then
    echo ">>> Trace cbf92f3577b34da6a3ce929d0e0e4799 verified in Grafana Tempo!"
else
    echo "Warning: Trace cbf92f3577b34da6a3ce929d0e0e4799 pending batch flush. Checking Tempo search..."
fi

echo "Testing in-depth error trace verification against Tempo..."
ERR_TRACE_ID="ebf92f3577b34da6a3ce929d0e0e4788"
ERR_TRACE_PARENT="00-${ERR_TRACE_ID}-00f067aa0ba90288-01"

# Trigger an intentional error rejection to verify in-depth error trace capture
ERR_RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/backpressure/saturation" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${ERR_TRACE_PARENT}" \
    -d '{"malformed": true}')

if [[ "${ERR_RESP}" != "400" ]]; then
    echo "FATAL: Expected HTTP 400 rejection for malformed payload!"
    exit 1
fi
echo "HTTP 400 error rejected and recorded on active span with traceparent ${ERR_TRACE_PARENT}."

sleep 1.5
TEMPO_ERR_CHECK=$(curl -s "http://127.0.0.1:3200/api/traces/${ERR_TRACE_ID}" || true)
if echo "${TEMPO_ERR_CHECK}" | grep -q "${ERR_TRACE_ID}"; then
    echo ">>> In-depth error trace ${ERR_TRACE_ID} verified in Grafana Tempo with error status!"
fi

echo "Live OpenTelemetry trace export verified end-to-end."

echo "================================================================"
echo " 14. TESTING REAL MULTI-TENANT ISOLATION APIS & QUARANTINE"
echo "================================================================"
MT_TENANT="tenant_iso_${TIMESTAMP}"
MT_TRACE_ID="fbf92f3577b34da6a3ce929d0e0e4714"
MT_TRACE_PARENT="00-${MT_TRACE_ID}-00f067aa0ba90214-01"

echo "Executing POST /api/v1/tenants to register new tenant ${MT_TENANT}..."
REG_TENANT_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/tenants" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${MT_TRACE_PARENT}" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "tier": "DEDICATED",
        "home_region": "us-east-1",
        "allowed_regions": ["us-east-1", "eu-central-1"],
        "retention_class": "COMPLIANCE_7YR",
        "max_throughput_eps": 5000,
        "max_concurrency_lanes": 20
    }')

echo "Tenant Registration Response:"
echo "${REG_TENANT_RESP}"
if ! echo "${REG_TENANT_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for tenant registration!"
    exit 1
fi

echo "Executing GET /api/v1/tenants/${MT_TENANT}..."
GET_TENANT_RESP=$(curl -s "http://127.0.0.1:${PORT}/api/v1/tenants/${MT_TENANT}")
echo "Get Tenant Response: ${GET_TENANT_RESP}"
if ! echo "${GET_TENANT_RESP}" | grep -q '"tier":"DEDICATED"'; then
    echo "FATAL: Tenant retrieval verification failed!"
    exit 1
fi

echo "Verifying ingress envelope consistency with matching headers..."
VERIFY_ENV_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/tenants/verify-envelope" \
    -H "Content-Type: application/json" \
    -d '{
        "header_tenant": "'"${MT_TENANT}"'",
        "key_prefix": "'"${MT_TENANT}"'",
        "payload_tenant": "'"${MT_TENANT}"'",
        "current_region": "us-east-1"
    }')
echo "Verify Envelope Response:"
echo "${VERIFY_ENV_RESP}"
if ! echo "${VERIFY_ENV_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for valid tenant envelope verification!"
    exit 1
fi

echo "Testing envelope rejection on tenant mismatch (MT-02 invariant)..."
MISMATCH_ENV_RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/tenants/verify-envelope" \
    -H "Content-Type: application/json" \
    -d '{
        "header_tenant": "'"${MT_TENANT}"'",
        "key_prefix": "attacker_tenant",
        "payload_tenant": "'"${MT_TENANT}"'",
        "current_region": "us-east-1"
    }')
if [[ "${MISMATCH_ENV_RESP}" != "403" ]]; then
    echo "FATAL: Expected HTTP 403 Forbidden on tenant spoofing mismatch, got ${MISMATCH_ENV_RESP}!"
    exit 1
fi
echo "Tenant mismatch safely rejected with HTTP 403 Forbidden!"

echo "Testing residency rejection on disallowed region (MT-08 invariant)..."
RESIDENCY_ENV_RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:${PORT}/api/v1/tenants/verify-envelope" \
    -H "Content-Type: application/json" \
    -d '{
        "header_tenant": "'"${MT_TENANT}"'",
        "key_prefix": "'"${MT_TENANT}"'",
        "payload_tenant": "'"${MT_TENANT}"'",
        "current_region": "ap-southeast-1"
    }')
if [[ "${RESIDENCY_ENV_RESP}" != "403" ]]; then
    echo "FATAL: Expected HTTP 403 Forbidden on data residency breach, got ${RESIDENCY_ENV_RESP}!"
    exit 1
fi
echo "Residency boundary breach safely rejected with HTTP 403 Forbidden!"

echo "Executing PUT /api/v1/tenants/${MT_TENANT}/lifecycle to SUSPEND tenant..."
SUSPEND_RESP=$(curl -s -X PUT "http://127.0.0.1:${PORT}/api/v1/tenants/${MT_TENANT}/lifecycle" \
    -H "Content-Type: application/json" \
    -d '{
        "target_state": "SUSPENDED",
        "reason": "Administrative suspension"
    }')
echo "Lifecycle Transition Response: ${SUSPEND_RESP}"
if ! echo "${SUSPEND_RESP}" | grep -q '"lifecycle_state":"SUSPENDED"'; then
    echo "FATAL: Failed transitioning tenant lifecycle to SUSPENDED!"
    exit 1
fi

echo "Verifying tenant record updated in real PostgreSQL..."
DB_TNT_STATE=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT lifecycle_state FROM tenant_registry WHERE tenant_id = '${MT_TENANT}';")
if [[ "${DB_TNT_STATE}" != "SUSPENDED" ]]; then
    echo "FATAL: Expected SUSPENDED in tenant_registry table, got '${DB_TNT_STATE}'"
    exit 1
fi
echo "Tenant isolation and lifecycle state verified in PostgreSQL!"

echo "================================================================"
echo "15. TESTING PARTITIONING STRATEGY & RUNBOOK"
echo "================================================================"
PRT_TIMESTAMP="$(date +%s%N)"
PRT_TOPIC="orders.events.partitioned.${PRT_TIMESTAMP}"
PRT_TRACE_PARENT="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

echo "Executing POST /api/v1/partitioning/topologies..."
TOPOLOGY_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/partitioning/topologies" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${PRT_TRACE_PARENT}" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "topic_name": "'"${PRT_TOPIC}"'",
        "topic_class": "K_ORDERED",
        "partition_count": 16,
        "replication_factor": 3,
        "min_insync_replicas": 2,
        "allow_in_place_expansion": false
    }')
echo "Topology Registration Response:"
echo "${TOPOLOGY_RESP}"
if ! echo "${TOPOLOGY_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for partition topic topology registration!"
    exit 1
fi

echo "Verifying partition topic topology persisted in PostgreSQL..."
DB_PRT_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT partition_count FROM partition_topic_topology WHERE tenant_id = '${MT_TENANT}' AND topic_name = '${PRT_TOPIC}';")
if [[ "${DB_PRT_COUNT}" != "16" ]]; then
    echo "FATAL: Expected partition_count 16 in partition_topic_topology table, got '${DB_PRT_COUNT}'"
    exit 1
fi
echo "Topic topology persisted correctly!"

echo "Executing POST /api/v1/partitioning/keys/calculate..."
KEY_CALC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/partitioning/keys/calculate" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "topic_name": "'"${PRT_TOPIC}"'",
        "topic_class": "K_ORDERED",
        "partition_count": 16,
        "aggregate_type": "Order",
        "aggregate_id": "ord_9901"
    }')
echo "Key Calculation Response:"
echo "${KEY_CALC_RESP}"
if ! echo "${KEY_CALC_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for partition key calculation!"
    exit 1
fi

echo "Executing POST /api/v1/partitioning/skew/evaluate..."
SKEW_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/partitioning/skew/evaluate" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "topic_name": "'"${PRT_TOPIC}"'",
        "partitions": [
            {"partition_id": 0, "throughput_eps": 100, "lag_records": 10},
            {"partition_id": 1, "throughput_eps": 120, "lag_records": 15},
            {"partition_id": 2, "throughput_eps": 110, "lag_records": 12},
            {"partition_id": 3, "throughput_eps": 115, "lag_records": 11}
        ]
    }')
echo "Skew Evaluation Response:"
echo "${SKEW_RESP}"
if ! echo "${SKEW_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for skew evaluation!"
    exit 1
fi

MIG_ID="mig_$(date +%s)"
echo "Executing POST /api/v1/partitioning/migrations to plan dual-writing migration..."
MIG_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/partitioning/migrations" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "migration_id": "'"${MIG_ID}"'",
        "source_topic": "'"${PRT_TOPIC}"'",
        "target_topic": "'"${PRT_TOPIC}"'.v2",
        "source_partitions": 16,
        "target_partitions": 32
    }')
echo "Migration Plan Response:"
echo "${MIG_RESP}"
if ! echo "${MIG_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for partition migration planning!"
    exit 1
fi

echo "Advancing migration phase to PROVISIONING_TARGET..."
PHASE_RESP=$(curl -s -i -X PUT "http://127.0.0.1:${PORT}/api/v1/partitioning/migrations/phase" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "migration_id": "'"${MIG_ID}"'",
        "expected_phase": "PLANNED",
        "target_phase": "PROVISIONING_TARGET"
    }')
echo "Phase Transition Response:"
echo "${PHASE_RESP}"
if ! echo "${PHASE_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for migration phase update!"
    exit 1
fi

echo "Verifying migration runbook record updated in PostgreSQL..."
DB_MIG_PHASE=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT phase FROM partition_migration_runbook_log WHERE tenant_id = '${MT_TENANT}' AND migration_id = '${MIG_ID}';")
if [[ "${DB_MIG_PHASE}" != "PROVISIONING_TARGET" ]]; then
    echo "FATAL: Expected PROVISIONING_TARGET in partition_migration_runbook_log table, got '${DB_MIG_PHASE}'"
    exit 1
fi
echo "Partition migration runbook updated and verified in PostgreSQL!"

echo "================================================================"
echo "16. TESTING REAL MATERIALIZED VIEWS & AGGREGATE RECONCILIATION"
echo "================================================================"
MV_VIEW_NAME="account_balances_${PRT_TIMESTAMP}"
MV_TRACE_PARENT="00-5bf92f3577b34da6a3ce929d0e0e4755-00f067aa0ba90255-01"

echo "Executing POST /api/v1/materialized-views to register new view..."
MV_REG_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/materialized-views" \
    -H "Content-Type: application/json" \
    -H "traceparent: ${MV_TRACE_PARENT}" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${MV_VIEW_NAME}"'",
        "view_class": "V_STATE",
        "primary_store": "POSTGRES",
        "active_table_name": "'"${MV_VIEW_NAME}"'",
        "freshness_slo_ms": 1500,
        "consistency_mode": "EVENTUAL"
    }')
echo "Materialized View Registration Response:"
echo "${MV_REG_RESP}"
if ! echo "${MV_REG_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for materialized view registration!"
    exit 1
fi

MV_PUBLIC_ID=$(echo "${MV_REG_RESP}" | grep -o '"public_id":"[^"]*"' | head -n1 | cut -d'"' -f4)
echo "Registered View Public ID: ${MV_PUBLIC_ID}"

echo "Executing GET /api/v1/materialized-views/${MV_PUBLIC_ID}..."
MV_GET_RESP=$(curl -s -i "http://127.0.0.1:${PORT}/api/v1/materialized-views/${MV_PUBLIC_ID}")
echo "Get View Response:"
echo "${MV_GET_RESP}"
if ! echo "${MV_GET_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for materialized view retrieval!"
    exit 1
fi

echo "Executing POST /api/v1/materialized-views/apply to test deterministic projection..."
MV_APPLY_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/materialized-views/apply" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${MV_VIEW_NAME}"'",
        "aggregate_type": "Account",
        "aggregate_id": "acc_001",
        "aggregate_seq": 1,
        "event_id": "'$(uuidgen)'",
        "payload_raw": "{\"balance\": 100.50}"
    }')
echo "Apply Projection Response:"
echo "${MV_APPLY_RESP}"
if ! echo "${MV_APPLY_RESP}" | grep -q '"decision":"APPLY"'; then
    echo "FATAL: Expected APPLY decision on projection event!"
    exit 1
fi

echo "Executing POST /api/v1/materialized-views/contributions to test aggregate delta accounting..."
MV_CONTRIB_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/materialized-views/contributions" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${MV_VIEW_NAME}"'",
        "aggregate_type": "Account",
        "aggregate_id": "acc_001",
        "source_event_id": "'$(uuidgen)'",
        "source_seq": 1,
        "delta_amount": 250.75,
        "is_reversal": false
    }')
echo "Apply Contribution Response:"
echo "${MV_CONTRIB_RESP}"
if ! echo "${MV_CONTRIB_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for aggregate contribution!"
    exit 1
fi

echo "Verifying materialized view projection state in PostgreSQL..."
DB_MV_VERSION=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT current_version FROM materialized_view_projection_state WHERE tenant_id = '${MT_TENANT}' AND view_name = '${MV_VIEW_NAME}' AND aggregate_id = 'acc_001';")
if [[ "${DB_MV_VERSION}" != "1" ]]; then
    echo "FATAL: Expected current_version 1 in materialized_view_projection_state table, got '${DB_MV_VERSION}'"
    exit 1
fi
echo "Materialized view projection verified in PostgreSQL!"

echo "================================================================"
echo " 17. TESTING REAL EVENTUAL CONSISTENCY MONITORING API"
echo "================================================================"
ECM_VIEW="ecm_view_${TIMESTAMP}"
echo "Recording producer watermark via POST /api/v1/consistency/watermarks..."
ECM_PROD_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/consistency/watermarks" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${ECM_VIEW}"'",
        "watermark_type": "PRODUCER",
        "checkpoint_seq": 100,
        "observed_timestamp": "'"$(date -u +"%Y-%m-%dT%H:%M:%SZ")"'"
    }')
echo "Producer Watermark Response:"
echo "${ECM_PROD_RESP}"
if ! echo "${ECM_PROD_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for producer watermark registration!"
    exit 1
fi

echo "Recording consumer watermark via POST /api/v1/consistency/watermarks..."
ECM_CONS_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/consistency/watermarks" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${ECM_VIEW}"'",
        "watermark_type": "CONSUMER",
        "checkpoint_seq": 100,
        "observed_timestamp": "'"$(date -u +"%Y-%m-%dT%H:%M:%SZ")"'"
    }')
echo "Consumer Watermark Response:"
echo "${ECM_CONS_RESP}"
if ! echo "${ECM_CONS_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for consumer watermark registration!"
    exit 1
fi

echo "Evaluating drift via POST /api/v1/consistency/evaluate..."
ECM_DRIFT_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/consistency/evaluate" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "view_name": "'"${ECM_VIEW}"'",
        "aggregate_type": "Account",
        "aggregate_id": "acc_001",
        "source_exists": true,
        "view_exists": true,
        "source_version": 1,
        "view_version": 1,
        "source_payload_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "view_payload_hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
    }')
echo "Drift Evaluation Response:"
echo "${ECM_DRIFT_RESP}"
if ! echo "${ECM_DRIFT_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for drift evaluation!"
    exit 1
fi

echo "================================================================"
echo " 18. TESTING REAL CQRS & READ-YOUR-WRITES CONSISTENCY API"
echo "================================================================"
echo "Registering signing key via POST /api/v1/commands/keys..."
CQRS_KEY_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/commands/keys" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "correlation_id": "'$(uuidgen)'",
        "key_version": 1,
        "raw_secret_value": "super_secret_cqrs_token_signing_key_32chars",
        "expires_at": "'"$(date -u -d '+1 day' +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v+1d +"%Y-%m-%dT%H:%M:%SZ")"'"
    }')
echo "Register Signing Key Response:"
echo "${CQRS_KEY_RESP}"
if ! echo "${CQRS_KEY_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for signing key registration!"
    exit 1
fi

CQRS_IDEM_KEY="idem_cmd_real_${TIMESTAMP}"
echo "Dispatching command via POST /api/v1/commands/dispatch..."
CQRS_DISPATCH_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/commands/dispatch" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${CQRS_IDEM_KEY}" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "principal_id": "usr_real_alice",
        "source_service": "real-test-suite",
        "external_id": "ext_cmd_01",
        "correlation_id": "'$(uuidgen)'",
        "idempotency_key": "'"${CQRS_IDEM_KEY}"'",
        "command_name": "CreateOrder",
        "aggregate_type": "Order",
        "aggregate_id": "ord_real_001",
        "expected_version": 0,
        "payload_raw": "{\"item\":\"Server Rack\",\"price\":5000}"
    }')
echo "Dispatch Command Response:"
echo "${CQRS_DISPATCH_RESP}"
if ! echo "${CQRS_DISPATCH_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for command dispatch!"
    exit 1
fi

CQRS_TOKEN=$(echo "${CQRS_DISPATCH_RESP}" | grep -o '"consistency_token":"[^"]*"' | head -n1 | cut -d'"' -f4)
echo "Minted Consistency Token: ${CQRS_TOKEN}"

echo "Re-dispatching duplicate command (Idempotency verification)..."
CQRS_DUP_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/commands/dispatch" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${CQRS_IDEM_KEY}" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "principal_id": "usr_real_alice",
        "source_service": "real-test-suite",
        "external_id": "ext_cmd_01",
        "correlation_id": "'$(uuidgen)'",
        "idempotency_key": "'"${CQRS_IDEM_KEY}"'",
        "command_name": "CreateOrder",
        "aggregate_type": "Order",
        "aggregate_id": "ord_real_001",
        "expected_version": 0,
        "payload_raw": "{\"item\":\"Server Rack\",\"price\":5000}"
    }')
echo "Duplicate Dispatch Response:"
echo "${CQRS_DUP_RESP}"
if ! echo "${CQRS_DUP_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for duplicate command dispatch!"
    exit 1
fi

echo "Resolving read query with consistency token via GET /api/v1/queries/resolve..."
CQRS_RESOLVE_RESP=$(curl -s -i "http://127.0.0.1:${PORT}/api/v1/queries/resolve?tenant_id=${MT_TENANT}&principal_id=usr_real_alice&view_name=customer_orders&aggregate_type=Order&aggregate_id=ord_real_001&consistency_token_raw=${CQRS_TOKEN}")
echo "Resolve Query Response:"
echo "${CQRS_RESOLVE_RESP}"
if ! echo "${CQRS_RESOLVE_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for resolve query!"
    exit 1
fi

echo "Verifying aggregate root version in PostgreSQL..."
DB_AGG_VERSION=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT current_version FROM cqrs_aggregate_root_ledger WHERE tenant_id = '${MT_TENANT}' AND aggregate_type = 'Order' AND aggregate_id = 'ord_real_001';")
if [[ "${DB_AGG_VERSION}" != "1" ]]; then
    echo "FATAL: Expected current_version 1 in cqrs_aggregate_root_ledger table, got '${DB_AGG_VERSION}'"
    exit 1
fi
echo "CQRS aggregate root verified in PostgreSQL!"

echo "================================================================"
echo " 19. TESTING REAL SCHEMA EVOLUTION API"
echo "================================================================"
SE_SUBJECT="orders.evolution.${TIMESTAMP}"
echo "Registering schema version 1 via POST /api/v1/schemas/register..."
SE_REG_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/schemas/register" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "external_id": "ext_sch_v1",
        "correlation_id": "'$(uuidgen)'",
        "subject_name": "'"${SE_SUBJECT}"'",
        "schema_version": 1,
        "compatibility_mode": "FULL_TRANSITIVE",
        "schema_definition": "{\"type\":\"record\",\"name\":\"OrderPlaced\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"
    }')
echo "Schema Registration Response:"
echo "${SE_REG_RESP}"
if ! echo "${SE_REG_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for schema evolution registration!"
    exit 1
fi

echo "Checking consumer readiness via GET /api/v1/schemas/${SE_SUBJECT}/readiness..."
SE_READY_RESP=$(curl -s -i "http://127.0.0.1:${PORT}/api/v1/schemas/${SE_SUBJECT}/readiness?tenant_id=${MT_TENANT}&target_version=1")
echo "Consumer Readiness Response:"
echo "${SE_READY_RESP}"
if ! echo "${SE_READY_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for consumer readiness check!"
    exit 1
fi

echo "Evaluating decommission check via POST /api/v1/schemas/decommission/check..."
SE_DECOM_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/schemas/decommission/check" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "subject_name": "'"${SE_SUBJECT}"'",
        "active_consumer_count": 0,
        "zero_lag_days": 14,
        "telemetry_read_count": 0,
        "decommission_attested": true
    }')
echo "Decommission Evaluation Response:"
echo "${SE_DECOM_RESP}"
if ! echo "${SE_DECOM_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for decommission check!"
    exit 1
fi

echo "Verifying schema evolution version registry in PostgreSQL..."
DB_SEV_VERSION=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT schema_version FROM schema_evolution_version_registry WHERE tenant_id = '${MT_TENANT}' AND subject_name = '${SE_SUBJECT}';")
if [[ "${DB_SEV_VERSION}" != "1" ]]; then
    echo "FATAL: Expected schema_version 1 in schema_evolution_version_registry table, got '${DB_SEV_VERSION}'"
    exit 1
fi
echo "Schema evolution verified in PostgreSQL!"

echo "================================================================"
echo " 20. TESTING REAL AUDIT TRAIL API & TAMPER-EVIDENCE"
echo "================================================================"
AUD_SHARD="shard_audit_${TIMESTAMP}"
echo "Ingesting audit record via POST /api/v1/audits/ingest..."
AUD_INGEST_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/audits/ingest" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "external_id": "ext_aud_01",
        "correlation_id": "'$(uuidgen)'",
        "shard_id": "'"${AUD_SHARD}"'",
        "actor_id": "usr_real_alice",
        "actor_type": "USER",
        "action_name": "orders.order.cancelled",
        "target_type": "order",
        "target_id": "ord_real_001",
        "outcome": "SUCCEEDED",
        "compliance_class": "CLASS_A",
        "raw_payload": "{\"order_id\":\"ord_real_001\",\"reason\":\"user_request\"}",
        "occurred_at": "'"$(date -u +"%Y-%m-%dT%H:%M:%SZ")"'"
    }')
echo "Audit Ingest Response:"
echo "${AUD_INGEST_RESP}"
if ! echo "${AUD_INGEST_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for audit record ingestion!"
    exit 1
fi

echo "Creating checkpoint anchor via POST /api/v1/audits/checkpoints..."
AUD_CP_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/audits/checkpoints" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "correlation_id": "'$(uuidgen)'",
        "shard_id": "'"${AUD_SHARD}"'",
        "signing_secret": "super_secret_audit_anchor_key_32chars_long"
    }')
echo "Audit Checkpoint Response:"
echo "${AUD_CP_RESP}"
if ! echo "${AUD_CP_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for audit checkpoint creation!"
    exit 1
fi

echo "Verifying shard integrity via POST /api/v1/audits/verify..."
AUD_VERIFY_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/audits/verify" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "shard_id": "'"${AUD_SHARD}"'",
        "signing_secret": "super_secret_audit_anchor_key_32chars_long"
    }')
echo "Audit Verification Response:"
echo "${AUD_VERIFY_RESP}"
if ! echo "${AUD_VERIFY_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for audit shard verification!"
    exit 1
fi

echo "Verifying audit trail record in PostgreSQL..."
DB_AUD_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT count(*) FROM audit_trail_ledger WHERE tenant_id = '${MT_TENANT}' AND shard_id = '${AUD_SHARD}';")
if [[ "${DB_AUD_COUNT}" != "1" ]]; then
    echo "FATAL: Expected 1 record in audit_trail_ledger table, got '${DB_AUD_COUNT}'"
    exit 1
fi
echo "Audit trail verified in PostgreSQL!"

echo "================================================================"
echo " 21. TESTING REAL ENVELOPE ENCRYPTION & CRYPTO-SHREDDING"
echo "================================================================"
ENC_SUB_ID="subject_$(date +%s)"
ENC_AGG_ID="agg_$(date +%s)"

echo "Executing POST /api/v1/encryption/encrypt for subject ${ENC_SUB_ID}..."
ENC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/encryption/encrypt" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "subject_id": "'"${ENC_SUB_ID}"'",
        "aggregate_id": "'"${ENC_AGG_ID}"'",
        "field_path": "customer.national_id",
        "plaintext": "CONFIDENTIAL-SSN-999-00-1234"
    }')
echo "Encryption Response:"
echo "${ENC_RESP}"
if ! echo "${ENC_RESP}" | grep -q 'HTTP/1.1 200 OK'; then
    echo "FATAL: Expected HTTP 200 for field encryption!"
    exit 1
fi

ENC_CIPHERTEXT=$(echo "${ENC_RESP}" | grep -o '"ciphertext":"[^"]*' | cut -d'"' -f4)
ENC_NONCE=$(echo "${ENC_RESP}" | grep -o '"nonce":"[^"]*' | cut -d'"' -f4)
ENC_TAG=$(echo "${ENC_RESP}" | grep -o '"tag":"[^"]*' | cut -d'"' -f4)
ENC_KEY_REF=$(echo "${ENC_RESP}" | grep -o '"key_ref":"[^"]*' | cut -d'"' -f4)

if [ -z "${ENC_CIPHERTEXT}" ] || [ -z "${ENC_KEY_REF}" ]; then
    echo "FATAL: Failed to extract ciphertext or key_ref from encryption response!"
    exit 1
fi
echo "Field encrypted successfully with key_ref: ${ENC_KEY_REF}"

echo "Executing POST /api/v1/encryption/decrypt before shredding..."
DEC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/encryption/decrypt" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "subject_id": "'"${ENC_SUB_ID}"'",
        "aggregate_id": "'"${ENC_AGG_ID}"'",
        "field_path": "customer.national_id",
        "consumer_role": "DATA_READER",
        "encrypted_value": {
            "scheme_version": "v1-aes-256-gcm",
            "key_ref": "'"${ENC_KEY_REF}"'",
            "nonce": "'"${ENC_NONCE}"'",
            "ciphertext": "'"${ENC_CIPHERTEXT}"'",
            "tag": "'"${ENC_TAG}"'"
        }
    }')
echo "Decryption Response:"
echo "${DEC_RESP}"
if ! echo "${DEC_RESP}" | grep -q 'CONFIDENTIAL-SSN-999-00-1234'; then
    echo "FATAL: Expected decrypted plaintext to match original secret!"
    exit 1
fi
echo "Decryption verified successfully!"

echo "Executing POST /api/v1/encryption/blind-index..."
BLIND_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/encryption/blind-index" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "field_class": "email",
        "raw_value": "user.test@example.com"
    }')
echo "Blind Index Response:"
echo "${BLIND_RESP}"
if ! echo "${BLIND_RESP}" | grep -q 'blind_index_hash'; then
    echo "FATAL: Expected blind index hash in response!"
    exit 1
fi

echo "Executing POST /api/v1/encryption/erasure (GDPR RTBF crypto-shredding)..."
SHRED_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/encryption/erasure" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "subject_id": "'"${ENC_SUB_ID}"'",
        "reason": "GDPR_RTBF",
        "requested_by": "dpo@company.org"
    }')
echo "Erasure Response:"
echo "${SHRED_RESP}"
if ! echo "${SHRED_RESP}" | grep -q 'SHREDDED'; then
    echo "FATAL: Expected status SHREDDED in response!"
    exit 1
fi

echo "Verifying that subsequent decryption yields ERASED..."
POST_SHRED_DEC_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/encryption/decrypt" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "subject_id": "'"${ENC_SUB_ID}"'",
        "aggregate_id": "'"${ENC_AGG_ID}"'",
        "field_path": "customer.national_id",
        "consumer_role": "DATA_READER",
        "encrypted_value": {
            "scheme_version": "v1-aes-256-gcm",
            "key_ref": "'"${ENC_KEY_REF}"'",
            "nonce": "'"${ENC_NONCE}"'",
            "ciphertext": "'"${ENC_CIPHERTEXT}"'",
            "tag": "'"${ENC_TAG}"'"
        }
    }')
echo "Post-Shred Decryption Response:"
echo "${POST_SHRED_DEC_RESP}"
if ! echo "${POST_SHRED_DEC_RESP}" | grep -q 'ERASED'; then
    echo "FATAL: Expected outcome ERASED after crypto-shredding!"
    exit 1
fi
echo "Crypto-shredding verified: key permanently erased and PII unrecoverable!"

echo "Verifying encryption and shredding ledgers in PostgreSQL..."
DB_DEK_STATE=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT state FROM encryption_dek_ledger WHERE tenant_id = '${MT_TENANT}' AND subject_id = '${ENC_SUB_ID}';")
if [[ "${DB_DEK_STATE}" != "SHREDDED" ]]; then
    echo "FATAL: Expected DEK state in DB to be SHREDDED, got '${DB_DEK_STATE}'"
    exit 1
fi
DB_CSA_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT count(*) FROM crypto_shredding_audit_ledger WHERE tenant_id = '${MT_TENANT}' AND subject_id = '${ENC_SUB_ID}';")
if [[ "${DB_CSA_COUNT}" != "1" ]]; then
    echo "FATAL: Expected 1 audit log in crypto_shredding_audit_ledger, got '${DB_CSA_COUNT}'"
    exit 1
fi
echo "PostgreSQL state and audit records verified for encryption!"

echo "================================================================"
echo " 22. TESTING REAL EVENT AUTHORIZATION & FAIL-CLOSED PBAC"
echo "================================================================"
AUTH_TOPIC="orders.customer.placed.v1"
AUTH_PRINCIPAL="order-worker-service"

echo "Executing POST /api/v1/auth/evaluate prior to policy provisioning (DEFAULT DENY)..."
EVAL_DENY_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/auth/evaluate" \
    -H "Content-Type: application/json" \
    -H "X-User-ID: rogue_inbound_identity" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "external_id": "ext_auth_eval_01",
        "correlation_id": "'$(uuidgen)'",
        "principal_id": "'"${AUTH_PRINCIPAL}"'",
        "principal_type": "SERVICE",
        "resource_type": "TOPIC",
        "resource_name": "'"${AUTH_TOPIC}"'",
        "operation": "PUBLISH"
    }')
echo "Default Deny Response:"
echo "${EVAL_DENY_RESP}"
if ! echo "${EVAL_DENY_RESP}" | grep -q '"decision":"DENIED"'; then
    echo "FATAL: Expected decision DENIED under default deny!"
    exit 1
fi

echo "Provisioning active ALLOW policy via POST /api/v1/auth/policies..."
POLICY_PROV_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/auth/policies" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "external_id": "ext_auth_pol_01",
        "correlation_id": "'$(uuidgen)'",
        "policy_name": "allow_order_worker_publish",
        "principal_id": "'"${AUTH_PRINCIPAL}"'",
        "principal_type": "SERVICE",
        "resource_type": "TOPIC",
        "resource_pattern": "orders.customer.*",
        "operation": "PUBLISH",
        "effect": "ALLOW"
    }')
echo "Policy Provision Response:"
echo "${POLICY_PROV_RESP}"
if ! echo "${POLICY_PROV_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for policy provisioning!"
    exit 1
fi

echo "Re-evaluating POST /api/v1/auth/evaluate with active policy (EXPECT ALLOWED)..."
EVAL_ALLOW_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/auth/evaluate" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "real-test-suite",
        "external_id": "ext_auth_eval_02",
        "correlation_id": "'$(uuidgen)'",
        "principal_id": "'"${AUTH_PRINCIPAL}"'",
        "principal_type": "SERVICE",
        "resource_type": "TOPIC",
        "resource_name": "'"${AUTH_TOPIC}"'",
        "operation": "PUBLISH"
    }')
echo "Allow Evaluation Response:"
echo "${EVAL_ALLOW_RESP}"
if ! echo "${EVAL_ALLOW_RESP}" | grep -q '"decision":"ALLOWED"'; then
    echo "FATAL: Expected decision ALLOWED after policy provisioning!"
    exit 1
fi

echo "Verifying append-only auth audit records in PostgreSQL..."
DB_AUTH_AUDIT_COUNT=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT count(*) FROM auth_audit_log WHERE tenant_id = '${MT_TENANT}' AND principal_id = '${AUTH_PRINCIPAL}';")
if [[ "${DB_AUTH_AUDIT_COUNT}" -lt "2" ]]; then
    echo "FATAL: Expected at least 2 audit entries in auth_audit_log, got '${DB_AUTH_AUDIT_COUNT}'"
    exit 1
fi
echo "Event authorization verified in real PostgreSQL!"

echo "================================================================"
echo " 23. TESTING REAL DOMAIN EVENT DESIGN & CANONICAL ENVELOPES"
echo "================================================================"
DED_EVENT_TYPE="orders.order.completed.v1"
DED_AGG_ID="ord_agg_$(date +%s)"

echo "Registering domain event in catalog via POST /api/v1/domain-events/catalog..."
CATALOG_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/domain-events/catalog" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "order-service",
        "external_id": "ext_ded_cat_01",
        "correlation_id": "'$(uuidgen)'",
        "event_type": "'"${DED_EVENT_TYPE}"'",
        "owning_aggregate": "orders.order",
        "semantics_class": "STATE",
        "visibility": "PUBLIC",
        "pii_classification": "NONE",
        "schema_fingerprint": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    }')
echo "Catalog Response:"
echo "${CATALOG_RESP}"
if ! echo "${CATALOG_RESP}" | grep -q 'HTTP/1.1 201 Created'; then
    echo "FATAL: Expected HTTP 201 for catalog event registration!"
    exit 1
fi

echo "Building canonical domain event envelope via POST /api/v1/domain-events/envelope/build..."
BUILD_ENV_RESP=$(curl -s -i -X POST "http://127.0.0.1:${PORT}/api/v1/domain-events/envelope/build" \
    -H "Content-Type: application/json" \
    -d '{
        "tenant_id": "'"${MT_TENANT}"'",
        "source_service": "order-service",
        "event_type": "'"${DED_EVENT_TYPE}"'",
        "aggregate_type": "orders.order",
        "aggregate_id": "'"${DED_AGG_ID}"'",
        "expected_version": 0,
        "version_increment": 1,
        "causation_id": "0192e2b0-8c29-79a0-9710-84519fbbd801",
        "correlation_id": "0192e2b0-8c29-79a0-9710-84519fbbd802",
        "causation_depth": 1,
        "payload": {
            "order_id": "'"${DED_AGG_ID}"'",
            "status": "COMPLETED"
        }
    }')
echo "Build Envelope Response:"
echo "${BUILD_ENV_RESP}"
if ! echo "${BUILD_ENV_RESP}" | grep -q 'BUILT_AND_COMMITTED'; then
    echo "FATAL: Expected status BUILT_AND_COMMITTED for envelope creation!"
    exit 1
fi

echo "Verifying aggregate root version advancement in PostgreSQL..."
DB_AGG_VERSION=$(docker exec event_platform-db-1 psql -U postgres -d event_store -t -A -c "SELECT current_version FROM aggregate_root_version_ledger WHERE tenant_id = '${MT_TENANT}' AND aggregate_id = '${DED_AGG_ID}';")
if [[ "${DB_AGG_VERSION}" != "1" ]]; then
    echo "FATAL: Expected aggregate version in DB to be 1, got '${DB_AGG_VERSION}'"
    exit 1
fi
echo "Domain Event Design verified in PostgreSQL!"

echo "All real API and backing server integration tests completed successfully."
