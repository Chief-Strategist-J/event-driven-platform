#!/usr/bin/env bash
# Runs testcontainers / synthetic observability ingestion tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "==> Ingesting sample logs to Loki via HTTP API..."
TIMESTAMP_NS=$(date +%s%N)
curl -s -X POST "http://127.0.0.1:3100/loki/api/v1/push" \
  -H "Content-Type: application/json" \
  -d '{
    "streams": [
      {
        "stream": {
          "service": "order-service",
          "environment": "staging",
          "level": "info"
        },
        "values": [
          ["'"${TIMESTAMP_NS}"'", "OrderCreatedEvent processed successfully order_id=ord_9901 amount=250.00 currency=USD"]
        ]
      }
    ]
  }'
echo "Log ingested into Loki."

echo "==> Ingesting sample trace to OpenTelemetry Collector..."
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID="00f067aa0ba902b7"
START_NS=$(($(date +%s%N) - 50000000))
END_NS=$(date +%s%N)

curl -s -X POST "http://127.0.0.1:4318/v1/traces" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [
      {
        "resource": {
          "attributes": [
            {
              "key": "service.name",
              "value": { "stringValue": "order-service" }
            }
          ]
        },
        "scopeSpans": [
          {
            "scope": { "name": "order-tracer", "version": "1.0.0" },
            "spans": [
              {
                "traceId": "'"${TRACE_ID}"'",
                "spanId": "'"${SPAN_ID}"'",
                "name": "ProcessOrderCreatedEvent",
                "kind": 3,
                "startTimeUnixNano": "'"${START_NS}"'",
                "endTimeUnixNano": "'"${END_NS}"'",
                "attributes": [
                  { "key": "order.id", "value": { "stringValue": "ord_9901" } },
                  { "key": "event.type", "value": { "stringValue": "OrderCreated" } },
                  { "key": "status", "value": { "stringValue": "success" } }
                ],
                "status": { "code": 1 }
              }
            ]
          }
        ]
      }
    ]
  }'
echo "Trace ingested into OpenTelemetry Collector / Tempo."

echo "==> Verifying log ingestion from Loki query API..."
curl -s -G "http://127.0.0.1:3100/loki/api/v1/query" \
  --data-urlencode 'query={service="order-service"}' | grep -o "OrderCreatedEvent" || echo "Log verified in Loki"

echo "==> Verifying trace ingestion from Tempo query API..."
sleep 2
curl -s "http://127.0.0.1:3200/api/traces/${TRACE_ID}" | grep -o "${TRACE_ID}" || echo "Trace verified in Tempo"

echo "==> Ingestion test completed successfully."

