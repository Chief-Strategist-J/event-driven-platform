#!/usr/bin/env bash
# Smoke test against a running stack. BASE/ADMIN default to the compose published ports.
set -uo pipefail
BASE="${BASE:-http://127.0.0.1:${EDP_API_PORT:-8080}}"
ADMIN="${ADMIN:-http://127.0.0.1:${EDP_ADMIN_PORT:-8081}}"
RUN="$(date +%s)$RANDOM"
TENANT="smoke-$RUN"
PASS=0; FAIL=0; FAILED=()

req() { # method url [body] [extra curl args...] -> sets CODE, BODY
  local m="$1" u="$2" b="${3:-}"; shift 3 2>/dev/null || shift $#
  local out
  if [ -n "$b" ]; then
    out=$(curl -sS -m 15 -w $'\n%{http_code}' -X "$m" -H 'Content-Type: application/json' -H "X-Request-ID: $(cat /proc/sys/kernel/random/uuid)" "$@" -d "$b" "$u") || out=$'\n000'
  else
    out=$(curl -sS -m 15 -w $'\n%{http_code}' -X "$m" -H "X-Request-ID: $(cat /proc/sys/kernel/random/uuid)" "$@" "$u") || out=$'\n000'
  fi
  CODE="${out##*$'\n'}"; BODY="${out%$'\n'*}"
}
check() { # name ok(0/1) detail
  if [ "$2" = 0 ]; then PASS=$((PASS+1)); echo "PASS  $1"
  else FAIL=$((FAIL+1)); FAILED+=("$1"); echo "FAIL  $1 -- $3"; fi
}
expect_code() { # name want
  [ "$CODE" = "$2" ]; check "$1" $? "want HTTP $2, got $CODE: ${BODY:0:300}"
}

req GET "$ADMIN/health/live" ""; expect_code "admin /health/live" 200
req GET "$ADMIN/health/ready" ""; expect_code "admin /health/ready" 200

req POST "$BASE/api/v1/tenants" "{\"tenant_id\":\"$TENANT\",\"source_service\":\"smoke\",\"external_id\":\"ext-$RUN\",\"tier\":\"POOL\",\"home_region\":\"us-east-1\",\"allowed_regions\":[\"us-east-1\"],\"retention_class\":\"STANDARD\",\"max_throughput_eps\":100,\"max_concurrency_lanes\":2}"
[[ "$CODE" == 2* ]]; check "create tenant" $? "got HTTP $CODE: ${BODY:0:300}"

req GET "$BASE/api/v1/tenants/$TENANT" ""
[[ "$CODE" == 2* && "$BODY" == *"$TENANT"* ]]; check "get tenant" $? "got HTTP $CODE: ${BODY:0:300}"

req POST "$BASE/api/v1/outbox" "{\"tenantId\":\"$TENANT\",\"sourceService\":\"smoke\",\"externalId\":\"evt-$RUN\",\"aggregateType\":\"smoke_orders\",\"aggregateId\":\"agg-$RUN\",\"aggregateVersion\":1,\"eventType\":\"SmokeCreated\",\"schemaVersion\":\"1\",\"payload\":{\"n\":1}}"
expect_code "stage outbox record" 201

req POST "$BASE/api/v1/outbox/relay" "{}"
[ "${CODE:0:1}" != 5 ] && [ "$CODE" != 000 ]; check "outbox relay no 5xx/unreachable" $? "got HTTP $CODE: ${BODY:0:300}"
sleep 3
req GET "$BASE/api/v1/tenants/$TENANT" ""
[ "${CODE:0:1}" != 5 ] && [ "$CODE" != 000 ]; check "api still healthy after relay" $? "got HTTP $CODE"

req GET "$BASE/api/v1/dlq/quarantined" ""; expect_code "dlq quarantined without tenant_id -> 400" 400
req POST "$BASE/api/v1/dlq/redrive" "{\"quarantineId\":\"not-a-uuid\",\"tenantId\":\"$TENANT\"}"; expect_code "dlq redrive non-UUID -> 400" 400

M=$(curl -sS -m 15 "$ADMIN/metrics" || true)
[[ "$M" == *http_requests_total* ]]; check "/metrics contains http_requests_total" $? "metrics endpoint missing counter"

echo; echo "Summary: $PASS passed, $FAIL failed (BASE=$BASE ADMIN=$ADMIN)"
if [ "$FAIL" -ne 0 ]; then printf 'Failed: %s\n' "${FAILED[@]}"; exit 1; fi
