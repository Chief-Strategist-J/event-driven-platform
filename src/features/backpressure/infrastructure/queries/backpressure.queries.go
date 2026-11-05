package queries

const (
	FlowUpsertBackpressureRecord = `
INSERT INTO backpressure_state (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    handler_id, dependency_id, state, pause_reason, concurrency_limit,
    paused_at, resumed_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13
)
ON CONFLICT (tenant_id, handler_id, dependency_id) DO UPDATE SET
    state = EXCLUDED.state,
    pause_reason = EXCLUDED.pause_reason,
    concurrency_limit = EXCLUDED.concurrency_limit,
    paused_at = EXCLUDED.paused_at,
    resumed_at = EXCLUDED.resumed_at,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, public_id, version, created_at, updated_at;
`

	FlowGetBackpressureRecord = `
SELECT
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    handler_id, dependency_id, state, pause_reason, concurrency_limit, version,
    paused_at, resumed_at, created_at, updated_at
FROM backpressure_state
WHERE tenant_id = $1 AND handler_id = $2 AND dependency_id = $3;
`

	FlowListBackpressureByTenant = `
SELECT
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    handler_id, dependency_id, state, pause_reason, concurrency_limit, version,
    paused_at, resumed_at, created_at, updated_at
FROM backpressure_state
WHERE tenant_id = $1
ORDER BY handler_id ASC, dependency_id ASC;
`

	FlowInsertBackpressureAudit = `
INSERT INTO backpressure_audit_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    handler_id, dependency_id, from_state, to_state, event, pause_reason, reason
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12, $13
) RETURNING created_at;
`

	FlowCheckBackpressureOrphan = `
SELECT id, tenant_id, handler_id, dependency_id
FROM backpressure_state
WHERE state = 'SATURATED'
  AND paused_at < NOW() - INTERVAL '25 minutes';
`
)

