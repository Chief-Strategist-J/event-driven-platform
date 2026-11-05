package queries

const (
	FlowInsertReplayRun = `
INSERT INTO replay_run (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    target_projection, mode, state, scope, start_offset, current_offset,
    end_offset, replay_consumer_group, version
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12,
    $13, $14, $15
) RETURNING id, public_id, version, created_at, updated_at;
`

	FlowUpdateReplayRunState = `
UPDATE replay_run SET
    state = $1,
    updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $2 AND id = $3
RETURNING id, public_id, state, version, updated_at;
`

	FlowUpdateReplayRunOffset = `
UPDATE replay_run SET
    current_offset = $1,
    state = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $3 AND id = $4
RETURNING id, public_id, current_offset, state, version, updated_at;
`

	FlowGetReplayRun = `
SELECT
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    target_projection, mode, state, scope, start_offset, current_offset,
    end_offset, replay_consumer_group, version, created_at, updated_at
FROM replay_run
WHERE tenant_id = $1 AND id = $2;
`

	FlowListReplayRunsByTenant = `
SELECT
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    target_projection, mode, state, scope, start_offset, current_offset,
    end_offset, replay_consumer_group, version, created_at, updated_at
FROM replay_run
WHERE tenant_id = $1
ORDER BY created_at DESC;
`

	FlowInsertReplayEffectLog = `
INSERT INTO replay_effect_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    run_id, effect_type, effect_mode, decision, idempotency_key, payload_digest
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12
) RETURNING created_at;
`

	FlowInsertReplayAuditLog = `
INSERT INTO replay_audit_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    run_id, from_state, to_state, event, reason
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11
) RETURNING created_at;
`
)

