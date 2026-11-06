package queries

const (
	FlowSelectIdempotencyRecord = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       idempotency_key, command_name, aggregate_type, aggregate_id,
       request_fingerprint, status, response_payload, consistency_token,
       version, created_at, updated_at
FROM cqrs_command_idempotency_ledger
WHERE tenant_id = $1 AND idempotency_key = $2;`

	FlowInsertIdempotencyRecord = `
INSERT INTO cqrs_command_idempotency_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    idempotency_key, command_name, aggregate_type, aggregate_id,
    request_fingerprint, status, response_payload, consistency_token,
    version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
);`

	FlowUpdateIdempotencyRecord = `
UPDATE cqrs_command_idempotency_ledger
SET status = $3,
    response_payload = $4,
    consistency_token = $5,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND tenant_id = $2;`

	FlowSelectAggregateForUpdate = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       aggregate_type, aggregate_id, current_version, state_payload, is_deleted,
       version, created_at, updated_at
FROM cqrs_aggregate_root_ledger
WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3
FOR UPDATE;`

	FlowUpsertAggregateMutation = `
INSERT INTO cqrs_aggregate_root_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    aggregate_type, aggregate_id, current_version, state_payload, is_deleted,
    version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO UPDATE
SET current_version = EXCLUDED.current_version,
    state_payload = EXCLUDED.state_payload,
    is_deleted = EXCLUDED.is_deleted,
    version = cqrs_aggregate_root_ledger.version + 1,
    updated_at = CURRENT_TIMESTAMP;`

	FlowSelectActiveSigningKey = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       key_version, secret_key_hash, raw_secret_value, status, expires_at,
       created_at, updated_at
FROM cqrs_token_signing_key_registry
WHERE tenant_id = $1 AND status = 'ACTIVE' AND expires_at > CURRENT_TIMESTAMP
ORDER BY key_version DESC
LIMIT 1;`

	FlowInsertSigningKey = `
INSERT INTO cqrs_token_signing_key_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    key_version, secret_key_hash, raw_secret_value, status, expires_at,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
);`
)

