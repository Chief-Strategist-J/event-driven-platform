package queries

const (
	FlowInsertRetryAttempt = `
INSERT INTO retry_attempt_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version,
    attempt_number, max_attempts, backoff_ms, due_at, status,
    failure_classification, idempotency_key, error_message, error_code, valid_until
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17,
    $18, $19, $20, $21, $22
) RETURNING created_at, updated_at;
`

	FlowGetDueRetryAttempts = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version,
    attempt_number, max_attempts, backoff_ms, due_at, status,
    failure_classification, idempotency_key, error_message, error_code, valid_until,
    created_at, updated_at
FROM retry_attempt_ledger
WHERE tenant_id = $1 AND status = 'SCHEDULED' AND due_at <= $2
ORDER BY due_at ASC
LIMIT $3;
`

	FlowUpdateRetryStatus = `
UPDATE retry_attempt_ledger
SET status = $3, updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND id = $2;
`
)
