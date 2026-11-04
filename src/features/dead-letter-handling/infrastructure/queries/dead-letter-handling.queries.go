package queries

const (
	FlowInsertDeadLetterQuarantine = `
INSERT INTO dead_letter_quarantine (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    topic, partition, source_offset, consumer_group, handler_id,
    event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
    payload, headers, failure_reason, error_code, error_fingerprint,
    status, redrive_count, max_redrive_attempts, quarantined_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21,
    $22, $23, $24, NOW(), NOW()
);`

	FlowGetQuarantinedByID = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    topic, partition, source_offset, consumer_group, handler_id,
    event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
    payload, headers, failure_reason, error_code, error_fingerprint,
    status, redrive_count, max_redrive_attempts, quarantined_at, updated_at
FROM dead_letter_quarantine
WHERE tenant_id = $1 AND id = $2;`

	FlowListQuarantined = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    topic, partition, source_offset, consumer_group, handler_id,
    event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
    payload, headers, failure_reason, error_code, error_fingerprint,
    status, redrive_count, max_redrive_attempts, quarantined_at, updated_at
FROM dead_letter_quarantine
WHERE tenant_id = $1 AND ($2 = '' OR handler_id = $2) AND ($3 = '' OR status = $3)
ORDER BY quarantined_at DESC
LIMIT $4 OFFSET $5;`

	FlowUpdateQuarantineDisposition = `
UPDATE dead_letter_quarantine
SET status = $3,
    redrive_count = $4,
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2;`

	FlowParkAggregateKey = `
INSERT INTO aggregate_parking_store (
    tenant_id, handler_id, aggregate_type, aggregate_id, is_parked, parked_reason, quarantine_id, parked_at, updated_at
) VALUES (
    $1, $2, $3, $4, TRUE, $5, $6, NOW(), NOW()
) ON CONFLICT (tenant_id, handler_id, aggregate_type, aggregate_id) DO UPDATE
SET is_parked = TRUE,
    parked_reason = $5,
    quarantine_id = $6,
    updated_at = NOW();`

	FlowUnparkAggregateKey = `
UPDATE aggregate_parking_store
SET is_parked = FALSE,
    updated_at = NOW()
WHERE tenant_id = $1 AND handler_id = $2 AND aggregate_type = $3 AND aggregate_id = $4;`

	FlowCheckKeyParked = `
SELECT EXISTS (
    SELECT 1 FROM aggregate_parking_store
    WHERE tenant_id = $1 AND handler_id = $2 AND aggregate_type = $3 AND aggregate_id = $4 AND is_parked = TRUE
);`
)

