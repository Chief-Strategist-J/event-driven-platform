package queries

const (
	FlowUpsertStoredOffset = `
INSERT INTO consumer_stored_offset (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, topic, partition, stored_offset, log_end_offset, lag,
    commit_epoch, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12,
    $13, $14
)
ON CONFLICT (tenant_id, group_id, topic, partition) DO UPDATE SET
    stored_offset = EXCLUDED.stored_offset,
    log_end_offset = EXCLUDED.log_end_offset,
    lag = EXCLUDED.lag,
    commit_epoch = consumer_stored_offset.commit_epoch + 1,
    metadata = EXCLUDED.metadata,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, public_id, commit_epoch, created_at, updated_at;
`

	FlowGetStoredOffset = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, topic, partition, stored_offset, log_end_offset, lag,
    commit_epoch, metadata, created_at, updated_at
FROM consumer_stored_offset
WHERE tenant_id = $1 AND group_id = $2 AND topic = $3 AND partition = $4;
`

	FlowInsertOffsetAuditLog = `
INSERT INTO consumer_offset_audit_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, topic, partition, operation_type, previous_offset,
    target_offset, reason
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11,
    $12, $13
) RETURNING created_at;
`

	FlowListStoredOffsetsByGroup = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    group_id, topic, partition, stored_offset, log_end_offset, lag,
    commit_epoch, metadata, created_at, updated_at
FROM consumer_stored_offset
WHERE tenant_id = $1 AND group_id = $2
ORDER BY topic ASC, partition ASC;
`
)

