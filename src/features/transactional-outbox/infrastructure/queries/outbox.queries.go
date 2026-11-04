package queries

const (
	FlowInsertOutboxRecord = `
INSERT INTO outbox (
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'PENDING', NOW(), NOW()
);`

	FlowFetchPendingOutboxForUpdateSkipLocked = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, retry_count, last_error, created_at, updated_at, processed_at
FROM outbox o
WHERE status = 'PENDING'
  AND NOT EXISTS (
      SELECT 1 FROM outbox p 
      WHERE p.tenant_id = o.tenant_id 
        AND p.aggregate_type = o.aggregate_type 
        AND p.aggregate_id = o.aggregate_id 
        AND p.status = 'PARKED'
  )
ORDER BY aggregate_version ASC, created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;`

	FlowMarkOutboxPublished = `
UPDATE outbox
SET status = 'PUBLISHED',
    processed_at = NOW(),
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2;`

	FlowMarkOutboxFailed = `
UPDATE outbox
SET retry_count = retry_count + 1,
    last_error = $3,
    status = CASE WHEN retry_count + 1 >= 5 THEN 'PARKED' ELSE 'PENDING' END,
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2;`

	FlowPurgeExpiredPublishedRecords = `
DELETE FROM outbox
WHERE status = 'PUBLISHED' 
  AND created_at < NOW() - INTERVAL '48 hours';`
)
