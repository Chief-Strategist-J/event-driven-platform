package queries

const (
	FlowClaimIdempotencyKey = `
INSERT INTO outbox_idempotency_keys (
    tenant_id, idempotency_key, record_id, created_at
) VALUES (
    $1, $2, $3, NOW()
) ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING record_id;`

	FlowInsertOutboxRecord = `
INSERT INTO outbox (
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'PENDING', NOW(), NOW()
);`

	FlowInsertAndDeleteOutboxRecordCDC = `
WITH inserted AS (
    INSERT INTO outbox (
        id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
        aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
        payload, headers, status, created_at, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'PENDING', NOW(), NOW()
    ) RETURNING tenant_id, id
)
DELETE FROM outbox WHERE (tenant_id, id) IN (SELECT tenant_id, id FROM inserted);`

	FlowClaimInFlightOutboxRecords = `
UPDATE outbox
SET status = 'IN_FLIGHT',
    worker_id = $1,
    lease_expires_at = NOW() + ($2 || ' seconds')::INTERVAL,
    updated_at = NOW()
WHERE id IN (
    SELECT o.id FROM outbox o
    WHERE (o.status = 'PENDING' OR (o.status = 'IN_FLIGHT' AND o.lease_expires_at < NOW()))
      AND NOT EXISTS (
          SELECT 1 FROM outbox p 
          WHERE p.tenant_id = o.tenant_id 
            AND p.aggregate_type = o.aggregate_type 
            AND p.aggregate_id = o.aggregate_id 
            AND p.status = 'PARKED'
      )
    ORDER BY o.aggregate_version ASC, o.created_at ASC
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
          aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
          payload, headers, status, worker_id, lease_expires_at, retry_count, last_error,
          created_at, updated_at, processed_at;`

	FlowFetchPendingOutboxForUpdateSkipLocked = `
SELECT 
    id, public_id, tenant_id, source_service, external_id, correlation_id, causation_id,
    aggregate_type, aggregate_id, aggregate_version, event_type, schema_version,
    payload, headers, status, worker_id, lease_expires_at, retry_count, last_error, created_at, updated_at, processed_at
FROM outbox o
WHERE (status = 'PENDING' OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW()))
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

	FlowMarkOutboxPublishedByWorker = `
UPDATE outbox
SET status = 'PUBLISHED',
    processed_at = NOW(),
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 AND (lease_expires_at IS NULL OR lease_expires_at >= NOW());`

	FlowMarkOutboxFailed = `
UPDATE outbox
SET retry_count = retry_count + 1,
    last_error = $3,
    status = CASE WHEN retry_count + 1 >= 5 THEN 'PARKED' ELSE 'PENDING' END,
    worker_id = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2;`

	FlowPurgeExpiredPublishedRecords = `
DELETE FROM outbox
WHERE status = 'PUBLISHED' 
  AND created_at < NOW() - INTERVAL '48 hours';`
)
