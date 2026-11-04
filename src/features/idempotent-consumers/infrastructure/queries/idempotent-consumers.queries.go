package queries

const (
	FlowInsertConsumerInboxRecord = `
INSERT INTO consumer_inbox (
    id, tenant_id, handler_id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_hash, processed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()
) ON CONFLICT (tenant_id, handler_id, event_id) DO NOTHING;`

	FlowGetAggregateProjection = `
SELECT 
    tenant_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
FROM consumer_projections
WHERE tenant_id = $1 AND aggregate_type = $2 AND aggregate_id = $3;`

	FlowUpsertAggregateProjection = `
INSERT INTO consumer_projections (
    tenant_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, FALSE, NOW()
) ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO UPDATE
SET last_version = $4,
    last_event_id = $5,
    last_payload_hash = $6,
    updated_at = NOW()
WHERE consumer_projections.last_version < $4;`

	FlowParkAggregateProjection = `
INSERT INTO consumer_projections (
    tenant_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
) VALUES (
    $1, $2, $3, 0, '', '', TRUE, NOW()
) ON CONFLICT (tenant_id, aggregate_type, aggregate_id) DO UPDATE
SET is_parked = TRUE,
    updated_at = NOW();`

	FlowStorePartitionOffset = `
INSERT INTO consumer_partition_offsets (
    topic, partition, offset_val, updated_at
) VALUES (
    $1, $2, $3, NOW()
) ON CONFLICT (topic, partition) DO UPDATE
SET offset_val = $3,
    updated_at = NOW();`
)
