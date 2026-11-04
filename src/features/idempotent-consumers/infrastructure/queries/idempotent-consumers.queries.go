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
    tenant_id, handler_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
FROM consumer_projections
WHERE tenant_id = $1 AND handler_id = $2 AND aggregate_type = $3 AND aggregate_id = $4;`

	FlowUpsertAggregateProjection = `
INSERT INTO consumer_projections (
    tenant_id, handler_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, FALSE, NOW()
) ON CONFLICT (tenant_id, handler_id, aggregate_type, aggregate_id) DO UPDATE
SET last_version = $5,
    last_event_id = $6,
    last_payload_hash = $7,
    updated_at = NOW()
WHERE consumer_projections.last_version < $5;`

	FlowParkAggregateProjection = `
INSERT INTO consumer_projections (
    tenant_id, handler_id, aggregate_type, aggregate_id, last_version, last_event_id, last_payload_hash, is_parked, updated_at
) VALUES (
    $1, $2, $3, $4, 0, '', '', TRUE, NOW()
) ON CONFLICT (tenant_id, handler_id, aggregate_type, aggregate_id) DO UPDATE
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
