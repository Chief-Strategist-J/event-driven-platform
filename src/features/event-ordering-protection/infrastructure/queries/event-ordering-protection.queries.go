package queries

const (
	FlowGetSequenceTrackerForUpdate = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       consumer_group, handler_id, aggregate_type, aggregate_id,
       last_applied_version, last_payload_hash, owner_epoch,
       is_held, hold_reason, blocking_version, updated_at
FROM aggregate_sequence_tracker
WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_type = $4 AND aggregate_id = $5
FOR UPDATE;`

	FlowUpsertSequenceTrackerApplied = `
INSERT INTO aggregate_sequence_tracker (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, handler_id, aggregate_type, aggregate_id,
    last_applied_version, last_payload_hash, owner_epoch, is_held, hold_reason, blocking_version, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, FALSE, NULL, NULL, NOW())
ON CONFLICT (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id)
DO UPDATE SET last_applied_version = EXCLUDED.last_applied_version,
              last_payload_hash = EXCLUDED.last_payload_hash,
              owner_epoch = EXCLUDED.owner_epoch,
              is_held = FALSE,
              hold_reason = NULL,
              blocking_version = NULL,
              updated_at = NOW()
WHERE aggregate_sequence_tracker.owner_epoch <= EXCLUDED.owner_epoch;`

	FlowHoldAggregateSequence = `
INSERT INTO aggregate_sequence_tracker (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, handler_id, aggregate_type, aggregate_id,
    last_applied_version, last_payload_hash, owner_epoch, is_held, hold_reason, blocking_version, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11, $12, TRUE, $13, $14, NOW())
ON CONFLICT (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id)
DO UPDATE SET is_held = TRUE,
              hold_reason = EXCLUDED.hold_reason,
              blocking_version = EXCLUDED.blocking_version,
              owner_epoch = EXCLUDED.owner_epoch,
              updated_at = NOW()
WHERE aggregate_sequence_tracker.owner_epoch <= EXCLUDED.owner_epoch;`

	FlowInsertHeldEvent = `
INSERT INTO sequence_held_event (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version,
    event_id, event_type, payload, payload_hash, headers, source_offset, arrived_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
ON CONFLICT (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version)
DO NOTHING;`

	FlowGetHeldEventsForDrain = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version,
       event_id, event_type, payload, payload_hash, headers, source_offset, arrived_at
FROM sequence_held_event
WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_type = $4 AND aggregate_id = $5
ORDER BY aggregate_version ASC;`

	FlowDeleteHeldEvent = `
DELETE FROM sequence_held_event
WHERE tenant_id = $1 AND consumer_group = $2 AND handler_id = $3 AND aggregate_type = $4 AND aggregate_id = $5 AND aggregate_version = $6;`

	FlowListHeldTrackers = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       consumer_group, handler_id, aggregate_type, aggregate_id,
       last_applied_version, last_payload_hash, owner_epoch,
       is_held, hold_reason, blocking_version, updated_at
FROM aggregate_sequence_tracker
WHERE tenant_id = $1 AND ($2 = '' OR consumer_group = $2) AND ($3 = '' OR handler_id = $3) AND is_held = TRUE
ORDER BY updated_at DESC
LIMIT $4 OFFSET $5;`
)

