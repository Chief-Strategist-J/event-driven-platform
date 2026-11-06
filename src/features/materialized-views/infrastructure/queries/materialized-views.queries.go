package queries

const (
	FlowInsertViewRegistry = `
INSERT INTO materialized_view_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, view_class, primary_store, active_table_name, freshness_slo_ms,
    consistency_mode, status, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FlowSelectViewRegistryByTenantAndViewName = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, view_class, primary_store, active_table_name, freshness_slo_ms,
       consistency_mode, status, version, created_at, updated_at
FROM materialized_view_registry
WHERE tenant_id = $1 AND view_name = $2;`

	FlowSelectViewRegistryByPublicID = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, view_class, primary_store, active_table_name, freshness_slo_ms,
       consistency_mode, status, version, created_at, updated_at
FROM materialized_view_registry
WHERE public_id = $1;`

	FlowUpdateViewRegistryStatus = `
UPDATE materialized_view_registry
SET status = $3, updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND view_name = $2;`

	FlowSelectProjectionStateByAggregate = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, aggregate_type, aggregate_id, current_version, last_event_id,
       payload_hash, materialized_at, version, created_at, updated_at
FROM materialized_view_projection_state
WHERE tenant_id = $1 AND view_name = $2 AND aggregate_type = $3 AND aggregate_id = $4;`

	FlowUpsertProjectionState = `
INSERT INTO materialized_view_projection_state (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, aggregate_type, aggregate_id, current_version, last_event_id,
    payload_hash, materialized_at, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (tenant_id, view_name, aggregate_type, aggregate_id) DO UPDATE
SET current_version = EXCLUDED.current_version,
    last_event_id = EXCLUDED.last_event_id,
    payload_hash = EXCLUDED.payload_hash,
    materialized_at = EXCLUDED.materialized_at,
    updated_at = CURRENT_TIMESTAMP;`

	FlowInsertHeldEvent = `
INSERT INTO materialized_view_held_events (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, aggregate_type, aggregate_id, expected_seq, received_seq,
    event_payload, held_until, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);`

	FlowSelectHeldEventsByAggregate = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, aggregate_type, aggregate_id, expected_seq, received_seq,
       event_payload, held_until, created_at
FROM materialized_view_held_events
WHERE tenant_id = $1 AND view_name = $2 AND aggregate_type = $3 AND aggregate_id = $4
ORDER BY received_seq ASC;`

	FlowDeleteHeldEventByID = `
DELETE FROM materialized_view_held_events
WHERE tenant_id = $1 AND id = $2;`

	FlowInsertRebuildLog = `
INSERT INTO materialized_view_rebuild_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    rebuild_id, view_name, source_store, target_shadow_table, target_lsn,
    current_cursor_seq, phase, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FlowSelectRebuildLogByRebuildID = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       rebuild_id, view_name, source_store, target_shadow_table, target_lsn,
       current_cursor_seq, phase, version, created_at, updated_at
FROM materialized_view_rebuild_log
WHERE tenant_id = $1 AND rebuild_id = $2;`

	FlowUpdateRebuildLogPhaseAndCursor = `
UPDATE materialized_view_rebuild_log
SET phase = $3, current_cursor_seq = $4, updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND rebuild_id = $2;`
)
