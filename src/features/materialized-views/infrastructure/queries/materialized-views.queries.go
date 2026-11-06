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
    view_name, aggregate_type, aggregate_id, expected_version, received_version,
    event_id, payload, held_reason, status, held_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FlowSelectHeldEventsByAggregate = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, aggregate_type, aggregate_id, expected_version, received_version,
       event_id, payload, held_reason, status, held_at
FROM materialized_view_held_events
WHERE tenant_id = $1 AND view_name = $2 AND aggregate_type = $3 AND aggregate_id = $4
ORDER BY received_version ASC;`

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

	FlowInsertAggContribution = `
INSERT INTO materialized_view_agg_contributions (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, aggregate_type, aggregate_id, source_event_id, source_seq,
    delta_amount, is_reversal, version, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
);`

	FlowSumAggContributionsByAggregate = `
SELECT COALESCE(SUM(delta_amount), 0.0000)
FROM materialized_view_agg_contributions
WHERE tenant_id = $1 AND view_name = $2 AND aggregate_type = $3 AND aggregate_id = $4;`

	FlowUpsertPeriodLedger = `
INSERT INTO materialized_view_period_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, period_key, period_status, base_amount, adjustment_amount,
    closing_position_lsn, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (tenant_id, view_name, period_key) DO UPDATE
SET base_amount = EXCLUDED.base_amount,
    period_status = EXCLUDED.period_status,
    closing_position_lsn = EXCLUDED.closing_position_lsn,
    updated_at = CURRENT_TIMESTAMP;`

	FlowSelectPeriodLedgerByPeriodKey = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, period_key, period_status, base_amount, adjustment_amount,
       closing_position_lsn, version, created_at, updated_at
FROM materialized_view_period_ledger
WHERE tenant_id = $1 AND view_name = $2 AND period_key = $3;`

	FlowUpdatePeriodLedgerAdjustment = `
UPDATE materialized_view_period_ledger
SET adjustment_amount = adjustment_amount + $4,
    updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $1 AND view_name = $2 AND period_key = $3;`
)

