package queries

const (
	FlowUpsertWatermark = `
INSERT INTO consistency_watermark_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, stream_name, partition_id, high_watermark_offset,
    consumer_offset, offset_lag, freshness_lag_ms, watermark_status,
    observed_at, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (tenant_id, view_name, stream_name, partition_id) DO UPDATE
SET high_watermark_offset = EXCLUDED.high_watermark_offset,
    consumer_offset = EXCLUDED.consumer_offset,
    offset_lag = EXCLUDED.offset_lag,
    freshness_lag_ms = EXCLUDED.freshness_lag_ms,
    watermark_status = EXCLUDED.watermark_status,
    observed_at = EXCLUDED.observed_at,
    version = consistency_watermark_ledger.version + 1,
    updated_at = CURRENT_TIMESTAMP;`

	FlowSelectWatermarksByView = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, stream_name, partition_id, high_watermark_offset,
       consumer_offset, offset_lag, freshness_lag_ms, watermark_status,
       observed_at, version, created_at, updated_at
FROM consistency_watermark_ledger
WHERE tenant_id = $1 AND view_name = $2
ORDER BY partition_id ASC;`

	FlowInsertDriftIncident = `
INSERT INTO consistency_drift_incident_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, aggregate_type, aggregate_id, drift_class, source_version,
    view_version, difference_payload, status, heal_attempts,
    quarantined_until, detected_at, resolved_at, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
);`

	FlowSelectActiveDriftIncidents = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, aggregate_type, aggregate_id, drift_class, source_version,
       view_version, difference_payload, status, heal_attempts,
       quarantined_until, detected_at, resolved_at, version, created_at, updated_at
FROM consistency_drift_incident_log
WHERE tenant_id = $1 AND view_name = $2 AND status <> 'RESOLVED'
ORDER BY detected_at DESC;`

	FlowSelectDriftIncidentByID = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, aggregate_type, aggregate_id, drift_class, source_version,
       view_version, difference_payload, status, heal_attempts,
       quarantined_until, detected_at, resolved_at, version, created_at, updated_at
FROM consistency_drift_incident_log
WHERE id = $1 AND tenant_id = $2;`

	FlowUpdateDriftIncidentStatus = `
UPDATE consistency_drift_incident_log
SET status = $3,
    heal_attempts = $4,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND tenant_id = $2;`

	FlowInsertCheckRun = `
INSERT INTO consistency_check_run_history (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, check_type, evaluated_keys, match_count, mismatch_count,
    unknown_count, coverage_ratio, execution_duration_ms, status, created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);`

	FlowSelectCheckRuns = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, check_type, evaluated_keys, match_count, mismatch_count,
       unknown_count, coverage_ratio, execution_duration_ms, status, created_at
FROM consistency_check_run_history
WHERE tenant_id = $1 AND view_name = $2
ORDER BY created_at DESC
LIMIT $3;`

	FlowInsertSilence = `
INSERT INTO consistency_view_silence_registry (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    view_name, owner_id, reason, expires_at, status, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
);`

	FlowSelectActiveSilence = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       view_name, owner_id, reason, expires_at, status, created_at, updated_at
FROM consistency_view_silence_registry
WHERE tenant_id = $1 AND view_name = $2 AND status = 'ACTIVE' AND expires_at > CURRENT_TIMESTAMP
ORDER BY expires_at DESC
LIMIT 1;`
)

