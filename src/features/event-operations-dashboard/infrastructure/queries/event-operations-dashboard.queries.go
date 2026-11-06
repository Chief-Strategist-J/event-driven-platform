package queries

const (
	UpsertPipelineHopQuery = `
INSERT INTO ops_pipeline_hop_ledger (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    flow_name, hop_code, tier, status, pause_reason, lag_seconds,
    oldest_held_age_seconds, runbook_url, last_reported_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
ON CONFLICT (tenant_id, flow_name, hop_code) DO UPDATE SET
    status = EXCLUDED.status,
    pause_reason = EXCLUDED.pause_reason,
    lag_seconds = EXCLUDED.lag_seconds,
    oldest_held_age_seconds = EXCLUDED.oldest_held_age_seconds,
    runbook_url = EXCLUDED.runbook_url,
    last_reported_at = EXCLUDED.last_reported_at,
    updated_at = EXCLUDED.updated_at;
`

	SelectHopsByFlowQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       flow_name, hop_code, tier, status, pause_reason, lag_seconds,
       oldest_held_age_seconds, runbook_url, last_reported_at, created_at, updated_at
FROM ops_pipeline_hop_ledger
WHERE tenant_id = $1 AND flow_name = $2;
`

	SelectHopByPublicIDQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       flow_name, hop_code, tier, status, pause_reason, lag_seconds,
       oldest_held_age_seconds, runbook_url, last_reported_at, created_at, updated_at
FROM ops_pipeline_hop_ledger
WHERE public_id = $1;
`

	InsertIncidentTriageQuery = `
INSERT INTO ops_incident_triage_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    incident_id, flow_name, localized_hop, severity, triage_action,
    c_ctl_action_link, triaged_by, triaged_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);
`

	SelectTriageByPublicIDQuery = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       incident_id, flow_name, localized_hop, severity, triage_action,
       c_ctl_action_link, triaged_by, triaged_at
FROM ops_incident_triage_log
WHERE public_id = $1;
`
)
