-- migration:      0026
-- description:    create event operations dashboard pipeline hop and incident triage ledgers
-- author:         chief-strategist
-- date:           2026-11-06T16:45:00Z
-- depends_on:     0025
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce OD-01 to OD-10, L0-L4 pipeline hop tracking, time-based lag, staleness tracking, and immutable triage logs per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS ops_pipeline_hop_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    flow_name VARCHAR(128) NOT NULL,
    hop_code VARCHAR(32) NOT NULL,
    tier VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL,
    pause_reason VARCHAR(64) NOT NULL DEFAULT 'NONE',
    lag_seconds NUMERIC(18,3) NOT NULL,
    oldest_held_age_seconds NUMERIC(18,3) NOT NULL,
    runbook_url VARCHAR(256) NOT NULL,
    last_reported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_ops_pipeline_hop_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_ops_pipeline_hop_public_id UNIQUE (public_id),
    CONSTRAINT uk_ops_pipeline_hop_tenant_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_ops_pipeline_hop_flow_hop UNIQUE (tenant_id, flow_name, hop_code),
    CONSTRAINT ck_ops_pipeline_hop_public_id_format CHECK (public_id ~ '^hop_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_ops_pipeline_hop_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_ops_pipeline_hop_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_ops_pipeline_hop_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_ops_pipeline_hop_flow_name_btrim CHECK (btrim(flow_name) = flow_name AND flow_name <> ''),
    CONSTRAINT ck_ops_pipeline_hop_code CHECK (hop_code IN ('H1_OUTBOX', 'H2_RELAY', 'H3_KAFKA', 'H4_CONSUMER', 'H5_AUTH_VIEW', 'H6_DERIVED_STORE')),
    CONSTRAINT ck_ops_pipeline_hop_tier CHECK (tier IN ('T0', 'T1', 'T2')),
    CONSTRAINT ck_ops_pipeline_hop_status CHECK (status IN ('OK', 'LAGGING', 'STALLED', 'STOPPED', 'PAUSED', 'UNKNOWN')),
    CONSTRAINT ck_ops_pipeline_hop_pause_reason CHECK (pause_reason IN ('NONE', 'BREAKER_OPEN', 'BACKPRESSURE', 'OPERATOR_HALT', 'REBALANCE', 'CORRUPT_HEAD')),
    CONSTRAINT ck_ops_pipeline_hop_lag_non_neg CHECK (lag_seconds >= 0),
    CONSTRAINT ck_ops_pipeline_hop_held_non_neg CHECK (oldest_held_age_seconds >= 0),
    CONSTRAINT ck_ops_pipeline_hop_runbook_btrim CHECK (btrim(runbook_url) = runbook_url AND runbook_url <> '')
);

CREATE TABLE IF NOT EXISTS ops_incident_triage_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    incident_id VARCHAR(128) NOT NULL,
    flow_name VARCHAR(128) NOT NULL,
    localized_hop VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    triage_action VARCHAR(64) NOT NULL,
    c_ctl_action_link VARCHAR(256) NOT NULL,
    triaged_by VARCHAR(128) NOT NULL,
    triaged_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_ops_incident_triage_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_ops_incident_triage_public_id UNIQUE (public_id),
    CONSTRAINT ck_ops_incident_triage_public_id_format CHECK (public_id ~ '^itl_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_ops_incident_triage_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_ops_incident_triage_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_ops_incident_triage_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_ops_incident_triage_incident_id_btrim CHECK (btrim(incident_id) = incident_id AND incident_id <> ''),
    CONSTRAINT ck_ops_incident_triage_flow_name_btrim CHECK (btrim(flow_name) = flow_name AND flow_name <> ''),
    CONSTRAINT ck_ops_incident_triage_localized_hop_btrim CHECK (btrim(localized_hop) = localized_hop AND localized_hop <> ''),
    CONSTRAINT ck_ops_incident_triage_severity CHECK (severity IN ('SEV0', 'SEV1', 'SEV2', 'INFO')),
    CONSTRAINT ck_ops_incident_triage_action_btrim CHECK (btrim(triage_action) = triage_action AND triage_action <> ''),
    CONSTRAINT ck_ops_incident_triage_link_btrim CHECK (btrim(c_ctl_action_link) = c_ctl_action_link AND c_ctl_action_link <> ''),
    CONSTRAINT ck_ops_incident_triage_by_btrim CHECK (btrim(triaged_by) = triaged_by AND triaged_by <> '')
);

CREATE INDEX IF NOT EXISTS idx_ops_pipeline_hop_status
    ON ops_pipeline_hop_ledger (tenant_id, flow_name, status);

CREATE INDEX IF NOT EXISTS idx_ops_incident_triage_eval
    ON ops_incident_triage_log (tenant_id, incident_id, triaged_at DESC);

-- Trigger: Immutability of incident triage log
CREATE OR REPLACE FUNCTION trg_prevent_ops_triage_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ops_incident_triage_log is strictly immutable: % operations are prohibited', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ops_triage_immutable ON ops_incident_triage_log;
CREATE TRIGGER trg_ops_triage_immutable
    BEFORE UPDATE OR DELETE ON ops_incident_triage_log
    FOR EACH ROW
    EXECUTE FUNCTION trg_prevent_ops_triage_mutation();
