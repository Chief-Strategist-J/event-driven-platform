-- migration:      0018
-- description:    create eventual consistency monitoring tables: watermark ledger, drift incident log, check run history, and silence registry
-- author:         chief-strategist
-- date:           2026-11-06T13:45:00Z
-- depends_on:     0017
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         track stream watermarks, lag SLAs, deterministic drift classifications, three-valued check executions, and alert silences with enforced TTL per WP-14. All columns non-nullable.

CREATE TABLE IF NOT EXISTS consistency_watermark_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    stream_name VARCHAR(128) NOT NULL,
    partition_id INT NOT NULL,
    high_watermark_offset BIGINT NOT NULL DEFAULT 0,
    consumer_offset BIGINT NOT NULL DEFAULT 0,
    offset_lag BIGINT NOT NULL DEFAULT 0,
    freshness_lag_ms BIGINT NOT NULL DEFAULT 0,
    watermark_status VARCHAR(32) NOT NULL DEFAULT 'IN_SYNC',
    observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consistency_watermark_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_consistency_watermark_ledger_public_id UNIQUE (public_id),
    CONSTRAINT uk_consistency_watermark_partition UNIQUE (tenant_id, view_name, stream_name, partition_id),
    CONSTRAINT uk_consistency_watermark_ledger_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consistency_watermark_ledger_public_id_format CHECK (public_id ~ '^ecw_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_consistency_watermark_ledger_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_consistency_watermark_ledger_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_consistency_watermark_ledger_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_consistency_watermark_ledger_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_consistency_watermark_ledger_stream_name_btrim CHECK (btrim(stream_name) = stream_name AND stream_name <> ''),
    CONSTRAINT ck_consistency_watermark_ledger_partition_id_nonneg CHECK (partition_id >= 0),
    CONSTRAINT ck_consistency_watermark_ledger_offsets_nonneg CHECK (high_watermark_offset >= 0 AND consumer_offset >= 0 AND offset_lag >= 0),
    CONSTRAINT ck_consistency_watermark_ledger_freshness_nonneg CHECK (freshness_lag_ms >= 0),
    CONSTRAINT ck_consistency_watermark_ledger_status CHECK (watermark_status IN ('IN_SYNC', 'LAGGING', 'STALLED'))
);

CREATE TABLE IF NOT EXISTS consistency_drift_incident_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    drift_class VARCHAR(32) NOT NULL,
    source_version BIGINT NOT NULL,
    view_version BIGINT NOT NULL,
    difference_payload TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DETECTED',
    heal_attempts INT NOT NULL DEFAULT 0,
    quarantined_until TIMESTAMPTZ NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consistency_drift_incident_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_consistency_drift_incident_log_public_id UNIQUE (public_id),
    CONSTRAINT uk_consistency_drift_incident_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consistency_drift_incident_log_public_id_format CHECK (public_id ~ '^ecd_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_consistency_drift_incident_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_aggregate_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_aggregate_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_consistency_drift_incident_log_drift_class CHECK (drift_class IN ('GHOST', 'MISSING', 'ATTRIBUTE_MISMATCH', 'VERSION_LAG', 'MANUAL_WRITE')),
    CONSTRAINT ck_consistency_drift_incident_log_status CHECK (status IN ('DETECTED', 'IN_TOLERANCE', 'CONFIRMED', 'AUTO_HEALING', 'RESOLVED', 'MANUAL_INTERVENTION')),
    CONSTRAINT ck_consistency_drift_incident_log_heal_attempts_nonneg CHECK (heal_attempts >= 0)
);

CREATE TABLE IF NOT EXISTS consistency_check_run_history (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    check_type VARCHAR(32) NOT NULL,
    evaluated_keys BIGINT NOT NULL DEFAULT 0,
    match_count BIGINT NOT NULL DEFAULT 0,
    mismatch_count BIGINT NOT NULL DEFAULT 0,
    unknown_count BIGINT NOT NULL DEFAULT 0,
    coverage_ratio NUMERIC(5,4) NOT NULL DEFAULT 1.0000,
    execution_duration_ms BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consistency_check_run_history PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_consistency_check_run_history_public_id UNIQUE (public_id),
    CONSTRAINT uk_consistency_check_run_history_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consistency_check_run_history_public_id_format CHECK (public_id ~ '^ecc_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_consistency_check_run_history_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_consistency_check_run_history_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_consistency_check_run_history_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_consistency_check_run_history_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_consistency_check_run_history_check_type CHECK (check_type IN ('CANARY_PROBE', 'WATERMARK_SWEEP', 'MERKLE_RANGE_AUDIT', 'INVARIANT_ASSERTION')),
    CONSTRAINT ck_consistency_check_run_history_status CHECK (status IN ('COMPLETED', 'DEGRADED', 'FAILED', 'ABORTED_SHED')),
    CONSTRAINT ck_consistency_check_run_history_counts_nonneg CHECK (evaluated_keys >= 0 AND match_count >= 0 AND mismatch_count >= 0 AND unknown_count >= 0),
    CONSTRAINT ck_consistency_check_run_history_coverage_range CHECK (coverage_ratio >= 0.0000 AND coverage_ratio <= 1.0000),
    CONSTRAINT ck_consistency_check_run_history_duration_nonneg CHECK (execution_duration_ms >= 0)
);

CREATE TABLE IF NOT EXISTS consistency_view_silence_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    reason TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consistency_view_silence_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_consistency_view_silence_registry_public_id UNIQUE (public_id),
    CONSTRAINT uk_consistency_view_silence_registry_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consistency_view_silence_registry_public_id_format CHECK (public_id ~ '^ecs_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_consistency_view_silence_registry_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_consistency_view_silence_registry_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_consistency_view_silence_registry_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_consistency_view_silence_registry_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_consistency_view_silence_registry_owner_id_btrim CHECK (btrim(owner_id) = owner_id AND owner_id <> ''),
    CONSTRAINT ck_consistency_view_silence_registry_status CHECK (status IN ('ACTIVE', 'EXPIRED', 'REVOKED'))
);

CREATE INDEX IF NOT EXISTS idx_watermark_view_status
    ON consistency_watermark_ledger (tenant_id, view_name, watermark_status);

CREATE INDEX IF NOT EXISTS idx_drift_incident_lookup
    ON consistency_drift_incident_log (tenant_id, view_name, aggregate_type, aggregate_id, status);

CREATE INDEX IF NOT EXISTS idx_drift_incident_quarantine
    ON consistency_drift_incident_log (status, quarantined_until);

CREATE INDEX IF NOT EXISTS idx_check_run_view
    ON consistency_check_run_history (tenant_id, view_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_silence_view_active
    ON consistency_view_silence_registry (tenant_id, view_name, status, expires_at);

CREATE OR REPLACE FUNCTION trg_prevent_watermark_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.view_name <> NEW.view_name OR OLD.stream_name <> NEW.stream_name OR OLD.partition_id <> NEW.partition_id THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in consistency_watermark_ledger';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_consistency_watermark_immutable ON consistency_watermark_ledger;
CREATE TRIGGER trg_consistency_watermark_immutable
    BEFORE UPDATE ON consistency_watermark_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_watermark_immutable_update();

CREATE OR REPLACE FUNCTION trg_prevent_drift_incident_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.view_name <> NEW.view_name OR OLD.aggregate_type <> NEW.aggregate_type OR OLD.aggregate_id <> NEW.aggregate_id THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in consistency_drift_incident_log';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_consistency_drift_incident_immutable ON consistency_drift_incident_log;
CREATE TRIGGER trg_consistency_drift_incident_immutable
    BEFORE UPDATE ON consistency_drift_incident_log
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_drift_incident_immutable_update();

