-- migration:      0020
-- description:    create schema evolution tables: version registry, consumer registry, dual-publish ledger, and incident log
-- author:         chief-strategist
-- date:           2026-11-06T14:24:00Z
-- depends_on:     0019
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce SE-01 (transitive compatibility), SE-06 (consumer readiness gating), SE-07 (decommission gates), and SE-11 (systemic failure detection) per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS schema_evolution_version_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    subject_name VARCHAR(128) NOT NULL,
    schema_version INT NOT NULL,
    compatibility_mode VARCHAR(32) NOT NULL DEFAULT 'FULL_TRANSITIVE',
    schema_fingerprint VARCHAR(64) NOT NULL,
    schema_definition TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_schema_evolution_version_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_schema_evolution_version_public_id UNIQUE (public_id),
    CONSTRAINT uk_schema_evolution_version_subject UNIQUE (tenant_id, subject_name, schema_version),
    CONSTRAINT uk_schema_evolution_version_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_schema_evolution_version_public_id_format CHECK (public_id ~ '^sev_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_schema_evolution_version_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_schema_evolution_version_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_schema_evolution_version_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_schema_evolution_version_subject_btrim CHECK (btrim(subject_name) = subject_name AND subject_name <> ''),
    CONSTRAINT ck_schema_evolution_version_pos CHECK (schema_version > 0),
    CONSTRAINT ck_schema_evolution_version_fingerprint_len CHECK (length(schema_fingerprint) = 64),
    CONSTRAINT ck_schema_evolution_version_compatibility CHECK (compatibility_mode IN ('FULL_TRANSITIVE', 'BACKWARD_TRANSITIVE', 'FORWARD_TRANSITIVE', 'FULL', 'BACKWARD', 'FORWARD')),
    CONSTRAINT ck_schema_evolution_version_status CHECK (status IN ('ACTIVE', 'DEPRECATED', 'RETIRED'))
);

CREATE TABLE IF NOT EXISTS schema_evolution_consumer_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    subject_name VARCHAR(128) NOT NULL,
    min_supported_version INT NOT NULL,
    max_supported_version INT NOT NULL,
    run_cadence VARCHAR(32) NOT NULL DEFAULT 'CONTINUOUS',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_schema_evolution_consumer_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_schema_evolution_consumer_public_id UNIQUE (public_id),
    CONSTRAINT uk_schema_evolution_consumer_group UNIQUE (tenant_id, consumer_group, subject_name),
    CONSTRAINT uk_schema_evolution_consumer_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_schema_evolution_consumer_public_id_format CHECK (public_id ~ '^sec_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_schema_evolution_consumer_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_schema_evolution_consumer_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_schema_evolution_consumer_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_schema_evolution_consumer_group_btrim CHECK (btrim(consumer_group) = consumer_group AND consumer_group <> ''),
    CONSTRAINT ck_schema_evolution_consumer_subject_btrim CHECK (btrim(subject_name) = subject_name AND subject_name <> ''),
    CONSTRAINT ck_schema_evolution_consumer_versions_valid CHECK (min_supported_version > 0 AND max_supported_version >= min_supported_version),
    CONSTRAINT ck_schema_evolution_consumer_cadence CHECK (run_cadence IN ('CONTINUOUS', 'HOURLY', 'DAILY', 'MONTHLY')),
    CONSTRAINT ck_schema_evolution_consumer_status CHECK (status IN ('ACTIVE', 'MIGRATED', 'DECOMMISSIONED'))
);

CREATE TABLE IF NOT EXISTS schema_evolution_dual_publish_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    subject_name VARCHAR(128) NOT NULL,
    legacy_topic VARCHAR(128) NOT NULL,
    modern_topic VARCHAR(128) NOT NULL,
    sunset_deadline TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    decommission_attested BOOLEAN NOT NULL DEFAULT FALSE,
    zero_lag_days INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_schema_evolution_dual_publish_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_schema_evolution_dual_publish_public_id UNIQUE (public_id),
    CONSTRAINT uk_schema_evolution_dual_publish_subject UNIQUE (tenant_id, subject_name),
    CONSTRAINT uk_schema_evolution_dual_publish_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_schema_evolution_dual_publish_public_id_format CHECK (public_id ~ '^sed_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_schema_evolution_dual_publish_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_subject_btrim CHECK (btrim(subject_name) = subject_name AND subject_name <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_legacy_btrim CHECK (btrim(legacy_topic) = legacy_topic AND legacy_topic <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_modern_btrim CHECK (btrim(modern_topic) = modern_topic AND modern_topic <> ''),
    CONSTRAINT ck_schema_evolution_dual_publish_status CHECK (status IN ('ACTIVE', 'DRAINING', 'DECOMMISSIONED', 'EXTENDED')),
    CONSTRAINT ck_schema_evolution_dual_publish_zero_lag_nonneg CHECK (zero_lag_days >= 0)
);

CREATE TABLE IF NOT EXISTS schema_evolution_incident_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    subject_name VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    incident_class VARCHAR(32) NOT NULL,
    payload_fingerprint VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_schema_evolution_incident_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_schema_evolution_incident_public_id UNIQUE (public_id),
    CONSTRAINT uk_schema_evolution_incident_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_schema_evolution_incident_public_id_format CHECK (public_id ~ '^sei_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_schema_evolution_incident_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_schema_evolution_incident_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_schema_evolution_incident_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_schema_evolution_incident_subject_btrim CHECK (btrim(subject_name) = subject_name AND subject_name <> ''),
    CONSTRAINT ck_schema_evolution_incident_consumer_btrim CHECK (btrim(consumer_group) = consumer_group AND consumer_group <> ''),
    CONSTRAINT ck_schema_evolution_incident_class CHECK (incident_class IN ('SINGLE_CORRUPT', 'SYSTEMIC_FAILURE', 'UNKNOWN_SCHEMA_ID', 'SEMANTIC_DRIFT')),
    CONSTRAINT ck_schema_evolution_incident_fingerprint_len CHECK (length(payload_fingerprint) = 64),
    CONSTRAINT ck_schema_evolution_incident_status CHECK (status IN ('OPEN', 'HALTED', 'DLQ_QUARANTINED', 'RESOLVED'))
);

CREATE INDEX IF NOT EXISTS idx_sev_subject_lookup
    ON schema_evolution_version_registry (tenant_id, subject_name, schema_version DESC);

CREATE INDEX IF NOT EXISTS idx_sec_consumer_check
    ON schema_evolution_consumer_registry (tenant_id, subject_name, status);

CREATE INDEX IF NOT EXISTS idx_sed_dual_publish_active
    ON schema_evolution_dual_publish_ledger (tenant_id, status, sunset_deadline);

CREATE OR REPLACE FUNCTION trg_prevent_sev_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.correlation_id <> NEW.correlation_id OR OLD.subject_name <> NEW.subject_name OR
       OLD.schema_version <> NEW.schema_version OR OLD.schema_fingerprint <> NEW.schema_fingerprint THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in schema_evolution_version_registry';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sev_immutable ON schema_evolution_version_registry;
CREATE TRIGGER trg_sev_immutable
    BEFORE UPDATE ON schema_evolution_version_registry
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_sev_immutable_update();

CREATE OR REPLACE FUNCTION trg_prevent_sec_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.correlation_id <> NEW.correlation_id OR OLD.consumer_group <> NEW.consumer_group OR
       OLD.subject_name <> NEW.subject_name THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in schema_evolution_consumer_registry';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sec_immutable ON schema_evolution_consumer_registry;
CREATE TRIGGER trg_sec_immutable
    BEFORE UPDATE ON schema_evolution_consumer_registry
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_sec_immutable_update();

