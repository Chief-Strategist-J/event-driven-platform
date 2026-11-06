-- migration:      0015
-- description:    create materialized view registry, projection state, held events, and rebuild log tables
-- author:         chief-strategist
-- date:           2026-11-06T12:22:00Z
-- depends_on:     0014
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist materialized view definitions, aggregate version state for monotonicity guarding, held out-of-order events, and zero-downtime blue/green view rebuild orchestration per WP-13 (MV-01 through MV-12). All columns are non-nullable.

CREATE TABLE IF NOT EXISTS materialized_view_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    view_class VARCHAR(32) NOT NULL,
    primary_store VARCHAR(32) NOT NULL DEFAULT 'POSTGRES',
    active_table_name VARCHAR(128) NOT NULL,
    freshness_slo_ms BIGINT NOT NULL DEFAULT 1000,
    consistency_mode VARCHAR(32) NOT NULL DEFAULT 'EVENTUAL',
    status VARCHAR(32) NOT NULL DEFAULT 'ONLINE',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_registry_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_registry_view_name UNIQUE (tenant_id, view_name),
    CONSTRAINT uk_materialized_view_registry_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_registry_public_id_format CHECK (public_id ~ '^mvr_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_materialized_view_registry_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_registry_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_registry_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_registry_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_registry_active_table_btrim CHECK (btrim(active_table_name) = active_table_name AND active_table_name <> ''),
    CONSTRAINT ck_materialized_view_registry_view_class CHECK (view_class IN ('V_STATE', 'V_AGG', 'V_JOIN', 'V_SEARCH', 'V_KV', 'V_REPORT')),
    CONSTRAINT ck_materialized_view_registry_primary_store CHECK (primary_store IN ('POSTGRES', 'REDIS', 'OPENSEARCH')),
    CONSTRAINT ck_materialized_view_registry_consistency_mode CHECK (consistency_mode IN ('EVENTUAL', 'TOKEN_BOUNDED', 'STRICT_SERIAL')),
    CONSTRAINT ck_materialized_view_registry_status CHECK (status IN ('ONLINE', 'REBUILDING', 'DEGRADED', 'DECOMMISSIONED')),
    CONSTRAINT ck_materialized_view_registry_freshness_slo_positive CHECK (freshness_slo_ms > 0)
);

CREATE TABLE IF NOT EXISTS materialized_view_projection_state (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    last_event_id UUID NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_projection_state PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_projection_state_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_projection_state_aggregate UNIQUE (tenant_id, view_name, aggregate_type, aggregate_id),
    CONSTRAINT uk_materialized_view_projection_state_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_projection_state_public_id_format CHECK (public_id ~ '^mps_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_materialized_view_projection_state_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_projection_state_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_projection_state_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_projection_state_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_projection_state_agg_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_materialized_view_projection_state_agg_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_materialized_view_projection_state_payload_hash_btrim CHECK (btrim(payload_hash) = payload_hash AND payload_hash <> ''),
    CONSTRAINT ck_materialized_view_projection_state_version_nonneg CHECK (current_version >= 0)
);

CREATE TABLE IF NOT EXISTS materialized_view_held_events (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    expected_version BIGINT NOT NULL,
    received_version BIGINT NOT NULL,
    event_id UUID NOT NULL,
    payload JSONB NOT NULL,
    held_reason VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'HELD',
    held_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_held_events PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_held_events_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_held_events_event UNIQUE (tenant_id, view_name, event_id),
    CONSTRAINT uk_materialized_view_held_events_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_held_events_public_id_format CHECK (public_id ~ '^mhe_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_materialized_view_held_events_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_held_events_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_held_events_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_held_events_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_held_events_agg_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_materialized_view_held_events_agg_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_materialized_view_held_events_reason_btrim CHECK (btrim(held_reason) = held_reason AND held_reason <> ''),
    CONSTRAINT ck_materialized_view_held_events_status CHECK (status IN ('HELD', 'DRAINED', 'DISCARDED')),
    CONSTRAINT ck_materialized_view_held_events_expected_nonneg CHECK (expected_version >= 0),
    CONSTRAINT ck_materialized_view_held_events_received_nonneg CHECK (received_version >= 0)
);

CREATE TABLE IF NOT EXISTS materialized_view_rebuild_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    rebuild_id VARCHAR(64) NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    source_store VARCHAR(32) NOT NULL DEFAULT 'POSTGRES',
    target_shadow_table VARCHAR(128) NOT NULL,
    target_lsn VARCHAR(64) NOT NULL,
    current_cursor_seq BIGINT NOT NULL DEFAULT 0,
    phase VARCHAR(32) NOT NULL DEFAULT 'INITIATED',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_rebuild_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_rebuild_log_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_rebuild_log_rebuild_id UNIQUE (rebuild_id),
    CONSTRAINT uk_materialized_view_rebuild_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_rebuild_log_public_id_format CHECK (public_id ~ '^mvr_reb_[0-9a-z]{12,24}$'),
    CONSTRAINT ck_materialized_view_rebuild_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_rebuild_id_btrim CHECK (btrim(rebuild_id) = rebuild_id AND rebuild_id <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_shadow_table_btrim CHECK (btrim(target_shadow_table) = target_shadow_table AND target_shadow_table <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_target_lsn_btrim CHECK (btrim(target_lsn) = target_lsn AND target_lsn <> ''),
    CONSTRAINT ck_materialized_view_rebuild_log_phase CHECK (phase IN ('INITIATED', 'HISTORICAL_REPLAY', 'STREAM_CATCHUP', 'PROMOTED', 'DECOMMISSIONED_OLD')),
    CONSTRAINT ck_materialized_view_rebuild_log_cursor_nonneg CHECK (current_cursor_seq >= 0)
);

CREATE OR REPLACE FUNCTION fn_prevent_materialized_view_registry_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR
       OLD.public_id <> NEW.public_id OR
       OLD.tenant_id <> NEW.tenant_id OR
       OLD.view_name <> NEW.view_name OR
       OLD.view_class <> NEW.view_class OR
       OLD.primary_store <> NEW.primary_store THEN
        RAISE EXCEPTION 'materialized_view_registry immutable identity column mutation forbidden';
    END IF;

    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_materialized_view_registry_immutable_update
BEFORE UPDATE ON materialized_view_registry
FOR EACH ROW EXECUTE FUNCTION fn_prevent_materialized_view_registry_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_materialized_view_projection_state_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR
       OLD.public_id <> NEW.public_id OR
       OLD.tenant_id <> NEW.tenant_id OR
       OLD.view_name <> NEW.view_name OR
       OLD.aggregate_type <> NEW.aggregate_type OR
       OLD.aggregate_id <> NEW.aggregate_id THEN
        RAISE EXCEPTION 'materialized_view_projection_state immutable aggregate identity mutation forbidden';
    END IF;

    IF NEW.current_version < OLD.current_version THEN
        RAISE EXCEPTION 'aggregate version regression forbidden: old version %, new version %', OLD.current_version, NEW.current_version;
    END IF;

    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_materialized_view_projection_state_immutable_update
BEFORE UPDATE ON materialized_view_projection_state
FOR EACH ROW EXECUTE FUNCTION fn_prevent_materialized_view_projection_state_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_materialized_view_rebuild_log_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR
       OLD.public_id <> NEW.public_id OR
       OLD.tenant_id <> NEW.tenant_id OR
       OLD.rebuild_id <> NEW.rebuild_id OR
       OLD.view_name <> NEW.view_name OR
       OLD.target_shadow_table <> NEW.target_shadow_table THEN
        RAISE EXCEPTION 'materialized_view_rebuild_log immutable target attributes mutation forbidden';
    END IF;

    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_materialized_view_rebuild_log_immutable_update
BEFORE UPDATE ON materialized_view_rebuild_log
FOR EACH ROW EXECUTE FUNCTION fn_prevent_materialized_view_rebuild_log_immutable_update();

