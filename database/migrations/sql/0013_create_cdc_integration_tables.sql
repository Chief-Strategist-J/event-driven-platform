-- migration:      0013
-- description:    create cdc connector inventory, signal log, heartbeat log, and canary reconciliation tables
-- author:         chief-strategist
-- date:           2026-11-06T10:22:00Z
-- depends_on:     0012
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist logical replication connector inventory, debezium signal coordination table, idle database heartbeat advancement log, and continuous end-to-end canary reconciliation per WP-11 (CDC-01 through CDC-12). All columns are non-nullable.

CREATE TABLE IF NOT EXISTS cdc_connector_inventory (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    connector_name VARCHAR(128) NOT NULL,
    slot_name VARCHAR(128) NOT NULL,
    publication_name VARCHAR(128) NOT NULL,
    mode VARCHAR(32) NOT NULL,
    state VARCHAR(32) NOT NULL,
    wal_retained_bytes BIGINT NOT NULL DEFAULT 0,
    wal_lag_bytes BIGINT NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cdc_connector_inventory PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cdc_connector_inventory_public_id UNIQUE (public_id),
    CONSTRAINT uk_cdc_connector_inventory_name UNIQUE (connector_name),
    CONSTRAINT uk_cdc_connector_inventory_slot UNIQUE (slot_name),
    CONSTRAINT uk_cdc_connector_inventory_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_cdc_connector_inventory_public_id_format CHECK (public_id ~ '^cdc_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cdc_connector_inventory_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cdc_connector_inventory_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cdc_connector_inventory_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cdc_connector_inventory_name_btrim CHECK (btrim(connector_name) = connector_name AND connector_name <> ''),
    CONSTRAINT ck_cdc_connector_inventory_slot_btrim CHECK (btrim(slot_name) = slot_name AND slot_name <> ''),
    CONSTRAINT ck_cdc_connector_inventory_publication_btrim CHECK (btrim(publication_name) = publication_name AND publication_name <> ''),
    CONSTRAINT ck_cdc_connector_inventory_mode CHECK (mode IN ('M_OUTBOX', 'M_RAWTABLE')),
    CONSTRAINT ck_cdc_connector_inventory_state CHECK (state IN ('UNREGISTERED', 'PROVISIONING', 'RUNNING', 'PAUSED', 'FAILED', 'DECOMMISSIONED')),
    CONSTRAINT ck_cdc_connector_inventory_wal_retained_nonneg CHECK (wal_retained_bytes >= 0),
    CONSTRAINT ck_cdc_connector_inventory_wal_lag_nonneg CHECK (wal_lag_bytes >= 0),
    CONSTRAINT ck_cdc_connector_inventory_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS cdc_signal_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    connector_name VARCHAR(128) NOT NULL,
    signal_type VARCHAR(64) NOT NULL,
    signal_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cdc_signal_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cdc_signal_log_public_id UNIQUE (public_id),
    CONSTRAINT fk_cdc_signal_log_connector FOREIGN KEY (connector_name) REFERENCES cdc_connector_inventory (connector_name) ON DELETE RESTRICT,
    CONSTRAINT ck_cdc_signal_log_public_id_format CHECK (public_id ~ '^sig_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cdc_signal_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cdc_signal_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cdc_signal_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cdc_signal_log_connector_btrim CHECK (btrim(connector_name) = connector_name AND connector_name <> ''),
    CONSTRAINT ck_cdc_signal_log_type CHECK (signal_type IN ('EXECUTE_SNAPSHOT', 'PAUSE_SNAPSHOT', 'RESUME_SNAPSHOT', 'CHECKPOINT')),
    CONSTRAINT ck_cdc_signal_log_processed_consistency CHECK ((processed = TRUE) = (processed_at IS NOT NULL)),
    CONSTRAINT ck_cdc_signal_log_payload_is_object CHECK (jsonb_typeof(signal_payload) = 'object')
);

CREATE TABLE IF NOT EXISTS cdc_heartbeat_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    connector_name VARCHAR(128) NOT NULL,
    lsn_position VARCHAR(64) NOT NULL,
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cdc_heartbeat_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cdc_heartbeat_log_public_id UNIQUE (public_id),
    CONSTRAINT fk_cdc_heartbeat_log_connector FOREIGN KEY (connector_name) REFERENCES cdc_connector_inventory (connector_name) ON DELETE RESTRICT,
    CONSTRAINT ck_cdc_heartbeat_log_public_id_format CHECK (public_id ~ '^hb_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cdc_heartbeat_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cdc_heartbeat_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cdc_heartbeat_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cdc_heartbeat_log_connector_btrim CHECK (btrim(connector_name) = connector_name AND connector_name <> ''),
    CONSTRAINT ck_cdc_heartbeat_log_lsn_btrim CHECK (btrim(lsn_position) = lsn_position AND lsn_position <> '')
);

CREATE TABLE IF NOT EXISTS cdc_canary_reconciliation_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    connector_name VARCHAR(128) NOT NULL,
    canary_event_id UUID NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    received_at TIMESTAMPTZ NULL,
    latency_ms BIGINT NOT NULL DEFAULT 0,
    reconciled BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_cdc_canary_reconciliation_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cdc_canary_reconciliation_log_public_id UNIQUE (public_id),
    CONSTRAINT uk_cdc_canary_reconciliation_log_canary_id UNIQUE (tenant_id, canary_event_id),
    CONSTRAINT fk_cdc_canary_reconciliation_connector FOREIGN KEY (connector_name) REFERENCES cdc_connector_inventory (connector_name) ON DELETE RESTRICT,
    CONSTRAINT ck_cdc_canary_public_id_format CHECK (public_id ~ '^cnr_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cdc_canary_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cdc_canary_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cdc_canary_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cdc_canary_connector_btrim CHECK (btrim(connector_name) = connector_name AND connector_name <> ''),
    CONSTRAINT ck_cdc_canary_latency_nonneg CHECK (latency_ms >= 0),
    CONSTRAINT ck_cdc_canary_reconciled_consistency CHECK ((reconciled = TRUE) = (received_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_cdc_connector_inventory_state ON cdc_connector_inventory (state);
CREATE INDEX IF NOT EXISTS idx_cdc_signal_log_unprocessed ON cdc_signal_log (connector_name, processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_cdc_heartbeat_log_connector ON cdc_heartbeat_log (connector_name, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_canary_reconciliation_unreconciled ON cdc_canary_reconciliation_log (connector_name, reconciled) WHERE reconciled = FALSE;

CREATE OR REPLACE FUNCTION fn_prevent_cdc_connector_inventory_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id THEN
        RAISE EXCEPTION 'cannot modify immutable cdc_connector_inventory.id: %', OLD.id USING ERRCODE = '23514';
    END IF;
    IF OLD.public_id <> NEW.public_id THEN
        RAISE EXCEPTION 'cannot modify immutable cdc_connector_inventory.public_id: %', OLD.public_id USING ERRCODE = '23514';
    END IF;
    IF OLD.tenant_id <> NEW.tenant_id THEN
        RAISE EXCEPTION 'cannot modify immutable cdc_connector_inventory.tenant_id: %', OLD.tenant_id USING ERRCODE = '23514';
    END IF;
    IF OLD.connector_name <> NEW.connector_name THEN
        RAISE EXCEPTION 'cannot modify immutable cdc_connector_inventory.connector_name: %', OLD.connector_name USING ERRCODE = '23514';
    END IF;
    IF OLD.slot_name <> NEW.slot_name THEN
        RAISE EXCEPTION 'cannot modify immutable cdc_connector_inventory.slot_name: %', OLD.slot_name USING ERRCODE = '23514';
    END IF;
    NEW.version := OLD.version + 1;
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_cdc_connector_inventory_immutable_update ON cdc_connector_inventory;
CREATE TRIGGER trg_prevent_cdc_connector_inventory_immutable_update
    BEFORE UPDATE ON cdc_connector_inventory
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_cdc_connector_inventory_immutable_update();

