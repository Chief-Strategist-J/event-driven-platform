-- migration:      0010
-- description:    create backpressure state and audit log tables
-- author:         chief-strategist
-- date:           2026-11-05T09:00:00Z
-- depends_on:     0009
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist adaptive backpressure control state and immutable audit log per WP-8 (BP-01 through BP-12). paused_at and resumed_at are nullable because timestamps are unpopulated when partition is in NORMAL state prior to any pause or after recovery is fully confirmed.

CREATE TABLE IF NOT EXISTS backpressure_state (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    handler_id VARCHAR(128) NOT NULL,
    dependency_id VARCHAR(128) NOT NULL,
    state VARCHAR(32) NOT NULL,
    pause_reason VARCHAR(64) NOT NULL DEFAULT '',
    concurrency_limit BIGINT NOT NULL DEFAULT 1,
    version BIGINT NOT NULL DEFAULT 1,
    paused_at TIMESTAMPTZ NULL,
    resumed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_backpressure_state PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_backpressure_state_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_backpressure_state_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uq_backpressure_state_handler_dep UNIQUE (tenant_id, handler_id, dependency_id),
    CONSTRAINT uq_backpressure_state_tenant_id UNIQUE (tenant_id, id),

    CONSTRAINT ck_backpressure_state_public_id_format
        CHECK (public_id ~ '^bp_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_backpressure_state_public_id_btrim
        CHECK (public_id = btrim(public_id) AND public_id <> ''),
    CONSTRAINT ck_backpressure_state_tenant_id_btrim
        CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT ck_backpressure_state_source_service_btrim
        CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_backpressure_state_external_id_btrim
        CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_backpressure_state_handler_id_btrim
        CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT ck_backpressure_state_dependency_id_btrim
        CHECK (dependency_id = btrim(dependency_id) AND dependency_id <> ''),
    CONSTRAINT ck_backpressure_state_pause_reason_btrim
        CHECK (pause_reason = btrim(pause_reason)),
    CONSTRAINT ck_backpressure_state_valid_state
        CHECK (state IN ('NORMAL', 'SATURATED', 'RECOVERING')),
    CONSTRAINT ck_backpressure_state_concurrency_min
        CHECK (concurrency_limit >= 1),
    CONSTRAINT ck_backpressure_state_version_min
        CHECK (version >= 1),
    CONSTRAINT ck_backpressure_state_paused_at_saturated
        CHECK (
            (state = 'SATURATED' AND paused_at IS NOT NULL)
            OR state <> 'SATURATED'
        )
);

CREATE INDEX IF NOT EXISTS idx_backpressure_state_tenant
    ON backpressure_state (tenant_id, state);

CREATE OR REPLACE FUNCTION fn_prevent_backpressure_state_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id
        OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id
        OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Immutable identifier modification rejected on backpressure_state';
    END IF;
    NEW.version = OLD.version + 1;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_backpressure_state_immutable_update ON backpressure_state;
CREATE TRIGGER trg_prevent_backpressure_state_immutable_update
    BEFORE UPDATE ON backpressure_state
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_backpressure_state_immutable_update();

CREATE TABLE IF NOT EXISTS backpressure_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    handler_id VARCHAR(128) NOT NULL,
    dependency_id VARCHAR(128) NOT NULL,
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    event VARCHAR(64) NOT NULL,
    pause_reason VARCHAR(64) NOT NULL DEFAULT '',
    reason VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_backpressure_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_backpressure_audit_log_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_backpressure_audit_log_external UNIQUE (tenant_id, source_service, external_id),

    CONSTRAINT ck_backpressure_audit_log_public_id_format
        CHECK (public_id ~ '^bpa_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_backpressure_audit_log_public_id_btrim
        CHECK (public_id = btrim(public_id) AND public_id <> ''),
    CONSTRAINT ck_backpressure_audit_log_tenant_id_btrim
        CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT ck_backpressure_audit_log_source_service_btrim
        CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_backpressure_audit_log_external_id_btrim
        CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_backpressure_audit_log_handler_id_btrim
        CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT ck_backpressure_audit_log_dependency_id_btrim
        CHECK (dependency_id = btrim(dependency_id) AND dependency_id <> ''),
    CONSTRAINT ck_backpressure_audit_log_pause_reason_btrim
        CHECK (pause_reason = btrim(pause_reason)),
    CONSTRAINT ck_backpressure_audit_log_reason_btrim
        CHECK (reason = btrim(reason) AND reason <> ''),
    CONSTRAINT ck_backpressure_audit_log_from_state
        CHECK (from_state IN ('NORMAL', 'SATURATED', 'RECOVERING')),
    CONSTRAINT ck_backpressure_audit_log_to_state
        CHECK (to_state IN ('NORMAL', 'SATURATED', 'RECOVERING')),
    CONSTRAINT ck_backpressure_audit_log_event
        CHECK (event IN ('SATURATION_DETECTED', 'RECOVERY_STARTED', 'RECOVERY_CONFIRMED'))
);

CREATE INDEX IF NOT EXISTS idx_backpressure_audit_tenant
    ON backpressure_audit_log (tenant_id, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_backpressure_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'backpressure_audit_log is append-only: modifications and deletions are prohibited';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_backpressure_audit_log_modification ON backpressure_audit_log;
CREATE TRIGGER trg_prevent_backpressure_audit_log_modification
    BEFORE UPDATE OR DELETE ON backpressure_audit_log
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_backpressure_audit_log_modification();

REVOKE UPDATE, DELETE ON backpressure_audit_log FROM PUBLIC;
