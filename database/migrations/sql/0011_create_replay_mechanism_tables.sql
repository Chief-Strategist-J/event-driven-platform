-- migration:      0011
-- description:    create replay run, effect log, and audit log tables
-- author:         chief-strategist
-- date:           2026-11-05T18:00:00Z
-- depends_on:     0010
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist historical replay run lifecycle, effect gateway decisions, and append-only state transition audit trail per WP-9 (RP-01 through RP-12). All columns are non-nullable.

CREATE TABLE IF NOT EXISTS replay_run (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    target_projection VARCHAR(128) NOT NULL,
    mode VARCHAR(32) NOT NULL,
    state VARCHAR(32) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    start_offset BIGINT NOT NULL DEFAULT 0,
    current_offset BIGINT NOT NULL DEFAULT 0,
    end_offset BIGINT NOT NULL DEFAULT 0,
    replay_consumer_group VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_replay_run PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_replay_run_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_replay_run_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uq_replay_run_tenant_id UNIQUE (tenant_id, id),

    CONSTRAINT ck_replay_run_public_id_format
        CHECK (public_id ~ '^rpl_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_replay_run_public_id_btrim
        CHECK (public_id = btrim(public_id) AND public_id <> ''),
    CONSTRAINT ck_replay_run_tenant_id_btrim
        CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT ck_replay_run_source_service_btrim
        CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_replay_run_external_id_btrim
        CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_replay_run_target_projection_btrim
        CHECK (target_projection = btrim(target_projection) AND target_projection <> ''),
    CONSTRAINT ck_replay_run_replay_consumer_group_btrim
        CHECK (replay_consumer_group = btrim(replay_consumer_group) AND replay_consumer_group <> ''),
    CONSTRAINT ck_replay_run_mode
        CHECK (mode IN ('REBUILD', 'HYDRATE', 'FORENSIC', 'RECOVER', 'REPAIR', 'LIVE')),
    CONSTRAINT ck_replay_run_state
        CHECK (state IN ('INITIALIZING', 'STREAMING', 'FOLLOWING', 'CAUGHT_UP', 'PROMOTED', 'COMPLETED', 'ABORTED')),
    CONSTRAINT ck_replay_run_scope
        CHECK (scope IN ('GLOBAL_TOPIC', 'TIME_WINDOW', 'AGGREGATE_SET')),
    CONSTRAINT ck_replay_run_start_offset_non_neg
        CHECK (start_offset >= 0),
    CONSTRAINT ck_replay_run_current_offset_min
        CHECK (current_offset >= start_offset),
    CONSTRAINT ck_replay_run_end_offset_valid
        CHECK (end_offset >= start_offset),
    CONSTRAINT ck_replay_run_version_min
        CHECK (version >= 1)
);

CREATE INDEX IF NOT EXISTS idx_replay_run_tenant_state
    ON replay_run (tenant_id, state, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_replay_run_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id
        OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id
        OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Immutable identifier modification rejected on replay_run';
    END IF;
    NEW.version = OLD.version + 1;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_replay_run_immutable_update ON replay_run;
CREATE TRIGGER trg_prevent_replay_run_immutable_update
    BEFORE UPDATE ON replay_run
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_replay_run_immutable_update();

CREATE TABLE IF NOT EXISTS replay_effect_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    run_id UUID NOT NULL,
    effect_type VARCHAR(64) NOT NULL,
    effect_mode VARCHAR(32) NOT NULL,
    decision VARCHAR(32) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    payload_digest VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_replay_effect_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_replay_effect_log_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_replay_effect_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT fk_replay_effect_log_run FOREIGN KEY (tenant_id, run_id)
        REFERENCES replay_run (tenant_id, id) ON DELETE RESTRICT,

    CONSTRAINT ck_replay_effect_log_public_id_format
        CHECK (public_id ~ '^rpe_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_replay_effect_log_public_id_btrim
        CHECK (public_id = btrim(public_id) AND public_id <> ''),
    CONSTRAINT ck_replay_effect_log_tenant_id_btrim
        CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT ck_replay_effect_log_source_service_btrim
        CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_replay_effect_log_external_id_btrim
        CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_replay_effect_log_idempotency_key_btrim
        CHECK (idempotency_key = btrim(idempotency_key) AND idempotency_key <> ''),
    CONSTRAINT ck_replay_effect_log_effect_type
        CHECK (effect_type IN ('EXTERNAL_PAYMENT', 'EXTERNAL_NOTIFICATION', 'OUTBOX_EVENT', 'SEARCH_INDEX', 'CACHE_WRITE')),
    CONSTRAINT ck_replay_effect_log_effect_mode
        CHECK (effect_mode IN ('SUPPRESS', 'ALLOW', 'RECONCILE')),
    CONSTRAINT ck_replay_effect_log_decision
        CHECK (decision IN ('SUPPRESSED', 'EXECUTED', 'RECONCILED'))
);

CREATE INDEX IF NOT EXISTS idx_replay_effect_log_run
    ON replay_effect_log (tenant_id, run_id, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_replay_effect_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'replay_effect_log is append-only: modifications and deletions are prohibited';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_replay_effect_log_modification ON replay_effect_log;
CREATE TRIGGER trg_prevent_replay_effect_log_modification
    BEFORE UPDATE OR DELETE ON replay_effect_log
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_replay_effect_log_modification();

REVOKE UPDATE, DELETE ON replay_effect_log FROM PUBLIC;

CREATE TABLE IF NOT EXISTS replay_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    run_id UUID NOT NULL,
    from_state VARCHAR(32) NOT NULL,
    to_state VARCHAR(32) NOT NULL,
    event VARCHAR(64) NOT NULL,
    reason VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_replay_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_replay_audit_log_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_replay_audit_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT fk_replay_audit_log_run FOREIGN KEY (tenant_id, run_id)
        REFERENCES replay_run (tenant_id, id) ON DELETE RESTRICT,

    CONSTRAINT ck_replay_audit_log_public_id_format
        CHECK (public_id ~ '^rpa_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_replay_audit_log_public_id_btrim
        CHECK (public_id = btrim(public_id) AND public_id <> ''),
    CONSTRAINT ck_replay_audit_log_tenant_id_btrim
        CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT ck_replay_audit_log_source_service_btrim
        CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_replay_audit_log_external_id_btrim
        CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_replay_audit_log_reason_btrim
        CHECK (reason = btrim(reason) AND reason <> ''),
    CONSTRAINT ck_replay_audit_log_from_state
        CHECK (from_state IN ('INITIALIZING', 'STREAMING', 'FOLLOWING', 'CAUGHT_UP', 'PROMOTED', 'COMPLETED', 'ABORTED')),
    CONSTRAINT ck_replay_audit_log_to_state
        CHECK (to_state IN ('INITIALIZING', 'STREAMING', 'FOLLOWING', 'CAUGHT_UP', 'PROMOTED', 'COMPLETED', 'ABORTED')),
    CONSTRAINT ck_replay_audit_log_event
        CHECK (event IN ('START_STREAMING', 'FOLLOW_LIVE', 'CATCH_UP_REACHED', 'PROMOTE_WRITER', 'COMPLETE_CUTOVER', 'ABORT_RUN'))
);

CREATE INDEX IF NOT EXISTS idx_replay_audit_log_run
    ON replay_audit_log (tenant_id, run_id, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_replay_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'replay_audit_log is append-only: modifications and deletions are prohibited';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_replay_audit_log_modification ON replay_audit_log;
CREATE TRIGGER trg_prevent_replay_audit_log_modification
    BEFORE UPDATE OR DELETE ON replay_audit_log
    FOR EACH ROW EXECUTE FUNCTION fn_prevent_replay_audit_log_modification();

REVOKE UPDATE, DELETE ON replay_audit_log FROM PUBLIC;

