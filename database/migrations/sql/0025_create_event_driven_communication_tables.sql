-- migration:      0025
-- description:    create saga definitions, saga instances, saga steps, and async command reply ledgers
-- author:         chief-strategist
-- date:           2026-11-06T16:43:00Z
-- depends_on:     0024
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce EC-01 (outbox pub/sub), EC-03 (durable timeouts), EC-04 (saga compensation symmetry), and EC-05 (late reply handling) per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS saga_definition_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_name VARCHAR(128) NOT NULL,
    total_steps INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_saga_definition_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_saga_definition_public_id UNIQUE (public_id),
    CONSTRAINT uk_saga_definition_name UNIQUE (tenant_id, saga_name),
    CONSTRAINT uk_saga_definition_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_saga_definition_public_id_format CHECK (public_id ~ '^sgd_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_saga_definition_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_saga_definition_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_saga_definition_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_saga_definition_name_btrim CHECK (btrim(saga_name) = saga_name AND saga_name <> ''),
    CONSTRAINT ck_saga_definition_steps_pos CHECK (total_steps > 0)
);

CREATE TABLE IF NOT EXISTS saga_instance_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_definition_id VARCHAR(32) NOT NULL,
    current_step_index INTEGER NOT NULL DEFAULT 0,
    state VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    deadline_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_saga_instance_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_saga_instance_public_id UNIQUE (public_id),
    CONSTRAINT uk_saga_instance_external UNIQUE (tenant_id, external_id),
    CONSTRAINT ck_saga_instance_public_id_format CHECK (public_id ~ '^sgi_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_saga_instance_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_saga_instance_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_saga_instance_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_saga_instance_def_id_format CHECK (saga_definition_id ~ '^sgd_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_saga_instance_step_non_neg CHECK (current_step_index >= 0),
    CONSTRAINT ck_saga_instance_state CHECK (state IN ('IN_PROGRESS', 'SUCCEEDED', 'COMPENSATING', 'COMPENSATED', 'FAILED', 'MANUAL_INTERVENTION'))
);

CREATE TABLE IF NOT EXISTS saga_step_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    saga_instance_id VARCHAR(32) NOT NULL,
    step_index INTEGER NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    step_type VARCHAR(32) NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    command_payload TEXT NOT NULL,
    reply_payload TEXT NOT NULL DEFAULT '{}',
    compensation_payload TEXT NOT NULL DEFAULT '{}',
    deadline_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_saga_step_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_saga_step_public_id UNIQUE (public_id),
    CONSTRAINT uk_saga_step_instance_idx UNIQUE (tenant_id, saga_instance_id, step_index),
    CONSTRAINT uk_saga_step_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_saga_step_public_id_format CHECK (public_id ~ '^sgs_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_saga_step_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_saga_step_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_saga_step_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_saga_step_instance_id_format CHECK (saga_instance_id ~ '^sgi_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_saga_step_index_non_neg CHECK (step_index >= 0),
    CONSTRAINT ck_saga_step_name_btrim CHECK (btrim(step_name) = step_name AND step_name <> ''),
    CONSTRAINT ck_saga_step_type CHECK (step_type IN ('COMPENSABLE', 'PIVOT', 'RETRIABLE')),
    CONSTRAINT ck_saga_step_state CHECK (state IN ('PENDING', 'SENT', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'COMPENSATING', 'COMPENSATED'))
);

CREATE TABLE IF NOT EXISTS async_command_reply_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    command_id VARCHAR(64) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    is_late_reply BOOLEAN NOT NULL DEFAULT FALSE,
    reply_payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_async_command_reply_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_async_command_reply_public_id UNIQUE (public_id),
    CONSTRAINT uk_async_command_reply_cmd UNIQUE (tenant_id, command_id),
    CONSTRAINT uk_async_command_reply_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_async_command_reply_public_id_format CHECK (public_id ~ '^acr_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_async_command_reply_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_async_command_reply_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_async_command_reply_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_async_command_reply_command_id_btrim CHECK (btrim(command_id) = command_id AND command_id <> ''),
    CONSTRAINT ck_async_command_reply_outcome CHECK (outcome IN ('SUCCEEDED', 'REJECTED', 'FAILED', 'TIMED_OUT'))
);

CREATE INDEX IF NOT EXISTS idx_saga_instance_state_lookup
    ON saga_instance_ledger (tenant_id, state, deadline_at);

CREATE INDEX IF NOT EXISTS idx_saga_step_instance_lookup
    ON saga_step_ledger (tenant_id, saga_instance_id, state);

CREATE INDEX IF NOT EXISTS idx_async_reply_cmd_lookup
    ON async_command_reply_ledger (tenant_id, command_id, outcome);

CREATE OR REPLACE FUNCTION trg_prevent_async_reply_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Async command replies are strictly immutable and cannot be updated or deleted';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_async_reply_no_update ON async_command_reply_ledger;
CREATE TRIGGER trg_async_reply_no_update
    BEFORE UPDATE OR DELETE ON async_command_reply_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_async_reply_mutation();

