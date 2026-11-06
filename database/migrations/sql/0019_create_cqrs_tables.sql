-- migration:      0019
-- description:    create cqrs command idempotency ledger, aggregate root ledger, and consistency token key registry
-- author:         chief-strategist
-- date:           2026-11-06T14:06:00Z
-- depends_on:     0018
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce CQ-01 (atomic idempotency by key), CQ-02 (concurrency control under lock), CQ-03 (expected version guards), and CQ-07 (signed token issuance) per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS cqrs_command_idempotency_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    command_name VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    response_payload TEXT NOT NULL DEFAULT '{}',
    consistency_token VARCHAR(256) NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cqrs_command_idempotency_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cqrs_command_idempotency_public_id UNIQUE (public_id),
    CONSTRAINT uk_cqrs_command_idempotency_key UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT uk_cqrs_command_idempotency_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_cqrs_command_idempotency_public_id_format CHECK (public_id ~ '^cmd_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cqrs_command_idempotency_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_key_btrim CHECK (btrim(idempotency_key) = idempotency_key AND idempotency_key <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_command_name_btrim CHECK (btrim(command_name) = command_name AND command_name <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_aggregate_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_aggregate_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_cqrs_command_idempotency_status CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'REJECTED', 'CONFLICT')),
    CONSTRAINT ck_cqrs_command_idempotency_fingerprint_length CHECK (length(request_fingerprint) = 64)
);

CREATE TABLE IF NOT EXISTS cqrs_aggregate_root_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    state_payload TEXT NOT NULL DEFAULT '{}',
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cqrs_aggregate_root_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cqrs_aggregate_root_public_id UNIQUE (public_id),
    CONSTRAINT uk_cqrs_aggregate_root_identity UNIQUE (tenant_id, aggregate_type, aggregate_id),
    CONSTRAINT uk_cqrs_aggregate_root_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_cqrs_aggregate_root_public_id_format CHECK (public_id ~ '^agg_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cqrs_aggregate_root_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cqrs_aggregate_root_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cqrs_aggregate_root_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cqrs_aggregate_root_aggregate_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_cqrs_aggregate_root_aggregate_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_cqrs_aggregate_root_version_nonneg CHECK (current_version >= 0)
);

CREATE TABLE IF NOT EXISTS cqrs_token_signing_key_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    key_version INT NOT NULL,
    secret_key_hash VARCHAR(64) NOT NULL,
    raw_secret_value VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cqrs_token_signing_key_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_cqrs_token_signing_key_public_id UNIQUE (public_id),
    CONSTRAINT uk_cqrs_token_signing_key_version UNIQUE (tenant_id, key_version),
    CONSTRAINT uk_cqrs_token_signing_key_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_cqrs_token_signing_key_public_id_format CHECK (public_id ~ '^ctk_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_cqrs_token_signing_key_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_cqrs_token_signing_key_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_cqrs_token_signing_key_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_cqrs_token_signing_key_version_pos CHECK (key_version > 0),
    CONSTRAINT ck_cqrs_token_signing_key_status CHECK (status IN ('ACTIVE', 'ROTATED', 'REVOKED'))
);

CREATE INDEX IF NOT EXISTS idx_cqrs_idempotency_lookup
    ON cqrs_command_idempotency_ledger (tenant_id, idempotency_key, status);

CREATE INDEX IF NOT EXISTS idx_cqrs_aggregate_lookup
    ON cqrs_aggregate_root_ledger (tenant_id, aggregate_type, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_cqrs_token_key_active
    ON cqrs_token_signing_key_registry (tenant_id, status, expires_at);

CREATE OR REPLACE FUNCTION trg_prevent_cqrs_idempotency_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.correlation_id <> NEW.correlation_id OR OLD.idempotency_key <> NEW.idempotency_key OR
       OLD.command_name <> NEW.command_name OR OLD.aggregate_type <> NEW.aggregate_type OR
       OLD.aggregate_id <> NEW.aggregate_id OR OLD.request_fingerprint <> NEW.request_fingerprint THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in cqrs_command_idempotency_ledger';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cqrs_idempotency_immutable ON cqrs_command_idempotency_ledger;
CREATE TRIGGER trg_cqrs_idempotency_immutable
    BEFORE UPDATE ON cqrs_command_idempotency_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_cqrs_idempotency_immutable_update();

CREATE OR REPLACE FUNCTION trg_prevent_cqrs_aggregate_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR
       OLD.aggregate_type <> NEW.aggregate_type OR OLD.aggregate_id <> NEW.aggregate_id THEN
        RAISE EXCEPTION 'Immutable columns cannot be updated in cqrs_aggregate_root_ledger';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cqrs_aggregate_immutable ON cqrs_aggregate_root_ledger;
CREATE TRIGGER trg_cqrs_aggregate_immutable
    BEFORE UPDATE ON cqrs_aggregate_root_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_cqrs_aggregate_immutable_update();
