-- migration:      0012
-- description:    create tenant registry, dek keystore, quota budgets, and audit log tables
-- author:         chief-strategist
-- date:           2026-11-06T09:48:00Z
-- depends_on:     0011
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist multi-tenant isolation registry, cryptographic key shredding store, noisy neighbor quota budgets, and append-only audit trail per WP-10 (MT-01 through MT-13). All columns are non-nullable.

CREATE TABLE IF NOT EXISTS tenant_registry (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    tier VARCHAR(32) NOT NULL,
    lifecycle_state VARCHAR(32) NOT NULL,
    home_region VARCHAR(64) NOT NULL,
    allowed_regions TEXT NOT NULL,
    retention_class VARCHAR(64) NOT NULL,
    max_throughput_eps INT NOT NULL DEFAULT 1000,
    max_concurrency_lanes INT NOT NULL DEFAULT 10,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_tenant_registry PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_tenant_registry_public_id UNIQUE (public_id),
    CONSTRAINT uk_tenant_registry_tenant_id UNIQUE (tenant_id),
    CONSTRAINT uk_tenant_registry_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_tenant_registry_public_id_format CHECK (public_id ~ '^tnt_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_tenant_registry_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_tenant_registry_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_tenant_registry_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_tenant_registry_tier CHECK (tier IN ('POOL', 'DEDICATED', 'SILO')),
    CONSTRAINT ck_tenant_registry_lifecycle_state CHECK (lifecycle_state IN ('ONBOARDING', 'ACTIVE', 'SUSPENDED', 'OFFBOARDING', 'OFFBOARDED')),
    CONSTRAINT ck_tenant_registry_home_region_btrim CHECK (btrim(home_region) = home_region AND home_region <> ''),
    CONSTRAINT ck_tenant_registry_allowed_regions_btrim CHECK (btrim(allowed_regions) = allowed_regions AND allowed_regions <> ''),
    CONSTRAINT ck_tenant_registry_retention_class_btrim CHECK (btrim(retention_class) = retention_class AND retention_class <> ''),
    CONSTRAINT ck_tenant_registry_max_throughput_positive CHECK (max_throughput_eps > 0),
    CONSTRAINT ck_tenant_registry_max_concurrency_positive CHECK (max_concurrency_lanes > 0),
    CONSTRAINT ck_tenant_registry_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS tenant_dek_keystore (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    period_key VARCHAR(32) NOT NULL,
    wrapped_dek BYTEA NOT NULL,
    kek_key_id VARCHAR(128) NOT NULL,
    algorithm VARCHAR(32) NOT NULL DEFAULT 'AES_256_GCM',
    status VARCHAR(32) NOT NULL,
    shredded_at TIMESTAMPTZ NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_tenant_dek_keystore PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_tenant_dek_keystore_public_id UNIQUE (public_id),
    CONSTRAINT uk_tenant_dek_keystore_subject_period UNIQUE (tenant_id, subject_id, period_key),
    CONSTRAINT fk_tenant_dek_keystore_registry FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tenant_dek_keystore_public_id_format CHECK (public_id ~ '^dek_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_tenant_dek_keystore_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_tenant_dek_keystore_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_tenant_dek_keystore_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_tenant_dek_keystore_subject_id_btrim CHECK (btrim(subject_id) = subject_id AND subject_id <> ''),
    CONSTRAINT ck_tenant_dek_keystore_period_key_btrim CHECK (btrim(period_key) = period_key AND period_key <> ''),
    CONSTRAINT ck_tenant_dek_keystore_kek_key_id_btrim CHECK (btrim(kek_key_id) = kek_key_id AND kek_key_id <> ''),
    CONSTRAINT ck_tenant_dek_keystore_algorithm_btrim CHECK (btrim(algorithm) = algorithm AND algorithm <> ''),
    CONSTRAINT ck_tenant_dek_keystore_status CHECK (status IN ('ACTIVE', 'SHREDDED')),
    CONSTRAINT ck_tenant_dek_keystore_shredded_consistency CHECK ((status = 'SHREDDED') = (shredded_at IS NOT NULL)),
    CONSTRAINT ck_tenant_dek_keystore_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS tenant_quota_budgets (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    held_keys_count INT NOT NULL DEFAULT 0,
    max_held_keys INT NOT NULL DEFAULT 1000,
    quarantine_count INT NOT NULL DEFAULT 0,
    max_quarantine_budget INT NOT NULL DEFAULT 500,
    is_throttled BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_tenant_quota_budgets PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_tenant_quota_budgets_public_id UNIQUE (public_id),
    CONSTRAINT uk_tenant_quota_budgets_tenant_id UNIQUE (tenant_id),
    CONSTRAINT fk_tenant_quota_budgets_registry FOREIGN KEY (tenant_id) REFERENCES tenant_registry (tenant_id) ON DELETE RESTRICT,
    CONSTRAINT ck_tenant_quota_budgets_public_id_format CHECK (public_id ~ '^qta_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_tenant_quota_budgets_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_tenant_quota_budgets_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_tenant_quota_budgets_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_tenant_quota_budgets_held_keys_nonneg CHECK (held_keys_count >= 0),
    CONSTRAINT ck_tenant_quota_budgets_max_held_keys_positive CHECK (max_held_keys > 0),
    CONSTRAINT ck_tenant_quota_budgets_quarantine_count_nonneg CHECK (quarantine_count >= 0),
    CONSTRAINT ck_tenant_quota_budgets_max_quarantine_positive CHECK (max_quarantine_budget > 0),
    CONSTRAINT ck_tenant_quota_budgets_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS tenant_isolation_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    action VARCHAR(64) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_tenant_isolation_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_tenant_isolation_audit_log_public_id UNIQUE (public_id),
    CONSTRAINT ck_tenant_isolation_audit_log_public_id_format CHECK (public_id ~ '^aud_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_tenant_isolation_audit_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_tenant_isolation_audit_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_tenant_isolation_audit_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_tenant_isolation_audit_log_action_btrim CHECK (btrim(action) = action AND action <> ''),
    CONSTRAINT ck_tenant_isolation_audit_log_details_is_object CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_tenant_registry_state ON tenant_registry (lifecycle_state);
CREATE INDEX IF NOT EXISTS idx_tenant_dek_keystore_subject ON tenant_dek_keystore (tenant_id, subject_id);
CREATE INDEX IF NOT EXISTS idx_tenant_quota_budgets_throttled ON tenant_quota_budgets (is_throttled);
CREATE INDEX IF NOT EXISTS idx_tenant_isolation_audit_log_created ON tenant_isolation_audit_log (tenant_id, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_tenant_registry_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_registry.id: %', OLD.id USING ERRCODE = '23514';
    END IF;
    IF OLD.public_id <> NEW.public_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_registry.public_id: %', OLD.public_id USING ERRCODE = '23514';
    END IF;
    IF OLD.tenant_id <> NEW.tenant_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_registry.tenant_id: %', OLD.tenant_id USING ERRCODE = '23514';
    END IF;
    IF OLD.correlation_id <> NEW.correlation_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_registry.correlation_id: %', OLD.correlation_id USING ERRCODE = '23514';
    END IF;
    NEW.version := OLD.version + 1;
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_tenant_registry_immutable_update ON tenant_registry;
CREATE TRIGGER trg_prevent_tenant_registry_immutable_update
    BEFORE UPDATE ON tenant_registry
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_tenant_registry_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_tenant_dek_keystore_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_dek_keystore.id: %', OLD.id USING ERRCODE = '23514';
    END IF;
    IF OLD.public_id <> NEW.public_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_dek_keystore.public_id: %', OLD.public_id USING ERRCODE = '23514';
    END IF;
    IF OLD.tenant_id <> NEW.tenant_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_dek_keystore.tenant_id: %', OLD.tenant_id USING ERRCODE = '23514';
    END IF;
    IF OLD.subject_id <> NEW.subject_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_dek_keystore.subject_id: %', OLD.subject_id USING ERRCODE = '23514';
    END IF;
    IF OLD.period_key <> NEW.period_key THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_dek_keystore.period_key: %', OLD.period_key USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'SHREDDED' AND NEW.status <> 'SHREDDED' THEN
        RAISE EXCEPTION 'cannot un-shred shredded dek in tenant_dek_keystore: %', OLD.id USING ERRCODE = '23514';
    END IF;
    NEW.version := OLD.version + 1;
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_tenant_dek_keystore_immutable_update ON tenant_dek_keystore;
CREATE TRIGGER trg_prevent_tenant_dek_keystore_immutable_update
    BEFORE UPDATE ON tenant_dek_keystore
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_tenant_dek_keystore_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_tenant_quota_budgets_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_quota_budgets.id: %', OLD.id USING ERRCODE = '23514';
    END IF;
    IF OLD.public_id <> NEW.public_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_quota_budgets.public_id: %', OLD.public_id USING ERRCODE = '23514';
    END IF;
    IF OLD.tenant_id <> NEW.tenant_id THEN
        RAISE EXCEPTION 'cannot modify immutable tenant_quota_budgets.tenant_id: %', OLD.tenant_id USING ERRCODE = '23514';
    END IF;
    NEW.version := OLD.version + 1;
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_tenant_quota_budgets_immutable_update ON tenant_quota_budgets;
CREATE TRIGGER trg_prevent_tenant_quota_budgets_immutable_update
    BEFORE UPDATE ON tenant_quota_budgets
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_tenant_quota_budgets_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_tenant_isolation_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'tenant_isolation_audit_log is append-only: updates and deletes are forbidden' USING ERRCODE = '23514';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_tenant_isolation_audit_log_modification ON tenant_isolation_audit_log;
CREATE TRIGGER trg_prevent_tenant_isolation_audit_log_modification
    BEFORE UPDATE OR DELETE ON tenant_isolation_audit_log
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_tenant_isolation_audit_log_modification();

