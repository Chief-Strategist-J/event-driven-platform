-- migration:      0022
-- description:    create key encryption keys, data encryption keys, and crypto-shredding ledgers
-- author:         chief-strategist
-- date:           2026-11-06T15:30:00Z
-- depends_on:     0021
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce envelope encryption, key lifecycle state machine (ACTIVE, ROTATED, COMPROMISED, SHREDDED), crypto-shredding audit log, and strict field-level ciphertext integrity per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS encryption_kek_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    key_name VARCHAR(128) NOT NULL,
    key_version INTEGER NOT NULL,
    kms_provider VARCHAR(32) NOT NULL,
    kms_key_arn VARCHAR(256) NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_encryption_kek_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_encryption_kek_public_id UNIQUE (public_id),
    CONSTRAINT uk_encryption_kek_name_version UNIQUE (tenant_id, key_name, key_version),
    CONSTRAINT ck_encryption_kek_public_id_format CHECK (public_id ~ '^kek_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_encryption_kek_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_encryption_kek_key_name_btrim CHECK (btrim(key_name) = key_name AND key_name <> ''),
    CONSTRAINT ck_encryption_kek_version_pos CHECK (key_version > 0),
    CONSTRAINT ck_encryption_kek_kms_provider CHECK (kms_provider IN ('AWS_KMS', 'GCP_KMS', 'VAULT', 'LOCAL_SIMULATED')),
    CONSTRAINT ck_encryption_kek_kms_arn_btrim CHECK (btrim(kms_key_arn) = kms_key_arn AND kms_key_arn <> ''),
    CONSTRAINT ck_encryption_kek_state CHECK (state IN ('ACTIVE', 'ROTATED', 'COMPROMISED', 'SHREDDED'))
);

CREATE TABLE IF NOT EXISTS encryption_dek_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    kek_public_id VARCHAR(32) NOT NULL,
    encrypted_dek_blob TEXT NOT NULL,
    algorithm VARCHAR(32) NOT NULL DEFAULT 'AES-256-GCM',
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rotated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shredded_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',

    CONSTRAINT pk_encryption_dek_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_encryption_dek_public_id UNIQUE (public_id),
    CONSTRAINT uk_encryption_dek_tenant_subject UNIQUE (tenant_id, subject_id),
    CONSTRAINT ck_encryption_dek_public_id_format CHECK (public_id ~ '^dek_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_encryption_dek_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_encryption_dek_subject_id_btrim CHECK (btrim(subject_id) = subject_id AND subject_id <> ''),
    CONSTRAINT ck_encryption_dek_kek_public_id_format CHECK (kek_public_id ~ '^kek_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_encryption_dek_blob_btrim CHECK (btrim(encrypted_dek_blob) = encrypted_dek_blob AND encrypted_dek_blob <> ''),
    CONSTRAINT ck_encryption_dek_algorithm CHECK (algorithm IN ('AES-256-GCM', 'CHACHA20-POLY1305')),
    CONSTRAINT ck_encryption_dek_state CHECK (state IN ('ACTIVE', 'ROTATED', 'COMPROMISED', 'SHREDDED'))
);

CREATE TABLE IF NOT EXISTS crypto_shredding_audit_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    dek_public_id VARCHAR(32) NOT NULL,
    reason VARCHAR(64) NOT NULL,
    requested_by VARCHAR(128) NOT NULL,
    shredded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_crypto_shredding_audit_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_crypto_shredding_audit_public_id UNIQUE (public_id),
    CONSTRAINT ck_crypto_shredding_public_id_format CHECK (public_id ~ '^csa_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_crypto_shredding_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_crypto_shredding_subject_id_btrim CHECK (btrim(subject_id) = subject_id AND subject_id <> ''),
    CONSTRAINT ck_crypto_shredding_dek_public_id_format CHECK (dek_public_id ~ '^dek_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_crypto_shredding_reason CHECK (reason IN ('GDPR_RTBF', 'CCPA_DELETION', 'TENANT_OFFBOARDING', 'SECURITY_BREACH')),
    CONSTRAINT ck_crypto_shredding_requested_by_btrim CHECK (btrim(requested_by) = requested_by AND requested_by <> '')
);

CREATE INDEX IF NOT EXISTS idx_encryption_kek_lookup
    ON encryption_kek_ledger (tenant_id, key_name, state);

CREATE INDEX IF NOT EXISTS idx_encryption_dek_subject_lookup
    ON encryption_dek_ledger (tenant_id, subject_id, state);

CREATE INDEX IF NOT EXISTS idx_crypto_shredding_subject_audit
    ON crypto_shredding_audit_ledger (tenant_id, subject_id, shredded_at DESC);
