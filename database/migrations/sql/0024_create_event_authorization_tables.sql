-- migration:      0024
-- description:    create authorization policy ledger and audit log
-- author:         chief-strategist
-- date:           2026-11-06T16:25:00Z
-- depends_on:     0023
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce PBAC/RBAC event authorization policies, 5 mandatory ID roles, fail-closed enforcement, and tamper-evident append-only audit trail per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS auth_policy_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    policy_name VARCHAR(128) NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    principal_type VARCHAR(32) NOT NULL,
    resource_type VARCHAR(32) NOT NULL,
    resource_pattern VARCHAR(256) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    effect VARCHAR(16) NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_auth_policy_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_auth_policy_public_id UNIQUE (public_id),
    CONSTRAINT uk_auth_policy_tenant_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_auth_policy_tenant_name UNIQUE (tenant_id, policy_name),
    CONSTRAINT ck_auth_policy_public_id_format CHECK (public_id ~ '^pol_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_auth_policy_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_auth_policy_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_auth_policy_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_auth_policy_name_btrim CHECK (btrim(policy_name) = policy_name AND policy_name <> ''),
    CONSTRAINT ck_auth_policy_principal_id_btrim CHECK (btrim(principal_id) = principal_id AND principal_id <> ''),
    CONSTRAINT ck_auth_policy_principal_type CHECK (principal_type IN ('SERVICE', 'USER', 'ROLE', 'GROUP')),
    CONSTRAINT ck_auth_policy_resource_type CHECK (resource_type IN ('TOPIC', 'CONSUMER_GROUP', 'FIELD', 'SCHEMA')),
    CONSTRAINT ck_auth_policy_resource_pattern_btrim CHECK (btrim(resource_pattern) = resource_pattern AND resource_pattern <> ''),
    CONSTRAINT ck_auth_policy_operation CHECK (operation IN ('PUBLISH', 'SUBSCRIBE', 'VIEW_PII', 'MANAGE', '*')),
    CONSTRAINT ck_auth_policy_effect CHECK (effect IN ('ALLOW', 'DENY')),
    CONSTRAINT ck_auth_policy_state CHECK (state IN ('ACTIVE', 'DISABLED', 'REVOKED'))
);

CREATE TABLE IF NOT EXISTS auth_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    principal_type VARCHAR(32) NOT NULL,
    resource_type VARCHAR(32) NOT NULL,
    resource_name VARCHAR(256) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    decision VARCHAR(16) NOT NULL,
    matched_policy_id VARCHAR(32) NOT NULL,
    reason VARCHAR(256) NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_auth_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_auth_audit_public_id UNIQUE (public_id),
    CONSTRAINT ck_auth_audit_public_id_format CHECK (public_id ~ '^aal_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_auth_audit_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_auth_audit_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_auth_audit_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_auth_audit_principal_id_btrim CHECK (btrim(principal_id) = principal_id AND principal_id <> ''),
    CONSTRAINT ck_auth_audit_principal_type CHECK (principal_type IN ('SERVICE', 'USER', 'ROLE', 'GROUP')),
    CONSTRAINT ck_auth_audit_resource_type CHECK (resource_type IN ('TOPIC', 'CONSUMER_GROUP', 'FIELD', 'SCHEMA')),
    CONSTRAINT ck_auth_audit_resource_name_btrim CHECK (btrim(resource_name) = resource_name AND resource_name <> ''),
    CONSTRAINT ck_auth_audit_operation CHECK (operation IN ('PUBLISH', 'SUBSCRIBE', 'VIEW_PII', 'MANAGE', '*')),
    CONSTRAINT ck_auth_audit_decision CHECK (decision IN ('ALLOWED', 'DENIED')),
    CONSTRAINT ck_auth_audit_matched_policy_format CHECK (matched_policy_id = 'NONE' OR matched_policy_id ~ '^pol_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_auth_audit_reason_btrim CHECK (btrim(reason) = reason AND reason <> '')
);

CREATE INDEX IF NOT EXISTS idx_auth_policy_eval
    ON auth_policy_ledger (tenant_id, principal_id, resource_type, operation, state);

CREATE INDEX IF NOT EXISTS idx_auth_audit_eval
    ON auth_audit_log (tenant_id, principal_id, evaluated_at DESC);

-- Trigger: Enforce Immutability of audit log (Reject UPDATE and DELETE)
CREATE OR REPLACE FUNCTION trg_prevent_auth_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'auth_audit_log is strictly immutable: % operations are prohibited', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_auth_audit_immutable ON auth_audit_log;
CREATE TRIGGER trg_auth_audit_immutable
    BEFORE UPDATE OR DELETE ON auth_audit_log
    FOR EACH ROW
    EXECUTE FUNCTION trg_prevent_auth_audit_mutation();
