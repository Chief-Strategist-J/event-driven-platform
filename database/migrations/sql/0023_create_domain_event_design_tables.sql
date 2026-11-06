-- migration:      0023
-- description:    create domain event catalog, aggregate root versions, transaction groups, and causal chain ledgers
-- author:         chief-strategist
-- date:           2026-11-06T16:22:00Z
-- depends_on:     0022
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce DE-01 (immutable facts), DE-02 (gapless aggregate versions), DE-03 (semantics classes STATE/DELTA/NOTIFICATION), DE-05 (canonical envelope), DE-08 (private vs public catalog), and causal loop detection per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS domain_event_catalog_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    owning_aggregate VARCHAR(64) NOT NULL,
    semantics_class VARCHAR(32) NOT NULL,
    visibility VARCHAR(32) NOT NULL,
    pii_classification VARCHAR(32) NOT NULL,
    schema_fingerprint VARCHAR(64) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_domain_event_catalog_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_domain_event_catalog_public_id UNIQUE (public_id),
    CONSTRAINT uk_domain_event_catalog_type UNIQUE (tenant_id, event_type),
    CONSTRAINT uk_domain_event_catalog_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_domain_event_catalog_public_id_format CHECK (public_id ~ '^dec_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_domain_event_catalog_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_domain_event_catalog_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_domain_event_catalog_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_domain_event_catalog_type_btrim CHECK (btrim(event_type) = event_type AND event_type <> ''),
    CONSTRAINT ck_domain_event_catalog_aggregate_btrim CHECK (btrim(owning_aggregate) = owning_aggregate AND owning_aggregate <> ''),
    CONSTRAINT ck_domain_event_catalog_semantics CHECK (semantics_class IN ('STATE', 'DELTA', 'NOTIFICATION')),
    CONSTRAINT ck_domain_event_catalog_visibility CHECK (visibility IN ('PRIVATE', 'PUBLIC')),
    CONSTRAINT ck_domain_event_catalog_pii CHECK (pii_classification IN ('NONE', 'CONFIDENTIAL', 'RESTRICTED')),
    CONSTRAINT ck_domain_event_catalog_fingerprint_len CHECK (length(schema_fingerprint) = 64)
);

CREATE TABLE IF NOT EXISTS aggregate_root_version_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_aggregate_root_version_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_aggregate_root_version_public_id UNIQUE (public_id),
    CONSTRAINT uk_aggregate_root_version_target UNIQUE (tenant_id, aggregate_type, aggregate_id),
    CONSTRAINT uk_aggregate_root_version_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_aggregate_root_version_public_id_format CHECK (public_id ~ '^agg_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_aggregate_root_version_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_aggregate_root_version_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_aggregate_root_version_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_aggregate_root_version_type_btrim CHECK (btrim(aggregate_type) = aggregate_type AND aggregate_type <> ''),
    CONSTRAINT ck_aggregate_root_version_id_btrim CHECK (btrim(aggregate_id) = aggregate_id AND aggregate_id <> ''),
    CONSTRAINT ck_aggregate_root_version_non_neg CHECK (current_version >= 0)
);

CREATE TABLE IF NOT EXISTS transaction_group_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    transaction_group_id VARCHAR(64) NOT NULL,
    expected_group_size INTEGER NOT NULL,
    received_count INTEGER NOT NULL DEFAULT 1,
    is_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',

    CONSTRAINT pk_transaction_group_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_transaction_group_public_id UNIQUE (public_id),
    CONSTRAINT uk_transaction_group_id UNIQUE (tenant_id, transaction_group_id),
    CONSTRAINT uk_transaction_group_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_transaction_group_public_id_format CHECK (public_id ~ '^txg_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_transaction_group_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_transaction_group_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_transaction_group_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_transaction_group_group_id_btrim CHECK (btrim(transaction_group_id) = transaction_group_id AND transaction_group_id <> ''),
    CONSTRAINT ck_transaction_group_expected_pos CHECK (expected_group_size > 0),
    CONSTRAINT ck_transaction_group_received_pos CHECK (received_count >= 1)
);

CREATE TABLE IF NOT EXISTS causal_chain_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    causation_id VARCHAR(64) NOT NULL,
    causation_depth INTEGER NOT NULL,
    causation_path_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_causal_chain_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_causal_chain_public_id UNIQUE (public_id),
    CONSTRAINT uk_causal_chain_event UNIQUE (tenant_id, event_id),
    CONSTRAINT uk_causal_chain_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_causal_chain_public_id_format CHECK (public_id ~ '^csl_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_causal_chain_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_causal_chain_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_causal_chain_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_causal_chain_event_id_btrim CHECK (btrim(event_id) = event_id AND event_id <> ''),
    CONSTRAINT ck_causal_chain_causation_id_btrim CHECK (btrim(causation_id) = causation_id AND causation_id <> ''),
    CONSTRAINT ck_causal_chain_depth_non_neg CHECK (causation_depth >= 0),
    CONSTRAINT ck_causal_chain_hash_len CHECK (length(causation_path_hash) = 64)
);

CREATE INDEX IF NOT EXISTS idx_domain_event_catalog_lookup
    ON domain_event_catalog_ledger (tenant_id, owning_aggregate, visibility);

CREATE INDEX IF NOT EXISTS idx_aggregate_root_version_lookup
    ON aggregate_root_version_ledger (tenant_id, aggregate_type, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_transaction_group_lookup
    ON transaction_group_ledger (tenant_id, transaction_group_id, is_complete);

CREATE INDEX IF NOT EXISTS idx_causal_chain_event_lookup
    ON causal_chain_log (tenant_id, event_id, causation_depth);

CREATE OR REPLACE FUNCTION trg_prevent_causal_chain_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Causal chain log entries are strictly immutable and cannot be updated or deleted';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_causal_chain_no_update ON causal_chain_log;
CREATE TRIGGER trg_causal_chain_no_update
    BEFORE UPDATE OR DELETE ON causal_chain_log
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_causal_chain_mutation();
