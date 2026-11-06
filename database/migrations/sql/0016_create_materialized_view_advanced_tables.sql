-- migration:      0016
-- description:    create materialized view aggregation contributions and financial period ledger tables
-- author:         chief-strategist
-- date:           2026-11-06T12:56:00Z
-- depends_on:     0015
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist per-source aggregate contribution deltas for idempotent reversal handling and periodic recomputation (V-AGG RC-03) and maintain frozen period ledgers with late-arriving adjustment accounting (V-REPORT F8.2) per WP-13. All columns are non-nullable.

CREATE TABLE IF NOT EXISTS materialized_view_agg_contributions (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    source_event_id UUID NOT NULL,
    source_seq BIGINT NOT NULL,
    delta_amount NUMERIC(18, 4) NOT NULL,
    is_reversal BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_agg_contributions PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_agg_contributions_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_agg_contributions_event UNIQUE (tenant_id, view_name, aggregate_type, aggregate_id, source_event_id),
    CONSTRAINT uk_materialized_view_agg_contributions_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_agg_contributions_public_id_format CHECK (public_id ~ '^mvac_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_materialized_view_agg_contributions_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_agg_contributions_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_agg_contributions_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_agg_contributions_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_agg_contributions_seq_positive CHECK (source_seq > 0)
);

CREATE TABLE IF NOT EXISTS materialized_view_period_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    view_name VARCHAR(128) NOT NULL,
    period_key VARCHAR(32) NOT NULL,
    period_status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    base_amount NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
    adjustment_amount NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
    closing_position_lsn VARCHAR(64) NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materialized_view_period_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_materialized_view_period_ledger_public_id UNIQUE (public_id),
    CONSTRAINT uk_materialized_view_period_ledger_period UNIQUE (tenant_id, view_name, period_key),
    CONSTRAINT uk_materialized_view_period_ledger_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_materialized_view_period_ledger_public_id_format CHECK (public_id ~ '^mvpl_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_materialized_view_period_ledger_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_materialized_view_period_ledger_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_materialized_view_period_ledger_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_materialized_view_period_ledger_view_name_btrim CHECK (btrim(view_name) = view_name AND view_name <> ''),
    CONSTRAINT ck_materialized_view_period_ledger_period_key_btrim CHECK (btrim(period_key) = period_key AND period_key <> ''),
    CONSTRAINT ck_materialized_view_period_ledger_status CHECK (period_status IN ('OPEN', 'FROZEN', 'CLOSED'))
);

CREATE OR REPLACE FUNCTION fn_prevent_materialized_view_agg_contributions_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'materialized_view_agg_contributions is append-only: mutations forbidden';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_materialized_view_agg_contributions_mutation
BEFORE UPDATE OR DELETE ON materialized_view_agg_contributions
FOR EACH ROW EXECUTE FUNCTION fn_prevent_materialized_view_agg_contributions_mutation();

