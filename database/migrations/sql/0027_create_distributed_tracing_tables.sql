-- migration:      0027
-- description:    create distributed tracing sampling policies, span audit, and propagation coverage ledgers
-- author:         chief-strategist
-- date:           2026-11-06T17:20:00Z
-- depends_on:     0026
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce TR-01 to TR-13, W3C traceparent propagation, PII redaction audit, dynamic tail/head sampling, and hop coverage metrics per 05-db-writing-rules.md. All columns non-nullable.

CREATE TABLE IF NOT EXISTS trace_sampling_policy_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    service_name VARCHAR(128) NOT NULL,
    flow_name VARCHAR(128) NOT NULL,
    sample_rate NUMERIC(5,4) NOT NULL DEFAULT 1.0000,
    priority INTEGER NOT NULL DEFAULT 100,
    always_sample_errors BOOLEAN NOT NULL DEFAULT TRUE,
    min_duration_ms INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_trace_sampling_policy_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_trace_sampling_policy_public_id UNIQUE (public_id),
    CONSTRAINT uk_trace_sampling_policy_flow UNIQUE (tenant_id, service_name, flow_name),
    CONSTRAINT uk_trace_sampling_policy_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_trace_sampling_policy_public_id_format CHECK (public_id ~ '^tsp_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_trace_sampling_policy_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_trace_sampling_policy_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_trace_sampling_policy_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_trace_sampling_policy_service_name_btrim CHECK (btrim(service_name) = service_name AND service_name <> ''),
    CONSTRAINT ck_trace_sampling_policy_flow_name_btrim CHECK (btrim(flow_name) = flow_name AND flow_name <> ''),
    CONSTRAINT ck_trace_sampling_policy_rate CHECK (sample_rate >= 0.0000 AND sample_rate <= 1.0000),
    CONSTRAINT ck_trace_sampling_policy_duration_non_neg CHECK (min_duration_ms >= 0)
);

CREATE TABLE IF NOT EXISTS trace_span_audit_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    trace_id VARCHAR(64) NOT NULL,
    span_id VARCHAR(32) NOT NULL,
    parent_span_id VARCHAR(32) NOT NULL DEFAULT '',
    service_name VARCHAR(128) NOT NULL,
    operation_name VARCHAR(128) NOT NULL,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    has_error BOOLEAN NOT NULL DEFAULT FALSE,
    sanitized_attributes_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_trace_span_audit_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_trace_span_audit_public_id UNIQUE (public_id),
    CONSTRAINT uk_trace_span_audit_span UNIQUE (tenant_id, trace_id, span_id),
    CONSTRAINT uk_trace_span_audit_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_trace_span_audit_public_id_format CHECK (public_id ~ '^tsa_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_trace_span_audit_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_trace_span_audit_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_trace_span_audit_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_trace_span_audit_trace_id_btrim CHECK (btrim(trace_id) = trace_id AND trace_id <> ''),
    CONSTRAINT ck_trace_span_audit_span_id_btrim CHECK (btrim(span_id) = span_id AND span_id <> ''),
    CONSTRAINT ck_trace_span_audit_service_btrim CHECK (btrim(service_name) = service_name AND service_name <> ''),
    CONSTRAINT ck_trace_span_audit_operation_btrim CHECK (btrim(operation_name) = operation_name AND operation_name <> ''),
    CONSTRAINT ck_trace_span_audit_duration_non_neg CHECK (duration_ms >= 0)
);

CREATE TABLE IF NOT EXISTS trace_propagation_coverage_ledger (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    hop_source VARCHAR(128) NOT NULL,
    hop_target VARCHAR(128) NOT NULL,
    hop_type VARCHAR(32) NOT NULL,
    total_spans BIGINT NOT NULL DEFAULT 0,
    propagated_spans BIGINT NOT NULL DEFAULT 0,
    coverage_ratio NUMERIC(5,4) NOT NULL DEFAULT 1.0000,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_trace_propagation_coverage_ledger PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_trace_propagation_coverage_public_id UNIQUE (public_id),
    CONSTRAINT uk_trace_propagation_coverage_hop UNIQUE (tenant_id, hop_source, hop_target, hop_type),
    CONSTRAINT uk_trace_propagation_coverage_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_trace_propagation_coverage_public_id_format CHECK (public_id ~ '^tpc_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_trace_propagation_coverage_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_trace_propagation_coverage_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_trace_propagation_coverage_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_trace_propagation_coverage_hop_source_btrim CHECK (btrim(hop_source) = hop_source AND hop_source <> ''),
    CONSTRAINT ck_trace_propagation_coverage_hop_target_btrim CHECK (btrim(hop_target) = hop_target AND hop_target <> ''),
    CONSTRAINT ck_trace_propagation_coverage_hop_type CHECK (hop_type IN ('CONNECT_RPC', 'OUTBOX_RELAY', 'KAFKA_CONSUMER', 'ASYNC_REACTION')),
    CONSTRAINT ck_trace_propagation_coverage_total_non_neg CHECK (total_spans >= 0),
    CONSTRAINT ck_trace_propagation_coverage_propagated_non_neg CHECK (propagated_spans >= 0),
    CONSTRAINT ck_trace_propagation_coverage_ratio CHECK (coverage_ratio >= 0.0000 AND coverage_ratio <= 1.0000)
);

CREATE INDEX IF NOT EXISTS idx_trace_span_audit_trace
    ON trace_span_audit_ledger (tenant_id, trace_id, duration_ms);

CREATE INDEX IF NOT EXISTS idx_trace_sampling_policy_lookup
    ON trace_sampling_policy_ledger (tenant_id, service_name, is_active);

CREATE INDEX IF NOT EXISTS idx_trace_coverage_hop_lookup
    ON trace_propagation_coverage_ledger (tenant_id, hop_source, hop_target);

CREATE OR REPLACE FUNCTION trg_prevent_trace_span_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Trace span audit logs are strictly immutable and cannot be updated or deleted';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_trace_span_audit_no_update ON trace_span_audit_ledger;
CREATE TRIGGER trg_trace_span_audit_no_update
    BEFORE UPDATE OR DELETE ON trace_span_audit_ledger
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_trace_span_audit_mutation();
