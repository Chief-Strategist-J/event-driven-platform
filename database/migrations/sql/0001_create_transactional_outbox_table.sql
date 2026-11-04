-- migration:      0001
-- description:    create transactional outbox table
-- author:         chief-strategist
-- date:           2026-11-04T10:30:00Z
-- depends_on:     NONE
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         initial schema for transactional-outbox feature enforcing at-least-once delivery, poller leases, idempotency, and monotonic version ordering

CREATE TABLE IF NOT EXISTS outbox_idempotency_keys (
    tenant_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    record_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_outbox_idempotency PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS outbox (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    schema_version VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    worker_id VARCHAR(64),
    lease_expires_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    CONSTRAINT pk_outbox PRIMARY KEY (tenant_id, id),
    CONSTRAINT outbox_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT outbox_external_uk UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT outbox_aggregate_version_uk UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version),
    CONSTRAINT outbox_public_id_ck CHECK (public_id ~ '^otb_[0-9a-zA-Z]{26}$'),
    CONSTRAINT outbox_source_service_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT outbox_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT outbox_causation_id_ck CHECK (causation_id = btrim(causation_id) AND causation_id <> ''),
    CONSTRAINT outbox_aggregate_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT outbox_aggregate_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT outbox_aggregate_version_ck CHECK (aggregate_version >= 1),
    CONSTRAINT outbox_event_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT outbox_schema_version_ck CHECK (schema_version = btrim(schema_version) AND schema_version <> ''),
    CONSTRAINT outbox_status_ck CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'FAILED', 'PARKED')),
    CONSTRAINT outbox_retry_count_ck CHECK (retry_count >= 0),
    CONSTRAINT outbox_payload_is_object_ck CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT outbox_headers_is_object_ck CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT outbox_processed_at_status_ck CHECK (
        (status = 'PUBLISHED' AND processed_at IS NOT NULL) OR
        (status <> 'PUBLISHED' AND processed_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending_relay 
    ON outbox (tenant_id, aggregate_type, status, aggregate_version ASC)
    WHERE status IN ('PENDING', 'IN_FLIGHT');

CREATE INDEX IF NOT EXISTS idx_outbox_correlation 
    ON outbox (tenant_id, correlation_id);

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate 
    ON outbox (tenant_id, aggregate_type, aggregate_id, aggregate_version);
