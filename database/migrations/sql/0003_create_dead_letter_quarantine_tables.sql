-- migration:      0003
-- description:    create dead letter quarantine and aggregate parking tables
-- author:         chief-strategist
-- date:           2026-11-04T14:35:00Z
-- depends_on:     0002
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         dead-letter quarantine and ordering-preserving key parking store conforming to 05-db-writing-rules.md

CREATE TABLE IF NOT EXISTS dead_letter_quarantine (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_reason TEXT NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_fingerprint VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'QUARANTINED',
    redrive_count INT NOT NULL DEFAULT 0,
    max_redrive_attempts INT NOT NULL DEFAULT 3,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dead_letter_quarantine PRIMARY KEY (tenant_id, id),
    CONSTRAINT dlq_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT dlq_external_uk UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT dlq_public_id_ck CHECK (public_id ~ '^dlq_[0-9a-zA-Z]{26}$'),
    CONSTRAINT dlq_tenant_id_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT dlq_source_service_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT dlq_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT dlq_topic_ck CHECK (topic = btrim(topic) AND topic <> ''),
    CONSTRAINT dlq_partition_ck CHECK (partition >= 0),
    CONSTRAINT dlq_source_offset_ck CHECK (source_offset >= 0),
    CONSTRAINT dlq_consumer_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT dlq_handler_id_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT dlq_aggregate_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT dlq_aggregate_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT dlq_aggregate_version_ck CHECK (aggregate_version >= 1),
    CONSTRAINT dlq_event_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT dlq_payload_is_object_ck CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT dlq_headers_is_object_ck CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT dlq_failure_reason_ck CHECK (failure_reason = btrim(failure_reason) AND failure_reason <> ''),
    CONSTRAINT dlq_error_code_ck CHECK (error_code = btrim(error_code) AND error_code <> ''),
    CONSTRAINT dlq_error_fingerprint_ck CHECK (error_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT dlq_status_ck CHECK (status IN ('QUARANTINED', 'REDRIVING', 'REDRIVEN_SUCCESS', 'COMPENSATED', 'DISCARDED')),
    CONSTRAINT dlq_redrive_count_ck CHECK (redrive_count >= 0),
    CONSTRAINT dlq_max_redrive_ck CHECK (max_redrive_attempts >= 1)
);

CREATE TABLE IF NOT EXISTS aggregate_parking_store (
    tenant_id VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    is_parked BOOLEAN NOT NULL DEFAULT TRUE,
    parked_reason TEXT NOT NULL,
    quarantine_id UUID NOT NULL,
    parked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_aggregate_parking_store PRIMARY KEY (tenant_id, handler_id, aggregate_type, aggregate_id),
    CONSTRAINT aggregate_parking_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT aggregate_parking_handler_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT aggregate_parking_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT aggregate_parking_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT aggregate_parking_reason_ck CHECK (parked_reason = btrim(parked_reason) AND parked_reason <> '')
);

CREATE INDEX IF NOT EXISTS idx_dlq_status_lookup
    ON dead_letter_quarantine (tenant_id, handler_id, status);

CREATE INDEX IF NOT EXISTS idx_dlq_aggregate_lookup
    ON dead_letter_quarantine (tenant_id, aggregate_type, aggregate_id);

CREATE INDEX IF NOT EXISTS idx_dlq_correlation
    ON dead_letter_quarantine (tenant_id, correlation_id);

