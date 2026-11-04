-- migration:      0002
-- description:    create consumer inbox and projection tables for idempotent consumption
-- author:         chief-strategist
-- date:           2026-11-04T13:40:00Z
-- depends_on:     0001
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         inbox deduplication and version guard tables for idempotent consumers

CREATE TABLE IF NOT EXISTS consumer_inbox (
    id UUID NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumer_inbox PRIMARY KEY (tenant_id, handler_id, event_id)
);

CREATE TABLE IF NOT EXISTS consumer_projections (
    tenant_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0,
    last_event_id UUID NOT NULL,
    last_payload_hash VARCHAR(64) NOT NULL,
    is_parked BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumer_projections PRIMARY KEY (tenant_id, aggregate_type, aggregate_id)
);

CREATE TABLE IF NOT EXISTS consumer_partition_offsets (
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    offset_val BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumer_partition_offsets PRIMARY KEY (topic, partition)
);

CREATE INDEX IF NOT EXISTS idx_consumer_inbox_time
    ON consumer_inbox (tenant_id, processed_at);
