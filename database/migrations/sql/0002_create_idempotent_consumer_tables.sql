-- migration:      0002
-- description:    create consumer inbox and projection tables for idempotent consumption
-- author:         chief-strategist
-- date:           2026-11-04T13:40:00Z
-- depends_on:     0001
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         inbox deduplication and version guard tables for idempotent consumers conforming to 05-db-writing-rules.md

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
    CONSTRAINT pk_consumer_inbox PRIMARY KEY (tenant_id, id),
    CONSTRAINT consumer_inbox_dedupe_uk UNIQUE (tenant_id, handler_id, event_id),
    CONSTRAINT consumer_inbox_tenant_id_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT consumer_inbox_handler_id_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT consumer_inbox_aggregate_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT consumer_inbox_aggregate_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT consumer_inbox_aggregate_version_ck CHECK (aggregate_version >= 1),
    CONSTRAINT consumer_inbox_event_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT consumer_inbox_payload_hash_ck CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS consumer_projections (
    tenant_id VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0,
    last_event_id UUID NOT NULL,
    last_payload_hash VARCHAR(64) NOT NULL,
    is_parked BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumer_projections PRIMARY KEY (tenant_id, handler_id, aggregate_type, aggregate_id),
    CONSTRAINT consumer_projections_tenant_id_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT consumer_projections_handler_id_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT consumer_projections_aggregate_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT consumer_projections_aggregate_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT consumer_projections_last_version_ck CHECK (last_version >= 0),
    CONSTRAINT consumer_projections_payload_hash_ck CHECK (last_payload_hash ~ '^[0-9a-f]{64}$' OR last_payload_hash = '')
);

CREATE TABLE IF NOT EXISTS consumer_partition_offsets (
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    offset_val BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumer_partition_offsets PRIMARY KEY (topic, partition),
    CONSTRAINT consumer_partition_offsets_topic_ck CHECK (topic = btrim(topic) AND topic <> ''),
    CONSTRAINT consumer_partition_offsets_partition_ck CHECK (partition >= 0),
    CONSTRAINT consumer_partition_offsets_offset_val_ck CHECK (offset_val >= 0)
);

CREATE INDEX IF NOT EXISTS idx_consumer_inbox_time
    ON consumer_inbox (tenant_id, processed_at);

CREATE INDEX IF NOT EXISTS idx_consumer_inbox_aggregate
    ON consumer_inbox (tenant_id, aggregate_type, aggregate_id, aggregate_version);

CREATE INDEX IF NOT EXISTS idx_consumer_projections_lookup
    ON consumer_projections (tenant_id, handler_id, aggregate_type, aggregate_id);
