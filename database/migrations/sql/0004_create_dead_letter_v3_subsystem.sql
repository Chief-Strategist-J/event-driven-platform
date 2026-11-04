-- migration:      0004
-- description:    create dead letter v3 subsystem tables (attempt record, parked events, consumed offsets)
-- author:         chief-strategist
-- date:           2026-11-04T14:50:00Z
-- depends_on:     0003
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         crash-safe poison pre-check, ordered parking buffer, and epoch-fenced offset storage conforming to v3 tree

CREATE TABLE IF NOT EXISTS attempt_record (
    tenant_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    attempt_count INT NOT NULL DEFAULT 1,
    first_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error_fingerprint VARCHAR(64),
    last_error_class VARCHAR(32),
    CONSTRAINT pk_attempt_record PRIMARY KEY (tenant_id, consumer_group, handler_id, event_id),
    CONSTRAINT attempt_record_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT attempt_record_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT attempt_record_handler_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT attempt_record_count_ck CHECK (attempt_count >= 1)
);

CREATE TABLE IF NOT EXISTS parked_event (
    id UUID NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_offset BIGINT NOT NULL,
    arrived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_parked_event PRIMARY KEY (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version),
    CONSTRAINT parked_event_event_uk UNIQUE (tenant_id, consumer_group, handler_id, event_id),
    CONSTRAINT parked_event_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT parked_event_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT parked_event_handler_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT parked_event_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT parked_event_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT parked_event_version_ck CHECK (aggregate_version >= 1),
    CONSTRAINT parked_event_payload_ck CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT parked_event_headers_ck CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT parked_event_offset_ck CHECK (source_offset >= 0)
);

CREATE TABLE IF NOT EXISTS consumed_offset (
    tenant_id VARCHAR(64) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    next_offset BIGINT NOT NULL,
    owner_epoch BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_consumed_offset PRIMARY KEY (tenant_id, consumer_group, topic, partition),
    CONSTRAINT consumed_offset_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT consumed_offset_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT consumed_offset_topic_ck CHECK (topic = btrim(topic) AND topic <> ''),
    CONSTRAINT consumed_offset_partition_ck CHECK (partition >= 0),
    CONSTRAINT consumed_offset_next_offset_ck CHECK (next_offset >= 0),
    CONSTRAINT consumed_offset_epoch_ck CHECK (owner_epoch >= 1)
);

ALTER TABLE aggregate_parking_store 
    ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS parked_event_count INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_parked_event_drain 
    ON parked_event (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version ASC);

