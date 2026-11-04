-- migration:      0006
-- description:    create event ordering protection tables (sequence tracker and durable held events)
-- author:         chief-strategist
-- date:           2026-11-04T16:50:00Z
-- depends_on:     0005
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         gap detection, exact-next application, epoch-fenced held key storage conforming to OR-01 to OR-13 and 05-db-writing-rules.md

CREATE TABLE IF NOT EXISTS aggregate_sequence_tracker (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    last_applied_version BIGINT NOT NULL DEFAULT 0,
    last_payload_hash VARCHAR(64) NOT NULL,
    owner_epoch BIGINT NOT NULL DEFAULT 0,
    is_held BOOLEAN NOT NULL DEFAULT FALSE,
    hold_reason VARCHAR(32),
    blocking_version BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_aggregate_sequence_tracker PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_sequence_tracker_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_sequence_tracker_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_sequence_tracker_aggregate UNIQUE (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id),
    CONSTRAINT seq_tracker_public_id_ck CHECK (public_id ~ '^seq_[0-9a-zA-Z]{26}$'),
    CONSTRAINT seq_tracker_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT seq_tracker_source_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT seq_tracker_external_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT seq_tracker_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT seq_tracker_handler_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT seq_tracker_agg_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT seq_tracker_agg_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT seq_tracker_version_ck CHECK (last_applied_version >= 0),
    CONSTRAINT seq_tracker_hash_ck CHECK (last_payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT seq_tracker_epoch_ck CHECK (owner_epoch >= 0),
    CONSTRAINT seq_tracker_hold_reason_ck CHECK (hold_reason IS NULL OR hold_reason IN ('GAP', 'PARKED_FAILURE', 'INTEGRITY', 'RESYNCING')),
    CONSTRAINT seq_tracker_blocking_ver_ck CHECK (blocking_version IS NULL OR blocking_version >= 1),
    CONSTRAINT seq_tracker_held_consistency_ck CHECK ((is_held = TRUE AND hold_reason IS NOT NULL) OR (is_held = FALSE AND hold_reason IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_sequence_tracker_held
ON aggregate_sequence_tracker (tenant_id, consumer_group, handler_id)
WHERE is_held = TRUE;

CREATE OR REPLACE FUNCTION trg_sequence_tracker_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id THEN
        RAISE EXCEPTION 'Immutable fields cannot be updated on aggregate_sequence_tracker' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sequence_tracker_immutability ON aggregate_sequence_tracker;
CREATE TRIGGER trg_sequence_tracker_immutability
BEFORE UPDATE ON aggregate_sequence_tracker
FOR EACH ROW EXECUTE FUNCTION trg_sequence_tracker_immutable();

CREATE TABLE IF NOT EXISTS sequence_held_event (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    handler_id VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_offset BIGINT NOT NULL,
    arrived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_sequence_held_event PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_sequence_held_event_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uk_sequence_held_event_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uk_sequence_held_event_version UNIQUE (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version),
    CONSTRAINT uk_sequence_held_event_event_id UNIQUE (tenant_id, consumer_group, handler_id, event_id),
    CONSTRAINT held_event_public_id_ck CHECK (public_id ~ '^hld_[0-9a-zA-Z]{26}$'),
    CONSTRAINT held_event_tenant_ck CHECK (tenant_id = btrim(tenant_id) AND tenant_id <> ''),
    CONSTRAINT held_event_source_ck CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT held_event_external_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT held_event_group_ck CHECK (consumer_group = btrim(consumer_group) AND consumer_group <> ''),
    CONSTRAINT held_event_handler_ck CHECK (handler_id = btrim(handler_id) AND handler_id <> ''),
    CONSTRAINT held_event_agg_type_ck CHECK (aggregate_type = btrim(aggregate_type) AND aggregate_type <> ''),
    CONSTRAINT held_event_agg_id_ck CHECK (aggregate_id = btrim(aggregate_id) AND aggregate_id <> ''),
    CONSTRAINT held_event_version_ck CHECK (aggregate_version >= 1),
    CONSTRAINT held_event_type_ck CHECK (event_type = btrim(event_type) AND event_type <> ''),
    CONSTRAINT held_event_payload_ck CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT held_event_hash_ck CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT held_event_offset_ck CHECK (source_offset >= 0)
);

CREATE INDEX IF NOT EXISTS idx_held_event_drain_order
ON sequence_held_event (tenant_id, consumer_group, handler_id, aggregate_type, aggregate_id, aggregate_version ASC);

CREATE OR REPLACE FUNCTION trg_sequence_held_event_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.correlation_id <> NEW.correlation_id OR OLD.event_id <> NEW.event_id THEN
        RAISE EXCEPTION 'Immutable fields cannot be updated on sequence_held_event' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sequence_held_event_immutability ON sequence_held_event;
CREATE TRIGGER trg_sequence_held_event_immutability
BEFORE UPDATE ON sequence_held_event
FOR EACH ROW EXECUTE FUNCTION trg_sequence_held_event_immutable();

