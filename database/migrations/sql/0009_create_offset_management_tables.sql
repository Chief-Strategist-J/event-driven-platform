-- migration:      0009
-- description:    create consumer stored offset and audit log tables
-- author:         chief-strategist
-- date:           2026-11-04T19:00:00Z
-- depends_on:     0008
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         enforce authoritative stored offset management, anti-regression, and immutable audit logs per WP-7

CREATE TABLE IF NOT EXISTS consumer_stored_offset (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    group_id VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    stored_offset BIGINT NOT NULL,
    log_end_offset BIGINT NOT NULL DEFAULT 0,
    lag BIGINT NOT NULL DEFAULT 0,
    commit_epoch BIGINT NOT NULL DEFAULT 1,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consumer_stored_offset PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_consumer_stored_offset_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_consumer_stored_offset_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uq_consumer_stored_offset_group_topic_part UNIQUE (tenant_id, group_id, topic, partition),
    CONSTRAINT ck_consumer_stored_offset_public_id CHECK (public_id ~ '^cso_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_consumer_stored_offset_source_service CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_consumer_stored_offset_external_id CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_consumer_stored_offset_group_id CHECK (group_id = btrim(group_id) AND group_id <> ''),
    CONSTRAINT ck_consumer_stored_offset_topic CHECK (topic = btrim(topic) AND topic <> ''),
    CONSTRAINT ck_consumer_stored_offset_partition CHECK (partition >= 0),
    CONSTRAINT ck_consumer_stored_offset_stored_non_neg CHECK (stored_offset >= 0),
    CONSTRAINT ck_consumer_stored_offset_lag_non_neg CHECK (lag >= 0),
    CONSTRAINT ck_consumer_stored_offset_epoch_non_neg CHECK (commit_epoch >= 1)
);

CREATE INDEX IF NOT EXISTS idx_consumer_stored_offset_group_topic 
ON consumer_stored_offset (tenant_id, group_id, topic);

CREATE INDEX IF NOT EXISTS idx_consumer_stored_offset_lag 
ON consumer_stored_offset (tenant_id, lag DESC);

CREATE TABLE IF NOT EXISTS consumer_offset_audit_log (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    group_id VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    operation_type VARCHAR(32) NOT NULL,
    previous_offset BIGINT NOT NULL,
    target_offset BIGINT NOT NULL,
    reason VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consumer_offset_audit_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_consumer_offset_audit_log_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_consumer_offset_audit_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consumer_offset_audit_log_public_id CHECK (public_id ~ '^coal_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_consumer_offset_audit_log_source_service CHECK (source_service = btrim(source_service) AND source_service <> ''),
    CONSTRAINT ck_consumer_offset_audit_log_external_id CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT ck_consumer_offset_audit_log_group_id CHECK (group_id = btrim(group_id) AND group_id <> ''),
    CONSTRAINT ck_consumer_offset_audit_log_topic CHECK (topic = btrim(topic) AND topic <> ''),
    CONSTRAINT ck_consumer_offset_audit_log_partition CHECK (partition >= 0),
    CONSTRAINT ck_consumer_offset_audit_log_op_type CHECK (operation_type IN ('COMMIT', 'REWIND', 'FAST_FORWARD', 'RESET')),
    CONSTRAINT ck_consumer_offset_audit_log_prev_non_neg CHECK (previous_offset >= 0),
    CONSTRAINT ck_consumer_offset_audit_log_target_non_neg CHECK (target_offset >= 0)
);

CREATE INDEX IF NOT EXISTS idx_consumer_offset_audit_log_group 
ON consumer_offset_audit_log (tenant_id, group_id, topic, partition, created_at DESC);

CREATE OR REPLACE FUNCTION fn_prevent_consumer_stored_offset_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Immutable identifier modification rejected on consumer_stored_offset';
    END IF;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_consumer_stored_offset_immutable_update ON consumer_stored_offset;
CREATE TRIGGER trg_prevent_consumer_stored_offset_immutable_update
BEFORE UPDATE ON consumer_stored_offset
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_consumer_stored_offset_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_consumer_offset_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'consumer_offset_audit_log is append-only: modifications and deletions are prohibited';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_consumer_offset_audit_log_modification ON consumer_offset_audit_log;
CREATE TRIGGER trg_prevent_consumer_offset_audit_log_modification
BEFORE UPDATE OR DELETE ON consumer_offset_audit_log
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_consumer_offset_audit_log_modification();
