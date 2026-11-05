-- migration:      0008
-- description:    create consumer group membership and rebalance log tables
-- author:         chief-strategist
-- date:           2026-11-04T18:30:00Z
-- depends_on:     0007
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         track consumer group members, cooperative sticky assignments, generation fences, and rebalance audit history. instance_id is nullable for dynamic non-static members.

CREATE TABLE IF NOT EXISTS consumer_group_member (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    group_id VARCHAR(128) NOT NULL,
    member_id VARCHAR(128) NOT NULL,
    instance_id VARCHAR(128),
    client_host VARCHAR(128) NOT NULL DEFAULT '',
    assignment_strategy VARCHAR(128) NOT NULL DEFAULT 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
    assigned_partitions JSONB NOT NULL DEFAULT '[]'::jsonb,
    generation_id BIGINT NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(32) NOT NULL DEFAULT 'JOINING',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consumer_group_member PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_consumer_group_member_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_consumer_group_member_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT uq_consumer_group_member_group_member UNIQUE (tenant_id, group_id, member_id),
    CONSTRAINT ck_consumer_group_member_public_id CHECK (public_id ~ '^cgm_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_consumer_group_member_status CHECK (status IN ('JOINING', 'STABLE', 'REBALANCING', 'DEAD')),
    CONSTRAINT ck_consumer_group_member_generation CHECK (generation_id >= 0),
    CONSTRAINT chk_cgm_tenant_id CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT chk_cgm_source_service CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT chk_cgm_external_id CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT chk_cgm_group_id CHECK (btrim(group_id) = group_id AND group_id <> ''),
    CONSTRAINT chk_cgm_member_id CHECK (btrim(member_id) = member_id AND member_id <> '')
);

CREATE INDEX IF NOT EXISTS idx_consumer_group_member_heartbeat 
ON consumer_group_member (tenant_id, group_id, status, heartbeat_at);

CREATE INDEX IF NOT EXISTS idx_consumer_group_member_instance 
ON consumer_group_member (tenant_id, group_id, instance_id) 
WHERE instance_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS consumer_group_rebalance_log (
    id UUID NOT NULL,
    public_id VARCHAR(30) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    group_id VARCHAR(128) NOT NULL,
    generation_id BIGINT NOT NULL,
    rebalance_reason VARCHAR(128) NOT NULL DEFAULT '',
    member_count INT NOT NULL DEFAULT 1,
    revoked_partitions JSONB NOT NULL DEFAULT '[]'::jsonb,
    assigned_partitions JSONB NOT NULL DEFAULT '[]'::jsonb,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_consumer_group_rebalance_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_consumer_group_rebalance_log_public_id UNIQUE (tenant_id, public_id),
    CONSTRAINT uq_consumer_group_rebalance_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_consumer_group_rebalance_log_public_id CHECK (public_id ~ '^cgr_[0-9a-zA-Z]{26}$'),
    CONSTRAINT ck_consumer_group_rebalance_log_generation CHECK (generation_id >= 0),
    CONSTRAINT ck_consumer_group_rebalance_log_members CHECK (member_count >= 0),
    CONSTRAINT chk_cgr_tenant_id CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT chk_cgr_source_service CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT chk_cgr_external_id CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT chk_cgr_group_id CHECK (btrim(group_id) = group_id AND group_id <> '')
);

CREATE OR REPLACE FUNCTION fn_prevent_consumer_group_member_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.public_id <> NEW.public_id OR OLD.tenant_id <> NEW.tenant_id OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'Immutable identifier modification rejected on consumer_group_member';
    END IF;
    NEW.version = OLD.version + 1;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_consumer_group_member_immutable_update ON consumer_group_member;
CREATE TRIGGER trg_prevent_consumer_group_member_immutable_update
BEFORE UPDATE ON consumer_group_member
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_consumer_group_member_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_consumer_group_rebalance_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'consumer_group_rebalance_log records are immutable and append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_consumer_group_rebalance_log_modification ON consumer_group_rebalance_log;
CREATE TRIGGER trg_prevent_consumer_group_rebalance_log_modification
BEFORE UPDATE OR DELETE ON consumer_group_rebalance_log
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_consumer_group_rebalance_log_modification();
