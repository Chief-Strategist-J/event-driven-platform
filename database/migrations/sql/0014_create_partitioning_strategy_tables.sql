-- migration:      0014
-- description:    create partition topic topology, skew metrics log, and migration runbook tables
-- author:         chief-strategist
-- date:           2026-11-06T11:15:00Z
-- depends_on:     0013
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         persist kafka topic partition topologies, immutable partition count rules for ordered topic classes, partition-level skew metrics, and blue/green stream migration runbook logs per WP-12 (PT-01 through PT-10). All columns are non-nullable.

CREATE TABLE IF NOT EXISTS partition_topic_topology (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    topic_name VARCHAR(128) NOT NULL,
    topic_class VARCHAR(32) NOT NULL,
    partition_count INTEGER NOT NULL,
    replication_factor INTEGER NOT NULL DEFAULT 3,
    min_insync_replicas INTEGER NOT NULL DEFAULT 2,
    allow_in_place_expansion BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_partition_topic_topology PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_partition_topic_topology_public_id UNIQUE (public_id),
    CONSTRAINT uk_partition_topic_topology_topic_name UNIQUE (topic_name),
    CONSTRAINT uk_partition_topic_topology_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_partition_topic_topology_public_id_format CHECK (public_id ~ '^prt_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_partition_topic_topology_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_partition_topic_topology_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_partition_topic_topology_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_partition_topic_topology_topic_name_btrim CHECK (btrim(topic_name) = topic_name AND topic_name <> ''),
    CONSTRAINT ck_partition_topic_topology_class CHECK (topic_class IN ('K_ORDERED', 'K_COMPACTED', 'K_COPART', 'K_UNORDERED')),
    CONSTRAINT ck_partition_topic_topology_partition_count CHECK (partition_count > 0),
    CONSTRAINT ck_partition_topic_topology_replication_factor CHECK (replication_factor >= 1),
    CONSTRAINT ck_partition_topic_topology_min_insync_replicas CHECK (min_insync_replicas >= 1),
    CONSTRAINT ck_partition_topic_topology_expansion_policy CHECK (
        (topic_class = 'K_UNORDERED') OR (allow_in_place_expansion = FALSE)
    )
);

CREATE TABLE IF NOT EXISTS partition_skew_metrics_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    topic_name VARCHAR(128) NOT NULL,
    max_throughput_eps NUMERIC(14, 2) NOT NULL,
    median_throughput_eps NUMERIC(14, 2) NOT NULL,
    skew_ratio NUMERIC(10, 4) NOT NULL,
    hot_partition_id INTEGER NOT NULL,
    is_alert_breached BOOLEAN NOT NULL DEFAULT FALSE,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_partition_skew_metrics_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_partition_skew_metrics_log_public_id UNIQUE (public_id),
    CONSTRAINT uk_partition_skew_metrics_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_partition_skew_metrics_log_public_id_format CHECK (public_id ~ '^psk_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_partition_skew_metrics_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_partition_skew_metrics_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_partition_skew_metrics_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_partition_skew_metrics_log_topic_name_btrim CHECK (btrim(topic_name) = topic_name AND topic_name <> ''),
    CONSTRAINT ck_partition_skew_metrics_log_max_eps_nonneg CHECK (max_throughput_eps >= 0),
    CONSTRAINT ck_partition_skew_metrics_log_median_eps_nonneg CHECK (median_throughput_eps >= 0),
    CONSTRAINT ck_partition_skew_metrics_log_skew_ratio_nonneg CHECK (skew_ratio >= 0),
    CONSTRAINT ck_partition_skew_metrics_log_hot_partition_nonneg CHECK (hot_partition_id >= 0)
);

CREATE TABLE IF NOT EXISTS partition_migration_runbook_log (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source_service VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    migration_id VARCHAR(64) NOT NULL,
    source_topic VARCHAR(128) NOT NULL,
    target_topic VARCHAR(128) NOT NULL,
    source_partitions INTEGER NOT NULL,
    target_partitions INTEGER NOT NULL,
    phase VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_partition_migration_runbook_log PRIMARY KEY (tenant_id, id),
    CONSTRAINT uk_partition_migration_runbook_log_public_id UNIQUE (public_id),
    CONSTRAINT uk_partition_migration_runbook_log_migration_id UNIQUE (migration_id),
    CONSTRAINT uk_partition_migration_runbook_log_external UNIQUE (tenant_id, source_service, external_id),
    CONSTRAINT ck_partition_migration_runbook_log_public_id_format CHECK (public_id ~ '^pmg_[0-9a-z]{16,26}$'),
    CONSTRAINT ck_partition_migration_runbook_log_tenant_id_btrim CHECK (btrim(tenant_id) = tenant_id AND tenant_id <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_source_service_btrim CHECK (btrim(source_service) = source_service AND source_service <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_external_id_btrim CHECK (btrim(external_id) = external_id AND external_id <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_migration_id_btrim CHECK (btrim(migration_id) = migration_id AND migration_id <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_source_topic_btrim CHECK (btrim(source_topic) = source_topic AND source_topic <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_target_topic_btrim CHECK (btrim(target_topic) = target_topic AND target_topic <> ''),
    CONSTRAINT ck_partition_migration_runbook_log_source_partitions CHECK (source_partitions > 0),
    CONSTRAINT ck_partition_migration_runbook_log_target_partitions CHECK (target_partitions > source_partitions),
    CONSTRAINT ck_partition_migration_runbook_log_phase CHECK (
        phase IN ('PLANNED', 'PROVISIONING_TARGET', 'DUAL_PUBLISHING', 'DRAINING_SOURCE', 'CUTOVER', 'DECOMMISSIONED')
    )
);

CREATE OR REPLACE FUNCTION fn_prevent_partition_topic_topology_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR
       OLD.public_id <> NEW.public_id OR
       OLD.tenant_id <> NEW.tenant_id OR
       OLD.topic_name <> NEW.topic_name OR
       OLD.topic_class <> NEW.topic_class THEN
        RAISE EXCEPTION 'partition_topic_topology immutable identity column mutation forbidden';
    END IF;

    IF OLD.allow_in_place_expansion = FALSE AND OLD.partition_count <> NEW.partition_count THEN
        RAISE EXCEPTION 'in-place partition count expansion is strictly forbidden for keyed topic class %', OLD.topic_class;
    END IF;

    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_partition_topic_topology_immutable_update
BEFORE UPDATE ON partition_topic_topology
FOR EACH ROW EXECUTE FUNCTION fn_prevent_partition_topic_topology_immutable_update();

CREATE OR REPLACE FUNCTION fn_prevent_partition_skew_metrics_log_update()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'partition_skew_metrics_log is append-only; update operations are forbidden';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_partition_skew_metrics_log_update
BEFORE UPDATE ON partition_skew_metrics_log
FOR EACH ROW EXECUTE FUNCTION fn_prevent_partition_skew_metrics_log_update();

CREATE OR REPLACE FUNCTION fn_prevent_partition_migration_runbook_log_immutable_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.id <> NEW.id OR
       OLD.public_id <> NEW.public_id OR
       OLD.tenant_id <> NEW.tenant_id OR
       OLD.migration_id <> NEW.migration_id OR
       OLD.source_topic <> NEW.source_topic OR
       OLD.target_topic <> NEW.target_topic OR
       OLD.source_partitions <> NEW.source_partitions OR
       OLD.target_partitions <> NEW.target_partitions THEN
        RAISE EXCEPTION 'partition_migration_runbook_log immutable fields cannot be altered';
    END IF;

    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_partition_migration_runbook_log_immutable_update
BEFORE UPDATE ON partition_migration_runbook_log
FOR EACH ROW EXECUTE FUNCTION fn_prevent_partition_migration_runbook_log_immutable_update();
