package queries

const (
	InsertTopicTopology = `
INSERT INTO partition_topic_topology (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    topic_name, topic_class, partition_count, replication_factor,
    min_insync_replicas, allow_in_place_expansion, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
);`

	SelectTopicTopology = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       topic_name, topic_class, partition_count, replication_factor,
       min_insync_replicas, allow_in_place_expansion, version, created_at, updated_at
FROM partition_topic_topology
WHERE tenant_id = $1 AND topic_name = $2;`

	InsertSkewMetrics = `
INSERT INTO partition_skew_metrics_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    topic_name, max_throughput_eps, median_throughput_eps, skew_ratio,
    hot_partition_id, is_alert_breached, measured_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
);`

	InsertMigrationRunbook = `
INSERT INTO partition_migration_runbook_log (
    id, public_id, tenant_id, source_service, external_id, correlation_id,
    migration_id, source_topic, target_topic, source_partitions, target_partitions,
    phase, version, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
);`

	SelectMigrationRunbook = `
SELECT id, public_id, tenant_id, source_service, external_id, correlation_id,
       migration_id, source_topic, target_topic, source_partitions, target_partitions,
       phase, version, created_at, updated_at
FROM partition_migration_runbook_log
WHERE tenant_id = $1 AND migration_id = $2;`

	UpdateMigrationPhase = `
UPDATE partition_migration_runbook_log
SET phase = $1, version = version + 1, updated_at = CURRENT_TIMESTAMP
WHERE tenant_id = $2 AND migration_id = $3 AND version = $4;`
)
