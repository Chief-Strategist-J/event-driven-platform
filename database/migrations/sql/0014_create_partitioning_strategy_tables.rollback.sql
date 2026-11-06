-- migration:      0014
-- description:    rollback partition topic topology, skew metrics log, and migration runbook tables
-- author:         chief-strategist
-- date:           2026-11-06T11:15:00Z
-- depends_on:     0014
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-12 partitioning strategy tables

DROP TRIGGER IF EXISTS trg_prevent_partition_migration_runbook_log_immutable_update ON partition_migration_runbook_log;
DROP FUNCTION IF EXISTS fn_prevent_partition_migration_runbook_log_immutable_update();

DROP TRIGGER IF EXISTS trg_prevent_partition_skew_metrics_log_update ON partition_skew_metrics_log;
DROP FUNCTION IF EXISTS fn_prevent_partition_skew_metrics_log_update();

DROP TRIGGER IF EXISTS trg_prevent_partition_topic_topology_immutable_update ON partition_topic_topology;
DROP FUNCTION IF EXISTS fn_prevent_partition_topic_topology_immutable_update();

DROP TABLE IF EXISTS partition_migration_runbook_log;
DROP TABLE IF EXISTS partition_skew_metrics_log;
DROP TABLE IF EXISTS partition_topic_topology;

