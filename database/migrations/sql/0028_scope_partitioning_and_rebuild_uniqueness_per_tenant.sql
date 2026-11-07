-- migration:      0028
-- description:    scope topic_name, migration_id and rebuild_id uniqueness per tenant instead of globally
-- author:         system-architect
-- date:           2026-11-07T00:00:00Z
-- depends_on:     0027
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         0014/0015 declared UNIQUE (topic_name), UNIQUE (migration_id) and UNIQUE (rebuild_id) globally, so one tenant could block another from registering the same topic/migration/rebuild id. All lookups are already tenant-scoped. materialized_view_registry.view_name is already UNIQUE (tenant_id, view_name) and is unchanged.

ALTER TABLE partition_topic_topology
    DROP CONSTRAINT IF EXISTS uk_partition_topic_topology_topic_name;
ALTER TABLE partition_topic_topology
    ADD CONSTRAINT uk_partition_topic_topology_tenant_topic_name UNIQUE (tenant_id, topic_name);

ALTER TABLE partition_migration_runbook_log
    DROP CONSTRAINT IF EXISTS uk_partition_migration_runbook_log_migration_id;
ALTER TABLE partition_migration_runbook_log
    ADD CONSTRAINT uk_partition_migration_runbook_log_tenant_migration_id UNIQUE (tenant_id, migration_id);

ALTER TABLE materialized_view_rebuild_log
    DROP CONSTRAINT IF EXISTS uk_materialized_view_rebuild_log_rebuild_id;
ALTER TABLE materialized_view_rebuild_log
    ADD CONSTRAINT uk_materialized_view_rebuild_log_tenant_rebuild_id UNIQUE (tenant_id, rebuild_id);
