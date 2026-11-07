-- rollback migration: 0028
-- description:        restore global uniqueness of topic_name, migration_id and rebuild_id (fails if tenants now share values)

ALTER TABLE materialized_view_rebuild_log
    DROP CONSTRAINT IF EXISTS uk_materialized_view_rebuild_log_tenant_rebuild_id;
ALTER TABLE materialized_view_rebuild_log
    ADD CONSTRAINT uk_materialized_view_rebuild_log_rebuild_id UNIQUE (rebuild_id);

ALTER TABLE partition_migration_runbook_log
    DROP CONSTRAINT IF EXISTS uk_partition_migration_runbook_log_tenant_migration_id;
ALTER TABLE partition_migration_runbook_log
    ADD CONSTRAINT uk_partition_migration_runbook_log_migration_id UNIQUE (migration_id);

ALTER TABLE partition_topic_topology
    DROP CONSTRAINT IF EXISTS uk_partition_topic_topology_tenant_topic_name;
ALTER TABLE partition_topic_topology
    ADD CONSTRAINT uk_partition_topic_topology_topic_name UNIQUE (topic_name);
