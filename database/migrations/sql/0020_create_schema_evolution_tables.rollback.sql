-- migration:      0020_rollback
-- description:    drop schema evolution tables, triggers, and functions
-- author:         chief-strategist
-- date:           2026-11-06T14:24:00Z
-- depends_on:     0019

DROP TRIGGER IF EXISTS trg_sec_immutable ON schema_evolution_consumer_registry;
DROP FUNCTION IF EXISTS trg_prevent_sec_immutable_update();

DROP TRIGGER IF EXISTS trg_sev_immutable ON schema_evolution_version_registry;
DROP FUNCTION IF EXISTS trg_prevent_sev_immutable_update();

DROP TABLE IF EXISTS schema_evolution_incident_log;
DROP TABLE IF EXISTS schema_evolution_dual_publish_ledger;
DROP TABLE IF EXISTS schema_evolution_consumer_registry;
DROP TABLE IF EXISTS schema_evolution_version_registry;

