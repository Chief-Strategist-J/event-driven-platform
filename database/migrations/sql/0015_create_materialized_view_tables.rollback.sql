-- migration:      0015
-- description:    rollback materialized view registry, projection state, held events, and rebuild log tables
-- author:         chief-strategist
-- date:           2026-11-06T12:22:00Z
-- depends_on:     0015
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-13 materialized view tables

DROP TRIGGER IF EXISTS trg_prevent_materialized_view_rebuild_log_immutable_update ON materialized_view_rebuild_log;
DROP FUNCTION IF EXISTS fn_prevent_materialized_view_rebuild_log_immutable_update();

DROP TRIGGER IF EXISTS trg_prevent_materialized_view_projection_state_immutable_update ON materialized_view_projection_state;
DROP FUNCTION IF EXISTS fn_prevent_materialized_view_projection_state_immutable_update();

DROP TRIGGER IF EXISTS trg_prevent_materialized_view_registry_immutable_update ON materialized_view_registry;
DROP FUNCTION IF EXISTS fn_prevent_materialized_view_registry_immutable_update();

DROP TABLE IF EXISTS materialized_view_rebuild_log;
DROP TABLE IF EXISTS materialized_view_held_events;
DROP TABLE IF EXISTS materialized_view_projection_state;
DROP TABLE IF EXISTS materialized_view_registry;

