-- migration:      0005
-- description:    rollback event versioning and upcaster registry tables
-- author:         chief-strategist
-- date:           2026-11-04T15:30:00Z
-- depends_on:     0005
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop triggers, functions, and tables for event versioning and upcaster registry

DROP TRIGGER IF EXISTS trg_event_upcaster_immutability ON event_upcaster_registry;
DROP FUNCTION IF EXISTS fn_event_upcaster_immutability();

DROP TRIGGER IF EXISTS trg_event_schema_version_immutability ON event_schema_version;
DROP FUNCTION IF EXISTS fn_event_schema_version_immutability();

DROP TABLE IF EXISTS event_upcaster_registry;
DROP TABLE IF EXISTS event_schema_version;
