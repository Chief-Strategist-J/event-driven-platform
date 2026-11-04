-- migration: 0005 rollback
DROP TRIGGER IF EXISTS trg_event_upcaster_immutability ON event_upcaster_registry;
DROP FUNCTION IF EXISTS trg_event_upcaster_immutable();
DROP TABLE IF EXISTS event_upcaster_registry;

DROP TRIGGER IF EXISTS trg_event_schema_version_immutability ON event_schema_version;
DROP FUNCTION IF EXISTS trg_event_schema_version_immutable();
DROP TABLE IF EXISTS event_schema_version;
