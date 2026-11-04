-- migration: 0004 rollback
DROP TABLE IF EXISTS attempt_record;
DROP TABLE IF EXISTS parked_event;
DROP TABLE IF EXISTS consumed_offset;
ALTER TABLE aggregate_parking_store DROP COLUMN IF EXISTS owner_epoch;
ALTER TABLE aggregate_parking_store DROP COLUMN IF EXISTS parked_event_count;

