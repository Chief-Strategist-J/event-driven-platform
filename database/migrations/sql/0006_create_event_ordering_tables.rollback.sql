-- migration: 0006 rollback
DROP TRIGGER IF EXISTS trg_sequence_held_event_immutability ON sequence_held_event;
DROP FUNCTION IF EXISTS trg_sequence_held_event_immutable();
DROP TABLE IF EXISTS sequence_held_event;

DROP TRIGGER IF EXISTS trg_sequence_tracker_immutability ON aggregate_sequence_tracker;
DROP FUNCTION IF EXISTS trg_sequence_tracker_immutable();
DROP TABLE IF EXISTS aggregate_sequence_tracker;

