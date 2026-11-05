-- migration:      0006
-- description:    rollback event ordering protection tables
-- author:         chief-strategist
-- date:           2026-11-04T16:50:00Z
-- depends_on:     0006
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop triggers, functions, and tables for aggregate sequence tracking and held events

DROP TRIGGER IF EXISTS trg_sequence_held_event_immutability ON sequence_held_event;
DROP FUNCTION IF EXISTS fn_sequence_held_event_immutability();

DROP TRIGGER IF EXISTS trg_sequence_tracker_immutability ON aggregate_sequence_tracker;
DROP FUNCTION IF EXISTS fn_sequence_tracker_immutability();

DROP TABLE IF EXISTS sequence_held_event;
DROP TABLE IF EXISTS aggregate_sequence_tracker;
