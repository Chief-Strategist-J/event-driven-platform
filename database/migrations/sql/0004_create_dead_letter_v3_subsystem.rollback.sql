-- migration:      0004
-- description:    rollback dead letter v3 subsystem tables
-- author:         chief-strategist
-- date:           2026-11-04T14:50:00Z
-- depends_on:     0004
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         drop attempt record, parked event, and consumed offset tables

DROP TABLE IF EXISTS attempt_record;
DROP TABLE IF EXISTS parked_event;
DROP TABLE IF EXISTS consumed_offset;
