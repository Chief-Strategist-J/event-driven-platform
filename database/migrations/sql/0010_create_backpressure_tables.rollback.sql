-- migration:      0010
-- description:    rollback backpressure state and audit log tables
-- author:         chief-strategist
-- date:           2026-11-05T09:00:00Z
-- depends_on:     0010
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-8 backpressure tables

DROP TRIGGER IF EXISTS trg_backpressure_state_immutable ON backpressure_state;
DROP FUNCTION IF EXISTS immutable_backpressure_state_ids();
DROP TABLE IF EXISTS backpressure_audit_log;
DROP TABLE IF EXISTS backpressure_state;

