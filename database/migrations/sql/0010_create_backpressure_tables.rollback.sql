-- migration:      0010
-- description:    rollback backpressure state and audit log tables
-- author:         chief-strategist
-- date:           2026-11-05T09:00:00Z
-- depends_on:     0010
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-8 backpressure tables

DROP TRIGGER IF EXISTS trg_prevent_backpressure_audit_log_modification ON backpressure_audit_log;
DROP FUNCTION IF EXISTS fn_prevent_backpressure_audit_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_backpressure_state_immutable_update ON backpressure_state;
DROP FUNCTION IF EXISTS fn_prevent_backpressure_state_immutable_update();
DROP TABLE IF EXISTS backpressure_audit_log;
DROP TABLE IF EXISTS backpressure_state;
