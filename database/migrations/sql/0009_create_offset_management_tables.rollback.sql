-- migration:      0009
-- description:    rollback consumer stored offset and audit log tables
-- author:         chief-strategist
-- date:           2026-11-04T19:00:00Z
-- depends_on:     0008
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback offset-management schema cleanly

DROP TRIGGER IF EXISTS trg_prevent_consumer_offset_audit_log_modification ON consumer_offset_audit_log;
DROP FUNCTION IF EXISTS fn_prevent_consumer_offset_audit_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_consumer_stored_offset_immutable_update ON consumer_stored_offset;
DROP FUNCTION IF EXISTS fn_prevent_consumer_stored_offset_immutable_update();
DROP TABLE IF EXISTS consumer_offset_audit_log CASCADE;
DROP TABLE IF EXISTS consumer_stored_offset CASCADE;
