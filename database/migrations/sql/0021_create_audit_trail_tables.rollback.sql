-- rollback:    0021
-- description: rollback audit trail tables and triggers

DROP TRIGGER IF EXISTS trg_audit_access_no_update ON audit_trail_access_log;
DROP TRIGGER IF EXISTS trg_audit_checkpoint_no_update ON audit_trail_checkpoint_ledger;
DROP TRIGGER IF EXISTS trg_audit_trail_no_update ON audit_trail_ledger;
DROP FUNCTION IF EXISTS trg_prevent_audit_trail_mutation();

DROP TABLE IF EXISTS audit_trail_access_log;
DROP TABLE IF EXISTS audit_trail_checkpoint_ledger;
DROP TABLE IF EXISTS audit_trail_ledger;
