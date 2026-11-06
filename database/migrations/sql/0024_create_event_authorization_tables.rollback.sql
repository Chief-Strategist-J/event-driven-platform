-- rollback:    0024
-- description: rollback authorization policy ledger and audit log

DROP INDEX IF EXISTS idx_auth_audit_eval;
DROP INDEX IF EXISTS idx_auth_policy_eval;

DROP TABLE IF EXISTS auth_audit_log CASCADE;
DROP TABLE IF EXISTS auth_policy_ledger CASCADE;
