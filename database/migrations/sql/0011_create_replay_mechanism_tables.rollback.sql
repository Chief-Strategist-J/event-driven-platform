-- migration:      0011
-- description:    rollback replay run, effect log, and audit log tables
-- author:         chief-strategist
-- date:           2026-11-05T18:00:00Z
-- depends_on:     0011
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-9 replay mechanism tables

DROP TRIGGER IF EXISTS trg_prevent_replay_audit_log_modification ON replay_audit_log;
DROP FUNCTION IF EXISTS fn_prevent_replay_audit_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_replay_effect_log_modification ON replay_effect_log;
DROP FUNCTION IF EXISTS fn_prevent_replay_effect_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_replay_run_immutable_update ON replay_run;
DROP FUNCTION IF EXISTS fn_prevent_replay_run_immutable_update();

DROP TABLE IF EXISTS replay_audit_log;
DROP TABLE IF EXISTS replay_effect_log;
DROP TABLE IF EXISTS replay_run;

