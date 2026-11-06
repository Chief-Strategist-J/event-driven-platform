-- migration:      0018_rollback
-- description:    drop eventual consistency monitoring tables and triggers
-- author:         chief-strategist
-- date:           2026-11-06T13:45:00Z
-- depends_on:     0017

DROP TRIGGER IF EXISTS trg_consistency_drift_incident_immutable ON consistency_drift_incident_log;
DROP FUNCTION IF EXISTS trg_prevent_drift_incident_immutable_update();

DROP TRIGGER IF EXISTS trg_consistency_watermark_immutable ON consistency_watermark_ledger;
DROP FUNCTION IF EXISTS trg_prevent_watermark_immutable_update();

DROP TABLE IF EXISTS consistency_view_silence_registry;
DROP TABLE IF EXISTS consistency_check_run_history;
DROP TABLE IF EXISTS consistency_drift_incident_log;
DROP TABLE IF EXISTS consistency_watermark_ledger;

