-- rollback:    0026
-- description: rollback event operations dashboard pipeline hop and triage ledgers

DROP TRIGGER IF EXISTS trg_ops_triage_immutable ON ops_incident_triage_log;
DROP FUNCTION IF EXISTS trg_prevent_ops_triage_mutation();

DROP INDEX IF EXISTS idx_ops_incident_triage_eval;
DROP INDEX IF EXISTS idx_ops_pipeline_hop_status;

DROP TABLE IF EXISTS ops_incident_triage_log CASCADE;
DROP TABLE IF EXISTS ops_pipeline_hop_ledger CASCADE;
