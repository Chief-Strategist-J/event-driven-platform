-- rollback migration: 0027
-- description:        drop trace propagation coverage ledger, trace span audit ledger, and trace sampling policy ledger

DROP TRIGGER IF EXISTS trg_trace_span_audit_no_update ON trace_span_audit_ledger;
DROP FUNCTION IF EXISTS trg_prevent_trace_span_audit_mutation();

DROP TABLE IF EXISTS trace_propagation_coverage_ledger;
DROP TABLE IF EXISTS trace_span_audit_ledger;
DROP TABLE IF EXISTS trace_sampling_policy_ledger;
