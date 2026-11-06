-- rollback migration: 0023
-- description:        drop causal chain log, transaction group ledger, aggregate root version ledger, and domain event catalog ledger

DROP TRIGGER IF EXISTS trg_causal_chain_no_update ON causal_chain_log;
DROP FUNCTION IF EXISTS trg_prevent_causal_chain_mutation();

DROP TABLE IF EXISTS causal_chain_log;
DROP TABLE IF EXISTS transaction_group_ledger;
DROP TABLE IF EXISTS aggregate_root_version_ledger;
DROP TABLE IF EXISTS domain_event_catalog_ledger;

